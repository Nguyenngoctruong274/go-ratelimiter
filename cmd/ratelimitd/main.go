// Command ratelimitd runs the go-ratelimiter HTTP service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/config"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/redisstore"
)

var errDraining = errors.New("shutting down")

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ratelimitd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancelStart := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStart()
	rdb, err := redisstore.Open(startCtx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer rdb.Close()
	logger.Info("redis ready")

	// Readiness turns false the moment shutdown starts, so load balancers
	// stop routing new requests while in-flight ones finish.
	var draining atomic.Bool
	ping := redisstore.Pinger(rdb)
	ready := func(ctx context.Context) error {
		if draining.Load() {
			return errDraining
		}
		return ping(ctx)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewHandler(logger, httpapi.Options{Ready: ready}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", slog.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutdown started", slog.Duration("drain", cfg.ShutdownDrain))
	draining.Store(true)
	time.Sleep(cfg.ShutdownDrain)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}
