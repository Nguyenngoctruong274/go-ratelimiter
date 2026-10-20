// Package config loads runtime configuration for ratelimitd from the environment.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config is the complete runtime configuration of the service.
type Config struct {
	HTTPAddr string
	// RedisURL points at the Redis that holds the shared limiter state,
	// e.g. redis://localhost:6379/0 or rediss://user:pass@host:6380/0.
	RedisURL        string
	ShutdownTimeout time.Duration
	// ShutdownDrain is how long to keep serving with /readyz failing before
	// closing the listener. 0 locally; a few seconds behind a load balancer.
	ShutdownDrain time.Duration
	LogLevel      string
}

// Load reads configuration from environment variables, applying defaults
// for optional values. It reports every invalid value at once so that a
// misconfigured deployment fails fast with a complete error message.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	cfg := Config{
		HTTPAddr:        valueOr(getenv("RL_HTTP_ADDR"), ":8080"),
		RedisURL:        getenv("RL_REDIS_URL"),
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        valueOr(getenv("RL_LOG_LEVEL"), "info"),
	}

	var errs []error

	if cfg.RedisURL == "" {
		errs = append(errs, errors.New("RL_REDIS_URL: required"))
	} else if u, err := url.Parse(cfg.RedisURL); err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Host == "" {
		errs = append(errs, errors.New("RL_REDIS_URL: must be a redis:// or rediss:// URL with a host"))
	}

	if raw := getenv("RL_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("RL_SHUTDOWN_TIMEOUT: %w", err))
		case d <= 0:
			errs = append(errs, errors.New("RL_SHUTDOWN_TIMEOUT: must be positive"))
		default:
			cfg.ShutdownTimeout = d
		}
	}

	if raw := getenv("RL_SHUTDOWN_DRAIN"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			errs = append(errs, errors.New("RL_SHUTDOWN_DRAIN: must be a non-negative duration"))
		} else {
			cfg.ShutdownDrain = d
		}
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("RL_LOG_LEVEL: unsupported level %s", strconv.Quote(cfg.LogLevel)))
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
