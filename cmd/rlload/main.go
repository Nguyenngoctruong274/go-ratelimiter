// Command rlload sends rate-limit checks to one or more ratelimitd
// instances and reports what was allowed, by whom, and how fast. With
// -want-allowed it exits non-zero unless exactly that many checks were
// allowed, which turns "N instances enforce one limit" into a pass/fail
// check against a running deployment.
//
//	rlload -targets http://localhost:8080,http://localhost:8081 \
//	       -policy login -key demo -requests 100 -want-allowed 5
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "rlload:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("rlload", flag.ContinueOnError)
	var (
		cfg         Config
		targets     string
		wantAllowed int
	)
	fs.StringVar(&targets, "targets", "http://localhost:8080", "comma-separated ratelimitd base URLs, used round robin")
	fs.StringVar(&cfg.Policy, "policy", "api", "policy to check")
	fs.StringVar(&cfg.Key, "key", "rlload", "client key")
	fs.IntVar(&cfg.Keys, "keys", 1, "spread checks over this many keys (<key>-<i>)")
	fs.StringVar(&cfg.Tier, "tier", "", "client tier")
	fs.IntVar(&cfg.Workers, "workers", 16, "concurrent workers")
	fs.IntVar(&cfg.Requests, "requests", 0, "stop after this many checks (0: no limit)")
	fs.DurationVar(&cfg.Duration, "duration", 0, "stop after this long (0: no limit)")
	fs.IntVar(&wantAllowed, "want-allowed", -1, "fail unless exactly this many checks are allowed (-1: no check)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, t := range strings.Split(targets, ",") {
		if t = strings.TrimSpace(t); t != "" {
			cfg.Targets = append(cfg.Targets, t)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: max(cfg.Workers, 2)},
	}
	rep, err := Run(ctx, cfg, client)
	if err != nil {
		return err
	}
	fmt.Fprint(out, rep)

	switch {
	case rep.Errors > 0:
		return fmt.Errorf("%d of %d checks failed", rep.Errors, rep.Checks)
	case wantAllowed >= 0 && rep.Allowed != wantAllowed:
		return fmt.Errorf("allowed %d, want exactly %d", rep.Allowed, wantAllowed)
	case rep.Checks == 0:
		return errors.New("no checks were sent")
	}
	return nil
}
