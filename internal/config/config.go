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

	// PolicyFile is the JSON policy file served by POST /v1/check.
	PolicyFile string
	// PolicyReload is how often the policy file is checked for changes;
	// 0 disables reloading.
	PolicyReload time.Duration

	// OnRedisFailure says what a check does while Redis is unavailable:
	// fail closed (503), fail open (allow), or decide locally.
	OnRedisFailure FailureMode
	// Replicas is the number of instances sharing Redis. In local mode
	// each one enforces 1/Replicas of every limit during an outage.
	Replicas int
	// RedisTimeout bounds one check on Redis.
	RedisTimeout     time.Duration
	BreakerThreshold int
	BreakerCooldown  time.Duration
	// PipelineSenders is how many pipelines of batched checks may be in
	// flight to Redis at once; 0 sends every check on its own.
	PipelineSenders int
}

// FailureMode is the value of RL_ON_REDIS_FAILURE.
type FailureMode string

// The supported failure modes.
const (
	FailClosed FailureMode = "closed"
	FailOpen   FailureMode = "open"
	FailLocal  FailureMode = "local"
)

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

		PolicyFile:       getenv("RL_POLICY_FILE"),
		PolicyReload:     5 * time.Second,
		OnRedisFailure:   FailureMode(valueOr(getenv("RL_ON_REDIS_FAILURE"), string(FailClosed))),
		Replicas:         1,
		RedisTimeout:     100 * time.Millisecond,
		BreakerThreshold: 5,
		BreakerCooldown:  5 * time.Second,
		PipelineSenders:  4,
	}

	var errs []error
	duration := func(name string, dst *time.Duration, allowZero bool) {
		raw := getenv(name)
		if raw == "" {
			return
		}
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		case d < 0 || (d == 0 && !allowZero):
			errs = append(errs, fmt.Errorf("%s: must be positive", name))
		default:
			*dst = d
		}
	}
	integer := func(name string, dst *int, allowZero bool) {
		raw := getenv(name)
		if raw == "" {
			return
		}
		n, err := strconv.Atoi(raw)
		switch {
		case err != nil || n < 0:
			errs = append(errs, fmt.Errorf("%s: must be a non-negative integer", name))
		case n == 0 && !allowZero:
			errs = append(errs, fmt.Errorf("%s: must be positive", name))
		default:
			*dst = n
		}
	}

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

	if cfg.PolicyFile == "" {
		errs = append(errs, errors.New("RL_POLICY_FILE: required"))
	}
	duration("RL_POLICY_RELOAD", &cfg.PolicyReload, true)
	switch cfg.OnRedisFailure {
	case FailClosed, FailOpen, FailLocal:
	default:
		errs = append(errs, fmt.Errorf("RL_ON_REDIS_FAILURE: must be closed, open or local, got %s", strconv.Quote(string(cfg.OnRedisFailure))))
	}
	integer("RL_REPLICAS", &cfg.Replicas, false)
	duration("RL_REDIS_TIMEOUT", &cfg.RedisTimeout, false)
	integer("RL_BREAKER_THRESHOLD", &cfg.BreakerThreshold, false)
	duration("RL_BREAKER_COOLDOWN", &cfg.BreakerCooldown, false)
	integer("RL_REDIS_PIPELINE_SENDERS", &cfg.PipelineSenders, true)

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
