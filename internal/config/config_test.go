package config

import (
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{"RL_REDIS_URL": "redis://localhost:6379/0", "RL_POLICY_FILE": "policies.json"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		HTTPAddr:         ":8080",
		RedisURL:         "redis://localhost:6379/0",
		ShutdownTimeout:  15 * time.Second,
		LogLevel:         "info",
		PolicyFile:       "policies.json",
		PolicyReload:     5 * time.Second,
		OnRedisFailure:   FailClosed,
		Replicas:         1,
		RedisTimeout:     100 * time.Millisecond,
		BreakerThreshold: 5,
		BreakerCooldown:  5 * time.Second,
		PipelineSenders:  4,
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{
		"RL_HTTP_ADDR":         "127.0.0.1:9090",
		"RL_REDIS_URL":         "rediss://user:pw@cache.internal:6380/2",
		"RL_SHUTDOWN_TIMEOUT":  "30s",
		"RL_SHUTDOWN_DRAIN":    "5s",
		"RL_LOG_LEVEL":         "debug",
		"RL_POLICY_FILE":       "/etc/rl/policies.json",
		"RL_POLICY_RELOAD":     "0",
		"RL_ON_REDIS_FAILURE":  "local",
		"RL_REPLICAS":          "4",
		"RL_REDIS_TIMEOUT":     "50ms",
		"RL_BREAKER_THRESHOLD": "3",
		"RL_BREAKER_COOLDOWN":  "2s",

		"RL_REDIS_PIPELINE_SENDERS": "0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.ShutdownTimeout != 30*time.Second ||
		cfg.ShutdownDrain != 5*time.Second || cfg.LogLevel != "debug" {
		t.Fatalf("unexpected config %+v", cfg)
	}
	if cfg.PolicyFile != "/etc/rl/policies.json" || cfg.PolicyReload != 0 || cfg.OnRedisFailure != FailLocal ||
		cfg.Replicas != 4 || cfg.RedisTimeout != 50*time.Millisecond || cfg.BreakerThreshold != 3 || cfg.BreakerCooldown != 2*time.Second ||
		cfg.PipelineSenders != 0 {
		t.Fatalf("unexpected service config %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string // every substring must appear in the error
	}{
		{
			name: "missing redis url",
			env:  map[string]string{},
			want: []string{"RL_REDIS_URL: required"},
		},
		{
			name: "wrong scheme",
			env:  map[string]string{"RL_REDIS_URL": "http://localhost:6379"},
			want: []string{"RL_REDIS_URL: must be"},
		},
		{
			name: "no host",
			env:  map[string]string{"RL_REDIS_URL": "redis://"},
			want: []string{"RL_REDIS_URL: must be"},
		},
		{
			name: "bad durations",
			env: map[string]string{
				"RL_REDIS_URL":        "redis://localhost:6379",
				"RL_SHUTDOWN_TIMEOUT": "0s",
				"RL_SHUTDOWN_DRAIN":   "-1s",
			},
			want: []string{"RL_SHUTDOWN_TIMEOUT: must be positive", "RL_SHUTDOWN_DRAIN"},
		},
		{
			name: "unparsable timeout",
			env:  map[string]string{"RL_REDIS_URL": "redis://localhost:6379", "RL_SHUTDOWN_TIMEOUT": "soon"},
			want: []string{"RL_SHUTDOWN_TIMEOUT"},
		},
		{
			name: "every error is reported at once",
			env:  map[string]string{"RL_LOG_LEVEL": "loud"},
			want: []string{"RL_REDIS_URL: required", "RL_POLICY_FILE: required", `RL_LOG_LEVEL: unsupported level "loud"`},
		},
		{
			name: "bad failure handling",
			env: map[string]string{
				"RL_REDIS_URL": "redis://localhost:6379", "RL_POLICY_FILE": "p.json",
				"RL_ON_REDIS_FAILURE": "maybe", "RL_REPLICAS": "0", "RL_REDIS_TIMEOUT": "0s",
				"RL_BREAKER_THRESHOLD": "many", "RL_BREAKER_COOLDOWN": "-1s", "RL_POLICY_RELOAD": "-5s",
				"RL_REDIS_PIPELINE_SENDERS": "-1",
			},
			want: []string{"RL_ON_REDIS_FAILURE: must be closed, open or local", "RL_REPLICAS: must be positive", "RL_REDIS_TIMEOUT: must be positive",
				"RL_BREAKER_THRESHOLD: must be a non-negative integer", "RL_BREAKER_COOLDOWN", "RL_POLICY_RELOAD",
				"RL_REDIS_PIPELINE_SENDERS: must be a non-negative integer"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(envFrom(tt.env))
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}
