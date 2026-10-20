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
	cfg, err := load(envFrom(map[string]string{"RL_REDIS_URL": "redis://localhost:6379/0"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		HTTPAddr:        ":8080",
		RedisURL:        "redis://localhost:6379/0",
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        "info",
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{
		"RL_HTTP_ADDR":        "127.0.0.1:9090",
		"RL_REDIS_URL":        "rediss://user:pw@cache.internal:6380/2",
		"RL_SHUTDOWN_TIMEOUT": "30s",
		"RL_SHUTDOWN_DRAIN":   "5s",
		"RL_LOG_LEVEL":        "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.ShutdownTimeout != 30*time.Second ||
		cfg.ShutdownDrain != 5*time.Second || cfg.LogLevel != "debug" {
		t.Fatalf("unexpected config %+v", cfg)
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
			want: []string{"RL_REDIS_URL: required", `RL_LOG_LEVEL: unsupported level "loud"`},
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
