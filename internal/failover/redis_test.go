package failover

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/breaker"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/middleware"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/redisstore"
)

// deadRedis returns a client for an address nobody listens on, so every
// command fails the way it does when Redis is down.
func deadRedis(t *testing.T) *redis.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// TestMiddlewareWithRedisDown puts the three ways of handling an outage
// side by side, each behind the real middleware with Redis unreachable.
func TestMiddlewareWithRedisDown(t *testing.T) {
	limit := limiter.PerMinute(4, 4)
	tests := []struct {
		name     string
		fallback bool
		failOpen bool
		// want is the status of each of 6 requests from one client.
		want []int
	}{
		{"fail closed", false, false, []int{503, 503, 503, 503, 503, 503}},
		{"fail open", false, true, []int{200, 200, 200, 200, 200, 200}},
		{"local fallback, 2 replicas", true, false, []int{200, 200, 429, 429, 429, 429}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{Replicas: 2, Logger: quiet, Breaker: breaker.Options{Threshold: 2, Cooldown: time.Minute}}
			if tt.fallback {
				opts.Fallback = memory.New(limiter.SystemClock{})
			}
			f, err := New(redisstore.NewGCRA(deadRedis(t), ""), opts)
			if err != nil {
				t.Fatal(err)
			}
			mw, err := middleware.New(middleware.Options{
				Limiter: f, Limit: limit, FailOpen: tt.failOpen, Logger: quiet,
				Key: func(*http.Request) (string, error) { return "client", nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))

			for i, want := range tt.want {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
				if rec.Code != want {
					t.Fatalf("request %d: status %d, want %d (%s)", i, rec.Code, want, rec.Body)
				}
			}
			if f.State() != breaker.Open {
				t.Fatalf("breaker %s after the outage, want open", f.State())
			}
		})
	}
}

// proxy forwards TCP to Redis until cut, which drops every connection and
// refuses new ones, the way a Redis crash or a network partition looks to
// the client.
type proxy struct {
	ln       net.Listener
	upstream string

	mu    sync.Mutex
	down  bool
	conns []net.Conn
}

func newProxy(t *testing.T, upstream string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, upstream: upstream}
	t.Cleanup(func() { _ = ln.Close(); p.setDown(true) })
	go p.serve()
	return p
}

func (p *proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *proxy) handle(c net.Conn) {
	var d net.Dialer
	up, err := d.DialContext(context.Background(), "tcp", p.upstream)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	if p.down {
		p.mu.Unlock()
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns = append(p.conns, c, up)
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	_, _ = io.Copy(c, up)
	_ = c.Close()
}

func (p *proxy) setDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
	}
}

// TestRedisOutageAndRecovery runs against a real Redis behind a proxy that
// the test cuts and restores: decisions move to the local share while Redis
// is gone and back to Redis once a probe succeeds.
func TestRedisOutageAndRecovery(t *testing.T) {
	raw := os.Getenv("RL_TEST_REDIS_URL")
	if raw == "" {
		t.Skip("RL_TEST_REDIS_URL not set; skipping Redis tests")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	px := newProxy(t, u.Host)
	u.Host = px.ln.Addr().String()
	ctx := context.Background()
	rdb, err := redisstore.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	var b [6]byte
	_, _ = rand.Read(b[:])
	f, err := New(redisstore.NewTokenBucket(rdb, "rltest:"+hex.EncodeToString(b[:])+":"), Options{
		Fallback: memory.New(limiter.SystemClock{}), Replicas: 5, Timeout: 200 * time.Millisecond, Logger: quiet,
		Breaker: breaker.Options{Threshold: 2, Cooldown: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	limit := limiter.PerMinute(10, 10)
	allow := func() limiter.Decision {
		t.Helper()
		d, err := f.Allow(ctx, "client", limit, 1)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		return d
	}

	if d := allow(); d.Limit != limit || d.Remaining != 9 {
		t.Fatalf("healthy: %+v, want Redis's decision under the full limit", d)
	}

	px.setDown(true)
	granted := 0
	for i := 0; i < 10; i++ {
		d := allow()
		if d.Limit != LocalShare(limit, 1, 5) {
			t.Fatalf("outage call %d decided under %s, want the local share", i, d.Limit)
		}
		if d.Allowed {
			granted++
		}
	}
	if granted != 2 || f.State() != breaker.Open {
		t.Fatalf("outage: granted %d, state %s; want 2 (10/5) and open", granted, f.State())
	}

	px.setDown(false)
	deadline := time.Now().Add(5 * time.Second)
	for f.State() != breaker.Closed {
		if time.Now().After(deadline) {
			t.Fatal("breaker did not close after Redis came back")
		}
		time.Sleep(20 * time.Millisecond)
		_ = allow()
	}
	// Redis kept its state through the outage: the first grant left 9, and
	// the probe that closed the breaker took one more.
	if d := allow(); d.Limit != limit || d.Remaining != 7 {
		t.Fatalf("recovered: %+v, want Redis's decision with 7 left", d)
	}
}

func TestDeadRedisErrorsAreFailures(t *testing.T) {
	f, err := New(redisstore.NewTokenBucket(deadRedis(t), ""), Options{Logger: quiet, Breaker: breaker.Options{Threshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Allow(context.Background(), "k", limiter.PerSecond(1, 1), 1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if f.State() != breaker.Open {
		t.Fatalf("state = %s, want open", f.State())
	}
}
