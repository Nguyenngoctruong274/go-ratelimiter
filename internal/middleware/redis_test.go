package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/redisstore"
)

// TestReplicasShareOneLimitThroughRedis runs three copies of an API, each
// with its own middleware and its own Redis connection pool, behind a
// "load balancer" that spreads one client's requests across them. With
// state in Redis they enforce a single limit: exactly Burst requests pass.
func TestReplicasShareOneLimitThroughRedis(t *testing.T) {
	url := os.Getenv("RL_TEST_REDIS_URL")
	if url == "" {
		t.Skip("RL_TEST_REDIS_URL not set; skipping Redis tests")
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "rltest:" + hex.EncodeToString(b[:]) + ":"

	lb := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	key, err := ByIP(IPOptions{TrustedProxies: lb})
	if err != nil {
		t.Fatal(err)
	}

	var replicas []http.Handler
	for i := 0; i < 3; i++ {
		rdb, err := redisstore.Open(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rdb.Close() })
		m, err := New(Options{
			Limiter: redisstore.NewTokenBucket(rdb, prefix),
			Limit:   limiter.PerMinute(1, 20),
			Key:     key,
			Logger:  quiet,
		})
		if err != nil {
			t.Fatal(err)
		}
		replicas = append(replicas, m(okHandler()))
	}

	var (
		wg      sync.WaitGroup
		ok, hit atomic.Int64
	)
	for i := 0; i < 90; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = "10.0.0.5:443" // the load balancer
			r.Header.Set("X-Forwarded-For", "198.51.100.23")
			rec := httptest.NewRecorder()
			replicas[i%len(replicas)].ServeHTTP(rec, r)
			switch rec.Code {
			case http.StatusOK:
				ok.Add(1)
			case http.StatusTooManyRequests:
				hit.Add(1)
			default:
				t.Errorf("status %d: %s", rec.Code, rec.Body)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 20 || hit.Load() != 70 {
		t.Fatalf("%d allowed, %d limited across 3 replicas; want 20 and 70", ok.Load(), hit.Load())
	}
}
