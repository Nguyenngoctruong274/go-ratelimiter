package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/config"
)

type decision struct {
	Allowed   bool  `json:"allowed"`
	Remaining int64 `json:"remaining"`
	Unlimited bool  `json:"unlimited"`
	Limit     *struct {
		Burst int64 `json:"burst"`
	} `json:"limit"`
}

const testPolicies = `{"policies": [
  {"name": "tb", "key": "ip", "limit": {"rate": 1, "period": "1h", "burst": %[1]d},
   "tiers": {"internal": {"unlimited": true}}},
  {"name": "gcra", "key": "ip", "algorithm": "gcra", "limit": {"rate": 1, "period": "1h", "burst": %[1]d}},
  {"name": "sw", "key": "ip", "algorithm": "sliding_window", "limit": {"rate": 1, "period": "1h", "burst": %[1]d}}
]}`

// cluster starts n ratelimitd instances, each with its own Redis pool,
// sharing one Redis and one policy file.
type cluster struct {
	urls   []string
	policy string
	client *http.Client
}

func startCluster(t *testing.T, n, burst int) *cluster {
	t.Helper()
	redisURL := os.Getenv("RL_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("RL_TEST_REDIS_URL not set; skipping Redis tests")
	}
	c := &cluster{policy: filepath.Join(t.TempDir(), "policies.json"), client: &http.Client{Timeout: 5 * time.Second}}
	c.writePolicies(t, burst)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := config.Config{
		RedisURL: redisURL, PolicyFile: c.policy, PolicyReload: 20 * time.Millisecond,
		OnRedisFailure: config.FailClosed, Replicas: n, RedisTimeout: time.Second,
		BreakerThreshold: 5, BreakerCooldown: time.Second,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < n; i++ {
		svc, err := newService(ctx, cfg, logger)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = svc.Close() })
		srv := httptest.NewServer(svc.handler)
		t.Cleanup(srv.Close)
		c.urls = append(c.urls, srv.URL)
	}
	return c
}

func (c *cluster) writePolicies(t *testing.T, burst int) {
	t.Helper()
	// Write and rename, so a reload never reads a half-written file.
	tmp := c.policy + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf(testPolicies, burst)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, c.policy); err != nil {
		t.Fatal(err)
	}
}

func (c *cluster) check(t *testing.T, instance int, body map[string]any) decision {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.urls[instance]+"/v1/check", bytes.NewReader(raw))
	if err != nil {
		t.Error(err)
		return decision{}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Error(err)
		return decision{}
	}
	defer resp.Body.Close()
	var d decision
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Errorf("instance %d: status %d: %s", instance, resp.StatusCode, b)
		return d
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Error(err)
	}
	return d
}

func uniqueKey(t *testing.T) string {
	t.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:])
	return t.Name() + ":" + hex.EncodeToString(b[:])
}

// TestInstancesEnforceOneLimit sends 240 concurrent checks for one client,
// spread over three instances, for each algorithm. Exactly Burst are
// allowed, and the grants report every remaining count from Burst-1 down
// to 0 once: each check saw the state the previous one left, whichever
// instance ran it.
func TestInstancesEnforceOneLimit(t *testing.T) {
	const instances, burst, requests = 3, 25, 240
	c := startCluster(t, instances, burst)

	for _, pol := range []string{"tb", "gcra", "sw"} {
		t.Run(pol, func(t *testing.T) {
			key := uniqueKey(t)
			var (
				wg        sync.WaitGroup
				mu        sync.Mutex
				remaining []int64
			)
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					d := c.check(t, i%instances, map[string]any{"policy": pol, "key": key})
					if d.Allowed {
						mu.Lock()
						remaining = append(remaining, d.Remaining)
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			if len(remaining) != burst {
				t.Fatalf("%d of %d allowed across %d instances, want exactly %d", len(remaining), requests, instances, burst)
			}
			sort.Slice(remaining, func(i, j int) bool { return remaining[i] < remaining[j] })
			for i, r := range remaining {
				if r != int64(i) {
					t.Fatalf("remaining counts %v, want 0..%d once each", remaining, burst-1)
				}
			}
		})
	}
}

func TestInstancesShareStateSequentially(t *testing.T) {
	c := startCluster(t, 3, 5)
	key := uniqueKey(t)
	for i := 0; i < 7; i++ {
		d := c.check(t, i%3, map[string]any{"policy": "gcra", "key": key})
		if wantAllowed, wantRemaining := i < 5, int64(max(4-i, 0)); d.Allowed != wantAllowed || d.Remaining != wantRemaining {
			t.Fatalf("check %d on instance %d: %+v, want allowed=%v remaining=%d", i, i%3, d, wantAllowed, wantRemaining)
		}
	}
	// A tier exempt from the policy is never counted.
	if d := c.check(t, 0, map[string]any{"policy": "tb", "key": key, "tier": "internal"}); !d.Allowed || !d.Unlimited {
		t.Fatalf("internal tier: %+v, want unlimited", d)
	}
}

// TestPolicyReloadReachesEveryInstance edits the shared policy file and
// waits for every instance to decide under the new limit.
func TestPolicyReloadReachesEveryInstance(t *testing.T) {
	c := startCluster(t, 3, 5)
	c.writePolicies(t, 9)

	deadline := time.Now().Add(5 * time.Second)
	for i := range c.urls {
		for {
			d := c.check(t, i, map[string]any{"policy": "tb", "key": uniqueKey(t), "cost": 0})
			if d.Limit != nil && d.Limit.Burst == 9 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("instance %d still on the old policy", i)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
