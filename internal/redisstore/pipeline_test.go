package redisstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// TestPipelineBatchesConcurrentChecks runs many concurrent checks of one
// key through pipelines on several instances: they must still be exactly
// as atomic as direct calls, and they must actually share round trips.
func TestPipelineBatchesConcurrentChecks(t *testing.T) {
	first := newTestClient(t)
	l := limiter.PerMinute(1, 100) // effectively no refill during the test

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			prefix := testPrefix(t, first)
			var (
				wg      sync.WaitGroup
				allowed atomic.Int64
				pipes   []*Pipeline
				base    []PipelineStats
			)
			for range 3 {
				p := NewPipeline(newTestClient(t), PipelineOptions{Senders: 2, MaxBatch: 16})
				t.Cleanup(func() { _ = p.Close() })
				pipes = append(pipes, p)
				store := alg.new(p, prefix)
				// Load the script first: on a cold script cache every
				// check of a batch would also need an EVAL after NOSCRIPT.
				if _, err := store.Allow(context.Background(), "warm", l, 0); err != nil {
					t.Fatal(err)
				}
				base = append(base, p.Stats())
				for range 100 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range 5 {
							d, err := store.Allow(context.Background(), "hot", l, 1)
							if err != nil {
								t.Error(err)
								return
							}
							if d.Allowed {
								allowed.Add(1)
							}
						}
					}()
				}
			}
			wg.Wait()
			if got := allowed.Load(); got != 100 {
				t.Fatalf("%d of 1500 checks allowed, want exactly 100", got)
			}
			for i, p := range pipes {
				s := p.Stats()
				s.Batches -= base[i].Batches
				s.Commands -= base[i].Commands
				if s.Commands != 500 {
					t.Errorf("pipeline %d sent %d commands, want 500", i, s.Commands)
				}
				if s.Commands > 16*s.Batches {
					t.Errorf("pipeline %d: %s exceeds MaxBatch 16", i, s)
				}
				// 100 goroutines and 2 senders: most checks wait for a
				// busy sender and leave together with others.
				if s.Batches*2 > s.Commands {
					t.Errorf("pipeline %d: %s, want at least 2 per round trip", i, s)
				}
			}
		})
	}
}

// TestPipelineIdleCheckIsOneRoundTrip: without contention nothing waits to
// be batched, so a lone check is sent at once, alone.
func TestPipelineIdleCheckIsOneRoundTrip(t *testing.T) {
	c := newTestClient(t)
	p := NewPipeline(c, PipelineOptions{})
	defer p.Close()
	store := NewGCRA(p, testPrefix(t, c))

	for i := range 10 {
		if _, err := store.Allow(context.Background(), fmt.Sprint("k", i), limiter.PerSecond(1, 1), 1); err != nil {
			t.Fatal(err)
		}
	}
	if s := p.Stats(); s != (PipelineStats{Batches: 10, Commands: 10}) {
		t.Fatalf("stats = %s, want 10 commands in 10 round trips", s)
	}
}

// TestPipelineReloadsScriptsInABatch: after SCRIPT FLUSH every EVALSHA of a
// batch fails with NOSCRIPT, and each check must recover through EVAL.
func TestPipelineReloadsScriptsInABatch(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	p := NewPipeline(c, PipelineOptions{Senders: 1})
	defer p.Close()

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			store := alg.new(p, testPrefix(t, c))
			if err := c.ScriptFlush(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for i := range 50 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := store.Allow(ctx, fmt.Sprint("k", i), limiter.PerSecond(1, 1), 1); err != nil {
						t.Errorf("after SCRIPT FLUSH: %v", err)
					}
				}()
			}
			wg.Wait()
		})
	}
}

// TestPipelineRedisDown: when the round trip fails, every check in the
// batch gets an error; none is left hanging or reported as allowed.
func TestPipelineRedisDown(t *testing.T) {
	down := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer down.Close()
	p := NewPipeline(down, PipelineOptions{Senders: 1})
	defer p.Close()
	store := NewTokenBucket(p, "")

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := store.Allow(context.Background(), fmt.Sprint("k", i), limiter.PerSecond(1, 1), 1)
			if err == nil || d.Allowed {
				t.Errorf("redis down: %+v, %v", d, err)
			}
		}()
	}
	wg.Wait()
}

// blackhole accepts connections and never answers, like a Redis that hangs.
func blackhole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// TestPipelineHonorsCallerDeadline: a check waiting on a batch that hangs
// returns when its own context ends, not when Redis gives up.
func TestPipelineHonorsCallerDeadline(t *testing.T) {
	hung := redis.NewClient(&redis.Options{Addr: blackhole(t), MaxRetries: -1, ReadTimeout: 5 * time.Second})
	defer hung.Close()
	p := NewPipeline(hung, PipelineOptions{Senders: 1})
	defer func() {
		_ = hung.Close() // unblocks the sender so Close does not wait 5s
		_ = p.Close()
	}()
	store := NewGCRA(p, "")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := store.Allow(ctx, "k", limiter.PerSecond(1, 1), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("returned after %s, want about 50ms", took)
	}
}

func TestPipelineClose(t *testing.T) {
	hung := redis.NewClient(&redis.Options{Addr: blackhole(t), MaxRetries: -1, ReadTimeout: 300 * time.Millisecond})
	defer hung.Close()
	p := NewPipeline(hung, PipelineOptions{Senders: 1})
	store := NewGCRA(p, "")

	inflight := make(chan error, 1)
	go func() {
		_, err := store.Allow(context.Background(), "k", limiter.PerSecond(1, 1), 1)
		inflight <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the sender take it

	done := make(chan struct{})
	go func() {
		_ = p.Close()
		_ = p.Close() // idempotent
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case err := <-inflight:
		if err == nil {
			t.Fatal("in-flight check succeeded against a hung Redis")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight check still waiting after Close")
	}

	if _, err := store.Allow(context.Background(), "k", limiter.PerSecond(1, 1), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v, want ErrClosed", err)
	}
}

// BenchmarkPipelineRedis compares direct calls with a Pipeline as the
// number of concurrent callers grows. Besides ns/op it reports throughput
// (checks/s), latency percentiles per check and the average batch size.
//
//	RL_TEST_REDIS_URL=redis://localhost:6379/15 go test -run '^$' -bench Pipeline ./internal/redisstore
func BenchmarkPipelineRedis(b *testing.B) {
	c := newTestClient(b)
	l := limiter.PerSecond(1_000_000, 1_000)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}

	for _, callers := range []int{1, 16, 64, 256} {
		for _, piped := range []bool{false, true} {
			name := fmt.Sprintf("callers=%d/direct", callers)
			var rdb redis.Scripter = c
			var p *Pipeline
			if piped {
				name = fmt.Sprintf("callers=%d/pipelined", callers)
				p = NewPipeline(c, PipelineOptions{})
				rdb = p
			}
			store := NewGCRA(rdb, testPrefix(b, c))
			b.Run(name, func(b *testing.B) {
				var before PipelineStats
				if p != nil {
					before = p.Stats()
				}
				lat := runCallers(b, callers, func(i int) error {
					_, err := store.Allow(context.Background(), keys[i%len(keys)], l, 1)
					return err
				})
				reportLatency(b, lat)
				if p != nil {
					s := p.Stats()
					b.ReportMetric(float64(s.Commands-before.Commands)/float64(max(1, s.Batches-before.Batches)), "checks/batch")
				}
			})
			if p != nil {
				_ = p.Close()
			}
		}
	}
}

// runCallers runs b.N calls of fn spread over a fixed number of goroutines
// and returns the latency of each call.
func runCallers(b *testing.B, callers int, fn func(i int) error) []time.Duration {
	var (
		next atomic.Int64
		wg   sync.WaitGroup
		mu   sync.Mutex
		all  = make([]time.Duration, 0, b.N)
	)
	b.ResetTimer()
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lat []time.Duration
			for {
				i := int(next.Add(1)) - 1
				if i >= b.N {
					break
				}
				start := time.Now()
				if err := fn(i); err != nil {
					b.Error(err)
					return
				}
				lat = append(lat, time.Since(start))
			}
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	b.StopTimer()
	return all
}

func reportLatency(b *testing.B, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "checks/s")
	slices.Sort(lat)
	pct := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))].Microseconds()) }
	b.ReportMetric(pct(0.50), "p50-µs")
	b.ReportMetric(pct(0.99), "p99-µs")
}
