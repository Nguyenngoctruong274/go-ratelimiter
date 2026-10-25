package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config describes one load run.
type Config struct {
	Targets  []string // base URLs of ratelimitd instances, used round robin
	Policy   string
	Key      string // client key; with Keys > 1, "<Key>-<i>"
	Keys     int
	Tier     string
	Workers  int
	Requests int           // stop after this many checks; 0 means no limit
	Duration time.Duration // stop after this long; 0 means no limit
}

func (c Config) validate() error {
	var errs []error
	if len(c.Targets) == 0 {
		errs = append(errs, errors.New("at least one target is required"))
	}
	if c.Policy == "" || c.Key == "" {
		errs = append(errs, errors.New("policy and key are required"))
	}
	if c.Workers <= 0 || c.Keys <= 0 {
		errs = append(errs, errors.New("workers and keys must be positive"))
	}
	if c.Requests < 0 || c.Duration < 0 {
		errs = append(errs, errors.New("requests and duration must not be negative"))
	}
	if c.Requests == 0 && c.Duration == 0 {
		errs = append(errs, errors.New("set requests, duration or both"))
	}
	return errors.Join(errs...)
}

// Report summarizes a run. A check is an error when it got no decision:
// a transport failure or any status other than 200.
type Report struct {
	Checks   int
	Allowed  int
	Denied   int
	Errors   int
	Elapsed  time.Duration
	P50, P99 time.Duration
	// AllowedBy counts grants per target: with a shared Redis their sum is
	// one limit, however the checks were spread.
	AllowedBy map[string]int
	// FirstError is the first error seen, to make failures diagnosable.
	FirstError string
}

// Rate is checks per second.
func (r Report) Rate() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Checks) / r.Elapsed.Seconds()
}

func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "checks %d in %s (%.0f/s): allowed %d, denied %d, errors %d\n",
		r.Checks, r.Elapsed.Round(time.Millisecond), r.Rate(), r.Allowed, r.Denied, r.Errors)
	fmt.Fprintf(&b, "latency p50 %s, p99 %s\n", r.P50, r.P99)
	targets := make([]string, 0, len(r.AllowedBy))
	for t := range r.AllowedBy {
		targets = append(targets, t)
	}
	slices.Sort(targets)
	for _, t := range targets {
		fmt.Fprintf(&b, "allowed by %s: %d\n", t, r.AllowedBy[t])
	}
	if r.FirstError != "" {
		fmt.Fprintf(&b, "first error: %s\n", r.FirstError)
	}
	return b.String()
}

type result struct {
	target  string
	allowed bool
	err     error
	latency time.Duration
}

// Run sends checks until the request budget, the duration or ctx runs out.
func Run(ctx context.Context, cfg Config, client *http.Client) (Report, error) {
	if err := cfg.validate(); err != nil {
		return Report{}, fmt.Errorf("invalid config: %w", err)
	}
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	var (
		next    atomic.Int64
		wg      sync.WaitGroup
		results = make(chan []result, cfg.Workers)
	)
	start := time.Now()
	for range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []result
			for ctx.Err() == nil {
				i := int(next.Add(1)) - 1
				if cfg.Requests > 0 && i >= cfg.Requests {
					break
				}
				r := checkOnce(ctx, client, cfg, i)
				if ctx.Err() != nil && r.err != nil {
					break // cut off by the deadline: not a failure of the service
				}
				mine = append(mine, r)
			}
			results <- mine
		}()
	}
	wg.Wait()
	close(results)

	rep := Report{Elapsed: time.Since(start), AllowedBy: map[string]int{}}
	var lat []time.Duration
	for rs := range results {
		for _, r := range rs {
			rep.Checks++
			lat = append(lat, r.latency)
			switch {
			case r.err != nil:
				rep.Errors++
				if rep.FirstError == "" {
					rep.FirstError = r.err.Error()
				}
			case r.allowed:
				rep.Allowed++
				rep.AllowedBy[r.target]++
			default:
				rep.Denied++
			}
		}
	}
	rep.P50, rep.P99 = percentile(lat, 0.50), percentile(lat, 0.99)
	return rep, nil
}

func percentile(lat []time.Duration, q float64) time.Duration {
	if len(lat) == 0 {
		return 0
	}
	slices.Sort(lat)
	return lat[int(q*float64(len(lat)-1))]
}

func checkOnce(ctx context.Context, client *http.Client, cfg Config, i int) result {
	target := cfg.Targets[i%len(cfg.Targets)]
	key := cfg.Key
	if cfg.Keys > 1 {
		key = fmt.Sprintf("%s-%d", cfg.Key, i%cfg.Keys)
	}
	body, _ := json.Marshal(map[string]string{"policy": cfg.Policy, "key": key, "tier": cfg.Tier})

	start := time.Now()
	allowed, err := post(ctx, client, strings.TrimRight(target, "/")+"/v1/check", body)
	return result{target: target, allowed: allowed, err: err, latency: time.Since(start)}
}

func post(ctx context.Context, client *http.Client, url string, body []byte) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return false, fmt.Errorf("%s: read body: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%s: status %d: %s", url, resp.StatusCode, bytes.TrimSpace(raw))
	}
	var d struct {
		Allowed *bool `json:"allowed"`
	}
	if err := json.Unmarshal(raw, &d); err != nil || d.Allowed == nil {
		return false, fmt.Errorf("%s: not a decision: %s", url, bytes.TrimSpace(raw))
	}
	return *d.Allowed, nil
}
