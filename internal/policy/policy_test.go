package policy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/middleware"
)

// fakeLimiter records calls and allows everything.
type fakeLimiter struct{ name string }

func (f fakeLimiter) Allow(context.Context, string, limiter.Limit, int64) (limiter.Decision, error) {
	return limiter.Decision{Allowed: true}, nil
}

func testOptions() Options {
	return Options{
		Limiters: map[string]limiter.Limiter{
			TokenBucket: fakeLimiter{"tb"}, GCRA: fakeLimiter{"gcra"}, SlidingWindow: fakeLimiter{"sw"},
		},
		User: func(r *http.Request) (string, bool) {
			u := r.Header.Get("X-Test-User")
			return u, u != ""
		},
		Tier: func(r *http.Request) string { return r.Header.Get("X-Test-Tier") },
	}
}

func loadExample(t *testing.T, opts Options) *Set {
	t.Helper()
	raw, err := os.ReadFile("testdata/policies.json")
	if err != nil {
		t.Fatal(err)
	}
	f, err := ParseBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	set, err := Compile(f, opts)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// summary renders checks as "policy key limit cost" lines.
func summary(checks []middleware.Check) []string {
	var out []string
	for _, c := range checks {
		out = append(out, c.Policy+" "+c.Key+" "+c.Limit.String()+" x"+strconv.FormatInt(c.Cost, 10)+" "+c.Limiter.(fakeLimiter).name)
	}
	return out
}

func TestResolveExample(t *testing.T) {
	set := loadExample(t, testOptions())
	if got := set.Names(); !slices.Equal(got, []string{"global", "login", "api", "export"}) {
		t.Fatalf("Names = %v", got)
	}

	tests := []struct {
		name    string
		method  string
		path    string
		headers map[string]string
		want    []string
	}{
		{
			name: "anonymous page view", method: "GET", path: "/",
			want: []string{"global ip:203.0.113.9 600/1m0s burst 100 x1 tb"},
		},
		{
			name: "login is limited twice: globally and per route", method: "POST", path: "/v1/login",
			want: []string{
				"global ip:203.0.113.9 600/1m0s burst 100 x1 tb",
				"login ip:203.0.113.9 5/1m0s burst 5 x1 gcra",
				"api ip:203.0.113.9 60/1m0s burst 10 x1 tb",
			},
		},
		{
			name: "GET on the login path does not match the POST-only rule", method: "GET", path: "/v1/login",
			want: []string{
				"global ip:203.0.113.9 600/1m0s burst 100 x1 tb",
				"api ip:203.0.113.9 60/1m0s burst 10 x1 tb",
			},
		},
		{
			name: "a pro user gets the pro tier", method: "GET", path: "/v1/items",
			headers: map[string]string{"X-Test-User": "42", "X-Test-Tier": "pro"},
			want: []string{
				"global ip:203.0.113.9 600/1m0s burst 100 x1 tb",
				"api u:42 1200/1m0s burst 200 x1 tb",
			},
		},
		{
			name: "an unknown tier falls back to the default limit", method: "GET", path: "/v1/items",
			headers: map[string]string{"X-Test-User": "42", "X-Test-Tier": "gold"},
			want: []string{
				"global ip:203.0.113.9 600/1m0s burst 100 x1 tb",
				"api u:42 60/1m0s burst 10 x1 tb",
			},
		},
		{
			name: "an unlimited tier skips the policy, not the others", method: "GET", path: "/v1/items",
			headers: map[string]string{"X-Test-User": "svc", "X-Test-Tier": "internal"},
			want:    []string{"global ip:203.0.113.9 600/1m0s burst 100 x1 tb"},
		},
		{
			name: "exports cost 10 on a sliding window", method: "POST", path: "/v1/exports",
			headers: map[string]string{"X-Test-User": "42"},
			want: []string{
				"global ip:203.0.113.9 600/1m0s burst 100 x1 tb",
				"api u:42 60/1m0s burst 10 x1 tb",
				"export u:42 100/1h0m0s burst 100 x10 sw",
			},
		},
		{
			name: "a user-keyed policy skips anonymous clients", method: "POST", path: "/v1/exports",
			want: []string{
				"global ip:203.0.113.9 600/1m0s burst 100 x1 tb",
				"api ip:203.0.113.9 60/1m0s burst 10 x1 tb",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, nil)
			r.RemoteAddr = "203.0.113.9:4000"
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}
			checks, err := set.Resolve(r)
			if err != nil {
				t.Fatal(err)
			}
			if got := summary(checks); !slices.Equal(got, tt.want) {
				t.Fatalf("got\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(tt.want, "\n  "))
			}
		})
	}
}

func TestMatchHost(t *testing.T) {
	m := MatchConfig{Host: "api.example.com"}
	for host, want := range map[string]bool{
		"api.example.com": true, "API.example.com:8443": true, "example.com": false, "[::1]:80": false,
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Host = host
		if got := m.matches(r); got != want {
			t.Errorf("host %q: %t, want %t", host, got, want)
		}
	}
}

func TestCompileReportsEveryProblem(t *testing.T) {
	const bad = `{"policies": [
		{"name": "a", "key": "ip", "limit": {"rate": 10, "period": "1s"}},
		{"name": "a", "key": "ip", "limit": {"rate": 10, "period": "1s"}},
		{"name": "bad name!", "key": "cookie", "algorithm": "leaky", "limit": {"rate": 1, "period": "soon"}},
		{"name": "c", "key": "header:", "match": {"methods": ["FETCH"], "path": "x", "path_prefix": "/y"}},
		{"name": "d", "key": "ip", "cost": 6, "limit": {"rate": 1, "period": "1s", "burst": 5}},
		{"name": "e", "key": "ip", "limit": {"rate": 1, "period": "1s"},
		 "tiers": {"pro": {"unlimited": true, "limit": {"rate": 1, "period": "1s"}}, "x": {"limit": {"rate": 0, "period": "1s"}}}},
		{"name": "f", "key": "ip", "algorithm": "sliding_window", "limit": {"rate": 1, "period": "1h", "burst": 2000000}}
	]}`
	f, err := ParseBytes([]byte(bad))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Compile(f, testOptions())
	if err == nil {
		t.Fatal("bad file compiled")
	}
	for _, want := range []string{
		`policies[1] "a".name: duplicate policy name`,
		`policies[2] "bad name!".name: must be`,
		`policies[2] "bad name!".key: unknown key "cookie"`,
		`policies[2] "bad name!".algorithm: unknown algorithm "leaky"`,
		`policies[2] "bad name!".limit: period: time: invalid duration "soon"`,
		`policies[3] "c".match.methods: unknown method "FETCH"`,
		`policies[3] "c".match: use path or path_prefix, not both`,
		`policies[3] "c".match: path "x" must start with /`,
		`policies[3] "c".key: bad header name ""`,
		`policies[3] "c".limit: required`,
		`policies[4] "d".limit: invalid cost: cost 6 with burst 5`,
		`policies[5] "e".tiers.pro: set limit or unlimited, not both`,
		`policies[5] "e".tiers.x.limit: invalid limit: rate must be positive`,
		`policies[6] "f".limit: invalid limit: sliding window`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
}

func TestCompileNeedsTheRightOptions(t *testing.T) {
	const doc = `{"policies": [{"name": "a", "key": "user", "algorithm": "gcra",
		"limit": {"rate": 1, "period": "1s"}, "tiers": {"pro": {"unlimited": true}}}]}`
	f, err := ParseBytes([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Compile(f, Options{Limiters: map[string]limiter.Limiter{TokenBucket: fakeLimiter{}}})
	for _, want := range []string{`"user" needs Options.User`, `no limiter configured for "gcra"`, "tiers need Options.Tier"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
}

func TestParseIsStrict(t *testing.T) {
	for name, doc := range map[string]string{
		"typo in a field": `{"policies": [{"name": "a", "key": "ip", "limit": {"rate": 1, "period": "1s", "burts": 5}}]}`,
		"trailing data":   `{"policies": []} {}`,
		"not json":        `policies: []`,
	} {
		if _, err := ParseBytes([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

const (
	strict  = `{"policies": [{"name": "api", "key": "ip", "limit": {"rate": 1, "period": "1m", "burst": 1}}]}`
	relaxed = `{"policies": [{"name": "api", "key": "ip", "limit": {"rate": 1, "period": "1m", "burst": 3}}]}`
)

func TestStoreReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policies.json")
	writeFile(t, path, strict)
	s, err := Open(path, testOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	burst := func() int64 {
		r := httptest.NewRequest("GET", "/", nil)
		checks, err := s.Resolve(r)
		if err != nil || len(checks) != 1 {
			t.Fatalf("resolve: %v %v", checks, err)
		}
		return checks[0].Limit.Burst
	}
	if burst() != 1 {
		t.Fatal("initial file not served")
	}

	if changed, err := s.Reload(); changed || err != nil {
		t.Fatalf("unchanged file: changed=%t err=%v", changed, err)
	}

	writeFile(t, path, relaxed)
	if changed, err := s.Reload(); !changed || err != nil || burst() != 3 {
		t.Fatalf("edit: changed=%t err=%v burst=%d", changed, err, burst())
	}

	// A broken edit is rejected as a whole; the last good policies stay.
	writeFile(t, path, `{"policies": [{"name": "api", "key": "ip", "limit": {"rate": -1, "period": "1m"}}]}`)
	if changed, err := s.Reload(); changed || err == nil || burst() != 3 {
		t.Fatalf("broken edit: changed=%t err=%v burst=%d", changed, err, burst())
	}
	if st := s.ReloadStats(); st != (ReloadStats{Applied: 2, Rejected: 1, Failing: true}) {
		t.Fatalf("after a broken edit: %+v", st)
	}
	// Fixing the file recovers, even if it is identical to the broken one's predecessor.
	writeFile(t, path, relaxed)
	if changed, err := s.Reload(); err != nil || changed {
		t.Fatalf("restored to the served content: changed=%t err=%v", changed, err)
	}
	writeFile(t, path, strict)
	if changed, err := s.Reload(); !changed || err != nil || burst() != 1 {
		t.Fatalf("second edit: changed=%t err=%v", changed, err)
	}
	// The first load counts as applied; unchanged checks count as nothing.
	if st := s.ReloadStats(); st != (ReloadStats{Applied: 3, Rejected: 1}) {
		t.Fatalf("after recovering: %+v", st)
	}
}

func TestOpenFailsOnABadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policies.json")
	if _, err := Open(path, testOptions(), nil); err == nil {
		t.Fatal("missing file accepted")
	}
	writeFile(t, path, `{"policies": [{"name": ""}]}`)
	if _, err := Open(path, testOptions(), nil); err == nil || !strings.Contains(err.Error(), "policies.json") {
		t.Fatalf("bad file: %v", err)
	}
}

func TestWatchReloadsWhileServing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policies.json")
	writeFile(t, path, strict)
	s, err := Open(path, testOptions(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx, 5*time.Millisecond) }()

	// Readers keep resolving during reloads; every answer comes from one
	// whole file, never a mix (run with -race).
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				checks, err := s.Resolve(httptest.NewRequest("GET", "/", nil))
				if err != nil || len(checks) != 1 || (checks[0].Limit.Burst != 1 && checks[0].Limit.Burst != 3) {
					t.Errorf("torn read: %v %v", checks, err)
					return
				}
			}
		}()
	}

	writeFile(t, path, relaxed)
	deadline := time.Now().Add(2 * time.Second)
	for s.Current().policies[0].limit.Burst != 3 {
		if time.Now().After(deadline) {
			t.Fatal("watch did not pick up the edit")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.Watch(context.Background(), 0); err == nil {
		t.Fatal("zero interval accepted")
	}
}

// TestPoliciesThroughMiddleware wires a Store into middleware.Enforce with
// a real limiter and checks what a client sees.
func TestPoliciesThroughMiddleware(t *testing.T) {
	const doc = `{"policies": [
		{"name": "global", "key": "ip", "limit": {"rate": 100, "period": "1m", "burst": 100}},
		{"name": "login", "match": {"methods": ["POST"], "path": "/login"}, "key": "ip",
		 "limit": {"rate": 2, "period": "1m", "burst": 2}}
	]}`
	path := filepath.Join(t.TempDir(), "p.json")
	writeFile(t, path, doc)
	lim := memory.New(limiter.SystemClock{})
	s, err := Open(path, Options{Limiters: map[string]limiter.Limiter{TokenBucket: lim}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := middleware.Enforce(s, middleware.EnforceOptions{LegacyHeaders: true})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))

	login := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/login", nil)
		r.RemoteAddr = "198.51.100.4:1"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}
	first := login()
	if first.Code != http.StatusNoContent {
		t.Fatalf("first login: %d", first.Code)
	}
	if got := first.Header().Get("RateLimit-Policy"); got != `"global";q=100;w=60, "login";q=2;w=60` {
		t.Fatalf("RateLimit-Policy = %q", got)
	}
	if got := first.Header().Get("RateLimit"); got != `"global";r=99;t=1, "login";r=1;t=30` {
		t.Fatalf("RateLimit = %q", got)
	}
	// The legacy headers describe the tightest policy.
	if first.Header().Get("X-RateLimit-Limit") != "2" || first.Header().Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("legacy headers: %v", first.Header())
	}

	login()
	third := login()
	if third.Code != http.StatusTooManyRequests || !strings.Contains(third.Body.String(), "rate limit exceeded: login") {
		t.Fatalf("third login: %d %s", third.Code, third.Body)
	}
	if third.Header().Get("Retry-After") != "30" {
		t.Fatalf("Retry-After = %q", third.Header().Get("Retry-After"))
	}

	// Other routes are only under the global policy.
	r := httptest.NewRequest("GET", "/home", nil)
	r.RemoteAddr = "198.51.100.4:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusNoContent || rec.Header().Get("RateLimit") != `"global";r=96;t=3` {
		t.Fatalf("other route: %d %q", rec.Code, rec.Header().Get("RateLimit"))
	}
}

func TestLookup(t *testing.T) {
	set := loadExample(t, testOptions())
	tests := []struct {
		name, policy, tier string
		want               Policy
		found              bool
	}{
		{"default limit", "api", "", Policy{Name: "api", Limit: limiter.PerMinute(60, 10), Cost: 1}, true},
		{"unknown tier gets the default", "api", "gold", Policy{Name: "api", Limit: limiter.PerMinute(60, 10), Cost: 1}, true},
		{"tier limit", "api", "pro", Policy{Name: "api", Limit: limiter.PerMinute(1200, 200), Cost: 1}, true},
		{"unlimited tier", "api", "internal", Policy{Name: "api", Cost: 1, Unlimited: true}, true},
		{"policy cost", "export", "", Policy{Name: "export", Limit: limiter.Limit{Rate: 100, Period: time.Hour, Burst: 100}, Cost: 10}, true},
		{"unknown policy", "nope", "", Policy{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := set.Lookup(tt.policy, tt.tier)
			if found != tt.found {
				t.Fatalf("found = %v, want %v", found, tt.found)
			}
			got.Limiter = nil
			if got != tt.want {
				t.Fatalf("Lookup(%q, %q) = %+v, want %+v", tt.policy, tt.tier, got, tt.want)
			}
		})
	}
	if p, _ := set.Lookup("export", ""); p.Limiter != (fakeLimiter{"sw"}) {
		t.Fatalf("export uses %v, want the sliding window limiter", p.Limiter)
	}
}
