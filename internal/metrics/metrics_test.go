package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func render(r *Registry) string {
	var b strings.Builder
	r.Write(&b)
	return b.String()
}

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	reqs := r.NewCounterVec("http_requests_total", "Requests served.", "method", "status")
	reqs.Inc("POST", "201")
	reqs.Add(2, "GET", "200")
	reqs.Inc("POST", "201")

	pending := r.NewGaugeVec("queue_depth", "Items waiting.")
	pending.Set(7)
	pending.Add(-2)

	r.NewGaugeFunc("goroutines", "Live goroutines.", func() float64 { return 3 })
	r.NewCounterFunc("waits_total", "Pool waits.", func() float64 { return 11 })

	lat := r.NewHistogramVec("latency_seconds", "Request latency.", []float64{0.1, 1}, "route")
	lat.Observe(0.05, "/a")
	lat.Observe(0.1, "/a") // on the boundary: le is inclusive
	lat.Observe(0.5, "/a")
	lat.Observe(3, "/a")

	want := `# HELP goroutines Live goroutines.
# TYPE goroutines gauge
goroutines 3
# HELP http_requests_total Requests served.
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 2
http_requests_total{method="POST",status="201"} 2
# HELP latency_seconds Request latency.
# TYPE latency_seconds histogram
latency_seconds_bucket{route="/a",le="0.1"} 2
latency_seconds_bucket{route="/a",le="1"} 3
latency_seconds_bucket{route="/a",le="+Inf"} 4
latency_seconds_sum{route="/a"} 3.65
latency_seconds_count{route="/a"} 4
# HELP queue_depth Items waiting.
# TYPE queue_depth gauge
queue_depth 5
# HELP waits_total Pool waits.
# TYPE waits_total counter
waits_total 11
`
	if got := render(r); got != want {
		t.Fatalf("exposition mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestLabelAndHelpEscaping(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("weird_total", "Line one\nline \\ two.", "v")
	c.Inc("a\"b\\c\nd")

	got := render(r)
	if !strings.Contains(got, `# HELP weird_total Line one\nline \\ two.`) {
		t.Fatalf("help not escaped:\n%s", got)
	}
	if !strings.Contains(got, `weird_total{v="a\"b\\c\nd"} 1`) {
		t.Fatalf("label not escaped:\n%s", got)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("ops_total", "Ops.", "kind")
	h := r.NewHistogramVec("op_seconds", "Op time.", []float64{1}, "kind")

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			kind := []string{"a", "b"}[g%2]
			for i := 0; i < 1000; i++ {
				c.Inc(kind)
				h.Observe(0.5, kind)
				if i%100 == 0 {
					_ = render(r) // scrape while writing
				}
			}
		}(g)
	}
	wg.Wait()

	if a, b := c.Value("a"), c.Value("b"); a != 8000 || b != 8000 {
		t.Fatalf("counts a=%v b=%v, want 8000 each", a, b)
	}
	if !strings.Contains(render(r), `op_seconds_count{kind="a"} 8000`) {
		t.Fatal("histogram lost observations")
	}
}

func TestMisuse(t *testing.T) {
	cases := map[string]func(){
		"duplicate name": func() {
			r := NewRegistry()
			r.NewCounterVec("x_total", "x")
			r.NewCounterVec("x_total", "x")
		},
		"invalid name":       func() { NewRegistry().NewCounterVec("1bad", "x") },
		"wrong label count":  func() { NewRegistry().NewCounterVec("y_total", "y", "a").Inc() },
		"counter decrease":   func() { NewRegistry().NewCounterVec("z_total", "z").Add(-1) },
		"unsorted buckets":   func() { NewRegistry().NewHistogramVec("h", "h", []float64{2, 1}) },
		"no buckets":         func() { NewRegistry().NewHistogramVec("h2", "h", nil) },
		"name with a hyphen": func() { NewRegistry().NewGaugeVec("bad-name", "x") },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}

func TestHandler(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("hits_total", "Hits.").Inc()

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("Content-Type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "hits_total 1\n") {
		t.Fatalf("body = %s", rec.Body)
	}
}

func BenchmarkCounterInc(b *testing.B) {
	c := NewRegistry().NewCounterVec("bench_total", "b", "route", "status")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc("/v1/check", "200")
		}
	})
}

func TestRegisterRuntime(t *testing.T) {
	r := NewRegistry()
	RegisterRuntime(r)
	out := render(r)
	for _, name := range []string{"go_goroutines ", "go_heap_inuse_bytes ", "go_gc_cycles_total ", "process_uptime_seconds "} {
		if !strings.Contains(out, "\n"+name) {
			t.Errorf("missing %s", name)
		}
	}
}
