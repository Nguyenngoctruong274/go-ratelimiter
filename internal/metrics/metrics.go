// Package metrics is a small, dependency-free implementation of the
// Prometheus text exposition format (version 0.0.4).
//
// It supports what this service needs and nothing more: counters, gauges and
// histograms with fixed label names, plus gauges computed at scrape time.
// Output is deterministic (families and series sorted), so it can be tested
// byte for byte.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families and renders them for scraping.
type Registry struct {
	mu       sync.Mutex
	families map[string]family
}

type family interface {
	describe() (name, help, kind string)
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: map[string]family{}}
}

func (r *Registry) register(f family) {
	name, _, _ := f.describe()
	if !validName(name) {
		panic(fmt.Sprintf("metrics: invalid metric name %q", name))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.families[name]; dup {
		panic(fmt.Sprintf("metrics: %s registered twice", name))
	}
	r.families[name] = f
}

// Write renders every family in the text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	names := make([]string, 0, len(r.families))
	for n := range r.families {
		names = append(names, n)
	}
	fams := make([]family, len(names))
	sort.Strings(names)
	for i, n := range names {
		fams[i] = r.families[n]
	}
	r.mu.Unlock()

	for _, f := range fams {
		name, help, kind := f.describe()
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(help), name, kind)
		f.write(w)
	}
}

// Handler serves the registry at a scrape endpoint.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

// ---- labels -----------------------------------------------------------------

// series keys label values joined by \xff, which cannot appear in UTF-8 text.
const sep = "\xff"

func labelPairs(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(values[i]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func checkLabels(metric string, names, values []string) {
	if len(values) != len(names) {
		panic(fmt.Sprintf("metrics: %s wants %d label values, got %d", metric, len(names), len(values)))
	}
}

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func validName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		letter := c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// atomicFloat is a float64 updated with compare-and-swap.
type atomicFloat struct{ bits atomic.Uint64 }

func (f *atomicFloat) add(d float64) {
	for {
		old := f.bits.Load()
		if f.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}

func (f *atomicFloat) set(v float64) { f.bits.Store(math.Float64bits(v)) }
func (f *atomicFloat) get() float64  { return math.Float64frombits(f.bits.Load()) }

// vec holds one value per label combination.
type vec[T any] struct {
	name, help string
	labels     []string
	newValue   func() *T

	mu     sync.RWMutex
	series map[string]*T
	values map[string][]string
}

func (v *vec[T]) with(values []string) *T {
	checkLabels(v.name, v.labels, values)
	key := strings.Join(values, sep)

	v.mu.RLock()
	s, ok := v.series[key]
	v.mu.RUnlock()
	if ok {
		return s
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if s, ok := v.series[key]; ok {
		return s
	}
	s = v.newValue()
	v.series[key] = s
	v.values[key] = append([]string(nil), values...)
	return s
}

func (v *vec[T]) sorted() (keys []string) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for k := range v.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func newVec[T any](name, help string, labels []string, newValue func() *T) *vec[T] {
	return &vec[T]{name: name, help: help, labels: labels, newValue: newValue,
		series: map[string]*T{}, values: map[string][]string{}}
}

// ---- counter ----------------------------------------------------------------

// CounterVec is a family of monotonically increasing counters.
type CounterVec struct{ v *vec[atomicFloat] }

// NewCounterVec registers a counter family with the given label names.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{v: newVec(name, help, labels, func() *atomicFloat { return &atomicFloat{} })}
	r.register(c)
	return c
}

// Add increases the series for values by d. Negative d panics: counters
// only go up, and a decrease would look like a process restart to Prometheus.
func (c *CounterVec) Add(d float64, values ...string) {
	if d < 0 {
		panic("metrics: counter " + c.v.name + " cannot decrease")
	}
	c.v.with(values).add(d)
}

// Inc adds one.
func (c *CounterVec) Inc(values ...string) { c.Add(1, values...) }

// Value returns the current value of a series (0 if never touched).
func (c *CounterVec) Value(values ...string) float64 {
	checkLabels(c.v.name, c.v.labels, values)
	c.v.mu.RLock()
	defer c.v.mu.RUnlock()
	if s, ok := c.v.series[strings.Join(values, sep)]; ok {
		return s.get()
	}
	return 0
}

func (c *CounterVec) describe() (string, string, string) { return c.v.name, c.v.help, "counter" }

func (c *CounterVec) write(w io.Writer) {
	for _, k := range c.v.sorted() {
		c.v.mu.RLock()
		s, vals := c.v.series[k], c.v.values[k]
		c.v.mu.RUnlock()
		fmt.Fprintf(w, "%s%s %s\n", c.v.name, labelPairs(c.v.labels, vals), formatFloat(s.get()))
	}
}

// ---- gauge ------------------------------------------------------------------

// GaugeVec is a family of values that can go up and down.
type GaugeVec struct{ v *vec[atomicFloat] }

// NewGaugeVec registers a gauge family.
func (r *Registry) NewGaugeVec(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{v: newVec(name, help, labels, func() *atomicFloat { return &atomicFloat{} })}
	r.register(g)
	return g
}

// Set sets the series for values.
func (g *GaugeVec) Set(v float64, values ...string) { g.v.with(values).set(v) }

// Add changes the series for values by d.
func (g *GaugeVec) Add(d float64, values ...string) { g.v.with(values).add(d) }

func (g *GaugeVec) describe() (string, string, string) { return g.v.name, g.v.help, "gauge" }

func (g *GaugeVec) write(w io.Writer) {
	for _, k := range g.v.sorted() {
		g.v.mu.RLock()
		s, vals := g.v.series[k], g.v.values[k]
		g.v.mu.RUnlock()
		fmt.Fprintf(w, "%s%s %s\n", g.v.name, labelPairs(g.v.labels, vals), formatFloat(s.get()))
	}
}

// gaugeFunc is a gauge whose value is computed on every scrape.
type gaugeFunc struct {
	name, help string
	fn         func() float64
}

// NewGaugeFunc registers a gauge read from fn at scrape time. fn must be
// fast and safe for concurrent use; it is the right tool for values that
// already live elsewhere (pool stats, queue depth) and would go stale if
// copied.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.register(&gaugeFunc{name: name, help: help, fn: fn})
}

func (g *gaugeFunc) describe() (string, string, string) { return g.name, g.help, "gauge" }
func (g *gaugeFunc) write(w io.Writer)                  { fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.fn())) }

// counterFunc is a counter whose value is read at scrape time from a source
// that only ever grows (for example sql.DBStats.WaitCount).
type counterFunc struct {
	name, help string
	fn         func() float64
}

// NewCounterFunc registers a counter read from fn at scrape time.
func (r *Registry) NewCounterFunc(name, help string, fn func() float64) {
	r.register(&counterFunc{name: name, help: help, fn: fn})
}

func (c *counterFunc) describe() (string, string, string) { return c.name, c.help, "counter" }
func (c *counterFunc) write(w io.Writer)                  { fmt.Fprintf(w, "%s %s\n", c.name, formatFloat(c.fn())) }

// ---- histogram --------------------------------------------------------------

// DefaultLatencyBuckets suit an API whose requests take milliseconds to a
// few seconds.
var DefaultLatencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// FastLatencyBuckets suit calls that take tens of microseconds to a few
// milliseconds, such as a rate-limit check and its Redis round trip.
var FastLatencyBuckets = []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1}

type histogram struct {
	upper  []float64
	counts []atomic.Uint64 // per bucket, not cumulative; the last is +Inf
	sum    atomicFloat
	count  atomic.Uint64
}

// HistogramVec is a family of histograms with fixed bucket bounds.
type HistogramVec struct {
	v      *vec[histogram]
	bounds []float64
}

// NewHistogramVec registers a histogram family. buckets are upper bounds in
// increasing order; +Inf is added automatically.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if !sort.Float64sAreSorted(buckets) || len(buckets) == 0 {
		panic("metrics: histogram " + name + " needs increasing buckets")
	}
	bounds := append([]float64(nil), buckets...)
	h := &HistogramVec{bounds: bounds}
	h.v = newVec(name, help, labels, func() *histogram {
		return &histogram{upper: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
	})
	r.register(h)
	return h
}

// Observe records one value.
func (h *HistogramVec) Observe(v float64, values ...string) {
	s := h.v.with(values)
	i := sort.SearchFloat64s(s.upper, v) // first bound >= v; len(upper) means +Inf
	s.counts[i].Add(1)
	s.sum.add(v)
	s.count.Add(1)
}

func (h *HistogramVec) describe() (string, string, string) { return h.v.name, h.v.help, "histogram" }

func (h *HistogramVec) write(w io.Writer) {
	names := append(append([]string(nil), h.v.labels...), "le")
	for _, k := range h.v.sorted() {
		h.v.mu.RLock()
		s, vals := h.v.series[k], h.v.values[k]
		h.v.mu.RUnlock()

		var cumulative uint64
		for i := range s.counts {
			cumulative += s.counts[i].Load()
			le := "+Inf"
			if i < len(s.upper) {
				le = formatFloat(s.upper[i])
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.v.name, labelPairs(names, append(append([]string(nil), vals...), le)), cumulative)
		}
		lp := labelPairs(h.v.labels, vals)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.v.name, lp, formatFloat(s.sum.get()))
		fmt.Fprintf(w, "%s_count%s %d\n", h.v.name, lp, s.count.Load())
	}
}
