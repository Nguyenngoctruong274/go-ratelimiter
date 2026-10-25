package metrics

import (
	"runtime"
	"time"
)

// RegisterRuntime exposes a few process-level signals: goroutines (leaks),
// heap in use (memory growth), GC cycles, and uptime (restarts).
func RegisterRuntime(r *Registry) {
	start := time.Now()
	r.NewGaugeFunc("go_goroutines", "Number of goroutines.", func() float64 {
		return float64(runtime.NumGoroutine())
	})
	r.NewGaugeFunc("go_heap_inuse_bytes", "Heap bytes in use.", func() float64 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return float64(m.HeapInuse)
	})
	r.NewCounterFunc("go_gc_cycles_total", "Completed GC cycles.", func() float64 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return float64(m.NumGC)
	})
	r.NewGaugeFunc("process_uptime_seconds", "Seconds since the process started.", func() float64 {
		return time.Since(start).Seconds()
	})
}
