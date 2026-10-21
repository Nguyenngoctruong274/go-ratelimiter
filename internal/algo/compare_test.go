package algo

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// slidingLog is the exact sliding window: it remembers every granted event
// and allows a request when fewer than burst events were granted in the
// last span. It is the yardstick for the accuracy comparison, not a store
// algorithm: its memory grows with the burst, and on Redis it would be a
// sorted set with one member per event.
type slidingLog struct {
	span, burst int64
	granted     []int64
}

// recent returns how many events were granted in (now - span, now].
func (s *slidingLog) recent(now int64) int64 {
	i := 0
	for i < len(s.granted) && s.granted[i] <= now-s.span {
		i++
	}
	s.granted = s.granted[i:]
	return int64(len(s.granted))
}

func (s *slidingLog) allow(now int64) bool {
	if s.recent(now) >= s.burst {
		return false
	}
	s.granted = append(s.granted, now)
	return true
}

// peakInWindow returns the largest number of times in any half-open
// interval [t, t+span). times must be sorted.
func peakInWindow(times []int64, span int64) int {
	peak, lo := 0, 0
	for hi, t := range times {
		for times[lo] <= t-span {
			lo++
		}
		peak = max(peak, hi-lo+1)
	}
	return peak
}

// traffic patterns over one minute, as request times in microseconds from a
// window boundary.
func trafficPatterns() []struct {
	name  string
	times []int64
} {
	var steady, edges, clumps []int64
	for t := int64(0); t < 60*sec; t += 50_000 { // 20/s, twice the limit
		steady = append(steady, t)
	}
	for w := int64(0); w < 60; w++ { // 20 requests in the last 100ms of every second
		for i := int64(0); i < 20; i++ {
			edges = append(edges, w*sec+900_000+i*5_000)
		}
	}
	rng := rand.New(rand.NewSource(4))
	for t := int64(0); t < 60*sec; t += int64(rng.ExpFloat64() * 100_000) { // ~10/s
		clumps = append(clumps, t)
		if rng.Intn(20) == 0 { // and now and then a client retrying in a tight loop
			for i := int64(1); i <= 15; i++ {
				clumps = append(clumps, t+i*2_000)
			}
			t += 30_000
		}
	}
	return []struct {
		name  string
		times []int64
	}{
		{"steady 20/s", steady},
		{"bursts at window edges", edges},
		{"random with retry storms", clumps},
	}
}

// TestAccuracyComparison replays the same traffic through the exact sliding
// log and the three store algorithms, all configured with one Limit, and
// measures how far each strays from the rule "at most Burst events in any
// window of Burst/Rate". Each algorithm is judged on its own history:
//
//   - over: granted although Burst events were already granted in the
//     last window, so some window ends up holding more than Burst;
//   - under: denied although the last window had room.
//
// Run with -v to print the table that the README shows.
func TestAccuracyComparison(t *testing.T) {
	l := limiter.PerSecond(10, 10)
	span, err := WindowSpan(l)
	if err != nil {
		t.Fatal(err)
	}
	base := 1_700_000_000 * sec

	names := []string{"sliding log (exact)", "sliding window", "token bucket", "GCRA"}
	var table strings.Builder
	table.WriteString("| Traffic | Algorithm | Allowed | Over | Under | Peak in any 1s |\n|---|---|---|---|---|---|\n")
	for _, p := range trafficPatterns() {
		var (
			exact  = slidingLog{span: span, burst: l.Burst}
			tb     Bucket
			gc     TAT
			sw     Window
			own    = make(map[string]*slidingLog)
			grants = make(map[string][]int64)
			over   = make(map[string]int)
			under  = make(map[string]int)
		)
		for _, name := range names {
			own[name] = &slidingLog{span: span, burst: l.Burst}
		}
		for i, at := range p.times {
			now := base + at
			nb, dtb, err := TokenBucket(tb, i > 0, l, now, 1)
			if err != nil {
				t.Fatal(err)
			}
			ng, dgc, err := GCRA(gc, i > 0, l, now, 1)
			if err != nil {
				t.Fatal(err)
			}
			nw, dsw, err := SlidingWindow(sw, i > 0, l, now, 1)
			if err != nil {
				t.Fatal(err)
			}
			tb, gc, sw = nb, ng, nw
			if dgc != dtb {
				t.Fatalf("%s, request %d: GCRA %+v differs from token bucket %+v", p.name, i, dgc, dtb)
			}

			decided := map[string]bool{names[0]: exact.allow(now), names[1]: dsw.Allowed, names[2]: dtb.Allowed, names[3]: dgc.Allowed}
			for name, ok := range decided {
				room := own[name].recent(now) < l.Burst
				switch {
				case ok && !room:
					over[name]++
				case !ok && room:
					under[name]++
				}
				if ok {
					own[name].granted = append(own[name].granted, now)
					grants[name] = append(grants[name], now)
				}
			}
		}

		for _, name := range names {
			peak := peakInWindow(grants[name], span)
			fmt.Fprintf(&table, "| %s | %s | %d | %d | %d | %d |\n", p.name, name, len(grants[name]), over[name], under[name], peak)

			// What each algorithm guarantees about any window of one span.
			switch name {
			case "sliding log (exact)":
				if peak > int(l.Burst) || over[name]+under[name] != 0 {
					t.Errorf("%s: the exact log is not exact: peak %d, over %d, under %d", p.name, peak, over[name], under[name])
				}
			case "sliding window": // only fixed windows hold at most Burst
				if peak >= 2*int(l.Burst) {
					t.Errorf("%s: sliding window peaked at %d", p.name, peak)
				}
			default: // a full bucket plus a span's worth of refill
				if peak > 2*int(l.Burst) || under[name] != 0 {
					t.Errorf("%s: %s peaked at %d, under-admitted %d", p.name, name, peak, under[name])
				}
			}
		}
	}
	t.Log("\n" + table.String())
}
