package pagerless

import (
	"math"
	"strconv"
	"time"
)

const (
	// maxSeen bounds the set of fingerprints already sent once. When it fills it resets, which
	// at worst re-sends a known fingerprint immediately (ingest groups it anyway).
	maxSeen = 10_000
	// maxBuckets bounds distinct groups per window. Overflow is counted, never stored.
	maxBuckets = 1_000
	// overflowRoute collects metrics once maxBuckets distinct routes exist in a window.
	overflowRoute = "(other)"
)

// bucket accumulates repeats of one group inside the current window.
type bucket struct {
	sample wireEvent // first occurrence in the window; carries type, error, tags, route
	count  int
	latSum float64 // ms, metrics only
	latMax float64
}

// aggregator implements in-process grouping. Errors and custom events with a fingerprint not seen
// before go out immediately; repeats are counted and flushed once per window. Request metrics are
// always windowed. Memory is bounded by maxSeen + maxBuckets. Not safe for concurrent use; the
// Client serialises access with its mutex.
type aggregator struct {
	seen        map[string]struct{}
	buckets     map[string]*bucket
	order       []string // insertion order, so flushes are deterministic
	windowStart time.Time
	overflow    int // events dropped because maxBuckets was reached
}

func newAggregator(now time.Time) *aggregator {
	return &aggregator{
		seen:        make(map[string]struct{}),
		buckets:     make(map[string]*bucket),
		windowStart: now,
	}
}

// groupKey groups repeats in-process: the fingerprint, plus the phase for startup errors, so a
// startup error and the same error later on are counted apart and each keeps its own tags.
func groupKey(ev wireEvent) string {
	if ev.Tags[tagPhase] == phaseStartup {
		return ev.Fingerprint + "\x00" + phaseStartup
	}
	return ev.Fingerprint
}

// addOccurrence records an error or custom event. It returns true when the event is new and must
// be sent now (count 1); otherwise it was folded into the window.
func (a *aggregator) addOccurrence(ev wireEvent) bool {
	fp := groupKey(ev)
	if _, ok := a.seen[fp]; !ok {
		if len(a.seen) >= maxSeen {
			clear(a.seen)
		}
		a.seen[fp] = struct{}{}
		return true
	}
	if b, ok := a.buckets[fp]; ok {
		b.count++
		return false
	}
	if len(a.buckets) >= maxBuckets {
		a.overflow++
		return false
	}
	a.buckets[fp] = &bucket{sample: ev, count: 1}
	a.order = append(a.order, fp)
	return false
}

// addRequest records one HTTP request for route and status with latency in ms.
func (a *aggregator) addRequest(route string, status int, latencyMs float64) {
	key := "m\x00" + route + "\x00" + strconv.Itoa(status)
	b, ok := a.buckets[key]
	if !ok {
		if len(a.buckets) >= maxBuckets {
			route = overflowRoute
			key = "m\x00" + route + "\x00" + strconv.Itoa(status)
			b, ok = a.buckets[key]
		}
		if !ok {
			b = &bucket{sample: wireEvent{Type: typeMetric, Route: route, Status: status}}
			a.buckets[key] = b
			a.order = append(a.order, key)
		}
	}
	b.count++
	b.latSum += latencyMs
	b.latMax = math.Max(b.latMax, latencyMs)
}

// flush returns one event per group in the window ending at now and starts a new window.
// IDs and envelope fields (service, env, release) are filled in by the caller.
func (a *aggregator) flush(now time.Time) []wireEvent {
	windowS := int(math.Round(now.Sub(a.windowStart).Seconds()))
	windowS = min(max(windowS, 1), maxWindowS)
	a.windowStart = now
	if len(a.order) == 0 {
		return nil
	}
	out := make([]wireEvent, 0, len(a.order))
	for _, key := range a.order {
		b := a.buckets[key]
		ev := b.sample
		ev.Count = b.count
		ev.WindowS = windowS
		if ev.Type == typeMetric {
			ev.Metric = &wireMetric{Name: "http.server.duration", Value: round1(b.latSum / float64(b.count)), Unit: "ms"}
			ev.Tags = map[string]string{"max_ms": strconv.FormatFloat(round1(b.latMax), 'f', -1, 64)}
		}
		out = append(out, ev)
	}
	clear(a.buckets)
	a.order = a.order[:0]
	return out
}

// takeOverflow returns and resets the overflow counter.
func (a *aggregator) takeOverflow() int {
	n := a.overflow
	a.overflow = 0
	return n
}

func round1(f float64) float64 { return math.Round(f*10) / 10 }
