package pagerless

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) add(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

var epoch = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

// testClient builds a client with no background goroutine, a fake clock, sequential IDs and no
// jitter, so tests drive flushes and sends by hand.
func testClient(t *testing.T, endpoint string, opts ...Option) (*Client, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: epoch}
	seq := 0
	var seqMu sync.Mutex
	base := []Option{
		WithEndpoint(endpoint),
		WithService("shop"),
		WithEnv("production"),
		WithRelease("2026.10.22-1"),
		func(c *config) {
			c.background = false
			c.now = clk.now
			c.jitter = func(d time.Duration) time.Duration { return d }
			c.newID = func() string {
				seqMu.Lock()
				defer seqMu.Unlock()
				seq++
				return fmt.Sprintf("evt_%08d", seq)
			}
		},
	}
	c, err := New("pl_test_key", append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	// Discard the init heartbeat and deploy marker so tests start from an empty queue.
	c.mu.Lock()
	c.queue = c.queue[:0]
	c.mu.Unlock()
	return c, clk
}

func (c *Client) pending() []wireEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]wireEvent(nil), c.queue...)
}

// recorder is a fake ingest endpoint.
type recorder struct {
	mu      sync.Mutex
	batches []wireBatch
	auth    []string
	status  []int // status to return per request; last value repeats; empty = 202
	calls   int
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	code := http.StatusAccepted
	if len(r.status) > 0 {
		code = r.status[min(r.calls, len(r.status)-1)]
	}
	r.calls++
	body, _ := io.ReadAll(req.Body)
	var b wireBatch
	if err := json.Unmarshal(body, &b); err == nil && code < 300 {
		r.batches = append(r.batches, b)
	}
	r.auth = append(r.auth, req.Header.Get("Authorization"))
	w.WriteHeader(code)
}

func (r *recorder) events() []wireEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []wireEvent
	for _, b := range r.batches {
		out = append(out, b.Events...)
	}
	return out
}

func newRecorder(t *testing.T, status ...int) (*recorder, *httptest.Server) {
	rec := &recorder{status: status}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	return rec, srv
}
