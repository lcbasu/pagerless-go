package pagerless

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestBackoffThenRecover(t *testing.T) {
	rec, srv := newRecorder(t, 503, 503, 503, 202)
	c, clk := testClient(t, srv.URL)
	c.Capture(errors.New("db down"))

	wantWaits := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	for i, want := range wantWaits {
		wait, more := c.sendPending(context.Background())
		if !more || wait != want {
			t.Fatalf("attempt %d: wait=%v more=%v, want %v", i+1, wait, more, want)
		}
		// Calling again before the backoff elapses must not hit the network.
		if w2, _ := c.sendPending(context.Background()); w2 != want {
			t.Fatalf("attempt %d: backoff not respected (%v)", i+1, w2)
		}
		if n := len(c.pending()); n != 1 {
			t.Fatalf("event must be kept while retrying, queue=%d", n)
		}
		clk.add(want)
	}
	if _, more := c.sendPending(context.Background()); more {
		t.Fatal("expected success")
	}
	if rec.calls != 4 || len(rec.events()) != 1 || c.failures != 0 {
		t.Fatalf("calls=%d events=%d failures=%d", rec.calls, len(rec.events()), c.failures)
	}
}

func TestBackoffCaps(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	if d := c.backoff(40); d != backoffMax {
		t.Fatalf("backoff(40) = %v", d)
	}
	for range 100 {
		if d := equalJitter(8 * time.Second); d < 4*time.Second || d >= 8*time.Second {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}

func TestPermanentRejectDropsBatch(t *testing.T) {
	rec, srv := newRecorder(t, http.StatusBadRequest)
	c, _ := testClient(t, srv.URL)
	c.Capture(errors.New("x"))
	if _, more := c.sendPending(context.Background()); more {
		t.Fatal("a 400 must not be retried")
	}
	if len(c.pending()) != 0 || rec.calls != 1 {
		t.Fatalf("queue=%d calls=%d", len(c.pending()), rec.calls)
	}
}

func TestUnreachableEndpointIsRetryable(t *testing.T) {
	c, _ := testClient(t, "http://127.0.0.1:1/v1/events")
	c.Capture(errors.New("x"))
	if _, more := c.sendPending(context.Background()); !more || len(c.pending()) != 1 {
		t.Fatal("network errors must keep the batch and back off")
	}
}

func TestQueueDropsOldestWhenFullAndReportsDrops(t *testing.T) {
	c, _ := testClient(t, "http://unused", WithMaxQueue(3))
	for i := range 5 {
		c.Capture(&payError{code: fmt.Sprint(i)}, Route(fmt.Sprintf("GET /r%d", i)))
	}
	q := c.pending()
	if len(q) != 3 || q[0].Route != "GET /r2" {
		t.Fatalf("want newest 3 kept, got %d starting %q", len(q), q[0].Route)
	}
	c.enqueueHeartbeat()
	q = c.pending()
	hb := q[len(q)-1]
	if hb.Type != typeHeartbeat || hb.Tags["dropped"] != "2" || c.dropped != 1 { // the event the heartbeat itself pushed out counts next time
		t.Fatalf("heartbeat must report drops: %+v", hb)
	}
}

func TestBatchesAreCappedAt500(t *testing.T) {
	rec, srv := newRecorder(t)
	c, _ := testClient(t, srv.URL)
	c.mu.Lock()
	for i := range 1200 {
		c.enqueueLocked(wireEvent{V: 1, ID: fmt.Sprintf("evt_%08d", i), Type: typeHeartbeat, Count: 1, Service: "s", Env: "e"})
	}
	c.mu.Unlock()
	c.sendPending(context.Background())
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.batches) != 3 || len(rec.batches[0].Events) != 500 || len(rec.batches[2].Events) != 200 {
		t.Fatalf("batches=%d", len(rec.batches))
	}
}

func TestParseRetryAfter(t *testing.T) {
	if parseRetryAfter("30") != 30*time.Second || parseRetryAfter("") != 0 || parseRetryAfter("soon") != 0 {
		t.Fatal("bad Retry-After parsing")
	}
}
