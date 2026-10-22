package pagerless

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"time"
)

type payError struct{ code string }

func (e *payError) Error() string { return "payment failed: " + e.code }

func siteA(c *Client, err error) { c.Capture(err, Route("POST /pay")) }
func siteB(c *Client, err error) { c.Capture(err, Route("POST /pay")) }

func lastFingerprint(t *testing.T, c *Client) string {
	t.Helper()
	q := c.pending()
	if len(q) == 0 {
		t.Fatal("nothing queued")
	}
	return q[len(q)-1].Fingerprint
}

func TestFingerprintStableAcrossCallsAndMessages(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	siteA(c, &payError{code: "card_declined"})
	fp1 := lastFingerprint(t, c)
	c.agg = newAggregator(epoch) // forget "seen" so the next one is queued too
	siteA(c, &payError{code: "insufficient_funds"})
	fp2 := lastFingerprint(t, c)
	if fp1 != fp2 {
		t.Fatalf("same site and type must group: %s != %s", fp1, fp2)
	}
	if len(fp1) != 32 {
		t.Fatalf("fingerprint length %d", len(fp1))
	}
}

func TestFingerprintSeparatesSiteTypeAndRoute(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	siteA(c, &payError{})
	a := lastFingerprint(t, c)
	siteB(c, &payError{})
	b := lastFingerprint(t, c)
	siteA(c, errors.New("plain"))
	typ := lastFingerprint(t, c)
	c.Capture(&payError{}, Route("POST /refund"))
	route := lastFingerprint(t, c)
	seen := map[string]string{a: "a", b: "b", typ: "type", route: "route"}
	if len(seen) != 4 {
		t.Fatalf("expected 4 distinct fingerprints, got %v", seen)
	}
}

func TestFingerprintIgnoresLineNumbersAndClosureIndex(t *testing.T) {
	f1 := []frame{{Function: "shop/pay.Charge", File: "/a/pay.go", Line: 10}, {Function: "shop/pay.Handler.func1", File: "/a/h.go", Line: 3}}
	f2 := []frame{{Function: "shop/pay.Charge", File: "/b/pay.go", Line: 99}, {Function: "shop/pay.Handler.func7", File: "/b/h.go", Line: 40}}
	if fingerprint(typeError, "*x.E", "GET /", f1) != fingerprint(typeError, "*x.E", "GET /", f2) {
		t.Fatal("line numbers, directories and closure indexes must not change the fingerprint")
	}
	f3 := []frame{{Function: "shop/pay.Refund", File: "/a/pay.go", Line: 10}}
	if fingerprint(typeError, "*x.E", "GET /", f1) == fingerprint(typeError, "*x.E", "GET /", f3) {
		t.Fatal("different functions must not group")
	}
}

func TestFingerprintSkipsFrameworkFrames(t *testing.T) {
	user := []frame{{Function: "shop.Handler", File: "h.go"}}
	withStd := []frame{{Function: "runtime.gopanic", File: "panic.go"}, {Function: "shop.Handler", File: "h.go"}, {Function: "net/http.HandlerFunc.ServeHTTP", File: "server.go"}}
	if fingerprint(typeError, "t", "r", user) != fingerprint(typeError, "t", "r", withStd) {
		t.Fatal("runtime and net/http frames must not affect grouping")
	}
}

func TestErrTypeLooksThroughWrappers(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errors.New("x"), "*errors.errorString"},
		{fmt.Errorf("charge: %w", &payError{}), "*pagerless.payError"},
		{fmt.Errorf("a: %w", fmt.Errorf("b: %w", fs.ErrNotExist)), "*errors.errorString"},
		{errors.Join(errors.New("a"), &payError{}), "*errors.joinError"},
		{&fs.PathError{Op: "open", Path: "/x", Err: fs.ErrNotExist}, "*fs.PathError"},
	}
	for _, tc := range cases {
		if got := errType(tc.err); got != tc.want {
			t.Errorf("errType(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestCaptureStackStartsAtCaller(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	siteA(c, &payError{})
	ev := c.pending()[0]
	if ev.Error == nil || ev.Error.Type != "*pagerless.payError" {
		t.Fatalf("bad error: %+v", ev.Error)
	}
	want := "github.com/lcbasu/pagerless-go.siteA()"
	if got := ev.Error.Stack; len(got) < len(want) || got[:len(want)] != want {
		t.Fatalf("stack should start at the caller, got:\n%s", got)
	}
}

func TestGroupingNewImmediatelyRepeatsWindowed(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	for range 5 {
		siteA(c, &payError{code: "declined"})
	}
	q := c.pending()
	if len(q) != 1 || q[0].Count != 1 || q[0].WindowS != 0 {
		t.Fatalf("first occurrence must be sent at once with count 1, got %+v", q)
	}
	clk.add(10 * time.Second)
	c.flushWindow()
	q = c.pending()
	if len(q) != 2 {
		t.Fatalf("want 2 queued, got %d", len(q))
	}
	if r := q[1]; r.Count != 4 || r.WindowS != 10 || r.Fingerprint != q[0].Fingerprint || r.ID == q[0].ID {
		t.Fatalf("repeats must flush as one counted event with a fresh id: %+v", r)
	}
	clk.add(10 * time.Second)
	c.flushWindow()
	if n := len(c.pending()); n != 2 {
		t.Fatalf("an empty window must send nothing, queue=%d", n)
	}
	// Seen fingerprints stay seen: the next repeat is windowed, not immediate.
	siteA(c, &payError{})
	if n := len(c.pending()); n != 2 {
		t.Fatalf("known fingerprint must not be sent immediately, queue=%d", n)
	}
}

func TestAggregatorBoundsMemory(t *testing.T) {
	a := newAggregator(epoch)
	for i := range maxBuckets + 50 {
		ev := wireEvent{Type: typeError, Fingerprint: fmt.Sprint(i)}
		a.addOccurrence(ev) // first: seen
		a.addOccurrence(ev) // repeat: bucket
	}
	if len(a.buckets) != maxBuckets || a.overflow != 50 {
		t.Fatalf("buckets=%d overflow=%d", len(a.buckets), a.overflow)
	}
	for i := range maxBuckets + 10 {
		a.addRequest(fmt.Sprintf("GET /r%d", i), 200, 1)
	}
	if len(a.buckets) > maxBuckets+1 {
		t.Fatalf("request routes must collapse into the overflow route, buckets=%d", len(a.buckets))
	}
	for i := range maxSeen + 5 {
		a.addOccurrence(wireEvent{Fingerprint: fmt.Sprint("s", i)})
	}
	if len(a.seen) > maxSeen {
		t.Fatalf("seen=%d exceeds bound", len(a.seen))
	}
}

func TestStartupErrorsTaggedAndGroupedApart(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	siteA(c, &payError{code: "boot"})
	siteA(c, &payError{code: "boot"})
	c.Capture(errors.New("ready"), Tags(map[string]string{"phase": "migrate"}))
	clk.add(startupWindow)
	siteA(c, &payError{code: "boot"})
	c.flushWindow()

	type row struct {
		phase   string
		count   int
		windowS int
	}
	var got []row
	for _, ev := range c.pending() {
		got = append(got, row{ev.Tags["phase"], ev.Count, ev.WindowS})
	}
	want := []row{{"startup", 1, 0}, {"migrate", 1, 0}, {"", 1, 0}, {"startup", 1, 30}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("phase/count/window = %v, want %v", got, want)
	}
	q := c.pending()
	if q[0].Fingerprint != q[2].Fingerprint {
		t.Fatalf("startup and later errors must share the fingerprint: %q vs %q", q[0].Fingerprint, q[2].Fingerprint)
	}
}
