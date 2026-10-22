package pagerless

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite golden files")

// TestPayloadGolden pins the exact JSON sent to ingest (the ingest API's IngestBatch).
func TestPayloadGolden(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	frames := []frame{{Function: "shop/pay.Charge", File: "/src/shop/pay/charge.go", Line: 42}}
	c.captureError("*stripe.CardError", "card 4242 4242 4242 4242 declined for maya@example.com", frames,
		[]CaptureOption{Route("POST /checkout"), Status(502), Tags(map[string]string{"plan": "pro", "api_key": "sk_live_x"})})
	c.Event("Refunds paused", Tell, map[string]string{"by": "ops"})
	c.Deploy("2026.10.22-2")
	c.recordRequest("POST /checkout", 502, 120*time.Millisecond)
	c.recordRequest("POST /checkout", 502, 80*time.Millisecond)
	clk.add(10 * time.Second)
	c.flushWindow()
	c.enqueueHeartbeat()

	got, err := json.MarshalIndent(wireBatch{Events: c.pending()}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "golden_batch.json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload changed (run go test -update if intended)\n got:\n%s\nwant:\n%s", got, want)
	}
}

// TestPayloadMatchesWireLimits checks every event against the IngestEvent constraints.
func TestPayloadMatchesWireLimits(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	long := string(bytes.Repeat([]byte("é"), 5000))
	c.captureError(long, long, nil, []CaptureOption{Route(long), Status(999), Tags(map[string]string{long: long})})
	c.Event(long, Severity("loud"), nil)
	for _, ev := range c.pending() {
		switch {
		case ev.V != 1, len(ev.ID) < 8, len(ev.ID) > maxID, ev.TS <= 0, ev.Service == "", len(ev.Route) > maxRoute,
			len(ev.Title) > maxTitle, ev.Count < 1, ev.Status != 0 && (ev.Status < 100 || ev.Status > 599):
			t.Fatalf("event violates wire limits: %+v", ev)
		}
		if ev.Error != nil && (len(ev.Error.Type) > maxErrType || len(ev.Error.Message) > maxErrMessage) {
			t.Fatal("error fields too long")
		}
		for k, v := range ev.Tags {
			if len(k) > maxTagKey || len(v) > maxTagValue {
				t.Fatal("tag too long")
			}
		}
		if ev.Type == typeCustom && ev.Severity != Tell {
			t.Fatalf("invalid severity must become tell, got %q", ev.Severity)
		}
	}
}

func TestInitSendsHeartbeatDeployAndFlushesOnClose(t *testing.T) {
	rec, srv := newRecorder(t)
	t.Setenv("PAGERLESS_KEY", "pl_test_from_env")
	t.Setenv("PAGERLESS_ENDPOINT", srv.URL)
	if err := Init("", WithService("shop"), WithRelease("v1")); err != nil {
		t.Fatal(err)
	}
	Capture(errors.New("first"))
	Capture(errors.New("first")) // repeat: windowed, flushed by Close
	Event("Payments recovered", Log, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Close(ctx); err != nil {
		t.Fatal(err)
	}
	types := map[string]int{}
	for _, e := range rec.events() {
		types[e.Type] += e.Count
		if e.V != 1 || e.Service != "shop" || e.Env != "production" || e.Release != "v1" {
			t.Fatalf("bad envelope: %+v", e)
		}
	}
	if types[typeHeartbeat] != 1 || types[typeDeploy] != 1 || types[typeError] != 2 || types[typeCustom] != 1 {
		t.Fatalf("got %v", types)
	}
	if rec.auth[0] != "Bearer pl_test_from_env" {
		t.Fatalf("auth header %q", rec.auth[0])
	}
	Capture(errors.New("after close")) // must be a silent no-op
}

func TestCloseGivesUpWhenEndpointDown(t *testing.T) {
	_, srv := newRecorder(t, http.StatusServiceUnavailable)
	c, err := New("k", WithEndpoint(srv.URL), WithService("s"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Close(ctx); err == nil {
		t.Fatal("expected undelivered error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Close must respect its context")
	}
}

func TestDeployIDIsStable(t *testing.T) {
	a := stableID("dep_", "shop", "production", "v1")
	if a != stableID("dep_", "shop", "production", "v1") || a == stableID("dep_", "shop", "production", "v2") {
		t.Fatal("deploy id must be deterministic per release")
	}
}

func TestNoKeyIsANoOp(t *testing.T) {
	t.Setenv("PAGERLESS_KEY", "")
	c, err := New("")
	if !errors.Is(err, ErrNoKey) {
		t.Fatalf("err = %v", err)
	}
	c.Capture(errors.New("x"))
	c.Event("t", Wake, nil)
	if len(c.pending()) != 0 {
		t.Fatal("disabled client must not queue")
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNilClientNeverPanics(t *testing.T) {
	var c *Client
	c.Capture(errors.New("x"))
	c.Event("t", Wake, nil)
	c.Deploy("v1")
	_ = c.Close(context.Background())
	std.Store(nil)
	Capture(errors.New("x"))
	Event("t", Wake, nil)
	Deploy("v")
	_ = Close(context.Background())
}

func TestInternalPanicIsContained(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	c.cfg.newID = func() string { panic("bug in sdk") }
	c.Capture(errors.New("x")) // must not panic
}
