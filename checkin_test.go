package pagerless

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type checkInCall struct {
	path string
	auth string
	body map[string]any
}

// checkInServer is a fake ingest that records check-ins and answers with status.
func checkInServer(t *testing.T, status int) (*httptest.Server, func() []checkInCall) {
	t.Helper()
	var mu sync.Mutex
	var calls []checkInCall
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("body is not JSON: %s", raw)
		}
		mu.Lock()
		calls = append(calls, checkInCall{path: r.URL.Path, auth: r.Header.Get("Authorization"), body: body})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []checkInCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]checkInCall(nil), calls...)
	}
}

func TestCheckInURL(t *testing.T) {
	cases := map[string]string{
		"https://events.pagerless.dev/v1/events":  "https://events.pagerless.dev/v1/checkins/nightly",
		"http://localhost:8787/ingest/v1/events":  "http://localhost:8787/ingest/v1/checkins/nightly",
		"http://localhost:8787/ingest/v1/events/": "http://localhost:8787/ingest/v1/checkins/nightly",
	}
	for in, want := range cases {
		if got := checkInURL(in, "nightly"); got != want {
			t.Errorf("checkInURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCheckInSendsOneRunWithSchedule(t *testing.T) {
	srv, calls := checkInServer(t, http.StatusAccepted)
	c, _ := testClient(t, srv.URL+"/v1/events")
	err := c.CheckIn(context.Background(), "Nightly-Backup",
		WithSchedule(Schedule{Cron: "30 3 * * *", TZ: "Europe/London", Grace: 15 * time.Minute, Level: Wake}),
		Took(1234*time.Millisecond), RunID("run-000001"))
	if err != nil {
		t.Fatal(err)
	}
	got := calls()
	if len(got) != 1 {
		t.Fatalf("calls = %d, want 1", len(got))
	}
	if got[0].path != "/v1/checkins/nightly-backup" || got[0].auth != "Bearer pl_test_key" {
		t.Fatalf("path %q auth %q", got[0].path, got[0].auth)
	}
	b := got[0].body
	if b["status"] != "ok" || b["durationMs"] != float64(1234) || b["id"] != "run-000001" || b["env"] != "production" {
		t.Fatalf("body = %v", b)
	}
	s, _ := b["schedule"].(map[string]any)
	if s["cron"] != "30 3 * * *" || s["tz"] != "Europe/London" || s["graceSeconds"] != float64(900) || s["level"] != "wake" {
		t.Fatalf("schedule = %v", s)
	}
	if _, ok := s["expectEverySeconds"]; ok {
		t.Fatalf("schedule has both cron and interval: %v", s)
	}
}

func TestCheckInIntervalDefaultsToTell(t *testing.T) {
	srv, calls := checkInServer(t, http.StatusAccepted)
	c, _ := testClient(t, srv.URL+"/v1/events")
	if err := c.CheckIn(context.Background(), "sync", WithSchedule(Schedule{Every: 6 * time.Hour, Level: Log})); err != nil {
		t.Fatal(err)
	}
	s, _ := calls()[0].body["schedule"].(map[string]any)
	if s["expectEverySeconds"] != float64(21600) || s["level"] != "tell" {
		t.Fatalf("schedule = %v", s)
	}
}

func TestCheckInErrors(t *testing.T) {
	srv, calls := checkInServer(t, http.StatusUnauthorized)
	c, _ := testClient(t, srv.URL+"/v1/events")
	if err := c.CheckIn(context.Background(), "-bad"); !errors.Is(err, ErrBadSlug) {
		t.Fatalf("bad slug err = %v", err)
	}
	if err := c.CheckIn(context.Background(), "sync"); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("refused err = %v", err)
	}
	if len(calls()) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls()))
	}
	var off *Client
	if err := off.CheckIn(context.Background(), "sync"); !errors.Is(err, ErrNoKey) {
		t.Fatalf("nil client err = %v", err)
	}
}

func TestMonitorReportsOkWithRunTime(t *testing.T) {
	srv, calls := checkInServer(t, http.StatusAccepted)
	c, clk := testClient(t, srv.URL+"/v1/events")
	err := c.Monitor(context.Background(), "report", Schedule{Every: 24 * time.Hour}, func(context.Context) error {
		clk.add(2500 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b := calls()[0].body
	if b["status"] != "ok" || b["durationMs"] != float64(2500) {
		t.Fatalf("body = %v", b)
	}
}

func TestMonitorReportsFailureAndReturnsIt(t *testing.T) {
	srv, calls := checkInServer(t, http.StatusAccepted)
	c, _ := testClient(t, srv.URL+"/v1/events")
	boom := errors.New("connect to postgres://admin:hunter2@db/prod failed")
	ctx, cancel := context.WithCancel(context.Background())
	err := c.Monitor(ctx, "report", Schedule{Cron: "@daily"}, func(context.Context) error {
		cancel() // the job's context ending must not stop the report
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the job's error", err)
	}
	b := calls()[0].body
	msg, _ := b["message"].(string)
	if b["status"] != "failed" || msg == "" || strings.Contains(msg, "hunter2") {
		t.Fatalf("body = %v", b)
	}
}

func TestMonitorReportsPanicAndRepanics(t *testing.T) {
	srv, calls := checkInServer(t, http.StatusAccepted)
	c, _ := testClient(t, srv.URL+"/v1/events")
	defer func() {
		if r := recover(); r != "boom" {
			t.Fatalf("recovered %v, want the job's panic", r)
		}
		if b := calls()[0].body; b["status"] != "failed" || b["message"] != "panic: boom" {
			t.Fatalf("body = %v", b)
		}
	}()
	_ = c.Monitor(context.Background(), "report", Schedule{Cron: "@hourly"}, func(context.Context) error { panic("boom") })
}

func TestMonitorWithoutKeyStillRunsTheJob(t *testing.T) {
	var off *Client
	ran := false
	err := off.Monitor(context.Background(), "x", Schedule{Cron: "@hourly"}, func(context.Context) error { ran = true; return nil })
	if err != nil || !ran {
		t.Fatalf("err = %v ran = %v", err, ran)
	}
}
