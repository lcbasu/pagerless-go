package pagerless

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func errorsIn(evs []wireEvent) []wireEvent {
	var out []wireEvent
	for _, e := range evs {
		if e.Type == typeError {
			out = append(out, e)
		}
	}
	return out
}

func TestMiddlewareRoutesNamesTheRouteFromTheRouter(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	h := c.MiddlewareRoutes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/7") {
			panic("order 7 is cursed")
		}
		w.WriteHeader(http.StatusAccepted)
	}), func(r *http.Request) string {
		if strings.HasPrefix(r.URL.Path, "/orders/") {
			return "/orders/{id}"
		}
		return ""
	})
	serve(h, "POST", "/orders/1")
	serve(h, "POST", "/orders/2")
	func() {
		defer func() { _ = recover() }()
		serve(h, "POST", "/orders/7")
	}()
	serve(h, "GET", "/files/123")
	clk.add(10 * time.Second)
	c.flushWindow()
	m := metricsByRoute(c.pending())
	if m["POST /orders/{id} Accepted"].Count != 2 {
		t.Fatalf("routes from the router: %v", m)
	}
	if m["POST /orders/{id} Internal Server Error"].Count != 1 {
		t.Fatalf("the panic counts as a 500 on its route: %v", m)
	}
	if _, ok := m["GET /files/:id Accepted"]; !ok {
		t.Fatalf("no router route falls back to the normalised path: %v", m)
	}
	errs := errorsIn(c.pending())
	if len(errs) != 1 || errs[0].Route != "POST /orders/{id}" {
		t.Fatalf("panic reported on the router's route: %+v", errs)
	}
}

func cursed() { panic(errors.New("cursed order")) }

func TestCapturePanicStartsAtThePanicSite(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	func() {
		defer func() {
			if v := recover(); v != nil {
				c.CapturePanic(v, Route("grpc /shop.Orders/Get"))
			}
		}()
		cursed()
	}()
	errs := errorsIn(c.pending())
	if len(errs) != 1 {
		t.Fatalf("want 1 error, got %d", len(errs))
	}
	e := errs[0]
	if e.Error.Type != "panic: *errors.errorString" || e.Route != "grpc /shop.Orders/Get" {
		t.Fatalf("bad event: %+v %+v", e, e.Error)
	}
	if !strings.HasPrefix(e.Error.Stack, "github.com/lcbasu/pagerless-go.cursed()") {
		t.Fatalf("stack must start at the panic:\n%s", e.Error.Stack)
	}
	c.CapturePanic(http.ErrAbortHandler)
	c.CapturePanic(nil)
	if len(errorsIn(c.pending())) != 1 {
		t.Fatal("ErrAbortHandler and nil are not problems")
	}
}

func TestRecordRequestFeedsTheWindow(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	c.RecordRequest("GET /a", 503, 20*time.Millisecond)
	c.RecordRequest("GET /a", 503, 40*time.Millisecond)
	clk.add(10 * time.Second)
	c.flushWindow()
	got := metricsByRoute(c.pending())["GET /a Service Unavailable"]
	if got.Count != 2 || got.Metric.Value != 30 {
		t.Fatalf("got %+v", got)
	}
	var nilClient *Client
	nilClient.RecordRequest("GET /a", 500, time.Millisecond) // no panic on nil
}

func TestErrorTypeOverridesTheGroupingType(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	c.Capture(errors.New("card declined"), ErrorType("PaymentDeclined"))
	c.Capture(errors.New("card declined again"), ErrorType("PaymentDeclined"))
	c.Capture(errors.New("card declined"))
	errs := errorsIn(c.pending())
	if len(errs) != 2 || errs[0].Error.Type != "PaymentDeclined" || errs[1].Error.Type != "*errors.errorString" {
		t.Fatalf("named type groups apart from the plain one: %+v", errs)
	}
}

func TestTagsAddUp(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	c.Capture(errors.New("x"), Tags(map[string]string{"a": "1", "phase": "run"}), Tags(map[string]string{"b": "2", "a": "3"}))
	e := errorsIn(c.pending())[0]
	if e.Tags["a"] != "3" || e.Tags["b"] != "2" || e.Tags["phase"] != "run" {
		t.Fatalf("tags: %v", e.Tags)
	}
}

func TestCheckInSlugMatchesConnect(t *testing.T) {
	// Mirrors connect's jobSlug: code and connect must name a job the same way.
	cases := map[string]string{
		"acme-edge (cron)":      "acme-edge-cron",
		"Nightly Report!":       "nightly-report",
		"reconcile-payouts":     "reconcile-payouts",
		"__weird__":             "weird__",
		"!!!":                   "job",
		strings.Repeat("a", 70): strings.Repeat("a", 64),
	}
	for in, want := range cases {
		if got := CheckInSlug(in); got != want {
			t.Errorf("CheckInSlug(%q) = %q, want %q", in, got, want)
		}
	}
}
