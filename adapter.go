package pagerless

import (
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Building blocks for framework adapters (github.com/lcbasu/pagerless-go/gin, .../grpc,
// .../temporal…) and for apps wiring a framework the SDK doesn't cover yet.

// Default returns the client Init started (nil before Init; every method is a no-op on nil).
// Adapters call it per request, so they can be installed before Init.
func Default() *Client { return std.Load() }

// RecordRequest feeds one finished request into the current window: route is the matched
// template ("GET /orders/:id"), status the response code. 5xx count as failures.
func (c *Client) RecordRequest(route string, status int, latency time.Duration) {
	c.recordRequest(route, status, latency)
}

// RecordRequest records a request on the default client.
func RecordRequest(route string, status int, latency time.Duration) {
	std.Load().recordRequest(route, status, latency)
}

// CapturePanic reports a value recovered from a panic. Call it inside the deferred function
// that recovered it, so the stack starts at the panic site. http.ErrAbortHandler (net/http's
// deliberate abort) is ignored. It only reports: re-panic or write the error yourself.
func (c *Client) CapturePanic(v any, opts ...CaptureOption) {
	if !c.active() || v == nil {
		return
	}
	if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		return
	}
	defer c.guard()
	typ, msg := panicInfo(v)
	c.captureError(typ, msg, fromPanicSite(callers(1)), opts)
}

// CapturePanic reports a recovered panic on the default client.
func CapturePanic(v any, opts ...CaptureOption) {
	c := std.Load()
	if !c.active() || v == nil {
		return
	}
	if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		return
	}
	defer c.guard()
	typ, msg := panicInfo(v)
	c.captureError(typ, msg, fromPanicSite(callers(1)), opts)
}

var slugUnsafe = regexp.MustCompile(`[^a-z0-9._-]+`)
var slugEdges = regexp.MustCompile(`^[^a-z0-9]+|-+$`)

// CheckInSlug turns a job or schedule name into a check-in name the way `npx pagerless connect`
// does ("Nightly Report" → "nightly-report"), so code and connect agree on the name.
func CheckInSlug(name string) string {
	s := slugUnsafe.ReplaceAllString(strings.ToLower(name), "-")
	s = slugEdges.ReplaceAllString(s, "")
	if len(s) > 64 {
		s = s[:64]
	}
	if s == "" {
		return "job"
	}
	return s
}
