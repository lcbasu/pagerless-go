package pagerless

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeRecovery mimics Gin's CustomRecovery: the framework recovers the panic itself, calls the
// app's hook and writes a 500, so nothing reaches net/http middleware.
func fakeRecovery(hook func(r *http.Request, v any), h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				hook(r, v)
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		h(w, r)
	})
}

func refundHandler(w http.ResponseWriter, r *http.Request) {
	var m map[string]int
	m["boom"]++ // nil map write: a real runtime panic
}

func TestRecoveredReportsAFrameworkRecoveredPanic(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	h := fakeRecovery(func(r *http.Request, v any) { c.Recovered(v, r, "/refunds/:id") }, refundHandler)

	rr := httptest.NewRecorder()
	// Middleware outside the framework still sees the 500, but never the panic.
	c.Middleware(h).ServeHTTP(rr, httptest.NewRequest("POST", "/refunds/42", nil))
	if rr.Code != 500 {
		t.Fatalf("framework writes the 500, got %d", rr.Code)
	}
	var errs []wireEvent
	for _, e := range c.pending() {
		if e.Type == typeError {
			errs = append(errs, e)
		}
	}
	if len(errs) != 1 {
		t.Fatalf("want 1 error event, got %d", len(errs))
	}
	ev := errs[0]
	if ev.Route != "POST /refunds/:id" || ev.Status != 500 {
		t.Fatalf("bad route/status: %+v", ev)
	}
	if !strings.HasPrefix(ev.Error.Type, "panic: ") || !strings.Contains(ev.Error.Message, "nil map") {
		t.Fatalf("bad error: %+v", ev.Error)
	}
	if !strings.HasPrefix(ev.Error.Stack, "github.com/lcbasu/pagerless-go.refundHandler()") {
		t.Fatalf("stack must start at the panic site, not the recovery hook:\n%s", ev.Error.Stack)
	}
}

func TestRecoveredGroupsByPanicSiteNotHook(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	hookA := fakeRecovery(func(r *http.Request, v any) { c.Recovered(v, r, "/r") }, refundHandler)
	hookB := fakeRecovery(func(r *http.Request, v any) { func() { c.Recovered(v, r, "/r") }() }, refundHandler)
	serve(hookA, "POST", "/r")
	serve(hookB, "POST", "/r")
	q := c.pending()
	if len(q) != 1 || q[0].Count != 1 {
		// The second occurrence is counted in the window, not queued again.
		t.Fatalf("same panic through different hooks must group: %+v", q)
	}
}

func TestRecoveredRouteFallbacks(t *testing.T) {
	r := httptest.NewRequest("GET", "/orders/123", nil)
	if got := recoveredRoute(r, ""); got != "GET /orders/:id" {
		t.Fatalf("empty route falls back to the normalised path, got %q", got)
	}
	if got := recoveredRoute(r, "GET /orders/{id}"); got != "GET /orders/{id}" {
		t.Fatalf("a route with a method is kept, got %q", got)
	}
	if got := recoveredRoute(nil, "/x"); got != "/x" {
		t.Fatalf("nil request keeps the route, got %q", got)
	}
}

func TestRecoveredIgnoresAbortAndNil(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	r := httptest.NewRequest("GET", "/stream", nil)
	c.Recovered(http.ErrAbortHandler, r, "")
	c.Recovered(nil, r, "")
	var nilClient *Client
	nilClient.Recovered(errors.New("x"), r, "") // off: no-op, no panic
	if n := len(c.pending()); n != 0 {
		t.Fatalf("queued %d events", n)
	}
}

func TestFromPanicSiteWithoutPanicKeepsStack(t *testing.T) {
	in := []frame{{Function: "shop.Hook"}, {Function: "shop.Main"}}
	if got := fromPanicSite(in); len(got) != 2 {
		t.Fatalf("no gopanic frame: keep the stack, got %+v", got)
	}
}

func TestFingerprintSkipsGinFrames(t *testing.T) {
	user := []frame{{Function: "shop.Handler", File: "h.go"}}
	withGin := []frame{{Function: "shop.Handler", File: "h.go"}, {Function: "github.com/gin-gonic/gin.(*Context).Next", File: "context.go"}}
	if fingerprint(typeError, "t", "r", user) != fingerprint(typeError, "t", "r", withGin) {
		t.Fatal("gin frames must not affect grouping")
	}
}
