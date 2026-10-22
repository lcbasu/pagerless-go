package pagerless

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func serve(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, target, nil))
	return rr
}

func metricsByRoute(evs []wireEvent) map[string]wireEvent {
	out := map[string]wireEvent{}
	for _, e := range evs {
		if e.Type == typeMetric {
			out[e.Route+" "+http.StatusText(e.Status)] = e
		}
	}
	return out
}

func TestMiddlewareCapturesStatusPerPattern(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "0" {
			http.Error(w, "nope", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("ok")) // implicit 200
	})
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) })
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) })
	mux.HandleFunc("GET /silent", func(w http.ResponseWriter, r *http.Request) {})
	h := c.Middleware(mux)

	serve(h, "GET", "/items/1")
	serve(h, "GET", "/items/2")
	serve(h, "GET", "/items/0")
	serve(h, "POST", "/orders")
	serve(h, "GET", "/boom")
	serve(h, "GET", "/silent")
	serve(h, "GET", "/wp-admin/login.php")

	clk.add(10 * time.Second)
	c.flushWindow()
	m := metricsByRoute(c.pending())
	checks := []struct {
		key   string
		count int
	}{
		{"GET /items/{id} OK", 2},
		{"GET /items/{id} Not Found", 1},
		{"POST /orders Created", 1},
		{"GET /boom Bad Gateway", 1},
		{"GET /silent OK", 1},
		{"GET (unmatched) Not Found", 1},
	}
	for _, ck := range checks {
		ev, ok := m[ck.key]
		if !ok {
			t.Errorf("missing metric %q (have %v)", ck.key, keys(m))
			continue
		}
		if ev.Count != ck.count || ev.WindowS != 10 || ev.Metric == nil || ev.Metric.Unit != "ms" || ev.Metric.Name != "http.server.duration" {
			t.Errorf("%s: bad event %+v", ck.key, ev)
		}
	}
	if len(m) != len(checks) {
		t.Errorf("unexpected metrics: %v", keys(m))
	}
}

func keys(m map[string]wireEvent) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestMiddlewareNormalizesPathWithoutPattern(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	h := c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	serve(h, "GET", "/users/123/orders/9f8c2d4e-1b2a-4c3d-8e9f-0a1b2c3d4e5f")
	serve(h, "GET", "/users/456/orders/0a1b2c3d-1b2a-4c3d-8e9f-0a1b2c3d4e5f")
	clk.add(10 * time.Second)
	c.flushWindow()
	ev, ok := metricsByRoute(c.pending())["GET /users/:id/orders/:id OK"]
	if !ok || ev.Count != 2 {
		t.Fatalf("paths with IDs must collapse: %v", keys(metricsByRoute(c.pending())))
	}
}

func payHandler(w http.ResponseWriter, r *http.Request) {
	var m map[string]int
	m["boom"]++ // nil map write: a real runtime panic
}

func TestMiddlewarePanicReportsAndRepanics(t *testing.T) {
	c, clk := testClient(t, "http://unused")
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pay/{id}", payHandler)
	h := c.Middleware(mux)

	var got any
	func() {
		defer func() { got = recover() }()
		serve(h, "POST", "/pay/7")
	}()
	if got == nil {
		t.Fatal("middleware must re-panic")
	}
	q := c.pending()
	if len(q) != 1 {
		t.Fatalf("want 1 error event, got %d", len(q))
	}
	ev := q[0]
	if ev.Type != typeError || ev.Route != "POST /pay/{id}" || ev.Status != 500 {
		t.Fatalf("bad panic event: %+v", ev)
	}
	if !strings.HasPrefix(ev.Error.Type, "panic: ") || !strings.Contains(ev.Error.Message, "nil map") {
		t.Fatalf("bad error: %+v", ev.Error)
	}
	if !strings.HasPrefix(ev.Error.Stack, "github.com/lcbasu/pagerless-go.payHandler()") {
		t.Fatalf("stack must start at the panic site:\n%s", ev.Error.Stack)
	}
	clk.add(10 * time.Second)
	c.flushWindow()
	if _, ok := metricsByRoute(c.pending())["POST /pay/{id} Internal Server Error"]; !ok {
		t.Fatal("panicked request must count as a 500")
	}
}

func TestMiddlewareIgnoresErrAbortHandler(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	h := c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) }))
	var got any
	func() {
		defer func() { got = recover() }()
		serve(h, "GET", "/stream")
	}()
	if err, ok := got.(error); !ok || !errors.Is(err, http.ErrAbortHandler) {
		t.Fatalf("must re-panic ErrAbortHandler, got %v", got)
	}
	if n := len(c.pending()); n != 0 {
		t.Fatalf("ErrAbortHandler is not a bug, queued %d", n)
	}
}

func TestMiddlewarePassesFlushThrough(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	h := c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
		}
		w.(http.Flusher).Flush()
	}))
	if rr := serve(h, "GET", "/sse"); !rr.Flushed {
		t.Fatal("flush not forwarded")
	}
}

func TestMiddlewareDisabledIsTransparent(t *testing.T) {
	std.Store(nil)
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	if rr := serve(h, "GET", "/"); rr.Code != http.StatusTeapot {
		t.Fatalf("code %d", rr.Code)
	}
}
