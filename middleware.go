package pagerless

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Middleware wraps h to record status and latency per route and to report panics. Wrap the
// outermost handler (usually your *http.ServeMux) so the matched Go 1.22+ pattern, e.g.
// "GET /orders/{id}", is the route. A panic is reported and then re-panicked, so net/http (or
// your own recovery) still handles it as before. Uses the default client at request time, so it
// can be installed before Init.
func Middleware(h http.Handler) http.Handler {
	return middleware(Default, h, nil)
}

// Middleware is the per-client version of the package-level Middleware.
func (c *Client) Middleware(h http.Handler) http.Handler {
	return middleware(func() *Client { return c }, h, nil)
}

// RouteFunc names the route template a request matched ("/orders/{id}"), read after the handler
// ran; "" when there is none. Routers that don't set r.Pattern (chi, gorilla/mux) supply one.
type RouteFunc func(r *http.Request) string

// MiddlewareRoutes is Middleware for routers that keep the matched route themselves: route names
// it. The adapters (github.com/lcbasu/pagerless-go/chi, .../gorillamux) are built on it.
func MiddlewareRoutes(h http.Handler, route RouteFunc) http.Handler {
	return middleware(Default, h, route)
}

// MiddlewareRoutes is the per-client version of the package-level MiddlewareRoutes.
func (c *Client) MiddlewareRoutes(h http.Handler, route RouteFunc) http.Handler {
	return middleware(func() *Client { return c }, h, route)
}

func middleware(client func() *Client, h http.Handler, name RouteFunc) http.Handler {
	routeOf := func(r *http.Request, status int) string {
		if name != nil {
			if n := name(r); n != "" {
				return methodRoute(r.Method, n)
			}
		}
		return routeOf(r, status)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := client()
		if !c.active() {
			h.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w}
		defer func() {
			latency := time.Since(start)
			if v := recover(); v != nil {
				// ErrAbortHandler is net/http's deliberate "abort quietly" signal, not a bug.
				if err, ok := v.(error); !ok || !errors.Is(err, http.ErrAbortHandler) {
					route := routeOf(r, http.StatusInternalServerError)
					c.reportPanic(v, route)
					c.recordRequest(route, http.StatusInternalServerError, latency)
				}
				panic(v)
			}
			c.recordRequest(routeOf(r, rw.code()), rw.code(), latency)
		}()
		h.ServeHTTP(rw, r)
	})
}

func (c *Client) reportPanic(v any, route string) {
	defer c.guard()
	typ, msg := panicInfo(v)
	c.captureError(typ, msg, callers(1), []CaptureOption{Route(route), Status(http.StatusInternalServerError)})
}

// methodRoute prefixes a template with the method unless it already has one ("GET /x").
func methodRoute(method, route string) string {
	if strings.ContainsAny(route, " \t") || method == "" {
		return route
	}
	return method + " " + route
}

// routeOf names the request's route. ServeMux sets r.Pattern on the same *Request we hold, so it
// is available after the handler ran. Without a pattern the path is normalised (IDs collapsed)
// to keep cardinality bounded; unmatched 404s collapse to one route.
func routeOf(r *http.Request, status int) string {
	if p := r.Pattern; p != "" {
		return methodRoute(r.Method, p)
	}
	return RouteFor(r.Method, r.URL.Path, status)
}

// RouteFor names a request no router template matched: the method and the path with ID-like
// segments collapsed ("GET /files/:id"), or "GET (unmatched)" for a 404, so routes stay few.
func RouteFor(method, path string, status int) string {
	if status == http.StatusNotFound {
		return method + " (unmatched)"
	}
	return method + " " + normalizePath(path)
}

var (
	numericSeg = regexp.MustCompile(`^\d+$`)
	uuidSeg    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	hexSeg     = regexp.MustCompile(`^[0-9a-fA-F]{16,}$`)
	digitRe    = regexp.MustCompile(`\d`)
)

// normalizePath replaces ID-like path segments with ":id".
func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	segs := strings.Split(p, "/")
	for i, s := range segs {
		switch {
		case s == "":
		case numericSeg.MatchString(s), uuidSeg.MatchString(s), hexSeg.MatchString(s):
			segs[i] = ":id"
		case len(s) >= 20 && digitRe.MatchString(s):
			segs[i] = ":id" // long mixed tokens: slugs with IDs, base62 keys
		}
	}
	return strings.Join(segs, "/")
}

// statusWriter records the status code. It forwards Flush and Hijack and supports
// http.ResponseController via Unwrap.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader && code >= 200 {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) code() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *statusWriter) Flush() {
	if !w.wroteHeader {
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if !w.wroteHeader {
		w.status = http.StatusSwitchingProtocols
		w.wroteHeader = true
	}
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
