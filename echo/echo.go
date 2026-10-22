// Package pagerlessecho reports Echo (v4) panics, 5xx errors and per-route health to Pagerless.
//
//	e := echo.New()
//	e.Use(middleware.Recover())         // or your own recovery
//	e.Use(pagerlessecho.Middleware())   // after recovery
//
// The middleware sits inside your recovery: it reports a panic and re-panics, so your recovery
// still answers as before. Routes are named by their template ("GET /orders/:id").
package pagerlessecho

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	pagerless "github.com/lcbasu/pagerless-go"
)

// Option configures Middleware.
type Option func(*config)

type config struct{ client func() *pagerless.Client }

// WithClient reports to c instead of the default client.
func WithClient(c *pagerless.Client) Option {
	return func(cfg *config) { cfg.client = func() *pagerless.Client { return c } }
}

// Middleware records status and latency per route template and reports panics and handler
// errors that become 5xx. It uses the default client at request time, so it can be installed
// before pagerless.Init.
func Middleware(opts ...Option) echo.MiddlewareFunc {
	cfg := config{client: pagerless.Default}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			pc := cfg.client()
			start := time.Now()
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				if err, ok := v.(error); !ok || !errors.Is(err, http.ErrAbortHandler) {
					route := routeName(c, http.StatusInternalServerError)
					pc.CapturePanic(v, pagerless.Route(route), pagerless.Status(http.StatusInternalServerError))
					pc.RecordRequest(route, http.StatusInternalServerError, time.Since(start))
				}
				panic(v)
			}()
			err := next(c)
			status := c.Response().Status
			if err != nil && !c.Response().Committed {
				// Echo's error handler writes the response after us: work out what it will send.
				status = http.StatusInternalServerError
				var he *echo.HTTPError
				if errors.As(err, &he) {
					status = he.Code
				}
			}
			route := routeName(c, status)
			if err != nil && status >= 500 {
				pc.Capture(err, pagerless.Route(route), pagerless.Status(status))
			}
			pc.RecordRequest(route, status, time.Since(start))
			return err
		}
	}
}

// routeName is the matched template ("GET /orders/:id"), or the collapsed path when none matched.
func routeName(c echo.Context, status int) string {
	r := c.Request()
	if p := c.Path(); p != "" && (status != http.StatusNotFound || routeExists(c.Echo(), r.Method, p)) {
		return r.Method + " " + p
	}
	return pagerless.RouteFor(r.Method, r.URL.Path, status)
}

// routeExists tells a real route's 404 ("no such order") from Echo's not-found catch-all.
func routeExists(e *echo.Echo, method, path string) bool {
	if e == nil {
		return false
	}
	for _, rt := range e.Routes() {
		if rt.Method == method && rt.Path == path {
			return true
		}
	}
	return false
}
