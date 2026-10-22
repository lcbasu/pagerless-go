// Package pagerlessgin reports Gin panics, 5xx errors and per-route health to Pagerless.
//
//	r := gin.New()
//	r.Use(gin.Recovery())             // or your own recovery
//	r.Use(pagerlessgin.Middleware())  // after recovery, before routes
//	r.GET("/orders/:id", getOrder)
//
// The middleware sits inside your recovery: it reports a panic and re-panics, so your recovery
// still writes the response as before. Routes are named by their template ("GET /orders/:id").
// Add it before registering routes: Gin only applies middleware to routes added after Use.
package pagerlessgin

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	pagerless "github.com/lcbasu/pagerless-go"
)

// Option configures Middleware.
type Option func(*config)

type config struct{ client func() *pagerless.Client }

// WithClient reports to c instead of the default client.
func WithClient(c *pagerless.Client) Option {
	return func(cfg *config) { cfg.client = func() *pagerless.Client { return c } }
}

// Middleware records status and latency per route template and reports panics and errors
// attached with c.Error on 5xx responses. It uses the default client at request time, so it can
// be installed before pagerless.Init.
func Middleware(opts ...Option) gin.HandlerFunc {
	cfg := config{client: pagerless.Default}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return func(c *gin.Context) {
		pc := cfg.client()
		start := time.Now()
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			// ErrAbortHandler is net/http's deliberate "abort quietly" signal, not a bug.
			if err, ok := v.(error); !ok || !errors.Is(err, http.ErrAbortHandler) {
				route := routeName(c, http.StatusInternalServerError)
				pc.CapturePanic(v, pagerless.Route(route), pagerless.Status(http.StatusInternalServerError))
				pc.RecordRequest(route, http.StatusInternalServerError, time.Since(start))
			}
			panic(v)
		}()
		c.Next()
		status := c.Writer.Status()
		route := routeName(c, status)
		if status >= 500 {
			if e := c.Errors.Last(); e != nil && e.Err != nil {
				pc.Capture(e.Err, pagerless.Route(route), pagerless.Status(status))
			}
		}
		pc.RecordRequest(route, status, time.Since(start))
	}
}

// routeName is the matched template ("GET /orders/:id"), or the collapsed path when none matched.
func routeName(c *gin.Context, status int) string {
	if p := c.FullPath(); p != "" {
		return c.Request.Method + " " + p
	}
	return pagerless.RouteFor(c.Request.Method, c.Request.URL.Path, status)
}
