// Package pagerlessfiberv3 reports Fiber v3 panics, 5xx errors and per-route health to Pagerless.
//
//	app := fiber.New()
//	app.Use(recover.New())                 // or your own recovery
//	app.Use(pagerlessfiberv3.Middleware()) // after recovery, before routes
//
// The middleware sits inside your recovery: it reports a panic and re-panics, so your recovery
// still answers as before. Routes are named by their template ("GET /orders/:id"). Add it
// before your routes: Fiber runs handlers in the order they were registered.
package pagerlessfiberv3

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
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
func Middleware(opts ...Option) fiber.Handler {
	cfg := config{client: pagerless.Default}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	var routes realRoutes
	return func(c fiber.Ctx) error {
		pc := cfg.client()
		start := time.Now()
		own := c.Route()
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			route := routes.name(c, own, http.StatusInternalServerError)
			pc.CapturePanic(v, pagerless.Route(route), pagerless.Status(http.StatusInternalServerError))
			pc.RecordRequest(route, http.StatusInternalServerError, time.Since(start))
			panic(v)
		}()
		err := c.Next()
		status := c.Response().StatusCode()
		if err != nil {
			// Fiber's error handler writes the response after us: work out what it will send.
			status = http.StatusInternalServerError
			var fe *fiber.Error
			if errors.As(err, &fe) {
				status = fe.Code
			}
		}
		route := routes.name(c, own, status)
		if err != nil && status >= 500 {
			pc.Capture(err, pagerless.Route(route), pagerless.Status(status))
		}
		pc.RecordRequest(route, status, time.Since(start))
		return err
	}
}

// realRoutes is the app's own routes (not app.Use middleware), read once on first use.
type realRoutes struct {
	once sync.Once
	set  map[string]bool
}

// name is the matched template ("GET /orders/:id"), or the collapsed path when no real route
// matched: the last route that ran is a middleware's (ours, or another app.Use).
func (rr *realRoutes) name(c fiber.Ctx, own *fiber.Route, status int) string {
	rr.once.Do(func() {
		rr.set = map[string]bool{}
		for _, r := range c.App().GetRoutes(true) {
			rr.set[r.Method+" "+r.Path] = true
		}
	})
	method := c.Method()
	if r := c.Route(); r != nil && r != own && rr.set[method+" "+r.Path] {
		return method + " " + r.Path
	}
	return pagerless.RouteFor(method, c.Path(), status)
}
