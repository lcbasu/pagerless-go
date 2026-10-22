// Package pagerlesschi names chi routes for Pagerless: status, latency and panics per route
// template ("GET /orders/{id}") instead of per raw path.
//
//	r := chi.NewRouter()
//	r.Use(middleware.Recoverer)    // or your own recovery
//	r.Use(pagerlesschi.Middleware) // after recovery, before routes
//
// It sits inside your recovery: a panic is reported and re-panicked, so your recovery still
// answers as before.
package pagerlesschi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	pagerless "github.com/lcbasu/pagerless-go"
)

// Middleware reports to the default client (looked up per request, so it can be installed
// before pagerless.Init).
func Middleware(next http.Handler) http.Handler {
	return pagerless.MiddlewareRoutes(next, Route)
}

// WithClient returns the middleware for a client other than the default one.
func WithClient(c *pagerless.Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler { return c.MiddlewareRoutes(next, Route) }
}

// Route is the route template chi matched ("/orders/{id}"), "" when none did.
func Route(r *http.Request) string {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return ""
	}
	return rctx.RoutePattern()
}
