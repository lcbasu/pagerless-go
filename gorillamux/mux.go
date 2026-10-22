// Package pagerlessmux names gorilla/mux routes for Pagerless: status, latency and panics per
// route template ("GET /orders/{id}") instead of per raw path.
//
//	r := mux.NewRouter()
//	r.Use(pagerlessmux.Middleware)
//
// mux runs middleware only for requests that matched a route; unmatched 404s are not counted.
// A panic is reported and re-panicked, so your own recovery still answers as before.
package pagerlessmux

import (
	"net/http"

	"github.com/gorilla/mux"
	pagerless "github.com/lcbasu/pagerless-go"
)

// Middleware reports to the default client (looked up per request, so it can be installed
// before pagerless.Init).
func Middleware(next http.Handler) http.Handler {
	return pagerless.MiddlewareRoutes(next, Route)
}

// WithClient returns the middleware for a client other than the default one.
func WithClient(c *pagerless.Client) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler { return c.MiddlewareRoutes(next, Route) }
}

// Route is the path template of the route mux matched ("/orders/{id}"), "" when none did.
func Route(r *http.Request) string {
	rt := mux.CurrentRoute(r)
	if rt == nil {
		return ""
	}
	t, err := rt.GetPathTemplate()
	if err != nil {
		return ""
	}
	return t
}
