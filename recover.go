package pagerless

import (
	"errors"
	"net/http"
	"strings"
)

// Recovered reports a panic that a framework's own recovery already caught. Gin's Recovery,
// Echo's Recover and Chi's Recoverer stop the panic before it reaches Middleware, so call this
// from their hook. route is the framework's matched pattern if it has one (Gin: c.FullPath(),
// e.g. "/orders/:id"); "" falls back to the request path with IDs collapsed. It only reports:
// writing the 500 stays with the framework. Call it inside the recovery hook (while the panic
// is being handled) so the stack points at the panic site.
//
//	r.Use(gin.CustomRecovery(func(c *gin.Context, v any) {
//		pagerless.Recovered(v, c.Request, c.FullPath())
//		c.AbortWithStatus(http.StatusInternalServerError)
//	}))
func Recovered(v any, r *http.Request, route string) {
	std.Load().recovered(v, r, route)
}

// Recovered is the per-client version of the package-level Recovered.
func (c *Client) Recovered(v any, r *http.Request, route string) {
	c.recovered(v, r, route)
}

func (c *Client) recovered(v any, r *http.Request, route string) {
	if !c.active() || v == nil {
		return
	}
	// ErrAbortHandler is net/http's deliberate "abort quietly" signal, not a bug.
	if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		return
	}
	defer c.guard()
	typ, msg := panicInfo(v)
	c.captureError(typ, msg, fromPanicSite(callers(2)), []CaptureOption{
		Route(recoveredRoute(r, route)), Status(http.StatusInternalServerError),
	})
}

// fromPanicSite drops the recovery plumbing above runtime.gopanic (the app's hook, the
// framework's deferred recover) so the stack and grouping start where the panic happened.
// Without a gopanic frame (called outside a recovery) the stack is kept as is.
func fromPanicSite(frames []frame) []frame {
	for i, f := range frames {
		if f.Function == "runtime.gopanic" {
			rest := frames[i+1:]
			for len(rest) > 0 && isRuntime(rest[0].Function) {
				rest = rest[1:]
			}
			if len(rest) > 0 {
				return rest
			}
		}
	}
	return frames
}

func recoveredRoute(r *http.Request, route string) string {
	if r == nil {
		return route
	}
	switch {
	case route == "":
		return routeOf(r, http.StatusInternalServerError)
	case strings.ContainsAny(route, " \t"):
		return route
	default:
		return r.Method + " " + route
	}
}
