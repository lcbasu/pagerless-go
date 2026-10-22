package pagerless

import (
	"context"
	"log/slog"
	"strings"
)

// SlogHandler wraps next so every record at Error level or above is also captured: the record's
// error attribute (any attr holding an error) gives the type, the message says what failed. All
// records still go to next unchanged. Uses the default client at log time.
//
//	slog.SetDefault(slog.New(pagerless.SlogHandler(slog.NewJSONHandler(os.Stderr, nil))))
//
// Don't wrap slog.Default().Handler() while it is still Go's built-in default handler: setting
// that back as the default makes the log package loop into itself. Wrap the handler you build.
func SlogHandler(next slog.Handler) slog.Handler {
	return &slogHandler{next: next, client: Default, level: slog.LevelError}
}

// SlogHandler is the per-client version of the package-level SlogHandler.
func (c *Client) SlogHandler(next slog.Handler) slog.Handler {
	return &slogHandler{next: next, client: func() *Client { return c }, level: slog.LevelError}
}

type slogHandler struct {
	next   slog.Handler
	client func() *Client
	level  slog.Level
	// attrs added with WithAttrs, searched for an error too (logger.With("err", err)).
	attrs []slog.Attr
}

func (h *slogHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level || h.next.Enabled(ctx, l)
}

func (h *slogHandler) Handle(ctx context.Context, r slog.Record) error {
	// The SDK's own diagnostics never loop back into it.
	if r.Level >= h.level && !strings.HasPrefix(r.Message, "pagerless:") {
		h.capture(r)
	}
	if !h.next.Enabled(ctx, r.Level) {
		return nil
	}
	return h.next.Handle(ctx, r)
}

func (h *slogHandler) capture(r slog.Record) {
	c := h.client()
	if !c.active() {
		return
	}
	defer c.guard()
	var found error
	find := func(a slog.Attr) bool {
		if err, ok := a.Value.Resolve().Any().(error); ok && err != nil {
			found = err
			return false
		}
		return true
	}
	r.Attrs(find)
	for _, a := range h.attrs {
		if found != nil {
			break
		}
		find(a)
	}
	typ, msg := "log.Error", r.Message
	if found != nil {
		typ = errType(found)
		if msg == "" {
			msg = found.Error()
		} else {
			msg += ": " + found.Error()
		}
	}
	frames := callers(2)
	for len(frames) > 0 && strings.HasPrefix(frames[0].Function, "log/slog.") {
		frames = frames[1:] // start at the app's log call, not inside slog
	}
	c.captureError(typ, msg, frames, []CaptureOption{Tags(map[string]string{"source": "slog"})})
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h
	n.next = h.next.WithAttrs(attrs)
	n.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &n
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	n := *h
	n.next = h.next.WithGroup(name)
	return &n
}
