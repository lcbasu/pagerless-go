package pagerless

import (
	"bytes"
	"context"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
)

func TestSlogHandlerCapturesErrorRecords(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	var buf bytes.Buffer
	log := slog.New(c.SlogHandler(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	log.Info("hello")                                                                             // below Error: not captured, and below Warn: not written
	log.Warn("disk at 80%")                                                                       // written, not captured
	log.Error("sync failed", "err", fs.ErrPermission)                                             // captured with the error's type
	log.With("shop", "s1").Error("payout stuck")                                                  // no error attr: a log.Error problem
	log.With("err", &fs.PathError{Op: "open", Path: "/data", Err: fs.ErrNotExist}).Error("retry") // error from With
	log.Error("pagerless: internal error, event dropped")

	errs := errorsIn(c.pending())
	if len(errs) != 3 {
		t.Fatalf("want 3 captured, got %d: %+v", len(errs), errs)
	}
	if errs[0].Error.Type != "*errors.errorString" || errs[0].Error.Message != "sync failed: permission denied" {
		t.Fatalf("first: %+v", errs[0].Error)
	}
	if errs[1].Error.Type != "log.Error" || errs[1].Error.Message != "payout stuck" {
		t.Fatalf("second: %+v", errs[1].Error)
	}
	if errs[2].Error.Message != "retry: open /data: file does not exist" || errs[2].Error.Type != "*fs.PathError" {
		t.Fatalf("third: %+v", errs[2].Error)
	}
	if errs[0].Tags["source"] != "slog" {
		t.Fatalf("tags: %v", errs[0].Tags)
	}
	if !strings.HasPrefix(errs[0].Error.Stack, "github.com/lcbasu/pagerless-go.TestSlogHandlerCapturesErrorRecords") {
		t.Fatalf("stack starts at the log call:\n%s", errs[0].Error.Stack)
	}
	out := buf.String()
	if strings.Contains(out, "hello") || !strings.Contains(out, "disk at 80%") || !strings.Contains(out, "sync failed") || !strings.Contains(out, "shop=s1") {
		t.Fatalf("records still reach the wrapped handler as before:\n%s", out)
	}
}

func TestSlogHandlerCapturesEvenWhenTheWrappedHandlerIsQuiet(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	var buf bytes.Buffer
	h := c.SlogHandler(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.Level(100)}))
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("Error must stay enabled so it is captured")
	}
	slog.New(h).WithGroup("job").Error("nightly export failed")
	if len(errorsIn(c.pending())) != 1 || buf.Len() != 0 {
		t.Fatalf("captured but not written: %q", buf.String())
	}
}
