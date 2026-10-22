package pagerless

import (
	"context"
	"os"
	"time"
)

// exit is os.Exit; tests replace it.
var (
	osExit = os.Exit
	exit   = osExit
)

// fatalFlush bounds how long Fatal waits to deliver before exiting.
const fatalFlush = 3 * time.Second

// Fatal reports err as the reason the process is exiting, delivers what's buffered (up to 3 s)
// and exits with status 1. Use it where you'd call log.Fatal: an exit right after start counts
// toward a crash loop in Pagerless, which a plain os.Exit never reports.
//
//	if err := srv.ListenAndServe(); err != nil {
//		pagerless.Fatal(err)
//	}
func Fatal(err error, opts ...CaptureOption) {
	std.Load().fatal(err, opts)
}

// Fatal is the per-client version of the package-level Fatal.
func (c *Client) Fatal(err error, opts ...CaptureOption) {
	c.fatal(err, opts)
}

func (c *Client) fatal(err error, opts []CaptureOption) {
	if c.active() && err != nil {
		func() {
			defer c.guard()
			opts = append([]CaptureOption{Tags(map[string]string{"exit": "fatal"})}, opts...)
			c.captureError(errType(err), err.Error(), callers(2), opts)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), fatalFlush)
		_ = c.Close(ctx)
		cancel()
	}
	exit(1)
}
