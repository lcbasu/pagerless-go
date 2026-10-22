package pagerless

import (
	"errors"
	"testing"
)

func TestFatalReportsDeliversAndExits(t *testing.T) {
	rec, srv := newRecorder(t)
	c, _ := testClient(t, srv.URL)
	code := -1
	exit = func(c int) { code = c }
	t.Cleanup(func() { exit = osExit })

	c.Fatal(errors.New("listen tcp :8080: address already in use"))

	if code != 1 {
		t.Fatalf("exit code %d", code)
	}
	var got *wireEvent
	for _, e := range rec.events() {
		if e.Type == typeError {
			got = &e
		}
	}
	if got == nil {
		t.Fatal("the reason must be delivered before exiting")
	}
	if got.Tags["exit"] != "fatal" || got.Tags[tagPhase] != phaseStartup {
		t.Fatalf("tags: %v", got.Tags)
	}
}

func TestFatalWithoutAClientStillExits(t *testing.T) {
	code := -1
	exit = func(c int) { code = c }
	t.Cleanup(func() { exit = osExit })
	var c *Client
	c.Fatal(errors.New("x"))
	if code != 1 {
		t.Fatal("must exit even when reporting is off")
	}
}
