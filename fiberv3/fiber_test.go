package pagerlessfiberv3

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
)

func do(t *testing.T, app *fiber.App, method, path string) int {
	t.Helper()
	res, err := app.Test(httptest.NewRequest(method, path, nil))
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode
}

func refund(c fiber.Ctx) error {
	var m map[string]int
	m[c.Params("id")]++ // nil map write: a real panic
	return nil
}

func TestFiberPanicsErrorsAndRoutes(t *testing.T) {
	srv := pagerlesstest.NewServer(t)
	pc, err := pagerless.New("pl_test", pagerless.WithEndpoint(srv.URL()))
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	app.Use(recover.New())
	app.Use(Middleware(WithClient(pc)))
	app.Post("/refunds/:id", refund)
	app.Get("/orders/:id", func(c fiber.Ctx) error {
		if c.Params("id") == "0" {
			return fiber.NewError(http.StatusNotFound, "no such order")
		}
		return c.SendString("ok")
	})
	app.Get("/report", func(c fiber.Ctx) error { return errors.New("warehouse timeout") })

	if code := do(t, app, "POST", "/refunds/9"); code != 500 {
		t.Fatalf("recover still answers 500, got %d", code)
	}
	do(t, app, "GET", "/orders/1")
	do(t, app, "GET", "/orders/0")
	if code := do(t, app, "GET", "/report"); code != 500 {
		t.Fatalf("got %d", code)
	}
	do(t, app, "GET", "/missing/42")
	_ = pc.Close(context.Background())

	errs := srv.Errors()
	if len(errs) != 2 {
		t.Fatalf("want panic + 500 error, got %+v", errs)
	}
	if errs[0].Route != "POST /refunds/:id" || !strings.HasPrefix(errs[0].Error.Type, "panic: ") {
		t.Fatalf("panic: %+v", errs[0])
	}
	if !strings.Contains(errs[0].Error.Stack, "pagerless-go/fiberv3.refund") {
		t.Fatalf("stack:\n%s", errs[0].Error.Stack)
	}
	if errs[1].Route != "GET /report" || errs[1].Error.Message != "warehouse timeout" {
		t.Fatalf("error: %+v", errs[1])
	}
	got := map[string]int{}
	for _, ev := range srv.Requests() {
		got[ev.Route+" "+http.StatusText(ev.Status)] += ev.Count
	}
	for k, n := range map[string]int{
		"POST /refunds/:id Internal Server Error": 1,
		"GET /orders/:id OK":                      1,
		"GET /orders/:id Not Found":               1,
		"GET /report Internal Server Error":       1,
		"GET (unmatched) Not Found":               1,
	} {
		if got[k] != n {
			t.Errorf("%s: got %d want %d (all %v)", k, got[k], n, got)
		}
	}
}
