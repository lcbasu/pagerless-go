package pagerlessechov5

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
)

func do(h http.Handler, method, path string) int {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr.Code
}

func refund(c *echo.Context) error {
	var m map[string]int
	m[c.Param("id")]++ // nil map write: a real panic
	return nil
}

func TestEchoPanicsErrorsAndRoutes(t *testing.T) {
	srv := pagerlesstest.NewServer(t)
	pc, err := pagerless.New("pl_test", pagerless.WithEndpoint(srv.URL()))
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(Middleware(WithClient(pc)))
	e.POST("/refunds/:id", refund)
	e.GET("/orders/:id", func(c *echo.Context) error {
		if c.Param("id") == "0" {
			return echo.NewHTTPError(http.StatusNotFound, "no such order")
		}
		return c.String(http.StatusOK, "ok")
	})
	e.GET("/report", func(c *echo.Context) error { return errors.New("warehouse timeout") })

	if code := do(e, "POST", "/refunds/9"); code != 500 {
		t.Fatalf("Recover still answers 500, got %d", code)
	}
	do(e, "GET", "/orders/1")
	do(e, "GET", "/orders/0")
	if code := do(e, "GET", "/report"); code != 500 {
		t.Fatalf("got %d", code)
	}
	do(e, "GET", "/missing/42")
	_ = pc.Close(context.Background())

	errs := srv.Errors()
	if len(errs) != 2 {
		t.Fatalf("want panic + 500 error, got %+v", errs)
	}
	if errs[0].Route != "POST /refunds/:id" || !strings.HasPrefix(errs[0].Error.Type, "panic: ") {
		t.Fatalf("panic: %+v", errs[0])
	}
	if !strings.Contains(errs[0].Error.Stack, "pagerless-go/echov5.refund") {
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
