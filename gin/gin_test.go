package pagerlessgin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
)

func init() { gin.SetMode(gin.TestMode) }

func newClient(t *testing.T, srv *pagerlesstest.Server) *pagerless.Client {
	t.Helper()
	c, err := pagerless.New("pl_test", pagerless.WithEndpoint(srv.URL()), pagerless.WithService("api"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func get(h http.Handler, method, path string) int {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr.Code
}

func chargeCard(c *gin.Context) {
	var m map[string]int
	m[c.Param("id")]++ // nil map write: a real panic
}

// The app's own recovery, like a production API that logs and writes its own 500 body.
func appRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recover() != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal"})
			}
		}()
		c.Next()
	}
}

func TestPanicIsReportedBeforeTheAppsRecoverySwallowsIt(t *testing.T) {
	srv := pagerlesstest.NewServer(t)
	pc := newClient(t, srv)
	r := gin.New()
	r.Use(appRecovery())
	r.Use(Middleware(WithClient(pc)))
	r.POST("/v1/cards/:id/charge", chargeCard)
	r.GET("/v1/orders/:id", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	r.GET("/v1/fail", func(c *gin.Context) {
		_ = c.Error(errors.New("db: connection refused"))
		c.Status(http.StatusBadGateway)
	})

	if code := get(r, "POST", "/v1/cards/42/charge"); code != 500 {
		t.Fatalf("the app's recovery still writes the 500, got %d", code)
	}
	get(r, "GET", "/v1/orders/1")
	get(r, "GET", "/v1/orders/2")
	get(r, "GET", "/v1/fail")
	get(r, "GET", "/nope/123")
	if err := pc.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	errs := srv.Errors()
	if len(errs) != 2 {
		t.Fatalf("want the panic and the 502's error, got %+v", errs)
	}
	p := errs[0]
	if p.Route != "POST /v1/cards/:id/charge" || p.Status != 500 || !strings.HasPrefix(p.Error.Type, "panic: ") {
		t.Fatalf("panic event: %+v %+v", p, p.Error)
	}
	if !strings.Contains(p.Error.Stack, "pagerless-go/gin.chargeCard") {
		t.Fatalf("stack should start at the handler:\n%s", p.Error.Stack)
	}
	if errs[1].Route != "GET /v1/fail" || errs[1].Error.Message != "db: connection refused" {
		t.Fatalf("5xx error: %+v", errs[1])
	}
	got := map[string]int{}
	for _, e := range srv.Requests() {
		got[e.Route+" "+http.StatusText(e.Status)] += e.Count
	}
	want := map[string]int{
		"POST /v1/cards/:id/charge Internal Server Error": 1,
		"GET /v1/orders/:id OK":                           2,
		"GET /v1/fail Bad Gateway":                        1,
		"GET (unmatched) Not Found":                       1,
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s: got %d, want %d (all: %v)", k, got[k], n, got)
		}
	}
}

func TestWithGinDefaultRecoveryToo(t *testing.T) {
	srv := pagerlesstest.NewServer(t)
	pc := newClient(t, srv)
	r := gin.New()
	r.Use(gin.CustomRecoveryWithWriter(nil, func(c *gin.Context, _ any) { c.AbortWithStatus(500) }))
	r.Use(Middleware(WithClient(pc)))
	r.GET("/boom", func(c *gin.Context) { panic("boom") })
	if code := get(r, "GET", "/boom"); code != 500 {
		t.Fatalf("got %d", code)
	}
	_ = pc.Close(context.Background())
	if errs := srv.Errors(); len(errs) != 1 || errs[0].Error.Message != "boom" {
		t.Fatalf("got %+v", errs)
	}
}

func TestDefaultClientBeforeInitIsANoop(t *testing.T) {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(Middleware())
	r.GET("/x", func(c *gin.Context) { c.Status(204) })
	if code := get(r, "GET", "/x"); code != 204 {
		t.Fatalf("got %d", code)
	}
}
