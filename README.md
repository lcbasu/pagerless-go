# Pagerless Go SDK

Reports errors, request health, heartbeats, deploys and scheduled-job runs from a Go service to
[Pagerless](https://pagerless.dev). The core package is standard library only; each framework
adapter is its own module, so you only pull in the framework you already use.

```bash
go get github.com/lcbasu/pagerless-go@v0.2.0
```

The package name is `pagerless`. The fastest setup is `npx pagerless connect` in your repo: it
finds your frameworks and Temporal schedules and adds the lines below for you.

## Setup

Set `PAGERLESS_KEY` in the service's environment (an ECS task secret, for example), then:

```go
import (
	"context"
	"net/http"

	pagerless "github.com/lcbasu/pagerless-go"
)

func main() {
	pagerless.Init("") // reads PAGERLESS_KEY; without it every call is a no-op
	defer pagerless.Close(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", getOrder)
	if err := http.ListenAndServe(":8080", pagerless.Middleware(mux)); err != nil {
		pagerless.Fatal(err) // reports why it exited, then exits 1
	}
}
```

`Middleware` records status and latency per route (the Go 1.22+ pattern, e.g. `GET /orders/{id}`)
and reports panics, then re-panics so your own recovery still runs.

## Frameworks

Every adapter reports panics before your framework's recovery swallows them (it sits inside the
recovery, reports, and re-panics, so the recovery still writes the 500), and names routes by
their template, not the raw path. Each takes `WithClient(c)` to use a client other than the
default one.

| Framework | Module | Wiring |
| --- | --- | --- |
| Gin | `github.com/lcbasu/pagerless-go/gin` | `r.Use(pagerlessgin.Middleware())` after your recovery, before routes |
| Echo v4 | `github.com/lcbasu/pagerless-go/echo` | `e.Use(pagerlessecho.Middleware())` after `middleware.Recover()` |
| Echo v5 | `github.com/lcbasu/pagerless-go/echov5` | `e.Use(pagerlessechov5.Middleware())` |
| Fiber v2 | `github.com/lcbasu/pagerless-go/fiber` | `app.Use(pagerlessfiber.Middleware())` after `recover.New()`, before routes |
| Fiber v3 | `github.com/lcbasu/pagerless-go/fiberv3` | `app.Use(pagerlessfiberv3.Middleware())` |
| chi | `github.com/lcbasu/pagerless-go/chi` | `r.Use(pagerlesschi.Middleware)` after `middleware.Recoverer` |
| gorilla/mux | `github.com/lcbasu/pagerless-go/gorillamux` | `r.Use(pagerlessmux.Middleware)` |
| gRPC | `github.com/lcbasu/pagerless-go/grpc` | `grpc.NewServer(pagerlessgrpc.ServerOptions()...)`, clients: `pagerlessgrpc.DialOptions()...` |
| Temporal | `github.com/lcbasu/pagerless-go/temporal` | `worker.Options{Interceptors: []interceptor.WorkerInterceptor{pagerlesstemporal.New(c)}}`, or on the client |

```bash
go get github.com/lcbasu/pagerless-go/gin@v0.2.0
```

```go
r := gin.New()
r.Use(gin.Logger(), myRecovery())
r.Use(pagerlessgin.Middleware()) // route "GET /orders/:id"
r.GET("/orders/:id", getOrder)
```

Gin and Fiber apply middleware only to routes registered after `Use`, so add it before your
routes. Errors attached with `c.Error(err)` (Gin) or returned by a handler (Echo, Fiber) are
reported when the response is a 5xx.

**gRPC.** Each method is a route (`gRPC /shop.Orders/Get`); status codes are recorded as their
HTTP equivalents (Internal → 500, Unavailable → 503), so failure rates work as for HTTP. Server
errors that mean a bug (Unknown, Internal, DataLoss) are reported. The client interceptors
report outbound calls that failed on the far side or the network (Unavailable, DeadlineExceeded,
Internal…), not answers like NotFound.

**Temporal.** The worker interceptor reports activity and workflow failures (named by an
ApplicationError's type, e.g. `CardDeclined`) and panics. Every Temporal Schedule becomes a
check-in: each scheduled run pings `/v1/checkins/<schedule-id>` when it ends, ok or failed, so a
schedule that stops firing or keeps failing pages you once. With the client passed to `New`,
each schedule's timing is read once and sent along, so Pagerless knows when the next run is due.
Skip schedules your app creates per customer with `pagerlesstemporal.SkipSchedules("agent-run-")`.

```go
c, _ := client.Dial(client.Options{})
w := worker.New(c, "orders", worker.Options{
	Interceptors: []interceptor.WorkerInterceptor{pagerlesstemporal.New(c)},
})
```

Or install it once on the client: it then covers every worker made from that client, and learns
each schedule's timing from the `CreateSchedule` calls the process makes. Use one place, not both.

```go
c, err := client.Dial(client.Options{
	Interceptors: []interceptor.ClientInterceptor{pagerlesstemporal.New(nil)},
})
```

Another framework? Report panics from its recovery hook with `pagerless.Recovered(v, r, route)`,
or build on `pagerless.CapturePanic`, `RecordRequest` and `MiddlewareRoutes`.

## Logs and exits

```go
// Every slog record at Error level is also captured (its error attr names the problem).
slog.SetDefault(slog.New(pagerless.SlogHandler(slog.NewJSONHandler(os.Stderr, nil))))

// Where you'd call log.Fatal: reports why, delivers what's buffered, exits 1.
// Exits right after start count toward a crash loop in Pagerless.
pagerless.Fatal(err)
```

Wrap a handler you build: setting a wrapper of Go's built-in default handler as the default makes
the `log` package loop into itself.

## Workers and jobs

```go
if err := syncOrders(ctx); err != nil {
	pagerless.Capture(err, pagerless.Route("job sync-orders"), pagerless.Tags(map[string]string{"shop": id}))
}

// A scheduled job outside Temporal: report each run.
err := pagerless.Monitor(ctx, "nightly-backup", pagerless.Schedule{Cron: "30 3 * * *"}, backup)

pagerless.Event("Refund queue backed up", pagerless.Wake, nil) // Wake, Tell or Log
```

Call `pagerless.Close(ctx)` on shutdown (SIGTERM on ECS) so the last window is sent.

## Testing

`github.com/lcbasu/pagerless-go/pagerlesstest` is a fake ingest: point a client at it with
`pagerless.WithEndpoint(srv.URL())`, run your code, close the client and read `srv.Errors()`,
`srv.Requests()` or `srv.CheckIns()`.

## Settings

| Variable | Option | Default |
| --- | --- | --- |
| `PAGERLESS_KEY` | `Init(key)` | none: reporting is off, `ErrNoKey` |
| `PAGERLESS_SERVICE` | `WithService` | executable name |
| `PAGERLESS_ENV` | `WithEnv` | `production` |
| `PAGERLESS_RELEASE` | `WithRelease` | VCS revision from the build info |
| `PAGERLESS_ENDPOINT` | `WithEndpoint` | `https://events.pagerless.dev/v1/events` |

Secrets and personal data are scrubbed before anything leaves the process. The SDK never blocks
the caller and never panics into your code.

## License

MIT
