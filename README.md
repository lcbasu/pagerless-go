# Pagerless Go SDK

Reports errors, request health, heartbeats and deploys from a Go service to
[Pagerless](https://pagerless.dev). Standard library only.

```bash
go get github.com/lcbasu/pagerless-go@v0.1.0
```

The package name is `pagerless`.


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
	http.ListenAndServe(":8080", pagerless.Middleware(mux))
}
```

`Middleware` records status and latency per route (the Go 1.22+ pattern, e.g. `GET /orders/{id}`)
and reports panics, then re-panics so your own recovery still runs.

## Gin

Gin's `Recovery` catches panics before they reach `net/http` middleware, so report them from its
hook with `Recovered`, and wrap the engine for status and latency:

```go
r := gin.New()
r.Use(gin.Logger(), gin.CustomRecovery(func(c *gin.Context, v any) {
	pagerless.Recovered(v, c.Request, c.FullPath()) // route "GET /orders/:id"
	c.AbortWithStatus(http.StatusInternalServerError)
}))
r.GET("/orders/:id", getOrder)
http.ListenAndServe(":8080", pagerless.Middleware(r))
```

The stack starts at the panic, not at the hook, so the same panic groups as one problem. Echo's
and Chi's recoverers work the same way: call `Recovered` from their hook.

## Workers and jobs

```go
if err := syncOrders(ctx); err != nil {
	pagerless.Capture(err, pagerless.Route("job sync-orders"), pagerless.Tags(map[string]string{"shop": id}))
}

go func() {
	defer func() {
		if v := recover(); v != nil {
			pagerless.Recovered(v, nil, "job sync-orders")
			panic(v) // or log and carry on
		}
	}()
	runJob()
}()

pagerless.Event("Refund queue backed up", pagerless.Wake, nil) // Wake, Tell or Log
```

Call `pagerless.Close(ctx)` on shutdown (SIGTERM on ECS) so the last window is sent.

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

## Releasing (maintainers)

Go sub-module tags carry the directory: `sdk/go/v0.1.0`, not `v0.1.0`.
From a clean checkout of the commit to release (normally `main` after merge):

```bash
make tag-sdk-go VERSION=v0.1.0
git push origin sdk/go/v0.1.0
```

## License

MIT
