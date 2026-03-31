# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Testing

```bash
# All modules (matches CI — excludes examples)
go test -race ./... $(go list ./... | grep -v /examples)

# Single module from workspace root
go test ./invoker/...
go test ./jetstream/...

# Single test by name
go test ./... -run TestRegisterSubscribers

# With coverage
go test -race -coverprofile=coverage.out -covermode=atomic $(go list ./... | grep -v /examples)
```

### Working with submodules

Each subpackage is a separate Go module. From the workspace root, `go.work` links them all so cross-module changes work without publishing. When editing multiple modules at once (e.g. root + `jetstream`), no `replace` directives are needed — `go.work` handles it.

```bash
# Tidy a specific submodule
cd jetstream && go mod tidy

# Tidy all
for mod in . invoker json nats jetstream redis telemetry prometheus examples; do
  (cd $mod && go mod tidy)
done
```

### Releasing

> **Before tagging a release**, update `docs/` and `CHANGELOG.md` to reflect any API changes:
> - `docs/transports/` — update Emit/Subscribe signatures, new options, new methods
> - `docs/core-concepts/` — update interfaces and option types
> - `docs/examples/` — update code snippets to match current API
> - `docs/api-reference/index.md` — add new exported types and functions
> - `CHANGELOG.md` — add a new `## [vX.Y.Z]` section with Added/Changed/Dependencies
> - `examples/*.go` — fix any breaking signature changes (these are compiled and must build)

Every release requires tagging **all 9 modules**. Root tag format: `vX.Y.Z`. Submodule tag format: `<name>/vX.Y.Z`.

```bash
git tag v0.3.0
for mod in invoker json nats jetstream redis telemetry prometheus examples; do
  git tag "${mod}/v0.3.0"
done
git push origin v0.3.0 invoker/v0.3.0 json/v0.3.0 nats/v0.3.0 jetstream/v0.3.0 redis/v0.3.0 telemetry/v0.3.0 prometheus/v0.3.0 examples/v0.3.0
```

After tagging, update `go.mod` files in modules that depend on the root (e.g. `jetstream/go.mod` references `github.com/isaquesb/go-event-bus`).

## Architecture

### Module layout

The root module (`github.com/isaquesb/go-event-bus`) defines all core interfaces and `LocalBus`. Every other capability is opt-in via a separate module:

| Module | Purpose |
|--------|---------|
| `invoker` | Execution pipeline: retry, circuit breaker, rate limit, idempotency, DLQ, metrics |
| `json` | Envelope serialization with OTel trace propagation and upcaster chain |
| `nats` | NATS Core bus (fire-and-forget) |
| `jetstream` | NATS JetStream bus (at-least-once, durable) |
| `redis` | Redis-backed idempotency and rate-limit stores |
| `telemetry` | OpenTelemetry `TracerInvoker` |
| `prometheus` | `PrometheusProvider` implementing `MetricProvider` |
| `examples` | Runnable examples (excluded from CI coverage) |

### Core interfaces (root module)

- **`Event`** — plain Go struct with `Name() string`. Optionally implements `WithId`, `WithVersion`, `OnErr`.
- **`Bus`** — `Subscribe` + `Close`. All three transports implement it.
- **`Invoker`** — single `Invoke(ctx, evt, handlerName, next)` method. Forms a chain via `invoker.NewChain(...)`.
- **`Subscriber`** — groups handlers: `Name() string` + `Events() map[string]HandleFn`. Registered via `RegisterSubscribers`.
- **`SubscribeOptionsProvider`** — optional interface on `Subscriber` to carry transport-specific subscribe options (e.g. JetStream stream name) through `RegisterSubscribers`.

### Emit options

All buses use `Emit(ctx, event, ...EmitOption)`. Subject defaults to `evt.Name()`; use `WithSubject` to override:

```go
bus.Emit(ctx, evt)                                      // subject = evt.Name()
bus.Emit(ctx, evt, event.WithSubject("chat.message.x")) // explicit subject
```

`ApplyEmitOptions(evt, opts...)` seeds `Subject` from `evt.Name()` before applying overrides.

### Invoker chain

Invokers wrap handler execution. Recommended order: `Tracer → Metrics → RateLimit → Idempotency → Retry → CircuitBreaker → DLQ`. Each invoker calls `next(ctx)` to proceed and can short-circuit on error. Error types (`ErrCircuitOpen`, `ErrRateLimited`, `ErrDuplicate`) are used by JetStream's ACK/NAK logic.

### JetStream specifics

- `NewBus` accepts the internal `jsBackend` interface (not `jetstream.JetStream` directly) — this enables testing without a real NATS connection. Pass a real `jetstream.JetStream` value; it satisfies the interface.
- `WithStream` is **required** on every `Subscribe` call. If missing, `ErrStreamRequired` is returned.
- For `RegisterSubscribers`, the subscriber must implement `SubscribeOptionsProvider` to supply `WithStream(...)`.
- ACK/NAK strategy: `nil` → Ack, `ErrDuplicate` → Ack, `ErrCircuitOpen` → NakWithDelay, `ErrRateLimited` → NakWithDelay, other → Nak, decode failure → Term.

### NATS bus specifics

- `Bus.nc` is typed as internal `natsConn` interface for testability. Pass `*nats.Conn`; it satisfies the interface.
- `EmitSync` uses NATS Request/Reply; response is decoded back into an event.
- `SubscribeSync` encodes the handler response and sends it back as the NATS reply.

### Testability pattern

Both `nats` and `jetstream` packages define narrow internal interfaces (`natsConn`, `jsBackend`) over their external dependencies. Tests use mock structs implementing these interfaces — no real NATS server needed. See `nats/bus_test.go` and `jetstream/bus_test.go` for examples.

### Context propagation

Handler goroutines MUST receive the subscription context, not `context.Background()` — cancellation signals and OTel span parenting depend on it. When implementing a new transport adapter's `Subscribe` method, pass the `ctx` parameter from `Subscribe(ctx, ...)` into the message handler closure, including the `registry.Decode(ctx, ...)` call. See `nats/bus.go` Subscribe for the correct pattern (fixed in CORR-05).
