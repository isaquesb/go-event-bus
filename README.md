# Event Bus (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/isaquesb/go-event-bus.svg)](https://pkg.go.dev/github.com/isaquesb/go-event-bus)
[![Go Report Card](https://goreportcard.com/badge/github.com/isaquesb/go-event-bus)](https://goreportcard.com/report/github.com/isaquesb/go-event-bus)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/isaquesb/go-event-bus/branch/main/graph/badge.svg)](https://codecov.io/gh/isaquesb/go-event-bus)
[![CI](https://github.com/isaquesb/go-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/isaquesb/go-event-bus/actions/workflows/ci.yml)

A modular, in-process and distributed **event bus abstraction for Go**, designed for
high-throughput systems with strong requirements around **observability, resilience,
and extensibility**.

This project provides:
- A **LocalBus** for in-process events
- A **NatsBus** for fire-and-forget distributed events
- A **JetStreamBus** for distributed, durable events
- A **pluggable Invoker Chain** for cross-cutting concerns such as:
  - tracing
  - retries
  - circuit breaking
  - rate limiting
  - idempotency
  - DLQ handling
- One **Observer** per bus that receives every failure and resilience signal, with
  backends for logs, Prometheus metrics and OpenTelemetry
- A **failure model** shared by every transport: each failed message is retried,
  dead-lettered or dropped by the same code, and reported exactly once

The core goal is to **decouple application modules** while keeping **explicit control**
over execution, failure semantics, and observability.

---

## Installation

```bash
go get github.com/isaquesb/go-event-bus
```

## Requirements

- Go 1.26+

## Quick Start

Compiled from [`examples/01_basic_local_bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/01_basic_local_bus.go):

```go
package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// UserCreated is emitted when a new user registers
type UserCreated struct {
	UserID   string
	Email    string
	UserName string
}

func (e UserCreated) Name() string { return "user.created" }

// SendWelcomeEmail handles user.created events
func SendWelcomeEmail(ctx context.Context, evt event.Event) error {
	e := evt.(UserCreated)
	slog.Info("sending welcome email", "user_id", e.UserID, "email", e.Email)
	// ... send email logic
	return nil
}

func main() {
	// An empty chain; see the invoker chain section for the recommended order
	// of the resilience invokers
	chain := invoker.NewChain()

	// Create the LocalBus
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       chain,
		MaxConcurrent: 10,
		// Observer receives every terminal failure once (Observer.Failure). The slog
		// observer is also the default when Observer is nil.
		Observer: event.NewSlogObserver(nil),
	})

	ctx := context.Background()

	// Register a handler
	if err := bus.Subscribe(ctx, "user.created", SendWelcomeEmail, event.WithHandlerName("send-welcome-email")); err != nil {
		slog.Error("subscribe failed", "error", err)
		return
	}

	// Synchronous - waits for all handlers to complete
	if _, err := bus.EmitSync(ctx, UserCreated{
		UserID:   "user-123",
		Email:    "john@example.com",
		UserName: "John Doe",
	}); err != nil {
		slog.Error("emit sync failed", "error", err)
	}

	// Asynchronous - fire and forget
	bus.Emit(ctx, UserCreated{UserID: "user-456", Email: "jane@example.com", UserName: "Jane Doe"})

	fmt.Println("Events emitted successfully")
}
```

Upgrading from v0.3.0? The [CHANGELOG](CHANGELOG.md) is the migration guide.

---

## Design Goals

- Explicit, composable execution pipeline (Invoker Chain)
- No hidden magic or framework lock-in
- First-class observability (one Observer: logs, metrics and traces)
- Failures are never silent: every terminal outcome reaches the Observer exactly once
- Works equally well for in-process and distributed events
- Optional dependencies via subpackages
- Production-grade resilience patterns
- One transport-neutral contract that every adapter follows

---

## Core Concepts

### Event

```go
type Event interface {
    Name() string
}
```

Optional interfaces extend behavior:

```go
type WithVersion interface {
    Version() int
}

type WithId interface {
    Id() string
}

// In invoker package:
type WithIdempotencyKey interface {
    IdempotencyKey() string
}

type WithRateLimitKey interface {
    RateLimitKey() string
}
```

Events are plain Go structs. Behavior is opt-in via interfaces.

> **Note:** `WithIdempotencyKey` takes precedence over `WithId` for idempotency key extraction.

---

## LocalBus

An in-process event dispatcher.

- Fire-and-forget
- No delivery guarantees: a delivery never repeats, so it is always the final one
- Suitable for domain events and internal decoupling

Supports synchronous and asynchronous dispatch, concurrency limits, an overflow
policy, graceful shutdown and a shared invoker chain.

---

## Emit API

All buses share a unified `Emit` signature with optional functional options:

```go
// Subject defaults to evt.Name()
bus.Emit(ctx, myEvent)

// Override subject (e.g. for hierarchical NATS/JetStream subjects)
bus.Emit(ctx, myEvent, event.WithSubject("chat.message.room-general"))
```

## Subscribe API

All buses use a unified Subscribe signature with functional options:

```go
bus.Subscribe(ctx, "user.created", handler,
    event.WithHandlerName("email-sender"),
)
```

JetStream adds stream-specific options:

```go
bus.Subscribe(ctx, "chat.message.*", handler,
    event.WithStream("CHAT_EVENTS"),
    event.WithHandlerName("persist-message"),
    event.WithConsumer("chat-processor"),
)
```

## RegisterSubscribers

All buses support bulk subscriber registration. Implement `Subscriber` to group related handlers:

```go
bus.RegisterSubscribers(ctx,
    NewEmailSubscriber(),
    NewBillingSubscriber(),
)
```

For JetStream, implement `SubscribeOptionsProvider` on the subscriber to carry stream options:

```go
func (s *ChatSubscriber) SubscribeOptions() []event.SubscribeOption {
    return []event.SubscribeOption{
        event.WithStream("CHAT_EVENTS"),
        event.WithConsumer("chat-processor"),
    }
}
```

---

## NatsBus

A simple NATS Core event bus for fire-and-forget messaging.

- No persistence
- Pub/Sub pattern
- Request/Reply support via `EmitSync`
- Suitable for ephemeral events

---

## JetStreamBus

A distributed event bus backed by NATS JetStream.

- Durable messages
- Fan-out and consumer groups
- Replay of dead-lettered messages
- Integrated invoker chain

Responsibilities:
- publish serialized events
- decode and dispatch handlers
- ACK / NAK / Term handling through the shared failure model
- DLQ via the invoker, with a transport backstop

If you compact a stream by subject, read the
[log-compaction guide](https://isaquesb.github.io/go-event-bus/guides/log-compaction/) first.

---

## Registry & Serialization

The Registry is responsible for:
- encoding events into envelopes
- decoding messages into events
- schema evolution via upcasting
- trace propagation

### Envelope

```json
{
  "name": "user.created",
  "version": 2,
  "ts": "2026-02-05T12:00:00Z",
  "id": "0194f3c2-8a41-7c3e-9d52-1b6f0e7a4c10",
  "correlation_id": "0194f3c2-7e10-7a2b-8c11-5d0e3f9b2a64",
  "causation_id": "0194f3c2-7e10-7a2b-8c11-5d0e3f9b2a64",
  "headers": {"tenant": "acme"},
  "trace_id": "...",
  "span_id": "...",
  "payload": {}
}
```

---

## Invoker Chain

Invokers form an explicit execution pipeline:

```go
type Invoker interface {
    Invoke(
        ctx context.Context,
        evt Event,
        handlerName string,
        next func(context.Context) error,
    ) error
}
```

Recommended order, outermost first:

```
Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler
```

```go
chain := invoker.NewChain(
    telemetry.NewTracerInvoker(tracer),
    invoker.NewRateLimiter(rateStore, rateCfg),
    invoker.NewIdempotency(idemStore, idemCfg),
    invoker.NewDLQ(dlqPublisher),
    invoker.NewRetry(retryPolicy),
    invoker.NewCircuitBreaker(breakerCfg),
)
```

The handler is the last link and is not part of `NewChain`. The reason for each position is on the
[composition page](https://isaquesb.github.io/go-event-bus/invoker-chain/composition/).

---

## Built-in Invokers

### Tracer (`telemetry.NewTracerInvoker`)
- OpenTelemetry compatible
- One consumer span per handler invocation
- Context propagated via envelope

### Rate Limiting
- Token bucket, per-key, with dynamic keys (e.g. userId)
- A rejected event fails with `ErrRateLimited` and the handler does not run

### Idempotency
- Store-backed, handler-scoped, with a TTL for stale in-progress markers
- A duplicate fails with `ErrDuplicate`, which the transport acknowledges

### DLQ
- Publishes a terminal failure through a pluggable `DLQPublisher`, once, with the original bytes
- Sits outside Retry, so only the final outcome is dead-lettered
- Returns an error that tells the transport to acknowledge

### Retry
- Exponential backoff with error classification
- Keeps the history of the attempts in the failure error and the DLQ record
- Gives up with `event.ErrRetriesExhausted`; a terminal error is never retried

### Circuit Breaker
- Closed / Open / Half-open, per handler
- Business terminal errors do not count as failures
- Every transition is reported to the Observer

There is no metrics invoker: latency and outcome come from the `HandlerDone` signal
that every transport emits to the Observer.

---

## Failure Semantics

Every transport settles a failed message with the same code:

- `event.InvokeHandler` runs the chain around the handler and emits `HandlerDone` for every invocation.
- `event.Settle` runs exactly once per handled message and maps the result through `event.Dispose` to
  Ack, Retry, RetryDelayed or Terminal.
- The DLQ invoker sits outside Retry. When the chain did not dead-letter a terminal failure, the
  transport backstop (`DLQPublisher` in the bus options) publishes it, and a message is never
  published twice.
- Dead-lettering is at-least-once, and the DLQ record keeps the original envelope bytes, so
  `jetstream.NewReplayer` can republish it unchanged.
- Every terminal outcome reaches `Observer.Failure` exactly once. A retry is not a failure.

See the [failure model](https://isaquesb.github.io/go-event-bus/architecture/failure-model/) for the
disposition table of every error on every transport, and the [DLQ and replay example](https://isaquesb.github.io/go-event-bus/examples/dlq-replay/).

---

## Redis Stores

Production-ready implementations for distributed scenarios:

### RedisIdempotencyStore
- Get-then-Put pattern with SET
- TTL per status (processing: 5m, completed: 24h, failed: 1h, dead_lettered: 1h)
- Automatic key expiration for stale lock recovery

### RedisRateLimitStore
- Lua script for atomic token bucket
- Supports rate, period, and burst configuration
- Sliding window with refill

---

## Observability

A bus reports everything that happens to a message through one `event.Observer`, set in the bus
options. Invokers and transports only call it, and each backend implements it:

| What you want | Backend |
|---|---|
| Logs | `event.NewSlogObserver`, the default when the bus has no Observer |
| Metrics | `prometheus.NewObserver`, or `telemetry.NewObserver` with `WithMeterProvider` for OpenTelemetry metrics |
| Traces | `telemetry.NewTracerInvoker` as the first invoker of the chain, plus `telemetry.NewObserver` for span events |

```go
bus := event.NewLocalBus(event.LocalBusOptions{
    Observer: event.MultiObserver(
        event.NewSlogObserver(nil),
        prometheus.NewObserver(reg),
    ),
})
```

Guarantees:
- Every terminal failure reaches `Observer.Failure` exactly once
- A panic in an observer never reaches the handler
- Trace propagation across buses
- Safe in multi-pod deployments

See [Observability](https://isaquesb.github.io/go-event-bus/observability/) for the signals, the
metric families and the log levels.

---

## Package Structure

```
github.com/isaquesb/go-event-bus/
├── event.go              # Core interfaces (Event, Invoker, Subscriber)
├── bus.go                # Bus interface
├── options.go            # EmitOption and SubscribeOption functional options
├── registry.go           # Registry interface & Envelope
├── local-bus.go          # In-process event bus
├── observer.go           # Observer interface, NopObserver, MultiObserver
├── slog_observer.go      # Default log Observer
├── settle.go             # Settle: the post-chain failure policy shared by every transport
├── invoke.go             # InvokeHandler: the single handler entry point
├── dlq.go                # DLQPublisher, DLQRecord, MemoryDLQ
├── lifecycle.go          # Shutdown state machine shared by every bus
├── invoker/              # Execution pipeline components
│   ├── chain.go          # Invoker chain composition
│   ├── retry.go          # Exponential backoff retries
│   ├── circuit-breaker.go # Circuit breaker pattern
│   ├── rate-limit.go     # Rate limiting invoker
│   ├── idempotency.go    # Idempotency check + MemoryStore
│   └── dlq.go            # Dead letter queue handler
├── json/                 # JSON serialization
│   └── registry.go       # Envelope encoding/decoding + upcasting
├── jetstream/            # NATS JetStream transport
│   ├── bus.go            # JetStream event bus
│   ├── dlq.go            # JetStream DLQ publisher
│   └── replayer.go       # Replay of dead-lettered messages
├── nats/                 # NATS Core transport
│   ├── bus.go            # Simple NATS bus (fire-and-forget)
│   └── errors.go         # Remote error type
├── redis/                # Redis-backed stores
│   ├── idempotency.go    # Redis idempotency store
│   └── rate-limit.go     # Redis rate limiter (token bucket)
├── telemetry/            # OpenTelemetry
│   ├── tracer.go         # Tracing invoker
│   ├── observer.go       # Observer: span events and OTel metrics
│   └── semconv.go        # Attribute and span event names
└── prometheus/           # Metrics
    └── observer.go       # Observer with a fixed Prometheus metric set
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

Please ensure all tests pass before submitting.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Documentation

For full documentation, visit [https://isaquesb.github.io/go-event-bus/](https://isaquesb.github.io/go-event-bus/).

- [Getting started](https://isaquesb.github.io/go-event-bus/getting-started/)
- [Examples](https://isaquesb.github.io/go-event-bus/examples/), compiled from the [`examples/`](examples/) directory
- [Observability](https://isaquesb.github.io/go-event-bus/observability/)
- [Guides](https://isaquesb.github.io/go-event-bus/guides/), including the [JetStream log-compaction guide](https://isaquesb.github.io/go-event-bus/guides/log-compaction/)
- [Adapter contract](https://isaquesb.github.io/go-event-bus/architecture/adapter-contract/): the rules a new transport adapter must follow
- [CHANGELOG](CHANGELOG.md): release notes and the v0.3.0 to v0.4.0 migration guide

See [ARCHITECTURE.md](ARCHITECTURE.md) for deeper design details.
