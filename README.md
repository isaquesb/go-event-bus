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
  - metrics
  - retries
  - circuit breaking
  - rate limiting
  - idempotency
  - DLQ handling

The core goal is to **decouple application modules** while keeping **explicit control**
over execution, failure semantics, and observability.

---

## Installation

```bash
go get github.com/isaquesb/go-event-bus
```

## Requirements

- Go 1.23+

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log/slog"

    event "github.com/isaquesb/go-event-bus"
    "github.com/isaquesb/go-event-bus/invoker"
)

// Define an event
type UserCreated struct {
    UserID string
    Email  string
}

func (e UserCreated) Name() string { return "user.created" }

func main() {
    // Create a bus with a minimal invoker chain
    bus := event.NewLocalBus(event.LocalBusOptions{
        Invoker:       invoker.NewChain(invoker.NewMetrics(nil)),
        MaxConcurrent: 10,
        OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
            slog.Error("handler failed", "event", evt.Name(), "handler", handler, "error", err)
        },
    })

    ctx := context.Background()

    // Subscribe to events
    bus.Subscribe(ctx, "user.created", func(ctx context.Context, evt event.Event) error {
        e := evt.(UserCreated)
        fmt.Printf("Welcome %s!\n", e.Email)
        return nil
    }, event.WithHandlerName("send-welcome"))

    // Emit an event
    bus.EmitSync(ctx, UserCreated{UserID: "123", Email: "john@example.com"})
}
```

---

## Design Goals

- Explicit, composable execution pipeline (Invoker Chain)
- No hidden magic or framework lock-in
- First-class observability (metrics + tracing)
- Works equally well for in-process and distributed events
- Optional dependencies via subpackages
- Production-grade resilience patterns

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
- No delivery guarantees
- Suitable for domain events and internal decoupling

Supports synchronous and asynchronous dispatch, concurrency limits,
and a shared invoker chain.

---

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
- Replay support
- Integrated invoker chain

Responsibilities:
- publish serialized events
- decode and dispatch handlers
- ACK / NAK handling
- DLQ via invoker

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
  "payload": {},
  "trace_id": "...",
  "span_id": "..."
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

Example chain:

```go
invoker := NewChain(
    TracingInvoker,
    MetricsInvoker,
    RateLimitInvoker,
    IdempotencyInvoker,
    RetryInvoker,
    CircuitBreakerInvoker,
    DLQInvoker,
)
```

---

## Built-in Invokers

### Tracing
- OpenTelemetry compatible
- One span per handler
- Context propagated via envelope

### Metrics
- Latency histograms
- Success/error counters
- State gauges

Metrics are per-pod and aggregated by Prometheus.

### Retry
- Exponential backoff
- Error classification
- Terminal errors forwarded to DLQ

### Circuit Breaker
- Closed / Open / Half-open
- Per-handler isolation
- Business terminal errors do not count as failures

### Rate Limiting
- Distributed
- Dynamic keys (e.g. userId)
- Burst control

### Idempotency
- Distributed
- Store-backed
- Handler-scoped
- TTL-based recovery

### DLQ
- Transport-agnostic
- Terminal invoker
- Pluggable publisher

---

## Redis Stores

Production-ready implementations for distributed scenarios:

### RedisIdempotencyStore
- Get-then-Put pattern with SET
- TTL per status (processing: 5m, completed: 24h, failed: 1h)
- Automatic key expiration for stale lock recovery

### RedisRateLimitStore
- Lua script for atomic token bucket
- Supports rate, period, and burst configuration
- Sliding window with refill

---

## Observability Guarantees

- Metrics from every invoker
- Trace propagation across buses
- Safe in multi-pod deployments
- Explicit failure semantics

---

## Package Structure

```
github.com/isaquesb/go-event-bus/
├── event.go              # Core interfaces (Event, Invoker, Subscriber)
├── bus.go                # Bus interface
├── options.go            # SubscribeOption functional options
├── registry.go           # Registry interface & Envelope
├── local-bus.go          # In-process event bus
├── invoker/              # Execution pipeline components
│   ├── chain.go          # Invoker chain composition
│   ├── metrics.go        # Latency & counter metrics
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
│   └── replayer.go       # Event replay from streams
├── nats/                 # NATS Core transport
│   └── bus.go            # Simple NATS bus (fire-and-forget)
├── redis/                # Redis-backed stores
│   ├── idempotency.go    # Redis idempotency store
│   └── rate-limit.go     # Redis rate limiter (token bucket)
├── telemetry/            # Observability
│   └── tracer.go         # OpenTelemetry tracing invoker
└── prometheus/           # Metrics
    └── provider.go       # Prometheus MetricProvider implementation
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

See [ARCHITECTURE.md](ARCHITECTURE.md) for deeper design details.
