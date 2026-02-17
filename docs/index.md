---
layout: default
title: Home
nav_order: 1
permalink: /
---

# Go Event Bus
{: .fs-9 }

A modular, in-process and distributed event bus abstraction for Go, designed for high-throughput systems with strong observability, resilience, and extensibility.
{: .fs-6 .fw-300 }

[Get Started]({{ site.baseurl }}/getting-started/){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/isaquesb/go-event-bus){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Overview

Go Event Bus provides a clean abstraction for event-driven architectures in Go, with pluggable transports, an explicit execution pipeline, and production-grade resilience patterns.

### Key Features

- **LocalBus** for in-process events with sync/async dispatch
- **NatsBus** for fire-and-forget distributed messaging
- **JetStreamBus** for durable, distributed events with replay
- **Invoker Chain** for composable cross-cutting concerns
- **Built-in invokers** for tracing, metrics, retry, circuit breaking, rate limiting, idempotency, and DLQ
- **Redis stores** for distributed idempotency and rate limiting
- **OpenTelemetry** tracing and **Prometheus** metrics out of the box

### Quick Example

```go
package main

import (
    "context"
    "fmt"

    event "github.com/isaquesb/go-event-bus"
    "github.com/isaquesb/go-event-bus/invoker"
)

type UserCreated struct {
    UserID string
    Email  string
}

func (e UserCreated) Name() string { return "user.created" }

func main() {
    bus := event.NewLocalBus(event.LocalBusOptions{
        Invoker:       invoker.NewChain(invoker.NewMetrics(nil)),
        MaxConcurrent: 10,
    })

    ctx := context.Background()

    bus.Subscribe(ctx, "user.created", func(ctx context.Context, evt event.Event) error {
        e := evt.(UserCreated)
        fmt.Printf("Welcome %s!\n", e.Email)
        return nil
    }, event.WithHandlerName("send-welcome"))

    bus.EmitSync(ctx, UserCreated{UserID: "123", Email: "john@example.com"})
}
```

### Architecture at a Glance

The library is composed of three orthogonal layers:

1. **Event Definition & Serialization** - Events, Registry, Envelopes
2. **Execution Pipeline (Invoker Chain)** - Cross-cutting concerns as composable middleware
3. **Transport / Delivery** - LocalBus, NATS Core, JetStream

These layers are intentionally decoupled, so you can mix and match based on your needs.
