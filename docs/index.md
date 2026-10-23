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

- **LocalBus** for in-process events with sync/async dispatch, an overflow policy and graceful shutdown
- **NatsBus** for fire-and-forget distributed messaging
- **JetStreamBus** for durable, distributed events with dead-letter replay
- **Invoker Chain** for composable cross-cutting concerns: tracing, rate limiting, idempotency, DLQ, retry and circuit breaking
- **Failure model** shared by every transport: each failed message is retried, dead-lettered or dropped by the same code, and reported to the Observer exactly once
- **Observer** per bus with backends for logs (`SlogObserver`, the default), **Prometheus** metrics and **OpenTelemetry** traces and metrics
- **Redis stores** for distributed idempotency and rate limiting
- **Causal metadata**: event id, correlation id and causation id travel in the envelope

### Where to go next

- [Getting started]({{ site.baseurl }}/getting-started/): install the modules and run the first bus
- [Invoker chain]({{ site.baseurl }}/invoker-chain/): the recommended order and the reason for each position
- [Observability]({{ site.baseurl }}/observability/): the Observer, logs, metrics and traces
- [Examples]({{ site.baseurl }}/examples/): complete examples, compiled with the repository
- [Guides]({{ site.baseurl }}/guides/): task-oriented guides, starting with JetStream log compaction
- [Adapter contract]({{ site.baseurl }}/architecture/adapter-contract/): the rules a new transport adapter must follow
- [Failure model]({{ site.baseurl }}/architecture/failure-model/): what happens to every error on every transport

### Quick Example

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
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       invoker.NewChain(),
		MaxConcurrent: 10,
		// Observer receives every terminal failure once (Observer.Failure). The slog
		// observer is also the default when Observer is nil.
		Observer: event.NewSlogObserver(nil),
	})

	ctx := context.Background()

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

	fmt.Println("Events emitted successfully")
}
```

### Architecture at a Glance

The library is composed of three orthogonal layers:

1. **Event Definition & Serialization** - Events, Registry, Envelopes
2. **Execution Pipeline (Invoker Chain)** - Cross-cutting concerns as composable middleware
3. **Transport / Delivery** - LocalBus, NATS Core, JetStream

These layers are intentionally decoupled, so you can mix and match based on your needs.

The [failure model]({{ site.baseurl }}/architecture/failure-model/) and the [Observer]({{ site.baseurl }}/observability/) cut across all three layers.
