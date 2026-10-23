---
layout: default
title: Installation
parent: Getting Started
nav_order: 1
---

# Installation

## Core Package

```bash
go get github.com/isaquesb/go-event-bus@latest
```

This gives you the core interfaces (`Event`, `Bus`, `Invoker`, `Observer`), `LocalBus`, the failure model (`Dispose`, `Settle`, dead-letter records) and the default log Observer. Every other capability is a separate module, listed below.

Upgrading from v0.3.0? The [CHANGELOG](https://github.com/isaquesb/go-event-bus/blob/main/CHANGELOG.md) is the migration guide: every breaking change has a before and an after snippet.

## Requirements

- **Go 1.26** or later

## Optional Dependencies

Install only the subpackages you need:

### Invoker Chain

```bash
go get github.com/isaquesb/go-event-bus/invoker@latest
```

Provides `invoker.NewChain` and the built-in invokers: retry, circuit breaker, rate limiter, idempotency and DLQ, with an in-memory idempotency store. See the [Invoker Chain]({{ site.baseurl }}/invoker-chain/) pages for the recommended order.

### JSON Serialization

```bash
go get github.com/isaquesb/go-event-bus/json@latest
```

Provides `json.Registry` for encoding/decoding events with envelope format and schema versioning via upcasters.

### NATS Core Transport

```bash
go get github.com/isaquesb/go-event-bus/nats@latest
```

Requires: `github.com/nats-io/nats.go`

### JetStream Transport

```bash
go get github.com/isaquesb/go-event-bus/jetstream@latest
```

Requires: `github.com/nats-io/nats.go/jetstream`

### Redis Stores

```bash
go get github.com/isaquesb/go-event-bus/redis@latest
```

Requires: `github.com/redis/go-redis/v9`

Provides distributed `IdempotencyStore` and `RateLimitStore` backed by Redis.

### OpenTelemetry

```bash
go get github.com/isaquesb/go-event-bus/telemetry@latest
```

Requires: `go.opentelemetry.io/otel`

Provides `telemetry.NewTracerInvoker` for consumer spans and `telemetry.NewObserver` for span events and OpenTelemetry metrics.

### Prometheus Metrics

```bash
go get github.com/isaquesb/go-event-bus/prometheus@latest
```

Requires: `github.com/prometheus/client_golang`

Provides `prometheus.NewObserver`, an `event.Observer` with a fixed Prometheus metric set.

## Modules at a glance

| Module | Import path | Needed for |
|---|---|---|
| core | `github.com/isaquesb/go-event-bus` | Always: interfaces, `LocalBus`, `Observer` |
| `invoker` | `.../invoker` | Retry, circuit breaker, rate limit, idempotency, DLQ |
| `json` | `.../json` | Envelope serialization for NATS and JetStream |
| `nats` | `.../nats` | NATS Core transport |
| `jetstream` | `.../jetstream` | JetStream transport, DLQ publisher, replayer |
| `redis` | `.../redis` | Distributed idempotency and rate-limit stores |
| `telemetry` | `.../telemetry` | OpenTelemetry tracing and metrics |
| `prometheus` | `.../prometheus` | Prometheus metrics |
| `examples` | `.../examples` | Not a dependency: the runnable examples, compiled with the repository |

Each module is versioned and tagged on its own (for example `jetstream/v0.4.0`), and the optional modules require the matching core release.
