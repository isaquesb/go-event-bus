---
layout: default
title: Installation
parent: Getting Started
nav_order: 1
---

# Installation

## Core Package

```bash
go get github.com/isaquesb/go-event-bus
```

This gives you the core interfaces (`Event`, `Bus`, `Invoker`), `LocalBus`, and the full `invoker` package (chain, retry, circuit breaker, rate limiter, idempotency, DLQ, metrics).

## Requirements

- **Go 1.23** or later

## Optional Dependencies

Install only the subpackages you need:

### JSON Serialization

```bash
go get github.com/isaquesb/go-event-bus/json
```

Provides `json.Registry` for encoding/decoding events with envelope format and schema versioning via upcasters.

### NATS Core Transport

```bash
go get github.com/isaquesb/go-event-bus/nats
```

Requires: `github.com/nats-io/nats.go`

### JetStream Transport

```bash
go get github.com/isaquesb/go-event-bus/jetstream
```

Requires: `github.com/nats-io/nats.go/jetstream`

### Redis Stores

```bash
go get github.com/isaquesb/go-event-bus/redis
```

Requires: `github.com/redis/go-redis/v9`

Provides distributed `IdempotencyStore` and `RateLimitStore` backed by Redis.

### OpenTelemetry Tracing

```bash
go get github.com/isaquesb/go-event-bus/telemetry
```

Requires: `go.opentelemetry.io/otel`

### Prometheus Metrics

```bash
go get github.com/isaquesb/go-event-bus/prometheus
```

Requires: `github.com/prometheus/client_golang`
