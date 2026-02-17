---
layout: default
title: Prometheus
parent: Observability
nav_order: 2
---

# Prometheus Provider

The `prometheus.Provider` implements `invoker.MetricProvider` using Prometheus client libraries.

## Setup

```go
import eventprom "github.com/isaquesb/go-event-bus/prometheus"

provider := eventprom.NewProvider("myapp")
```

The namespace is prepended to all metric names: `myapp_eventbus_invoker_calls_total`.

### Options

```go
// Custom registerer (useful for testing)
provider := eventprom.NewProvider("myapp",
    eventprom.WithRegisterer(customRegistry),
)
```

## How It Works

The provider dynamically creates and caches Prometheus metrics (counters, histograms, gauges) based on the metric names and labels provided by invokers. Metrics are created on first use and reused thereafter.

## Metric Names

All invokers emit metrics with the `eventbus_` prefix:

### Core Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `eventbus_invoker_calls_total` | Counter | event, handler, result | Metrics invoker |
| `eventbus_invoker_latency_seconds` | Histogram | event, handler, result | Metrics invoker |

### Retry Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `eventbus_retry_attempt_total` | Counter | handler | Retry invoker |
| `eventbus_retry_success_total` | Counter | handler | Retry invoker |
| `eventbus_retry_terminal_total` | Counter | handler | Retry invoker |
| `eventbus_retry_exhausted_total` | Counter | handler | Retry invoker |
| `eventbus_retry_backoff_ms` | Histogram | handler | Retry invoker |

### Circuit Breaker Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `eventbus_circuit_state` | Gauge | handler | Circuit breaker |
| `eventbus_circuit_open_total` | Counter | handler | Circuit breaker |
| `eventbus_circuit_half_open_total` | Counter | handler | Circuit breaker |
| `eventbus_circuit_blocked_total` | Counter | handler | Circuit breaker |

### Rate Limit Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `eventbus_ratelimit_allowed_total` | Counter | handler | Rate limiter |
| `eventbus_ratelimit_blocked_total` | Counter | handler | Rate limiter |
| `eventbus_ratelimit_error_total` | Counter | handler | Rate limiter |

### Idempotency Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `eventbus_idempotency_duplicate_total` | Counter | handler, reason | Idempotency |
| `eventbus_idempotency_lock_acquired_total` | Counter | handler | Idempotency |
| `eventbus_idempotency_executed_total` | Counter | handler | Idempotency |
| `eventbus_idempotency_processing_stale_total` | Counter | handler | Idempotency |
| `eventbus_idempotency_error_total` | Counter | handler, stage | Idempotency |

### DLQ Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `eventbus_dlq_published_total` | Counter | handler | DLQ invoker |
| `eventbus_dlq_publish_error_total` | Counter | handler | DLQ invoker |

## Histogram Buckets

The `eventbus_invoker_latency_seconds` histogram uses custom buckets optimized for event processing:

```
0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
```

All other histograms use Prometheus default buckets.
