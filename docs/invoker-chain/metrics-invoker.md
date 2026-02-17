---
layout: default
title: Metrics Invoker
parent: Invoker Chain
nav_order: 6
---

# Metrics Invoker

The metrics invoker records latency and success/failure counts for every handler invocation.

## Configuration

```go
invoker.NewMetrics(provider) // MetricProvider implementation
invoker.NewMetrics(nil)      // Uses NoopProvider (silent)
```

## MetricProvider Interface

```go
type MetricProvider interface {
    IncCounter(name string, n int64, labels Labels)
    ObserveHistogram(name string, value float64, labels Labels)
    SetGauge(name string, value float64, labels Labels)
}
```

Where `Labels` is `map[string]string`.

### Built-in Providers

- **NoopProvider**: Discards all metrics (default when `nil` is passed)
- **Prometheus Provider**: `prometheus.NewProvider(namespace)` (see [Prometheus]({{ site.baseurl }}/observability/prometheus/))

## Collected Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `eventbus_invoker_latency_seconds` | Histogram | event, handler, result | Handler execution latency |
| `eventbus_invoker_calls_total` | Counter | event, handler, result | Total handler invocations |

The `result` label is `"ok"` on success and `"err"` on failure.

## Position in Chain

The metrics invoker should be placed **early** in the chain (after tracing) to capture the full execution time including retries, circuit breaker decisions, etc.
