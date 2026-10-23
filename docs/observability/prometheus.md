---
layout: default
title: Prometheus
parent: Observability
nav_order: 4
---

# Prometheus

The `prometheus` module provides `prometheus.NewObserver`, an `event.Observer` that records a fixed set of eleven Prometheus metric families from the bus signals. It replaces the `Provider` of earlier versions: metrics are no longer created on demand from names, so the set cannot drift and a label mismatch cannot panic on the handler goroutine.

```bash
go get github.com/isaquesb/go-event-bus/prometheus
```

## Setup

Build one Observer per registry and pass it as the bus Observer:

```go
import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	event "github.com/isaquesb/go-event-bus"
	eventbusprom "github.com/isaquesb/go-event-bus/prometheus"
)

reg := prometheus.NewRegistry()
obs := eventbusprom.NewObserver(reg, eventbusprom.WithNamespace("myapp"))

bus := event.NewLocalBus(event.LocalBusOptions{Observer: obs})

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

`WithNamespace("myapp")` prefixes every name with `myapp_`, so the handler counter is `myapp_eventbus_handler_total`. The same option works on `nats.BusOptions.Observer` and `jetstream.BusOptions.Observer`.

### Next to the log observer

Setting an Observer replaces the default `SlogObserver`. To keep the log lines as well, combine both with `event.MultiObserver`:

```go
bus := event.NewLocalBus(event.LocalBusOptions{
	Observer: event.MultiObserver(
		event.NewSlogObserver(nil),
		obs,
	),
})
```

`MultiObserver` calls the children in order and isolates a panic in any of them from the handler. A `SlogObserver` that you create yourself is not flushed by the bus, so call its `Flush` on shutdown. See [logs]({{ site.baseurl }}/observability/logs/).

## Rules

- **A nil registry** means `prometheus.DefaultRegisterer`.
- **Registering twice on one registry panics**, like `prometheus.MustRegister`. Build one Observer per registry, or give each its own `WithNamespace`.
- **The histogram buckets** are `prometheus.DefaultBuckets` (`0.001` to `10` seconds), the same as the OpenTelemetry Observer.
- **`Failure` adds no series, by design.** Terminal outcomes are already in `eventbus_handler_total` (`dropped`, `dead_lettered`) and `eventbus_dlq_records_total`, and counting them again would double count.
- **`eventbus_breaker_state` is absent** for a handler whose breaker never changed state. Absent means closed.
- **Labels are bounded.** Handler and event names, plus the closed outcome, reason and state sets. No event id, subject or error text.

## Metrics

Every family, with its labels, its OpenTelemetry twin and the meaning of each `outcome`, is on the [metric reference]({{ site.baseurl }}/observability/metrics/).

The OpenTelemetry metrics API of the `telemetry` module exposes the same set. Pick one backend per process.
