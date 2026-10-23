---
layout: default
title: Observability
nav_order: 8
has_children: true
permalink: /observability/
---

# Observability

A bus reports everything that happens to a message through one object, the `event.Observer`. There is exactly one Observer per bus, set in the bus options. Invokers and transports only call it, and each backend implements it:

| What you want | Backend |
|---|---|
| Logs | `event.SlogObserver`, the default when the bus has no Observer |
| Metrics | `prometheus.NewObserver`, or `telemetry.NewObserver` with `WithMeterProvider` for OpenTelemetry metrics |
| Traces | `telemetry.NewTracerInvoker` as the first invoker of the chain, plus `telemetry.NewObserver` for span events |

Latency and outcome counters come from the `HandlerDone` signal that every transport emits. There is no metrics invoker.

Several backends run side by side with `event.MultiObserver`, which also isolates a panic in any of them from the handler.

## Pages

- [Observer]({{ site.baseurl }}/observability/observer/): the ten signals, when each fires, `Failure` exactly once per terminal outcome, writing and combining observers
- [Metrics]({{ site.baseurl }}/observability/metrics/): the eleven metric families, their OpenTelemetry names, label vocabularies and what each `outcome` means
- [Prometheus]({{ site.baseurl }}/observability/prometheus/): setup of `prometheus.NewObserver`
- [Tracing]({{ site.baseurl }}/observability/tracing/): `telemetry.NewTracerInvoker`, spans and span events
