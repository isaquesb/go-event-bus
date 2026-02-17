---
layout: default
title: Observability
nav_order: 8
has_children: true
permalink: /observability/
---

# Observability

Go Event Bus provides first-class observability through two mechanisms:

- **Tracing** via OpenTelemetry - One span per handler invocation, with trace propagation across transport boundaries
- **Metrics** via a pluggable `MetricProvider` interface - Latency, success/failure counts, and state gauges from every invoker

Every invoker in the chain emits its own metrics, giving complete visibility into the execution pipeline.
