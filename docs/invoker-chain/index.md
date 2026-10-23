---
layout: default
title: Invoker Chain
nav_order: 4
has_children: true
permalink: /invoker-chain/
---

# Invoker Chain

The invoker chain is the execution pipeline for event handlers. Every handler invocation flows through the chain, outermost invoker first:

```
Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler
```

Each invoker has a single responsibility and is stateless or externally backed. The reason for each position is on the [Composition]({{ site.baseurl }}/invoker-chain/composition/) page.

Latency and outcome are not measured by an invoker: the bus emits `HandlerDone` to its Observer for every invocation. See [Observability]({{ site.baseurl }}/observability/).
