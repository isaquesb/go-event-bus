---
layout: default
title: Invoker Chain
nav_order: 4
has_children: true
permalink: /invoker-chain/
---

# Invoker Chain

The invoker chain is the execution pipeline for event handlers. Every handler invocation flows through the chain:

```
Tracer
 └─ Metrics
     └─ RateLimiter (may abort early)
         └─ Idempotency (may skip execution)
             └─ Retry (re-executes on failure)
                 └─ CircuitBreaker (health decisions)
                     └─ DLQ (last resort)
                         └─ Handler
```

Each invoker has a single responsibility, is stateless or externally backed, and emits its own metrics.
