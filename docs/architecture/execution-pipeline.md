---
layout: default
title: Execution Pipeline
parent: Architecture
nav_order: 1
---

# Execution Pipeline

## Invoker Chain Depth

Handlers are never executed directly. Instead, execution flows through a chain of invokers:

```
Invoker → Invoker → ... → Handler
```

Each invoker:
- Has a single responsibility
- Is stateless or externally backed
- Emits its own metrics

The chain uses recursive function composition. Each invoker receives a `next` function that calls the next invoker, until the final handler is reached.

## Recommended Order

```
Tracer
 └─ Metrics
     └─ RateLimiter (may abort early)
         └─ Idempotency (may skip execution)
             └─ Retry (re-executes handler)
                 └─ CircuitBreaker (health decisions)
                     └─ DLQ (last resort)
                         └─ Handler
```

### Rationale

1. **Observability wraps everything** - Tracing and metrics must see all execution including retries and rejections
2. **Cheap rejections happen early** - Rate limiting and idempotency reject fast without wasting resources
3. **Retries before health decisions** - Retry exhaustion occurs before the circuit breaker evaluates health
4. **DLQ is terminal** - The dead letter queue is the last resort, isolated at the end

## Context Flow

Context flows through the chain and can be enriched at each layer:
- Tracing adds span context
- Each invoker can add metadata
- The handler receives the final enriched context

## Error Flow

Errors propagate back through the chain:
- Each invoker can inspect, classify, or transform errors
- The retry invoker re-executes on retryable errors
- The DLQ invoker captures terminal errors
- Unhandled errors bubble up to the bus's `OnErr` callback
