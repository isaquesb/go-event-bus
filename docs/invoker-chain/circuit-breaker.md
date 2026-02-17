---
layout: default
title: Circuit Breaker
parent: Invoker Chain
nav_order: 3
---

# Circuit Breaker Invoker

The circuit breaker prevents cascade failures by monitoring handler health per handler.

## Configuration

```go
invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
    FailureThreshold: 5,
    SuccessThreshold: 2,
    OpenTimeout:      30 * time.Second,
}, metrics)
```

| Field | Type | Description |
|---|---|---|
| `FailureThreshold` | `int` | Consecutive failures before opening |
| `SuccessThreshold` | `int` | Successes in half-open to close |
| `OpenTimeout` | `time.Duration` | Time before transitioning to half-open |

## States

```
     ┌──────────┐   failures >= threshold   ┌──────────┐
     │  Closed  │ ────────────────────────→ │   Open   │
     └──────────┘                           └──────────┘
          ↑                                      │
          │ successes >= threshold     timeout    │
          │                                      ↓
     ┌──────────────┐                     ┌──────────────┐
     │              │ ←────────────────── │  Half-Open   │
     └──────────────┘                     └──────────────┘
                          (on failure → Open again)
```

- **Closed**: Normal operation. Failures increment counter; successes reset it.
- **Open**: All requests immediately return `ErrCircuitOpen`. After `OpenTimeout`, transitions to half-open.
- **Half-Open**: Trial requests are allowed. Successes close the circuit; any failure reopens it.

## Per-Handler Isolation

Each handler gets its own circuit breaker instance. A failing "send-email" handler won't affect a healthy "update-db" handler.

## Terminal Error Handling

Errors marked with `ErrSendToDLQ` (from the retry invoker) do **not** count as failures for the circuit breaker. This prevents business-level terminal errors from tripping the circuit.

## Metrics

| Metric | Type | Description |
|---|---|---|
| `eventbus_circuit_state` | Gauge | Current state (0=closed, 1=open, 2=half-open) |
| `eventbus_circuit_open_total` | Counter | Times circuit opened |
| `eventbus_circuit_half_open_total` | Counter | Times circuit went half-open |
| `eventbus_circuit_blocked_total` | Counter | Requests blocked by open circuit |
