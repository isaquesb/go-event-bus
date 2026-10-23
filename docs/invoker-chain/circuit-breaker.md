---
layout: default
title: Circuit Breaker
parent: Invoker Chain
nav_order: 3
---

# Circuit Breaker Invoker

The circuit breaker prevents cascade failures by monitoring the health of each handler. While a handler keeps failing it rejects calls with `ErrCircuitOpen` instead of running it, and after a pause it lets one call through to find out whether the handler recovered.

## Configuration

```go
invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
    FailureThreshold: 5,
    SuccessThreshold: 2,
    OpenTimeout:      30 * time.Second,
})
```

| Field | Type | Description |
|---|---|---|
| `FailureThreshold` | `int` | Consecutive counted failures that open the circuit |
| `SuccessThreshold` | `int` | Successful probes in half-open that close it |
| `OpenTimeout` | `time.Duration` | How long the circuit stays open before the first probe is admitted. It is also the lease of a probe |
| `OnStateChange` | `func(ctx, event.BreakerTransition)` | Optional callback for every state transition, see [OnStateChange](#onstatechange) |

The circuit breaker is the innermost invoker, next to the handler, see the [composition page]({{ site.baseurl }}/invoker-chain/composition/) for why.

## States

```
     ┌──────────┐   failures >= threshold   ┌──────────┐
     │  Closed  │ ────────────────────────→ │   Open   │
     └──────────┘                           └──────────┘
          ↑                                      │
          │ successes >= threshold   first call  │
          │                          after the   │
          │                          timeout     ↓
     ┌──────────────┐                     ┌──────────────┐
     │              │ ←────────────────── │  Half-Open   │
     └──────────────┘                     └──────────────┘
                          (probe fails → Open again)
```

- **Closed**: normal operation. A counted failure adds to the consecutive count; a success resets it.
- **Open**: every call returns `ErrCircuitOpen` without running the handler.
- **Half-open**: exactly one probe runs at a time. Concurrent callers get `ErrCircuitOpen`.

### The open to half-open transition is lazy

There are no timers. The circuit moves from open to half-open when the **first call after `OpenTimeout`** arrives, and that call becomes the probe. The transition carries reason `open_timeout_elapsed`. Until a call arrives, `State(handler)` keeps reporting `open`.

### The single probe

In half-open only one call is admitted at a time:

- A successful probe frees the slot for the next caller, until `SuccessThreshold` successes close the circuit (reason `probe_succeeded`).
- A counted probe failure reopens the circuit (reason `probe_failed`).
- A neutral probe outcome (see below) keeps the circuit half-open and frees the slot.
- The probe holds a lease of `OpenTimeout`. A probe still running after that is superseded by the next arrival, and its late completion is ignored. A zero `OpenTimeout` never expires the lease.

## Which errors count

The rule is the same in every state, and the classifier is not configurable.

| Counted as a failure | Neutral: never opens, closes or resets anything |
|---|---|
| Unclassified errors and `Retryable()` errors | Terminal errors: `PermanentError`, `ErrSendToDLQ`, a panic |
| Errors with `Retryable() == false` that are not terminal | Context cancellation |
| A handler timeout set by the transport (`ErrHandlerTimeout`) | A bare `context.DeadlineExceeded` on a live context, such as a downstream client timeout |
| | Flow control: `ErrDuplicate`, `ErrDeadLettered`, `ErrDeadLetterFailed`, `ErrInProgress`, `ErrRateLimited`, `ErrCircuitOpen`, `ErrUnknownVersion` |

Business-level terminal errors therefore never trip the circuit. In half-open a neutral outcome is not a success either: it does not count toward `SuccessThreshold`.

## Per-handler isolation

Each handler name gets its own breaker state. A failing `send-email` handler does not affect a healthy `update-db` handler. `State(handler)` returns the `event.BreakerState` (`closed`, `open` or `half_open`) for one handler, `closed` for one that never ran. It never creates a breaker and is safe to call from `OnStateChange`.

## Transitions

Every state change is reported as an `event.BreakerTransition`:

| Field | Meaning |
|---|---|
| `Handler` | Handler name the breaker protects |
| `From`, `To` | State before and after |
| `Reason` | `failure_threshold`, `open_timeout_elapsed`, `probe_succeeded` or `probe_failed` |
| `Failures` | Consecutive failure count when the change is reported: the count that reached the threshold for `failure_threshold`; the count kept from the opening for `open_timeout_elapsed` and `probe_failed`; 0 for `probe_succeeded` |
| `At` | When the change happened |
| `Seq` | Increases by one on every transition of the same handler breaker |
| `BreakerID` | Identifies the `CircuitBreaker` instance, unique in the process and assigned at construction. Together with `Handler` it scopes `Seq`. Zero means unknown |

Transitions can reach an Observer from different goroutines, so do not assume an arrival order: sort by `Seq` within one `(BreakerID, Handler)`. Two breakers that serve the same handler name have different `BreakerID` values and independent `Seq` counters.

The same transition goes to `Observer.BreakerTransition` first and then to `OnStateChange`.

### OnStateChange

`OnStateChange` is the hook for code that needs to react to a transition, such as paging or pausing a consumer.

- It runs **after the breaker lock is released**, on the goroutine of the message that caused the transition, with that message's context.
- It **must not block**: no network call, no unbuffered channel send, no lock held elsewhere. Hand the transition to another goroutine.
- It may call `State` and `Invoke` on the same breaker. Calls from different goroutines may overlap.
- A panic in it, or in the Observer, is recovered and reported as an `InternalError` signal (component `circuit_breaker`, op `panic`). It never fails the message.

A complete setup, copied from [`examples/10_failure_hooks.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/10_failure_hooks.go):

```go
// Transitions are handed to a buffered channel with a non-blocking send and
// processed elsewhere.
transitions := make(chan event.BreakerTransition, 16)

breaker := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
	FailureThreshold: 5,
	SuccessThreshold: 2,
	OpenTimeout:      30 * time.Second,
	// OnStateChange runs on the goroutine of the message that caused the
	// transition. NEVER block in it: no network call, no unbuffered send, no
	// lock held elsewhere. Drop the signal when the consumer is slow.
	OnStateChange: func(ctx context.Context, t event.BreakerTransition) {
		select {
		case transitions <- t:
		default:
			// consumer is behind: drop rather than block the handler
		}
	},
})
```

## Transports

An open circuit is a flow-control failure: the handler never ran, so the message is retried later. On JetStream the redelivery delay is `BusOptions.CircuitOpenDelay`, which should roughly match `OpenTimeout`. On the final delivery the chain DLQ dead-letters it. The result for every transport is in the [failure model]({{ site.baseurl }}/architecture/failure-model/).

## Signals

The breaker emits `BreakerTransition` for each state change and `InternalError` for a callback that panicked. The breaker state is also reported with each terminal failure. State, transition counts and the metric names are on the [Observability]({{ site.baseurl }}/observability/) pages; the breaker itself keeps no metrics.
