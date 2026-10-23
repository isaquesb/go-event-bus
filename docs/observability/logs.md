---
layout: default
title: Logs
parent: Observability
nav_order: 2
---

# Logs

`event.SlogObserver` turns the bus signals into structured `log/slog` records. It is the **default Observer of every bus**: when `Observer` is `nil` in `LocalBusOptions`, `nats.BusOptions` or `jetstream.BusOptions`, the bus installs `event.NewSlogObserver(nil)`, so failures are never silent with zero configuration. Pass `event.NopObserver{}` to silence everything.

```go
obs := event.NewSlogObserver(logger, event.WithLogEvery(30*time.Second))
defer obs.Flush()

bus := event.NewLocalBus(event.LocalBusOptions{Observer: obs})
```

`NewSlogObserver(logger, opts...)` writes to `logger`. A `nil` logger means `slog.Default()`, resolved at log time, so the observer follows later `slog.SetDefault` calls.

## What is logged

| Signal | Level | Message |
|---|---|---|
| `Failure` | `Error`, or `Warn` for a dead-lettered message and for outcome `rejected` | `eventbus: message failed` |
| `DeadLettered` | `Info` | `eventbus: message dead-lettered` |
| `DecodeFailed` | `Warn` | `eventbus: decode failed` |
| `DLQPublishFailed` | `Error` | `eventbus: dead-letter publish failed` |
| `StoreError` | `Error` | `eventbus: store error` |
| `IdempotencyStale` | `Warn` | `eventbus: stale idempotency lock` |
| `InternalError` | `Error` | `eventbus: internal error` |
| `BreakerTransition` | `Warn` when the breaker opens, `Info` otherwise | `eventbus: circuit breaker transition` |
| `RetryAttempt` | `Debug` | `eventbus: retry scheduled` |
| `HandlerDone` | nothing | Metrics and traces cover it |

The `Failure` record carries `outcome`, `reason`, `handler`, `event`, `event_id`, `correlation_id`, `attempts`, `delivery_count`, `max_deliver`, `subject`, `invoker`, `breaker`, `history_len` and `err`. `event` is left out on a decode failure, and `event_id`, `correlation_id` and `breaker` when they are empty. When the cause is a recovered panic, `Failure` and `InternalError` also carry a `stack` attribute with the stack of the panic. The payload is never logged: the signals do not contain it.

## Deduplication

A failure storm should produce one line and a summary, not thousands of lines. The observer deduplicates by a key of **signal, handler, outcome and reason**:

- The first record of a key in a window is logged at once.
- Repeats inside the window are only counted.
- When the window ends, one **summary record** is logged at the level of the first record, with the message `eventbus: repeated log records suppressed` and the attributes `signal`, `handler`, `outcome`, `reason`, `suppressed` (how many records were left out) and `window`.

```go
// Within a 30s window the first record is logged and the rest are counted
// into one summary record.
event.NewSlogObserver(nil, event.WithLogEvery(30*time.Second))
```

| Setting | Behavior |
|---|---|
| Default window | `event.DefaultLogEvery`, 10 seconds |
| `WithLogEvery(d)` | Sets the window |
| `WithLogEvery(0)` | Disables deduplication, every record is logged. A negative value does the same |

Details that matter:

- **Circuit breaker transitions and retry attempts are never deduplicated.** Every one is logged.
- **The key space is capped** at 1024 keys. When it is full, keys whose window ended with nothing suppressed are evicted first. If it is still full, the record is logged untracked: a record is never lost to the cap, it just is not deduplicated.
- **Deduplication only affects logs.** Metrics and traces stay exact, so a dashboard still counts every failure.
- The key parts that signals do not have are left empty. For `StoreError` the reason is `store:op`, for `InternalError` it is `component:op`.

### Flush

`Flush` logs every pending summary at once and stops all window timers. Without it, the suppressed count of a window that is still open at shutdown would be lost.

- A bus **flushes only the default observer it created itself**, at the end of `Shutdown`, on every return path. `Close` calls `Shutdown`, so it flushes too.
- An observer **you create** is never flushed by the bus. Its owner calls `Flush`, as in the snippet at the top of the page. This also applies when you wrap it in `event.MultiObserver`.

## Combining with other backends

A custom Observer replaces the default one. To keep the log lines next to metrics or your own alerts, add the `SlogObserver` to a `MultiObserver`, see [Observer]({{ site.baseurl }}/observability/observer/#combining-observers) and [Prometheus]({{ site.baseurl }}/observability/prometheus/).
