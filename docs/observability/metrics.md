---
layout: default
title: Metrics
parent: Observability
nav_order: 3
---

# Metric reference

The metric set is fixed. Two Observers record it: `prometheus.NewObserver` ([setup]({{ site.baseurl }}/observability/prometheus/)) and `telemetry.NewObserver(telemetry.WithMeterProvider(mp))` through the OpenTelemetry metrics API. Both declare every instrument once, up front, with fixed label names, so a label mismatch cannot panic on the handler goroutine. Use one metrics backend per process, not both, or every message is counted twice.

Both families are fed by the same [Observer signals]({{ site.baseurl }}/observability/observer/). There is no metrics invoker: latency and outcome come from `HandlerDone`.

## The eleven families

| Prometheus name | Type | Labels | Fed by | OpenTelemetry name | Unit |
|---|---|---|---|---|---|
| `eventbus_handler_duration_seconds` | histogram | `handler`, `event`, `outcome` | `HandlerDone` | `eventbus.handler.duration` | `s` |
| `eventbus_handler_total` | counter | `handler`, `event`, `outcome` | `HandlerDone` | `eventbus.handler` | `{message}` |
| `eventbus_retry_attempts_total` | counter | `handler`, `event`, `reason` | `RetryAttempt` | `eventbus.retry.attempts` | `{attempt}` |
| `eventbus_breaker_transitions_total` | counter | `handler`, `from`, `to`, `reason` | `BreakerTransition` | `eventbus.breaker.transitions` | `{transition}` |
| `eventbus_breaker_state` | gauge | `handler` | `BreakerTransition` | `eventbus.breaker.state` | `{state}` |
| `eventbus_dlq_records_total` | counter | `handler`, `event`, `reason` | `DeadLettered` | `eventbus.dlq.records` | `{record}` |
| `eventbus_dlq_publish_errors_total` | counter | `handler`, `event` | `DLQPublishFailed` | `eventbus.dlq.publish.errors` | `{error}` |
| `eventbus_decode_failures_total` | counter | `reason` | `DecodeFailed` | `eventbus.decode.failures` | `{failure}` |
| `eventbus_store_errors_total` | counter | `handler`, `store`, `op` | `StoreError` | `eventbus.store.errors` | `{error}` |
| `eventbus_idempotency_stale_total` | counter | `handler` | `IdempotencyStale` | `eventbus.idempotency.stale` | `{lock}` |
| `eventbus_internal_errors_total` | counter | `component`, `op` | `InternalError` | `eventbus.internal.errors` | `{error}` |

`Failure` adds no series in either backend, by design. Terminal outcomes are already counted by `eventbus_handler_total` (as `dropped` or `dead_lettered`) and by `eventbus_dlq_records_total`, so counting them again would double count.

`WithNamespace(ns)` on the Prometheus Observer prefixes every name with `ns_`, so `WithNamespace("shop")` gives `shop_eventbus_handler_total`. The OpenTelemetry names carry no namespace. The OpenTelemetry-to-Prometheus translation maps dots to underscores, appends `_seconds` for the unit `s` and `_total` to counters, so the two sets line up.

The histogram buckets are the same in both backends: `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10` seconds (`prometheus.DefaultBuckets`).

## Label vocabularies

Every label value is either a handler or event name you chose, or a member of a closed set. Event ids, subjects and error text are **never** labels, and neither is anything taken from the payload. Keep it that way in your own dashboards and recording rules.

| Label | Values |
|---|---|
| `handler`, `event` | The handler name (`event.WithHandlerName`) and the event name (`Name()`) |
| `outcome` | `ok`, `retry`, `duplicate`, `in_progress`, `rate_limited`, `circuit_open`, `dead_lettered`, `dropped`, `interrupted`, `failed` |
| `reason` on `eventbus_retry_attempts_total` | The attempt class: `transient`, `terminal`, `timeout`, `panic` |
| `reason` on `eventbus_dlq_records_total` | The stable dead-letter reason of the message, for example `retries_exhausted`, `delivery_exhausted`, `terminal`, `panic`, `circuit_open`, `rate_limited`, `unknown_version`, `in_progress` or a `decode_*` reason. The full set is the reason vocabulary of the error classification page |
| `reason` on `eventbus_decode_failures_total` | `decode_malformed`, `decode_unknown_event`, `decode_missing_upcaster`, `decode_failed` |
| `from`, `to` | `closed`, `open`, `half_open` |
| `reason` on `eventbus_breaker_transitions_total` | `failure_threshold`, `open_timeout_elapsed`, `probe_succeeded`, `probe_failed` |
| `store` | `idempotency`, `rate_limit` |
| `op` on `eventbus_store_errors_total` | For `idempotency`: `get`, `put_processing`, `put_completed`, `put_failed`, `put_dead_lettered`. For `rate_limit`: `allow` |
| `component` | `transport`, `circuit_breaker`, `event` |
| `op` on `eventbus_internal_errors_total` | `consume`, `ack`, `nak`, `term`, `reply`, `encode_reply`, `panic`, `unsubscribe`, `drain` |

The reasons and their meaning are listed on the [error classification]({{ site.baseurl }}/invoker-chain/error-classification/) page.

## What `outcome` means

`eventbus_handler_total{outcome}` counts one handler chain invocation per increment, with what happened to the message:

| Outcome | Meaning |
|---|---|
| `ok` | The chain succeeded |
| `duplicate` | The idempotency store had already completed this execution |
| `retry` | The delivery failed and the transport will redeliver it |
| `circuit_open`, `rate_limited`, `in_progress` | The chain was refused by the circuit breaker, the rate limiter or an execution in progress, and **the message will be retried** |
| `dead_lettered` | The message was published to the DLQ |
| `dropped` | The message was lost, with no DLQ publisher configured |
| `interrupted` | The subscription or the bus ended while the handler ran |
| `failed` | A terminal failure that was not dead-lettered, for example a failed DLQ publish |

The flow-control outcomes always mean "will be retried", never "lost". On the **final delivery** nothing retries the message, so the loss is counted as what it is: `dropped` when no DLQ publisher is configured, `dead_lettered` when the dead-letter publish succeeded, `failed` when it did not. The outcome therefore agrees with the single `Failure` signal of that delivery.

Two consequences for alerting:

- A rule that watches `circuit_open` or `rate_limited` sees pressure, not loss. To count lost messages, alert on `dropped` and `failed`, and watch `dead_lettered` and `eventbus_dlq_records_total` for the DLQ rate.
- A message that could not be decoded never reaches a handler, so it does not appear in `eventbus_handler_total`. It is counted by `eventbus_decode_failures_total`.

## The breaker gauge

`eventbus_breaker_state` is `0` for closed, `1` for open and `2` for half open. It is set only from `BreakerTransition`, so a handler whose series is absent has a breaker that never changed state, which means closed.

Transitions are delivered on the goroutine of the message that caused them and can arrive out of order. The gauge ignores a late one: it is ordered by `Seq` **per breaker instance**, that is per pair of `handler` and `BreakerID`. An older `Seq` of the same breaker never overwrites a newer state. A breaker that is rebuilt for the same handler name gets a new `BreakerID` and starts a new order, and the gauge follows it. The series itself is labeled by `handler` only.

The open-to-half-open transition is emitted lazily, when the first probe is admitted after `OpenTimeout`. Between the timeout and the first call the gauge still reads open.

## Migrating from v0.3.0

The v0.3.0 Provider created metrics on demand under other names. The old to new table is in the [CHANGELOG](https://github.com/isaquesb/go-event-bus/blob/main/CHANGELOG.md), which is the migration guide for this release. This page documents the current set only.
