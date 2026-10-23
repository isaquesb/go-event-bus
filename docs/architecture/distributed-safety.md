---
layout: default
title: Distributed Safety
parent: Architecture
nav_order: 3
---

# Distributed Safety

## Multi-Pod Considerations

When running multiple instances, in-memory stores are insufficient. Go Event Bus provides Redis-backed stores for distributed safety. This page states what they guarantee across pods, and where the guarantees end.

## Idempotency

- Implemented via external store (Redis in production)
- Guards against duplicates across pods
- Handler-scoped keys: `{event_key}:{handler_name}`
- Key extraction priority: `WithIdempotencyKey` > `WithId`

### Statuses and what a second delivery sees

| Record found | Second delivery |
|---|---|
| `completed` | `ErrDuplicate`: acknowledged, the handler does not run |
| `processing`, younger than `ProcessingTTL` | `ErrInProgress`: retried later, never acknowledged as a duplicate |
| `processing`, older than `ProcessingTTL` | Taken over and executed, with an `IdempotencyStale` signal |
| `failed` | Executed again |
| `dead_lettered` | Executed again: the message was moved to the DLQ, its work was not done |

The in-progress protection is what keeps a second pod from running a handler that a first pod is still running. A message that finds a fresh `processing` marker is retried with a delay, and on the final delivery the transport backstop dead-letters it. It is not dropped as a duplicate, because the pod that holds the marker may have crashed. A store failure on any call is a `StoreError` signal and fails the delivery, so the transport retries it.

### Redis Implementation

- **Get-then-Put** pattern with `SET` command
- **Status-based TTL**: processing (5m, fixed), completed (24h), failed and dead_lettered (1h)
- **Stale lock recovery**: if a pod crashes while processing, the marker ages past `ProcessingTTL` (or expires after 5 minutes) and another pod can take it over
- The invoker checks existence via `Get` before `Put`

See [Redis Idempotency Store]({{ site.baseurl }}/distributed-stores/redis-idempotency/) for the keys and the TTL table.

## Rate Limiting

- Distributed token bucket
- Keyed by event-defined identity (`WithRateLimitKey`)
- Supports burst control
- A rejected message is retried later by the transport; a store failure is a `StoreError` signal and also fails the delivery, it does not let the message through

### Redis Implementation

- **Lua script** for atomic check-and-decrement
- **Token bucket** with refill based on elapsed time, read from the Redis server clock
- Configurable rate, period, and burst

## Dead-letter queue

- DLQ records are **at-least-once**. A crash between the dead-letter publish and the acknowledgement, or a lost acknowledgement, can publish the same record twice. The publish carries no message id, so the DLQ stream does not deduplicate. Whatever consumes the DLQ must tolerate duplicates.
- The bus needs the same publisher as the DLQ invoker so failures the chain cannot see (undecodable messages, rate-limit and in-progress rejections on the final delivery) are dead-lettered too. See [DLQ Invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/).
- A replay runs the handler again, from scratch. Combined with idempotency this is intended: a `dead_lettered` record does not block it.
- A stream that is compacted (one message kept per subject) changes what a consumer can see and what a replay can restore, and it must never hold the DLQ. See the [log-compaction guide]({{ site.baseurl }}/guides/log-compaction/).

## Publishing and duplicates

- JetStream publishes every message with `Nats-Msg-Id` set to the event id, and the stream deduplicates inside its duplicate window (2 minutes by default). A publish retry after a lost acknowledgement carries the same id, so it is stored once.
- An application-level re-`Emit` of the same business event is deduplicated only if it carries the same id, because a generated id differs on every call. Set the id with `event.WithID` or give the event an `Id()` method.
- The window ends: a duplicate that arrives later than the window is stored. The idempotency store is the guard for that case.

## Ordering

There is no first-in-first-out (FIFO) guarantee across concurrent handlers. LocalBus tasks have no ordering guarantee, several pods consume the same stream, and a message that is retried with a delay is redelivered after later ones. Make handlers independent of arrival order, for example with version numbers in the event.

## Consistency Guarantees

The distributed stores provide **best-effort consistency**:

- Idempotency: At-most-once processing under normal conditions. The check (`Get`) and the marker (`Put`) are two store calls, so two pods that receive the same key at the same instant can both run. Stale lock recovery allows re-processing after pod failure.
- Delivery: at-least-once on JetStream. A handler can run more than once for the same message, and a dead-lettered message can appear in the DLQ more than once.
- Rate limiting: Approximate rate enforcement. The refill follows the Redis server clock, and a store outage fails deliveries instead of bypassing the limit.

For strict exactly-once processing, combine idempotency with transactional handlers.

What happens to each error on each transport is in the [failure model]({{ site.baseurl }}/architecture/failure-model/).
