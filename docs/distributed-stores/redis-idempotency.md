---
layout: default
title: Redis Idempotency Store
parent: Distributed Stores
nav_order: 1
---

# Redis Idempotency Store

The `redis.IdempotencyStore` implements the `invoker.IdempotencyStore` interface using Redis for distributed duplicate detection. It stores the record the [idempotency invoker]({{ site.baseurl }}/invoker-chain/idempotency/) writes, with a time-to-live that depends on the status.

## Setup

```go
import (
    "github.com/redis/go-redis/v9"
    eventredis "github.com/isaquesb/go-event-bus/redis"
)

rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

store := eventredis.NewIdempotencyStore(rdb)
```

Use with the idempotency invoker:

```go
invoker.NewIdempotency(store, invoker.IdempotencyConfig{
    ProcessingTTL: 5 * time.Minute,
})
```

## Redis Key Structure

The store uses the key it is given, with no prefix of its own. The idempotency invoker builds it as `{idempotency_key}:{handler_name}`.

Example: `order:order-12345:process-order`

### Value

JSON-encoded `IdempotencyRecord`. The record has no JSON tags, so the field names are the Go names, and `EndedAt` is `null` while the status is `processing`:

```json
{"Status": "completed", "StartedAt": "2026-02-05T12:00:00Z", "EndedAt": "2026-02-05T12:00:01Z"}
```

### TTL by Status

| Status | TTL | Rationale |
|---|---|---|
| `processing` | 5 minutes | Lock recovery for crashed pods |
| `completed` | 24 hours | Duplicate prevention window |
| `failed` | 1 hour | Allow retry after failure |
| `dead_lettered` | 1 hour | The message is in the DLQ; a replay is allowed to run again |

`failed` and `dead_lettered` share the default branch of the store: any status other than `processing` and `completed` is kept for one hour. These two statuses are not completions, so a delivery that finds one runs the handler again; the TTL only bounds how long the record is kept.

## Operations

### Put

Uses `SET` with the TTL of the status. It overwrites any existing value, which is how a record moves from `processing` to `completed`, `failed` or `dead_lettered`, and the new status brings its own TTL.

### Get

Uses `GET`. Returns `(record, true, nil)` if found, `(empty, false, nil)` if not found. A Redis or decoding error is returned as an error, and the idempotency invoker reports it as a `StoreError` signal (`Store` `idempotency`, `Op` `get`) and fails the delivery, which the transport retries.

### Delete

Uses `DEL` to remove a key.

## Stale Lock Recovery

If a pod crashes while processing (status `processing`), the marker is not removed. Two things end it:

- The invoker compares the marker's age with `ProcessingTTL`. An older marker is taken over by the next delivery, and the invoker emits the `IdempotencyStale` signal.
- Redis expires the key 5 minutes after it was written.

The `processing` TTL in Redis is fixed at 5 minutes and does not follow `ProcessingTTL`. A `ProcessingTTL` above 5 minutes therefore has no effect with this store, because the key is gone first. A `ProcessingTTL` below 5 minutes takes the marker over earlier. Until the marker is stale, a delivery that finds it gets `ErrInProgress` and is retried later, never acknowledged as a duplicate.

## Errors

A failing `Get` or `Put` is a store error, reported as a `StoreError` signal on the invoker. A failing `Put` of the first `processing` marker or a failing `Get` stops the handler and returns the error. A failing final `Put` leaves the result of the handler as it is and the `processing` marker to expire. The signal names are on the [Observability]({{ site.baseurl }}/observability/) pages.
