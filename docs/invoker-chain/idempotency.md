---
layout: default
title: Idempotency
parent: Invoker Chain
nav_order: 5
---

# Idempotency Invoker

The idempotency invoker prevents duplicate event processing using an external store. It records each execution as `processing`, then as `completed`, `failed` or `dead_lettered`, and uses that record to skip a duplicate, to hold off a concurrent run, and to take over a run that crashed.

## Configuration

```go
invoker.NewIdempotency(
    store,   // IdempotencyStore implementation
    invoker.IdempotencyConfig{
        ProcessingTTL: 5 * time.Minute,
    },
)
```

| Field | Type | Description |
|---|---|---|
| `ProcessingTTL` | `time.Duration` | How long a `processing` marker is respected before another execution may take it over (default: 5m) |
| `KeyFunc` | `func(Event, string) string` | Declared on the config but not consulted by the invoker: the key always comes from the event, see [Key Extraction](#key-extraction) |

Idempotency sits outside the DLQ invoker, so it sees the `ErrDeadLettered` the DLQ returns. The reason for each position is on the [composition page]({{ site.baseurl }}/invoker-chain/composition/).

## IdempotencyStore Interface

```go
type IdempotencyStore interface {
    Get(ctx context.Context, key string) (IdempotencyRecord, bool, error)
    Put(ctx context.Context, key string, rec IdempotencyRecord) error
    Delete(ctx context.Context, key string) error
}
```

### IdempotencyRecord

```go
type IdempotencyRecord struct {
    Status    IdempotencyStatus  // "processing", "completed", "failed", "dead_lettered"
    StartedAt time.Time
    EndedAt   *time.Time
}
```

`StartedAt` is when the `processing` marker was written. The final record keeps the same `StartedAt` and adds `EndedAt`.

### Statuses

| Status | Written when | A later delivery with the same key |
|---|---|---|
| `processing` | Before the handler runs | `ErrInProgress` while younger than `ProcessingTTL`; taken over when older |
| `completed` | The handler returned `nil` | `ErrDuplicate`: acknowledged, the handler does not run |
| `failed` | The handler returned an error that was not dead-lettered | Runs again |
| `dead_lettered` | The error is `ErrDeadLettered`: the DLQ invoker published the message | Runs again |

Only `completed` is a completion. A `dead_lettered` record says the message was moved to the DLQ, not that its work was done, so a replay of the dead-lettered message with the same key executes the handler. That is what makes a replay useful.

## Built-in Stores

### MemoryIdempotencyStore

For testing and single-instance deployments:

```go
store := invoker.NewMemoryIdempotencyStore()
```

### RedisIdempotencyStore

For production multi-pod deployments:

```go
store := redis.NewIdempotencyStore(redisClient)
```

See [Redis Idempotency Store]({{ site.baseurl }}/distributed-stores/redis-idempotency/) for details.

## Key Extraction

Keys are extracted from events with priority:

1. `WithIdempotencyKey` → `event.IdempotencyKey()`
2. `WithId` → `event.Id()`
3. Neither → **idempotency check is skipped**

The handler name is appended to the key: `"payment:pay-001:process-payment"`

## Flow

1. Extract the key from the event. Without one, run the handler.
2. `Get` the record. A store error is returned as it is, with a `StoreError` signal, and the handler does not run.
3. Decide from the record:
   - **`completed`**: return `ErrDuplicate`. The transport acknowledges the message.
   - **`processing`, younger than `ProcessingTTL`**: return `ErrInProgress`. Another execution may still be running, or its pod may have crashed, so the message is **not** acknowledged as a duplicate: the transport retries it later, and on the final delivery the transport backstop dead-letters it.
   - **`processing`, older than `ProcessingTTL`**: the earlier run is presumed dead. Take it over, emit `IdempotencyStale` with the marker's age, and continue.
   - **`failed`, `dead_lettered` or no record**: continue.
4. `Put` a `processing` record with `StartedAt` set to now.
5. Run the handler.
6. `Put` the final record: `completed` for `nil`, `dead_lettered` for `ErrDeadLettered`, `failed` for any other error. The error goes back unchanged.

A failing final `Put` does not change the handler result. It leaves the `processing` marker in place to age out, and is reported as a `StoreError`.

The check in step 2 and the write in step 4 are two store calls, not one atomic operation, so two pods that receive the same key at the same instant can both run. The marker closes the window for everything after that. See [Distributed Safety]({{ site.baseurl }}/architecture/distributed-safety/).

## Signals

| Signal | When |
|---|---|
| `StoreError` | A store call failed. `Store` is `idempotency` and `Op` is `get`, `put_processing`, `put_completed`, `put_failed` or `put_dead_lettered` |
| `IdempotencyStale` | A `processing` marker older than `ProcessingTTL` was taken over. Carries the handler, the event and the marker's age |

A duplicate and an execution in progress emit no diagnostic signal of their own: they are `HandlerDone` outcomes (`duplicate` and `in_progress`). The names of the signals and the metrics are on the [Observability]({{ site.baseurl }}/observability/) pages. The invoker keeps no metrics.
