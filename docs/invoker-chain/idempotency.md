---
layout: default
title: Idempotency
parent: Invoker Chain
nav_order: 5
---

# Idempotency Invoker

The idempotency invoker prevents duplicate event processing using an external store.

## Configuration

```go
invoker.NewIdempotency(
    store,   // IdempotencyStore implementation
    invoker.IdempotencyConfig{
        ProcessingTTL: 5 * time.Minute,
    },
    metrics,
)
```

| Field | Type | Description |
|---|---|---|
| `ProcessingTTL` | `time.Duration` | How long a "processing" lock is valid (default: 5m) |
| `KeyFunc` | `func(Event, string) string` | Optional custom key extractor |

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
    Status    IdempotencyStatus  // "processing", "completed", "failed"
    StartedAt time.Time
    EndedAt   *time.Time
}
```

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

1. Extract key from event
2. Check store: does key exist?
   - **Completed** → return `ErrDuplicate`
   - **Processing** (within TTL) → return `ErrDuplicate`
   - **Processing** (stale) → continue (lock recovery)
3. Put "processing" record
4. Execute handler
5. Update record to "completed" or "failed"

## Metrics

| Metric | Type | Description |
|---|---|---|
| `eventbus_idempotency_duplicate_total` | Counter | Duplicate events blocked |
| `eventbus_idempotency_lock_acquired_total` | Counter | Processing locks acquired |
| `eventbus_idempotency_executed_total` | Counter | Handlers executed |
| `eventbus_idempotency_processing_stale_total` | Counter | Stale locks recovered |
| `eventbus_idempotency_error_total` | Counter | Store errors |
