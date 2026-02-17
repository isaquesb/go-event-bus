---
layout: default
title: Redis Idempotency Store
parent: Distributed Stores
nav_order: 1
---

# Redis Idempotency Store

The `redis.IdempotencyStore` implements the `invoker.IdempotencyStore` interface using Redis for distributed duplicate detection.

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
}, metrics)
```

## Redis Key Structure

Keys follow the pattern: `{idempotency_key}:{handler_name}`

Example: `order:order-12345:process-order`

### Value

JSON-encoded `IdempotencyRecord`:

```json
{"status": "completed", "started_at": "2026-02-05T12:00:00Z", "ended_at": "2026-02-05T12:00:01Z"}
```

### TTL by Status

| Status | TTL | Rationale |
|---|---|---|
| `processing` | 5 minutes | Lock recovery for crashed pods |
| `completed` | 24 hours | Duplicate prevention window |
| `failed` | 1 hour | Allow retry after failure |

## Operations

### Put

Uses `SET` with TTL based on status. Overwrites existing values (used when transitioning from "processing" to "completed"/"failed").

### Get

Uses `GET`. Returns `(record, true, nil)` if found, `(empty, false, nil)` if not found.

### Delete

Uses `DEL` to remove a key.

## Stale Lock Recovery

If a pod crashes while processing (status = "processing"), the TTL ensures the lock expires after 5 minutes. The idempotency invoker detects stale locks and re-acquires them.
