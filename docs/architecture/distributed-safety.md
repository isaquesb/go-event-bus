---
layout: default
title: Distributed Safety
parent: Architecture
nav_order: 3
---

# Distributed Safety

## Multi-Pod Considerations

When running multiple instances, in-memory stores are insufficient. Go Event Bus provides Redis-backed stores for distributed safety.

## Idempotency

- Implemented via external store (Redis in production)
- Guards against duplicates across pods
- Handler-scoped keys: `{event_key}:{handler_name}`
- Key extraction priority: `WithIdempotencyKey` > `WithId`

### Redis Implementation

- **Get-then-Put** pattern with `SET` command
- **Status-based TTL**: processing (5m), completed (24h), failed (1h)
- **Stale lock recovery** via TTL expiration - if a pod crashes while processing, the lock expires and another pod can retry
- The invoker checks existence via `Get` before `Put`

## Rate Limiting

- Distributed token bucket
- Keyed by event-defined identity (`WithRateLimitKey`)
- Supports burst control

### Redis Implementation

- **Lua script** for atomic check-and-decrement
- **Sliding window** with token refill based on elapsed time
- Configurable rate, period, and burst

## Consistency Guarantees

The distributed stores provide **best-effort consistency**:

- Idempotency: At-most-once processing under normal conditions. Stale lock recovery allows re-processing after pod failure.
- Rate limiting: Approximate rate enforcement. Token refill is based on wall-clock time, not logical time.

For strict exactly-once processing, combine idempotency with transactional handlers.
