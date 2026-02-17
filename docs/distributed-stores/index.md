---
layout: default
title: Distributed Stores
nav_order: 7
has_children: true
permalink: /distributed-stores/
---

# Distributed Stores

For multi-pod deployments, the in-memory stores won't work across instances. Go Event Bus provides Redis-backed implementations for:

- **Idempotency Store** - Prevents duplicate processing across pods
- **Rate Limit Store** - Enforces rate limits across pods

Both require `github.com/redis/go-redis/v9`.

```bash
go get github.com/isaquesb/go-event-bus/redis
```
