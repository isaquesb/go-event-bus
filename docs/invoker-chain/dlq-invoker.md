---
layout: default
title: DLQ Invoker
parent: Invoker Chain
nav_order: 7
---

# DLQ (Dead Letter Queue) Invoker

The DLQ invoker is the terminal handler for permanent failures. It captures events that cannot be processed and publishes them to a dead letter queue.

## Configuration

```go
invoker.NewDLQ(publisher, metrics)
```

## DLQPublisher Interface

```go
type DLQPublisher interface {
    Publish(ctx context.Context, evt event.Event, cause error) error
}
```

### Built-in Implementations

- **JetStream DLQ**: `jetstream.NewDQL(js, registry, prefix)` - Publishes to a JetStream stream
- **Custom**: Implement the interface for your backend (Kafka, database, etc.)

## How It Works

The DLQ invoker only captures errors marked with `ErrSendToDLQ`:

```go
var ErrSendToDLQ = errors.New("send to dlq")
```

This sentinel error is returned by the retry invoker when it encounters a **terminal error** (permanent failure). The flow:

1. Handler returns `PermanentError{Err: ...}` or implements `Terminal() == true`
2. Retry invoker classifies it as terminal → returns `ErrSendToDLQ`
3. DLQ invoker catches `ErrSendToDLQ` → publishes to DLQ
4. Returns `nil` (error is considered handled)

Any other error type passes through the DLQ invoker unchanged.

## Position in Chain

The DLQ invoker must be the **last invoker** in the chain, just before the handler. It is the terminal safety net.

## Metrics

| Metric | Type | Description |
|---|---|---|
| `eventbus_dlq_published_total` | Counter | Events sent to DLQ |
| `eventbus_dlq_publish_error_total` | Counter | DLQ publish failures |
