---
layout: default
title: Subscribe Options
parent: Core Concepts
nav_order: 5
---

# Subscribe Options

All buses accept functional options when subscribing:

```go
bus.Subscribe(ctx, subject, handler, opts...)
```

## SubscribeOptions Struct

```go
type SubscribeOptions struct {
    HandlerName string
    Stream      string
    Consumer    string
    MaxDeliver  int
    BackOff     []time.Duration
}
```

## Available Options

### WithHandlerName

```go
event.WithHandlerName("send-welcome-email")
```

Identifies the handler for metrics, logging, and idempotency key scoping. If not set, defaults to the subject name.

### WithStream

```go
event.WithStream("CHAT_EVENTS")
```

Sets the JetStream stream name. **Required** for JetStream subscriptions.

### WithConsumer

```go
event.WithConsumer("chat-processor")
```

Sets the durable consumer name for JetStream. Defaults to the subject name if not set.

### WithMaxDeliver

```go
event.WithMaxDeliver(5)
```

Limits redelivery attempts per message in JetStream. If zero, messages are redelivered indefinitely (a warning is logged).

### WithBackOff

```go
event.WithBackOff([]time.Duration{
    1 * time.Second,
    5 * time.Second,
    30 * time.Second,
    1 * time.Minute,
})
```

Sets delays between JetStream redelivery attempts. If nil, a default progressive backoff is used.

## ApplyOptions Helper

```go
cfg := event.ApplyOptions(opts...)
```

Applies all options and returns the resulting `SubscribeOptions` struct.
