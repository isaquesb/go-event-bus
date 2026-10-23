---
layout: default
title: DLQ & Replay
parent: Examples
nav_order: 9
---

# DLQ & Replay

Demonstrates dead-lettering a message that cannot be handled and replaying it later, on JetStream and on the LocalBus.

Source: [`examples/09_dlq_replay.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/09_dlq_replay.go)

## How it fits together

- Put the DLQ invoker **outside** Retry in the chain: Retry exhausts its attempts first, then the DLQ dead-letters the final outcome once. Only a final delivery is dead-lettered; an earlier one is redelivered by the broker. See [DLQ Invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/).
- Configure the **same** publisher on the DLQ invoker and on the bus (`DLQPublisher`). The bus is the transport backstop for what the chain cannot see: undecodable messages and flow-control failures on the final delivery. Nothing is published twice.
- The DLQ is at-least-once: a crash or a failed ack can publish the same record again, so whatever consumes the DLQ stream must tolerate duplicates.
- A JetStream record is the verbatim original envelope plus `Eventbus-Dlq-*` headers (reason, attempts, handler, original subject, event id, retry history). Replay republishes the envelope byte for byte to the original subject and drops the `Eventbus-Dlq-*` headers. See [JetStream Bus]({{ site.baseurl }}/transports/jetstream-bus/).

## The event

```go
// InvoiceIssued is the event whose handler fails in these examples.
type InvoiceIssued struct {
	InvoiceID string `json:"invoice_id"`
	Amount    int    `json:"amount"`
}

func (e *InvoiceIssued) Name() string { return "invoice.issued" }

// Id is the dedup id. On JetStream it becomes Nats-Msg-Id, so a re-emit of the
// same invoice inside the stream's duplicate window is not stored twice.
func (e *InvoiceIssued) Id() string { return e.InvoiceID }
```

## JetStream: DLQ outside Retry, bus backstop, replay

```go
// The DLQ stream must exist and must match the DLQ subject prefix. The
// default prefix is "dlq.", so the original subject invoice.issued is
// stored as dlq.invoice.issued. Never compact or truncate this stream: it
// is the only copy of the failed messages.
if _, err := js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
	Name:     "INVOICES_DLQ",
	Subjects: []string{"dlq.>"},
	MaxAge:   30 * 24 * time.Hour,
}); err != nil {
	slog.Error("failed to create the DLQ stream", "error", err)
	return
}

registry := eventjson.NewRegistry()
registry.Register("invoice.issued", func() event.Event { return &InvoiceIssued{} }, 1)

// One publisher, used twice below.
dlq := jetstream.NewDLQ(js, jetstream.DefaultDLQPrefix)

// DLQ outside Retry. On JetStream keep MaxAttempts low (or drop Retry and
// leave the delay to the consumer BackOff): the in-process attempts multiply
// with the broker redeliveries, MaxAttempts x MaxDeliver.
chain := invoker.NewChain(
	invoker.NewDLQ(dlq),
	invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
	}),
)

// The same publisher is the bus backstop (DLQPublisher).
bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
	Invoker:      chain,
	DLQPublisher: dlq,
	Observer:     event.NewSlogObserver(nil),
})
defer func() { _ = bus.Close() }()

if err := bus.Subscribe(ctx, "invoice.issued", func(ctx context.Context, evt event.Event) error {
	inv := evt.(*InvoiceIssued)
	slog.Info("billing invoice", "invoice_id", inv.InvoiceID)
	// ... a persistent failure ends in the DLQ after the final delivery
	return fmt.Errorf("billing backend unavailable")
},
	event.WithStream("INVOICES"),
	event.WithHandlerName("bill-invoice"),
	event.WithMaxDeliver(5),
	event.WithBackOff([]time.Duration{time.Second, 5 * time.Second, 30 * time.Second, time.Minute}),
); err != nil {
	slog.Error("failed to subscribe", "error", err)
	return
}
```

### Replay

```go
// Later, once the backend is fixed: replay the DLQ. Each record goes back,
// byte for byte, to its original subject. FilterSubject narrows a DLQ
// stream shared with other subjects; ReplayResult counts the outcome.
res, err := jetstream.NewReplayer(js).Replay(ctx, jetstream.ReplayOptions{
	FromStream:    "INVOICES_DLQ",
	FilterSubject: "dlq.invoice.>",
	Limit:         100,
})
if err != nil {
	slog.Error("replay failed", "error", err, "replayed", res.Replayed, "failed", res.Failed)
	return
}
slog.Info("replay completed", "replayed", res.Replayed, "failed", res.Failed)

// A replayed message is handled again from scratch, possibly after newer
// state exists: handlers must tolerate duplicates and stale state.
```

### Runtime proof of the JetStream part

The JetStream code above is **compile-only**: the examples module builds it but does not run it, because it needs a NATS server. Its runtime proof is the real-server round trip `TestIntegration_ReplayRoundTrip` in [`jetstream/replay_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/replay_integration_test.go), run by the integration job (`scripts/ci-local.sh --integration`).

## LocalBus: the in-memory DLQ

`event.MemoryDLQ` is a concurrency-safe in-memory `DLQPublisher` for tests and examples. It is the same type behind the invoker DLQ and the bus.

```go
dlq := event.NewMemoryDLQ()

chain := invoker.NewChain(
	invoker.NewDLQ(dlq),
	invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}),
)

bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:      chain,
	DLQPublisher: dlq,
	Observer:     event.NewSlogObserver(nil),
})
defer func() { _ = bus.Close() }()

// EmitSync returns nil when the failure was dead-lettered: the message was
// handled by moving it to the DLQ.
if _, err := bus.EmitSync(ctx, &InvoiceIssued{InvoiceID: "inv-9"}); err != nil {
	slog.Error("emit sync failed", "error", err)
}

for _, rec := range dlq.Records() {
	slog.Info("dead-lettered",
		"handler", rec.Handler,
		"reason", rec.Reason,
		"attempts", rec.Attempts,
		"event_id", rec.EventID,
		"history", len(rec.History),
	)
}
```

## Runnable output

The LocalBus part runs under `go test` as `Example_memoryDLQ` in [`examples/runnable_examples_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/runnable_examples_test.go). Its `// Output:` block is checked by `go test` in the examples module.

```go
func Example_memoryDLQ() {
	dlq := event.NewMemoryDLQ()
	chain := invoker.NewChain(
		invoker.NewDLQ(dlq),
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    2 * time.Millisecond,
		}),
	)
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      chain,
		DLQPublisher: dlq,
		Observer:     event.NopObserver{},
	})
	defer func() { _ = bus.Close() }()

	ctx := context.Background()
	_ = bus.Subscribe(ctx, "invoice.issued", func(context.Context, event.Event) error {
		return errors.New("billing backend unavailable")
	}, event.WithHandlerName("billing"))

	// A dead-lettered failure is handled, so EmitSync returns nil.
	_, err := bus.EmitSync(ctx, &InvoiceIssued{}, event.WithID("inv-9"))
	fmt.Println("emit err == nil:", err == nil)

	for _, rec := range dlq.Records() {
		fmt.Printf("record handler=%s reason=%s attempts=%d event=%s\n", rec.Handler, rec.Reason, rec.Attempts, rec.EventID)
	}

	// Output:
	// emit err == nil: true
	// record handler=billing reason=retries_exhausted attempts=3 event=inv-9
}
```

Output:

```
emit err == nil: true
record handler=billing reason=retries_exhausted attempts=3 event=inv-9
```
