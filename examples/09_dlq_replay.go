// Example: Dead-letter queue and replay
//
// A message that cannot be handled is dead-lettered once, on its final
// delivery, and can be replayed later to the subject it came from.
//
//   - Put the DLQ invoker OUTSIDE Retry in the chain: Retry exhausts its
//     attempts first, then the DLQ dead-letters the final outcome once. Only a
//     final delivery is dead-lettered; an earlier one is redelivered by the
//     broker.
//   - Configure the SAME publisher on the DLQ invoker AND on the bus
//     (BusOptions.DLQPublisher). The bus is the transport backstop for what the
//     chain cannot see: undecodable messages and flow-control failures on the
//     final delivery. Nothing is published twice.
//   - The DLQ is at-least-once: a crash or a failed ack can publish the same
//     record again, so whatever consumes the DLQ stream must tolerate
//     duplicates.
//   - A JetStream record is the verbatim original envelope plus Eventbus-Dlq-*
//     headers (reason, attempts, handler, original subject, event id, retry
//     history). Replay republishes the envelope byte for byte to the original
//     subject and drops the Eventbus-Dlq-* headers.
//
// The JetStream part below is compile-only. Its runtime proof is the real
// server round trip TestIntegration_ReplayRoundTrip in
// jetstream/replay_integration_test.go, run by the integration job. The
// LocalBus part (ExampleLocalMemoryDLQ) has a checked twin in
// runnable_examples_test.go: Example_memoryDLQ.
package examples

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/isaquesb/go-event-bus/jetstream"
	eventjson "github.com/isaquesb/go-event-bus/json"

	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Event Definitions
// =============================================================================

// InvoiceIssued is the event whose handler fails in these examples.
type InvoiceIssued struct {
	InvoiceID string `json:"invoice_id"`
	Amount    int    `json:"amount"`
}

func (e *InvoiceIssued) Name() string { return "invoice.issued" }

// Id is the dedup id. On JetStream it becomes Nats-Msg-Id, so a re-emit of the
// same invoice inside the stream's duplicate window is not stored twice.
func (e *InvoiceIssued) Id() string { return e.InvoiceID }

// =============================================================================
// JetStream: DLQ outside Retry, bus backstop, replay (compile-only)
// =============================================================================

func ExampleDLQAndReplay() {
	ctx := context.Background()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("failed to connect to NATS", "error", err)
		return
	}
	defer nc.Close()

	js, err := natsjs.New(nc)
	if err != nil {
		slog.Error("failed to get JetStream", "error", err)
		return
	}

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
}

// =============================================================================
// LocalBus: the in-memory DLQ (compile-only)
// =============================================================================

func ExampleLocalMemoryDLQ() {
	// MemoryDLQ is a concurrency-safe in-memory DLQPublisher for tests and
	// examples. It is the same type behind the invoker DLQ and the bus.
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

	ctx := context.Background()
	if err := bus.Subscribe(ctx, "invoice.issued", func(ctx context.Context, evt event.Event) error {
		return fmt.Errorf("billing backend unavailable")
	}, event.WithHandlerName("bill-invoice")); err != nil {
		slog.Error("subscribe failed", "error", err)
		return
	}

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
}
