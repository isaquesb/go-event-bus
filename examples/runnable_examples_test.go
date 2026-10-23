package examples

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// Example_causationCorrelation shows the ids EmitSync propagates from a parent
// handler to the event it emits. It is the checked form of
// 08_causation_correlation.go.
func Example_causationCorrelation() {
	// NopObserver keeps the default slog observer from writing to stderr.
	bus := event.NewLocalBus(event.LocalBusOptions{Observer: event.NopObserver{}})
	defer func() { _ = bus.Close() }()

	ctx := context.Background()

	_ = bus.Subscribe(ctx, "checkout.started", func(ctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		fmt.Printf("checkout id=%s correlation=%s causation=%q\n", md.ID, md.CorrelationID, md.CausationID)
		// The handler ctx links the new event to this one.
		_, err := bus.EmitSync(ctx, PaymentRequested{}, event.WithID("pay-1"))
		return err
	}, event.WithHandlerName("start-checkout"))

	_ = bus.Subscribe(ctx, "payment.requested", func(ctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		fmt.Printf("payment  id=%s correlation=%s causation=%q\n", md.ID, md.CorrelationID, md.CausationID)
		return nil
	}, event.WithHandlerName("request-payment"))

	_, _ = bus.EmitSync(ctx, CheckoutStarted{}, event.WithID("checkout-1"))

	// Output:
	// checkout id=checkout-1 correlation=checkout-1 causation=""
	// payment  id=pay-1 correlation=checkout-1 causation="checkout-1"
}

// Example_memoryDLQ shows the in-memory DLQ publisher on a LocalBus. The DLQ
// invoker sits outside Retry, so it stores the final outcome once. It is the
// checked form of ExampleLocalMemoryDLQ in 09_dlq_replay.go.
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

// printingObserver prints the one Failure it receives. Embedding
// event.NopObserver implements the other Observer methods.
type printingObserver struct {
	event.NopObserver
}

func (printingObserver) Failure(_ context.Context, f event.FailureContext) {
	fmt.Printf("failure handler=%s outcome=%s reason=%s attempts=%d delivery=%d/%d\n",
		f.Handler, f.Outcome, f.Reason, f.Attempts, f.Delivery.Count, f.Delivery.Max)
}

// Example_failureObserver shows Observer.Failure: one call per terminal
// outcome, here a message dropped after Retry gave up and no DLQ publisher was
// configured. It is the checked form of 10_failure_hooks.go.
func Example_failureObserver() {
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: invoker.NewChain(
			invoker.NewRetry(invoker.RetryPolicy{
				MaxAttempts: 2,
				BaseDelay:   time.Millisecond,
				MaxDelay:    2 * time.Millisecond,
			}),
		),
		Observer: printingObserver{},
	})
	defer func() { _ = bus.Close() }()

	ctx := context.Background()
	_ = bus.Subscribe(ctx, "invoice.issued", func(context.Context, event.Event) error {
		return errors.New("billing backend unavailable")
	}, event.WithHandlerName("billing"))

	_, _ = bus.EmitSync(ctx, &InvoiceIssued{}, event.WithID("inv-9"))

	// Output:
	// failure handler=billing outcome=dropped reason=retries_exhausted attempts=2 delivery=1/1
}
