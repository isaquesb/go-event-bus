package examples

import (
	"context"
	"fmt"

	"github.com/isaquesb/go-event-bus"
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
