//go:build integration

package jetstream

import (
	"context"
	"errors"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go/jetstream"
)

// shOrder is the event of the shutdown integration tests.
type shOrder struct {
	ID string `json:"id"`
}

func (e *shOrder) Name() string { return "sh.order" }

// shSetup creates a stream, a bus with the given Observer and a subscription of fn
// on base+".order". It returns the bus, the stream, the consumer name and the
// subject.
func shSetup(t *testing.T, js jetstream.JetStream, obs event.Observer, fn event.HandleFn) (bus *Bus, stream, consumer, subject string) {
	t.Helper()
	base := uniqueName(t, "sh")
	stream = newStream(t, js, base+".>")
	consumer = uniqueName(t, "C")
	subject = base + ".order"

	reg := newTestRegistry()
	reg.Register("sh.order", func() event.Event { return &shOrder{} }, 1)

	bus = NewBus(js, reg, BusOptions{
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		Observer:         obs,
	})
	t.Cleanup(func() { _ = bus.Close() })

	// The Subscribe ctx is the parent of every handler ctx, so it must outlive
	// the test body.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := bus.Subscribe(ctx, subject, fn,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(5),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return bus, stream, consumer, subject
}

// TestIntegration_ShutdownBoundedWithBlockedHandler proves a handler blocked
// forever cannot hold Shutdown hostage: the deadline returns promptly and the
// handler is cancelled with cause ErrShutdownInterrupted.
func TestIntegration_ShutdownBoundedWithBlockedHandler(t *testing.T) {
	_, js := connect(t)

	started := make(chan struct{}, 1)
	cause := make(chan error, 1)
	bus, _, _, subject := shSetup(t, js, nil, func(ctx context.Context, _ event.Event) error {
		started <- struct{}{}
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	})

	emitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := bus.Emit(emitCtx, &shOrder{ID: "1"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start within 10s")
	}

	sctx, scancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer scancel()
	begin := time.Now()
	err := bus.Shutdown(sctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Errorf("Shutdown took %v, want it bounded by the 500ms context", elapsed)
	}

	select {
	case got := <-cause:
		if !errors.Is(got, event.ErrShutdownInterrupted) {
			t.Errorf("handler cause = %v, want ErrShutdownInterrupted", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not cancelled within 5s of the Shutdown deadline")
	}

	if err := bus.Emit(emitCtx, &shOrder{ID: "2"}, event.WithSubject(subject)); !errors.Is(err, event.ErrBusClosed) {
		t.Errorf("Emit after Shutdown = %v, want ErrBusClosed", err)
	}
}

// TestIntegration_HandlerPanicDoesNotCrash proves a panicking handler is
// reported while the process keeps running, and that the message is terminated
// with a reason instead of being redelivered.
func TestIntegration_HandlerPanicDoesNotCrash(t *testing.T) {
	_, js := connect(t)

	reported := make(chan event.FailureContext, 4)
	bus, stream, consumer, subject := shSetup(t, js, &joRecorder{ch: reported}, func(context.Context, event.Event) error {
		panic("sh: handler exploded")
	})

	emitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := bus.Emit(emitCtx, &shOrder{ID: "1"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	select {
	case f := <-reported:
		if !errors.Is(f.Cause, event.ErrHandlerPanic) {
			t.Errorf("Failure cause = %v, want ErrHandlerPanic", f.Cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Observer.Failure did not receive the panic within 5s")
	}

	info := waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.NumAckPending == 0 && ci.Delivered.Consumer == 1
	})
	t.Logf("panic terminated: delivered=%d ack_pending=%d", info.Delivered.Consumer, info.NumAckPending)
}
