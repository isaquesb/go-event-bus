package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go/jetstream"
)

// rbPanicInvoker is an Invoker that panics before calling next.
type rbPanicInvoker struct{}

func (rbPanicInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	panic("rb: invoker exploded")
}

// rbPolicy is the delivery policy of the panic tests: three deliveries.
func rbPolicy() deliveryPolicy {
	return deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
}

// rbEmit emits an mdEvent named rb.order through bus and returns the payload.
func rbEmit(t *testing.T, bus *Bus, js *mdJS) []byte {
	t.Helper()
	if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}, event.WithID("id-rb")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return js.payload(t, 0).data
}

func TestJetStreamBus_PanicDeadLetteredThenTermed(t *testing.T) {
	js := &mdJS{}
	dlq := event.NewMemoryDLQ()
	errs := &joRecorder{}
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{DLQPublisher: dlq, Observer: errs})
	payload := rbEmit(t, bus, js)

	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		panic("rb: handler exploded")
	}, rbPolicy())(msg)

	got := msg.snapshot()
	if want := []string{"dead_lettered: panic"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}

	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Reason != event.ReasonPanic {
		t.Fatalf("records = %+v, want one with reason panic", recs)
	}
	if stack := recs[0].FailureHeaders()[event.HeaderDLQPanicStack]; stack == "" {
		t.Error("DLQ record has no panic stack header")
	}

	if n := errs.failureCount(); n != 1 {
		t.Fatalf("Failure calls = %d, want 1", n)
	}
	last := errs.lastCause()
	if !errors.Is(last, event.ErrDeadLettered) || !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("Failure cause = %v, want to match ErrDeadLettered and ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_PanicWithoutPublisherDropped(t *testing.T) {
	js := &mdJS{}
	errs := &joRecorder{}
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{Observer: errs})
	payload := rbEmit(t, bus, js)

	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		panic("rb: handler exploded")
	}, rbPolicy())(msg)

	got := msg.snapshot()
	if want := []string{"dropped: panic"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if n := errs.failureCount(); n != 1 {
		t.Fatalf("Failure calls = %d, want 1", n)
	}
	if last := errs.lastCause(); !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("Failure cause = %v, want to match ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_InvokerPanicRecovered(t *testing.T) {
	js := &mdJS{}
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{
		Invoker:      rbPanicInvoker{},
		DLQPublisher: event.NewMemoryDLQ(),
	})
	payload := rbEmit(t, bus, js)

	ran := false
	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		ran = true
		return nil
	}, rbPolicy())(msg)

	if ran {
		t.Error("handler ran although the invoker panicked before next")
	}
	got := msg.snapshot()
	if len(got.termReasons) != 1 || !strings.HasSuffix(got.termReasons[0], "panic") {
		t.Errorf("termReasons = %q, want one reason ending in panic", got.termReasons)
	}
}

// --- shutdown fakes ---

// rbConsumeCtx is a jetstream.ConsumeContext that counts Drain and Stop and
// reports Closed only when the test calls finish.
type rbConsumeCtx struct {
	mu     sync.Mutex
	drains int
	stops  int
	closed chan struct{}
	once   sync.Once
}

func newRbConsumeCtx() *rbConsumeCtx { return &rbConsumeCtx{closed: make(chan struct{})} }

func (c *rbConsumeCtx) Drain() {
	c.mu.Lock()
	c.drains++
	c.mu.Unlock()
}

func (c *rbConsumeCtx) Stop() {
	c.mu.Lock()
	c.stops++
	c.mu.Unlock()
}

func (c *rbConsumeCtx) Closed() <-chan struct{} { return c.closed }
func (c *rbConsumeCtx) finish()                 { c.once.Do(func() { close(c.closed) }) }

func (c *rbConsumeCtx) counts() (drains, stops int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drains, c.stops
}

// rbConsumer is a mockConsumer whose Consume captures the MessageHandler and
// returns the controllable rbConsumeCtx.
type rbConsumer struct {
	mockConsumer
	cc      *rbConsumeCtx
	handler jetstream.MessageHandler
}

func (c *rbConsumer) Consume(h jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	c.handler = h
	return c.cc, nil
}

// rbJS is an mdJS that hands out one rbConsumer.
type rbJS struct {
	mdJS
	cons *rbConsumer
}

func (j *rbJS) CreateOrUpdateConsumer(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return j.cons, nil
}

// rbGate is a channel closed at most once.
type rbGate struct {
	ch   chan struct{}
	once sync.Once
}

func newRbGate() *rbGate { return &rbGate{ch: make(chan struct{})} }
func (g *rbGate) open()  { g.once.Do(func() { close(g.ch) }) }

// rbShutdownBus builds a bus over an rbJS, subscribes fn to rb.order with
// MaxDeliver 3 and emits one event. It returns the bus, the consumer, the js and
// the recorded payload of the emitted event.
func rbShutdownBus(t *testing.T, opts BusOptions, fn event.HandleFn) (*Bus, *rbConsumer, *rbJS, []byte) {
	t.Helper()
	cons := &rbConsumer{cc: newRbConsumeCtx()}
	js := &rbJS{cons: cons}
	bus := newMdBus(js, newMdRegistry("rb.order", "rb.child"), opts)
	err := bus.Subscribe(context.Background(), "rb.order", fn,
		event.WithStream("S"),
		event.WithConsumer("C"),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{time.Second}),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}, event.WithID("id-rb")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return bus, cons, js, js.payload(t, 0).data
}

// rbDeliver delivers msg through the captured MessageHandler from a goroutine
// and returns a channel closed when the handler returns.
func rbDeliver(cons *rbConsumer, msg *mdMsg) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		cons.handler(msg)
	}()
	return done
}

// rbShutdown runs bus.Shutdown(ctx) in a goroutine and returns its result channel.
func rbShutdown(bus *Bus, ctx context.Context) <-chan error {
	res := make(chan error, 1)
	go func() { res <- bus.Shutdown(ctx) }()
	return res
}

func TestJetStreamBus_ShutdownDrainsAndHandsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := newRbGate()
		defer gate.open()
		started := make(chan struct{}, 4)
		var runs int
		var mu sync.Mutex
		bus, cons, js, payload := rbShutdownBus(t, BusOptions{}, func(context.Context, event.Event) error {
			mu.Lock()
			runs++
			mu.Unlock()
			started <- struct{}{}
			<-gate.ch
			return nil
		})
		defer cons.cc.finish()

		first := &mdMsg{data: payload, numDelivered: 1}
		firstDone := rbDeliver(cons, first)
		synctest.Wait()
		<-started

		res := rbShutdown(bus, context.Background())
		synctest.Wait()
		select {
		case err := <-res:
			t.Fatalf("Shutdown returned %v while a handler was in flight", err)
		default:
		}
		if drains, _ := cons.cc.counts(); drains != 1 {
			t.Errorf("Drain calls = %d, want 1", drains)
		}

		if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Emit while closing = %v, want ErrBusClosed", err)
		}
		err := bus.Subscribe(context.Background(), "rb.other", noopHandler,
			event.WithStream("S"), event.WithConsumer("C2"), event.WithMaxDeliver(3),
			event.WithBackOff([]time.Duration{time.Second}))
		if !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Subscribe while closing = %v, want ErrBusClosed", err)
		}

		// A message delivered while closing goes back to the server unstarted.
		second := &mdMsg{data: js.payload(t, 0).data, numDelivered: 1}
		<-rbDeliver(cons, second)
		if got := second.snapshot(); got.naks != 1 || got.acks != 0 || len(got.termReasons) != 0 {
			t.Errorf("second snapshot = %+v, want a single Nak", got)
		}
		mu.Lock()
		if runs != 1 {
			t.Errorf("handler runs = %d, want 1 (the hand-back never runs the handler)", runs)
		}
		mu.Unlock()

		gate.open()
		<-firstDone
		cons.cc.finish()
		if err := <-res; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		if got := first.snapshot(); got.acks != 1 {
			t.Errorf("first snapshot = %+v, want a single Ack", got)
		}
	})
}

func TestJetStreamBus_NestedEmitDuringDrainAccepted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := newRbGate()
		defer gate.open()
		started := make(chan struct{}, 1)
		nested := make(chan error, 1)
		var bus *Bus
		bus, cons, js, payload := rbShutdownBus(t, BusOptions{}, func(ctx context.Context, _ event.Event) error {
			started <- struct{}{}
			<-gate.ch
			nested <- bus.Emit(ctx, &mdEvent{name: "rb.child"})
			return nil
		})
		defer cons.cc.finish()

		msg := &mdMsg{data: payload, numDelivered: 1}
		done := rbDeliver(cons, msg)
		synctest.Wait()
		<-started

		res := rbShutdown(bus, context.Background())
		synctest.Wait()

		if err := bus.Emit(context.Background(), &mdEvent{name: "rb.child"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("external Emit during the drain = %v, want ErrBusClosed", err)
		}
		if n := js.count(); n != 1 {
			t.Fatalf("published = %d before the handler emits, want 1", n)
		}

		gate.open()
		if err := <-nested; err != nil {
			t.Errorf("nested Emit during the drain = %v, want nil", err)
		}
		<-done
		cons.cc.finish()
		if err := <-res; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		if n := js.count(); n != 2 {
			t.Errorf("published = %d, want 2 (the nested emit was accepted)", n)
		}
	})
}

func TestJetStreamBus_ShutdownDeadlineInterrupts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		delivered    uint64
		wantFailures int
	}{
		{"non-final delivery", 1, 0},
		{"final delivery", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				errs := &joRecorder{}
				dlq := event.NewMemoryDLQ()
				cause := make(chan error, 1)
				started := make(chan struct{}, 1)
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: dlq, Observer: errs},
					func(ctx context.Context, _ event.Event) error {
						started <- struct{}{}
						<-ctx.Done()
						cause <- context.Cause(ctx)
						return ctx.Err()
					})

				msg := &mdMsg{data: payload, numDelivered: tc.delivered}
				done := rbDeliver(cons, msg)
				synctest.Wait()
				<-started

				sctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err := bus.Shutdown(sctx)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
				}
				<-done
				if got := <-cause; !errors.Is(got, event.ErrShutdownInterrupted) {
					t.Errorf("handler cause = %v, want ErrShutdownInterrupted", got)
				}
				if _, stops := cons.cc.counts(); stops != 1 {
					t.Errorf("Stop calls = %d, want 1", stops)
				}
				if got := msg.snapshot(); got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 || len(got.nakDelays) != 0 {
					t.Errorf("snapshot = %+v, want a single plain Nak", got)
				}
				if dlq.Len() != 0 {
					t.Errorf("DLQ records = %d, want 0 (an interrupted message is never dead-lettered)", dlq.Len())
				}
				_, errList := errs.snapshot()
				if len(errList) != tc.wantFailures {
					t.Fatalf("Failure calls = %d, want %d", len(errList), tc.wantFailures)
				}
				if tc.wantFailures == 1 && !errors.Is(errList[0], event.ErrShutdownInterrupted) {
					t.Errorf("Failure cause = %v, want ErrShutdownInterrupted", errList[0])
				}
			})
		})
	}
}

func TestJetStreamBus_HandBackFinalDeliveryReported(t *testing.T) {
	for _, tc := range []struct {
		name         string
		delivered    uint64
		wantFailures int
	}{
		{"non-final delivery", 1, 0},
		{"final delivery", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				errs := &joRecorder{}
				dlq := event.NewMemoryDLQ()
				ran := false
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: dlq, Observer: errs},
					func(context.Context, event.Event) error {
						ran = true
						return nil
					})
				defer cons.cc.finish()

				res := rbShutdown(bus, context.Background())
				synctest.Wait()

				msg := &mdMsg{data: payload, numDelivered: tc.delivered}
				<-rbDeliver(cons, msg)

				if ran {
					t.Error("handler ran for a message delivered while closing")
				}
				if got := msg.snapshot(); got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 {
					t.Errorf("snapshot = %+v, want a single Nak", got)
				}
				if dlq.Len() != 0 {
					t.Errorf("DLQ records = %d, want 0", dlq.Len())
				}
				evts, errList := errs.snapshot()
				if len(errList) != tc.wantFailures {
					t.Fatalf("Failure calls = %d, want %d", len(errList), tc.wantFailures)
				}
				if tc.wantFailures == 1 {
					if evts[0] != nil {
						t.Errorf("Failure event = %v, want nil", evts[0])
					}
					if !errors.Is(errList[0], event.ErrShutdownInterrupted) {
						t.Errorf("Failure cause = %v, want ErrShutdownInterrupted", errList[0])
					}
				}

				cons.cc.finish()
				if err := <-res; err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

func TestJetStreamBus_CloseBoundedWhenDrainHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus, cons, _, _ := rbShutdownBus(t, BusOptions{ShutdownTimeout: time.Second}, noopHandler)

		start := time.Now()
		err := bus.Close()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Close took %v of fake time, want exactly the 1s ShutdownTimeout", elapsed)
		}
		if _, stops := cons.cc.counts(); stops != 1 {
			t.Errorf("Stop calls = %d, want 1", stops)
		}
	})
}

func TestJetStreamBus_CloseWithImmediateDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := newTestBus(&mockJS{})
		err := bus.Subscribe(context.Background(), "rb.order", noopHandler,
			event.WithStream("S"), event.WithConsumer("C"), event.WithMaxDeliver(3),
			event.WithBackOff([]time.Duration{time.Second}))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Close(); err != nil {
			t.Errorf("Close = %v, want nil", err)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("second Shutdown = %v, want nil", err)
		}
		if err := bus.Emit(context.Background(), &fakeEvent{name: "rb.order"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Emit after Close = %v, want ErrBusClosed", err)
		}
	})
}

// --- callback panic recovery (WR-01) ---

// wrPanicObserver is a raw Observer that records every signal, including the
// Failure it panics in. WithObserver wraps it with event.MultiObserver (IN-03),
// so the panic is logged once and never reaches the msgHandler recover: the
// message gets the disposition it would get from a well-behaved observer. The
// recover is exercised by panics that are not observer panics (Decode, the DLQ
// publisher).
type wrPanicObserver struct{ *joRecorder }

func (o wrPanicObserver) Failure(ctx context.Context, f event.FailureContext) {
	o.joRecorder.Failure(ctx, f)
	panic("wr: observer boom")
}

// wrPanicRegistry panics in Decode; Encode and EncodeWith are promoted.
type wrPanicRegistry struct{ *mdRegistry }

func (wrPanicRegistry) Decode(context.Context, []byte) (context.Context, event.Event, error) {
	panic("wr: decode boom")
}

// wrPanicDLQ is a DLQPublisher whose Publish panics.
type wrPanicDLQ struct{}

func (wrPanicDLQ) Publish(context.Context, event.DLQRecord) error { panic("wr: dlq boom") }

// wrPanicEvent is an rb.order event whose OnErr method panics.
type wrPanicEvent struct {
	EventID string `json:"event_id"`
	Payload string `json:"payload"`
}

func (*wrPanicEvent) Name() string                         { return "rb.order" }
func (*wrPanicEvent) OnErr(context.Context, error, string) { panic("wr: event onerr boom") }

func wrPanicEventRegistry() *mdRegistry {
	reg := newMdRegistry()
	reg.Register("rb.order", func() event.Event { return &wrPanicEvent{} }, 1)
	return reg
}

func wrFails(context.Context, event.Event) error { return errors.New("wr: failed") }

func wrPermanent(context.Context, event.Event) error { return dispPermanent{} }

// wrBus builds the bus under test over reg, recording into rec. A raw bus gets
// wrPanicObserver in place of the MultiObserver NewBus installs; WithObserver
// isolates it again in every handler context.
func wrBus(js *mdJS, reg event.Registry, opts BusOptions, rec *joRecorder, raw bool) *Bus {
	opts.Observer = rec
	bus := newMdBus(js, reg, opts)
	if raw {
		bus.observer = wrPanicObserver{rec}
	}
	return bus
}

// TestJetStreamBus_CallbackPanicRecoveredAndNaked covers the hand-back with
// NakWithDelay: a non-final delivery with no settlement. The final delivery
// without settlement is reported and termed instead, see
// TestJetStreamBus_CallbackPanicOnFinalDeliveryReported.
func TestJetStreamBus_CallbackPanicRecoveredAndNaked(t *testing.T) {
	for _, row := range []struct {
		name      string
		delivered uint64
		wantDelay time.Duration
		wantPanic string
		raw       bool
		reg       func() event.Registry
		opts      BusOptions
		fn        event.HandleFn
	}{
		{
			name: "DLQ publisher, first delivery", delivered: 1, wantDelay: 10 * time.Millisecond, wantPanic: "wr: dlq boom",
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: BusOptions{DLQPublisher: wrPanicDLQ{}}, fn: wrPermanent,
		},
		{
			name: "decode", delivered: 2, wantDelay: 20 * time.Millisecond, wantPanic: "wr: decode boom",
			reg: func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			fn:  func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				js := &mdJS{}
				rec := &joRecorder{}
				// The payload is emitted through a plain registry; the bus under
				// test decodes with the row's registry.
				payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
				bus := wrBus(js, row.reg(), row.opts, rec, row.raw)

				msg := &mdMsg{data: payload, numDelivered: row.delivered}
				bus.msgHandler(context.Background(), "h", row.fn, rbPolicy())(msg)

				got := msg.snapshot()
				if want := []time.Duration{row.wantDelay}; !slices.Equal(got.nakDelays, want) {
					t.Errorf("nakDelays = %v, want %v", got.nakDelays, want)
				}
				if got.naks != 0 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 {
					t.Errorf("snapshot = %+v, want only one NakWithDelay", got)
				}
				if !joPanicReported(rec, row.wantPanic) {
					t.Errorf("no InternalError with Op panic and value %q, got %+v", row.wantPanic, rec.internalErrors())
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown = %v, want nil (the in-flight count was released)", err)
				}
			})
		})
	}
}

// TestJetStreamBus_CallbackPanicOnFinalDeliveryReported covers G-09-1: a panic
// outside the handler on the final delivery, before Settle dead-lettered or
// reported the message, reaches Observer.Failure once (outcome dropped, reason
// panic, nil event) and ends the message with TermWithReason. The server would
// not redeliver it, so a hand-back would lose it. The decode rows also stand for
// an upcaster panic, since upcasters run inside registry.Decode.
func TestJetStreamBus_CallbackPanicOnFinalDeliveryReported(t *testing.T) {
	for _, row := range []struct {
		name         string
		wantPanic    string
		raw          bool
		nop          bool
		reg          func() event.Registry
		opts         BusOptions
		fn           event.HandleFn
		wantFailures int
		wantObserver string // a second panic, raised by the observer itself
	}{
		{
			name: "decode", wantPanic: "wr: decode boom", wantFailures: 1,
			reg: func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			fn:  func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
		{
			name: "DLQ publisher", wantPanic: "wr: dlq boom", wantFailures: 1,
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: BusOptions{DLQPublisher: wrPanicDLQ{}},
			fn:   wrFails,
		},
		{
			name: "decode, observer panics while reporting", wantPanic: "wr: decode boom", wantFailures: 1,
			wantObserver: "wr: observer boom", raw: true,
			reg: func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			fn:  func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
		{
			name: "decode, NopObserver", wantPanic: "wr: decode boom", nop: true,
			reg: func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			fn:  func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				js := &mdJS{}
				rec := &joRecorder{}
				payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
				var bus *Bus
				if row.nop {
					opts := row.opts
					opts.Observer = event.NopObserver{}
					bus = newMdBus(js, row.reg(), opts)
				} else {
					bus = wrBus(js, row.reg(), row.opts, rec, row.raw)
				}

				// numDelivered equals rbPolicy().maxDeliver: the final delivery.
				msg := &mdMsg{data: payload, numDelivered: 3}
				bus.msgHandler(context.Background(), "h", row.fn, rbPolicy())(msg)

				got := msg.snapshot()
				if want := []string{termReasonPanicOnFinal}; !slices.Equal(got.termReasons, want) {
					t.Errorf("termReasons = %q, want %q", got.termReasons, want)
				}
				if got.naks != 0 || got.acks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
					t.Errorf("snapshot = %+v, want only one TermWithReason", got)
				}
				if !row.nop && !joPanicReported(rec, row.wantPanic) {
					t.Errorf("no InternalError with Op panic and value %q, got %+v", row.wantPanic, rec.internalErrors())
				}

				fails := rec.failures()
				if len(fails) != row.wantFailures {
					t.Fatalf("Failure calls = %d, want %d", len(fails), row.wantFailures)
				}
				if row.wantFailures == 1 {
					f := fails[0]
					if f.Event != nil {
						t.Errorf("Failure event = %v, want nil", f.Event)
					}
					if f.Outcome != event.OutcomeDropped || f.Reason != event.ReasonPanic {
						t.Errorf("Failure outcome/reason = %q/%q, want %q/%q", f.Outcome, f.Reason, event.OutcomeDropped, event.ReasonPanic)
					}
					var pe *event.PanicError
					if !errors.As(f.Cause, &pe) {
						t.Fatalf("Failure cause = %v, want a *event.PanicError", f.Cause)
					}
					if fmt.Sprint(pe.Value) != row.wantPanic {
						t.Errorf("PanicError value = %v, want %q", pe.Value, row.wantPanic)
					}
					if !errors.Is(f.Cause, event.ErrHandlerPanic) {
						t.Errorf("Failure cause = %v, want to match ErrHandlerPanic", f.Cause)
					}
					if f.Delivery.Count != 3 || f.Delivery.System != "nats" || f.Delivery.Raw != nil {
						t.Errorf("Failure delivery = %+v, want Count 3, System nats, nil Raw", f.Delivery)
					}
				}
				if row.wantObserver != "" && !joPanicReported(rec, row.wantObserver) {
					t.Errorf("no InternalError with Op panic and value %q, got %+v", row.wantObserver, rec.internalErrors())
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

// TestJetStreamBus_ObserverPanicAfterSettleIsolated: the observer panics in
// Failure after Settle dead-lettered or reported the message. WithObserver
// isolates the panic, so the message keeps its settled disposition and the
// recover has nothing to do.
func TestJetStreamBus_ObserverPanicAfterSettleIsolated(t *testing.T) {
	for _, row := range []struct {
		name        string
		wantDLQ     int
		wantTerms   []string
		wantAcks    int
		wantOutcome event.Outcome
		reg         func() event.Registry
		fn          event.HandleFn
	}{
		{
			name: "dead-lettered, observer Failure", wantDLQ: 1, wantTerms: []string{"dead_lettered: terminal"}, wantOutcome: event.OutcomeDeadLettered,
			reg: func() event.Registry { return newMdRegistry("rb.order") },
			fn:  wrPermanent,
		},
		{
			name: "chain dead-lettered, observer Failure", wantDLQ: 0, wantAcks: 1, wantOutcome: event.OutcomeDeadLettered,
			reg: func() event.Registry { return newMdRegistry("rb.order") },
			fn: func(context.Context, event.Event) error {
				return &event.DeadLetteredError{Cause: dispPermanent{}, Reason: event.ReasonTerminal}
			},
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				js := &mdJS{}
				payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
				dlq := event.NewMemoryDLQ()
				rec := &joRecorder{}
				bus := wrBus(js, row.reg(), BusOptions{DLQPublisher: dlq}, rec, true)

				msg := &mdMsg{data: payload, numDelivered: 1}
				bus.msgHandler(context.Background(), "h", row.fn, rbPolicy())(msg)

				got := msg.snapshot()
				if !slices.Equal(got.termReasons, row.wantTerms) {
					t.Errorf("termReasons = %q, want %q", got.termReasons, row.wantTerms)
				}
				if got.naks != 0 || got.acks != row.wantAcks || got.plainTerms != 0 || len(got.nakDelays) != 0 {
					t.Errorf("snapshot = %+v, want the settled disposition only (no redelivery)", got)
				}
				if dlq.Len() != row.wantDLQ {
					t.Errorf("DLQ records = %d, want %d", dlq.Len(), row.wantDLQ)
				}
				if joPanicReported(rec, "wr: observer boom") {
					t.Errorf("InternalError for an observer panic that WithObserver isolates: %+v", rec.internalErrors())
				}
				fails := rec.failures()
				if len(fails) != 1 || fails[0].Outcome != row.wantOutcome {
					t.Errorf("Failure calls = %+v, want exactly one with outcome %q", fails, row.wantOutcome)
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

// TestJetStreamBus_DroppedThenObserverPanicIsolated is the dropped row of the
// after-settlement observer panic: no publisher, so Settle ends the message as
// dropped and the observer panics on the Failure. The panic is isolated and the
// message is termed with the dropped reason.
func TestJetStreamBus_DroppedThenObserverPanicIsolated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		js := &mdJS{}
		payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
		rec := &joRecorder{}
		bus := wrBus(js, newMdRegistry("rb.order"), BusOptions{}, rec, true)

		msg := &mdMsg{data: payload, numDelivered: 1}
		bus.msgHandler(context.Background(), "h", wrPermanent, rbPolicy())(msg)

		got := msg.snapshot()
		if want := []string{"dropped: terminal"}; !slices.Equal(got.termReasons, want) {
			t.Errorf("termReasons = %q, want %q", got.termReasons, want)
		}
		fails := rec.failures()
		if len(fails) != 1 || fails[0].Outcome != event.OutcomeDropped {
			t.Errorf("Failure calls = %+v, want exactly one with outcome dropped", fails)
		}
		if joPanicReported(rec, "wr: observer boom") {
			t.Errorf("InternalError for an observer panic that WithObserver isolates: %+v", rec.internalErrors())
		}
	})
}

// TestJetStreamBus_EventOnErrPanicContained: a panic in the event's own OnErr
// method is contained by Settle (InternalError, Component event), so the
// message still gets its normal disposition and no recover is needed.
func TestJetStreamBus_EventOnErrPanicContained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		js := &mdJS{}
		payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
		dlq := event.NewMemoryDLQ()
		rec := &joRecorder{}
		bus := wrBus(js, wrPanicEventRegistry(), BusOptions{DLQPublisher: dlq}, rec, false)

		msg := &mdMsg{data: payload, numDelivered: 1}
		bus.msgHandler(context.Background(), "h", wrPermanent, rbPolicy())(msg)

		got := msg.snapshot()
		if want := []string{"dead_lettered: terminal"}; !slices.Equal(got.termReasons, want) {
			t.Errorf("termReasons = %q, want %q", got.termReasons, want)
		}
		if dlq.Len() != 1 {
			t.Errorf("DLQ records = %d, want 1", dlq.Len())
		}
		if !joPanicReported(rec, "wr: event onerr boom") {
			t.Errorf("no InternalError with Op panic and value wr: event onerr boom, got %+v", rec.internalErrors())
		}
		if n := rec.failureCount(); n != 1 {
			t.Errorf("Failure calls = %d, want 1", n)
		}
	})
}

// wrGatedMsg is an mdMsg whose NakWithDelay and TermWithReason announce
// themselves on entered, then block on gate before they are recorded.
type wrGatedMsg struct {
	*mdMsg
	entered chan struct{}
	once    sync.Once
	gate    *rbGate
}

func newWrGatedMsg(payload []byte, delivered uint64) *wrGatedMsg {
	return &wrGatedMsg{
		mdMsg:   &mdMsg{data: payload, numDelivered: delivered},
		entered: make(chan struct{}),
		gate:    newRbGate(),
	}
}

func (m *wrGatedMsg) enter() {
	m.once.Do(func() { close(m.entered) })
	<-m.gate.ch
}

func (m *wrGatedMsg) NakWithDelay(d time.Duration) error {
	m.enter()
	return m.mdMsg.NakWithDelay(d)
}

func (m *wrGatedMsg) TermWithReason(reason string) error {
	m.enter()
	return m.mdMsg.TermWithReason(reason)
}

// TestJetStreamBus_RecoverActsBeforeShutdownReturns covers G-09-2 on JetStream:
// the recover's broker action runs while the message is still counted in
// flight, so Shutdown does not return before it.
func TestJetStreamBus_RecoverActsBeforeShutdownReturns(t *testing.T) {
	for _, row := range []struct {
		name          string
		delivered     uint64
		wantNakDelays []time.Duration
		wantTerms     []string
		wantFailures  int64
	}{
		{"non-final: NakWithDelay", 1, []time.Duration{time.Second}, nil, 0},
		{"final: Failure and TermWithReason", 3, nil, []string{termReasonPanicOnFinal}, 1},
	} {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				errs := &joRecorder{}
				handlerGate := newRbGate()
				defer handlerGate.open()
				started := make(chan struct{}, 1)
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: wrPanicDLQ{}, Observer: errs},
					func(context.Context, event.Event) error {
						started <- struct{}{}
						<-handlerGate.ch
						return dispPermanent{}
					})
				defer cons.cc.finish()

				msg := newWrGatedMsg(payload, row.delivered)
				defer msg.gate.open()
				done := make(chan struct{})
				go func() {
					defer close(done)
					cons.handler(msg)
				}()
				synctest.Wait()
				<-started

				res := rbShutdown(bus, context.Background())
				synctest.Wait()
				cons.cc.finish()
				synctest.Wait()
				select {
				case err := <-res:
					t.Fatalf("Shutdown returned %v while a handler was in flight", err)
				default:
				}

				handlerGate.open()
				<-msg.entered
				synctest.Wait()
				select {
				case err := <-res:
					t.Fatalf("Shutdown returned %v before the recover's broker action went out", err)
				default:
				}
				if _, stops := cons.cc.counts(); stops != 0 {
					t.Errorf("Stop calls = %d before the recover acted, want 0", stops)
				}
				if n := errs.failureCount(); n != row.wantFailures {
					t.Errorf("Failure calls = %d while the action is pending, want %d", n, row.wantFailures)
				}

				msg.gate.open()
				<-done
				if err := <-res; err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}

				got := msg.snapshot()
				if !slices.Equal(got.nakDelays, row.wantNakDelays) || !slices.Equal(got.termReasons, row.wantTerms) {
					t.Errorf("snapshot = %+v, want nakDelays %v and termReasons %q", got, row.wantNakDelays, row.wantTerms)
				}
				if got.naks != 0 || got.acks != 0 || got.plainTerms != 0 {
					t.Errorf("snapshot = %+v, want no plain Nak, Ack or Term", got)
				}
				if !joPanicReported(errs, "wr: dlq boom") {
					t.Errorf("no InternalError with Op panic and value wr: dlq boom, got %+v", errs.internalErrors())
				}
				if drains, stops := cons.cc.counts(); drains != 1 || stops != 0 {
					t.Errorf("Drain/Stop calls = %d/%d, want 1/0", drains, stops)
				}
			})
		})
	}
}

// TestJetStreamBus_HandBackObserverPanicNakedOnce: the observer panics while a
// message is handed back during Shutdown. The panic is isolated, so the message
// is still Naked exactly once.
func TestJetStreamBus_HandBackObserverPanicNakedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &joRecorder{}
		bus, cons, _, payload := rbShutdownBus(t, BusOptions{Observer: rec}, noopHandler)
		bus.observer = wrPanicObserver{rec}
		defer cons.cc.finish()

		res := rbShutdown(bus, context.Background())
		synctest.Wait()

		msg := &mdMsg{data: payload, numDelivered: 3}
		<-rbDeliver(cons, msg)

		got := msg.snapshot()
		if got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 || len(got.nakDelays) != 0 {
			t.Errorf("snapshot = %+v, want exactly one plain Nak", got)
		}
		if joPanicReported(rec, "wr: observer boom") {
			t.Errorf("InternalError for an observer panic that WithObserver isolates: %+v", rec.internalErrors())
		}

		cons.cc.finish()
		if err := <-res; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
	})
}

// --- concurrent Shutdown (WR-02) ---

func TestJetStreamBus_ConcurrentShutdownWaitsForDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus, cons, _, _ := rbShutdownBus(t, BusOptions{}, noopHandler)
		defer cons.cc.finish()

		first := rbShutdown(bus, context.Background())
		synctest.Wait()
		second := rbShutdown(bus, context.Background())
		synctest.Wait()

		for i, res := range []<-chan error{first, second} {
			select {
			case err := <-res:
				t.Fatalf("Shutdown #%d returned %v while a consumer was still draining", i+1, err)
			default:
			}
		}
		if drains, _ := cons.cc.counts(); drains != 1 {
			t.Errorf("Drain calls = %d, want 1", drains)
		}

		cons.cc.finish()
		for i, res := range []<-chan error{first, second} {
			if err := <-res; err != nil {
				t.Errorf("Shutdown #%d = %v, want nil", i+1, err)
			}
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("third Shutdown = %v, want nil", err)
		}
		if drains, stops := cons.cc.counts(); drains != 1 || stops != 0 {
			t.Errorf("Drain/Stop calls = %d/%d, want 1/0", drains, stops)
		}
	})
}

func TestJetStreamBus_ConcurrentShutdownPeerDeadlineForces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := make(chan error, 1)
		started := make(chan struct{}, 1)
		bus, cons, _, payload := rbShutdownBus(t, BusOptions{}, func(ctx context.Context, _ event.Event) error {
			started <- struct{}{}
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return ctx.Err()
		})
		defer cons.cc.finish()

		msg := &mdMsg{data: payload, numDelivered: 1}
		done := rbDeliver(cons, msg)
		synctest.Wait()
		<-started

		first := rbShutdown(bus, context.Background())
		synctest.Wait()

		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		err := bus.Shutdown(sctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Shutdown = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("second Shutdown took %v of fake time, want exactly 1s", elapsed)
		}

		<-done
		if got := <-cause; !errors.Is(got, event.ErrShutdownInterrupted) {
			t.Errorf("handler cause = %v, want ErrShutdownInterrupted", got)
		}
		if err := <-first; err != nil {
			t.Errorf("first Shutdown = %v, want nil once no handler is in flight", err)
		}
		if got := msg.snapshot(); got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 || len(got.nakDelays) != 0 {
			t.Errorf("snapshot = %+v, want a single plain Nak", got)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("later Shutdown = %v, want nil", err)
		}
		if drains, stops := cons.cc.counts(); drains != 1 || stops != 1 {
			t.Errorf("Drain/Stop calls = %d/%d, want 1/1", drains, stops)
		}
	})
}

// --- default observer flush (D-25 WR-05) ---

// dcLog is one captured slog record.
type dcLog struct {
	msg   string
	attrs map[string]any
}

// dcCapture is a minimal slog.Handler recording every record with its attrs.
type dcCapture struct {
	mu   *sync.Mutex
	recs *[]dcLog
}

func newDcCapture() dcCapture { return dcCapture{mu: &sync.Mutex{}, recs: &[]dcLog{}} }

func (c dcCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c dcCapture) Handle(_ context.Context, r slog.Record) error {
	rec := dcLog{msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	c.mu.Lock()
	*c.recs = append(*c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c dcCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c dcCapture) WithGroup(string) slog.Handler      { return c }

func (c dcCapture) records() []dcLog {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dcLog(nil), *c.recs...)
}

// dcInstallDefault installs a capturing default logger until the test ends. A
// test that uses it must not run in parallel.
func dcInstallDefault(t *testing.T) dcCapture {
	t.Helper()
	c := newDcCapture()
	prev := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

// dcSummaries returns the dedup summary records of capt.
func dcSummaries(capt dcCapture) []dcLog {
	var out []dcLog
	for _, r := range capt.records() {
		if r.msg == "eventbus: repeated log records suppressed" {
			out = append(out, r)
		}
	}
	return out
}

// dcThreeDropsBus builds a bus through NewBus (so a nil Observer gets the
// default SlogObserver) with a subscription whose handler fails terminally, and
// delivers the same message three times: the log dedup holds one record and
// counts two suppressed. The consumer is returned to be finished by the caller.
func dcThreeDropsBus(t *testing.T, opts BusOptions) (*Bus, *rbConsumer) {
	t.Helper()
	cons := &rbConsumer{cc: newRbConsumeCtx()}
	js := &rbJS{cons: cons}
	opts.DLQPublisher = event.NewMemoryDLQ()
	bus := NewBus(js, newMdRegistry("rb.order"), opts)
	err := bus.Subscribe(context.Background(), "rb.order", joTerminalFn,
		event.WithStream("S"),
		event.WithConsumer("C"),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{time.Second}),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}, event.WithID("id-dc")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	payload := js.payload(t, 0).data
	for i := 0; i < 3; i++ {
		cons.handler(&mdMsg{data: payload, numDelivered: 1, subject: "rb.order"})
	}
	return bus, cons
}

// dcCheckSummary asserts the flush logged the dedup summary of the Failure
// signal with suppressed 2. A dead-lettered message also dedups its DeadLettered
// signal, so only the failure summary is selected.
func dcCheckSummary(t *testing.T, capt dcCapture) {
	t.Helper()
	var sums []dcLog
	for _, r := range dcSummaries(capt) {
		if r.attrs["signal"] == "failure" {
			sums = append(sums, r)
		}
	}
	if len(sums) != 1 {
		t.Fatalf("failure summary records = %d, want 1: %+v", len(sums), capt.records())
	}
	if got := sums[0].attrs["suppressed"]; got != int64(2) {
		t.Errorf("summary suppressed = %v, want 2", got)
	}
}

// TestJetStreamBus_ShutdownFlushesDefaultObserver: the default SlogObserver of a
// bus built without Observer logs its pending dedup summary on Shutdown and
// Close (WR-05).
func TestJetStreamBus_ShutdownFlushesDefaultObserver(t *testing.T) {
	for _, closeIt := range []bool{false, true} {
		name := "Shutdown"
		if closeIt {
			name = "Close"
		}
		t.Run(name, func(t *testing.T) {
			capt := dcInstallDefault(t)
			bus, cons := dcThreeDropsBus(t, BusOptions{})
			cons.cc.finish()

			if n := len(dcSummaries(capt)); n != 0 {
				t.Fatalf("summaries before Shutdown = %d, want 0", n)
			}
			var err error
			if closeIt {
				err = bus.Close()
			} else {
				err = bus.Shutdown(context.Background())
			}
			if err != nil {
				t.Fatalf("%s = %v", name, err)
			}
			dcCheckSummary(t, capt)
		})
	}
}

// TestJetStreamBus_ShutdownFlushesOnExpiredContext: the flush also runs when the
// consumer does not close before the Shutdown ctx expires.
func TestJetStreamBus_ShutdownFlushesOnExpiredContext(t *testing.T) {
	capt := dcInstallDefault(t)
	bus, cons := dcThreeDropsBus(t, BusOptions{})
	defer cons.cc.finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v, want context.Canceled", err)
	}
	dcCheckSummary(t, capt)
}

// TestJetStreamBus_ShutdownLeavesUserObserver: an Observer supplied by the
// caller is never flushed by the bus.
func TestJetStreamBus_ShutdownLeavesUserObserver(t *testing.T) {
	capt := newDcCapture()
	bus, cons := dcThreeDropsBus(t, BusOptions{Observer: event.NewSlogObserver(slog.New(capt))})
	cons.cc.finish()

	if err := bus.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if n := len(dcSummaries(capt)); n != 0 {
		t.Errorf("summaries after Shutdown = %d, want 0: the bus does not flush a user observer", n)
	}
}
