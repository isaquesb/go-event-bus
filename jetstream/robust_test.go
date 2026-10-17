package jetstream

import (
	"context"
	"errors"
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
	var errs dispErrCounter
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
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

	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	last := errs.lastErr()
	if !errors.Is(last, event.ErrDeadLettered) || !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("OnErr err = %v, want to match ErrDeadLettered and ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_PanicWithoutPublisherDropped(t *testing.T) {
	js := &mdJS{}
	var errs dispErrCounter
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{OnErr: errs.onErr})
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
	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if last := errs.lastErr(); !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("OnErr err = %v, want to match ErrHandlerPanic", last)
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

// rbErrs records every OnErr call.
type rbErrs struct {
	mu   sync.Mutex
	evts []event.Event
	errs []error
}

func (r *rbErrs) onErr(_ context.Context, evt event.Event, err error, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evts = append(r.evts, evt)
	r.errs = append(r.errs, err)
}

func (r *rbErrs) snapshot() ([]event.Event, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.evts), slices.Clone(r.errs)
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
		name      string
		delivered uint64
		wantOnErr int
	}{
		{"non-final delivery", 1, 0},
		{"final delivery", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var errs rbErrs
				dlq := event.NewMemoryDLQ()
				cause := make(chan error, 1)
				started := make(chan struct{}, 1)
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: dlq, OnErr: errs.onErr},
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
				if len(errList) != tc.wantOnErr {
					t.Fatalf("OnErr calls = %d, want %d", len(errList), tc.wantOnErr)
				}
				if tc.wantOnErr == 1 && !errors.Is(errList[0], event.ErrShutdownInterrupted) {
					t.Errorf("OnErr err = %v, want ErrShutdownInterrupted", errList[0])
				}
			})
		})
	}
}

func TestJetStreamBus_HandBackFinalDeliveryReported(t *testing.T) {
	for _, tc := range []struct {
		name      string
		delivered uint64
		wantOnErr int
	}{
		{"non-final delivery", 1, 0},
		{"final delivery", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var errs rbErrs
				dlq := event.NewMemoryDLQ()
				ran := false
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: dlq, OnErr: errs.onErr},
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
				if len(errList) != tc.wantOnErr {
					t.Fatalf("OnErr calls = %d, want %d", len(errList), tc.wantOnErr)
				}
				if tc.wantOnErr == 1 {
					if evts[0] != nil {
						t.Errorf("OnErr evt = %v, want nil", evts[0])
					}
					if !errors.Is(errList[0], event.ErrShutdownInterrupted) {
						t.Errorf("OnErr err = %v, want ErrShutdownInterrupted", errList[0])
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
