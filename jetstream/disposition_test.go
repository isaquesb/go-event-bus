package jetstream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// dispPermanent is an error that is never worth retrying.
type dispPermanent struct{}

func (dispPermanent) Error() string  { return "disp: permanent failure" }
func (dispPermanent) Terminal() bool { return true }

// dispEvent is the event of the disposition tests. Its OnErr method counts the
// terminal reports it receives through dispEventOnErr.
type dispEvent struct {
	name    string
	EventID string `json:"event_id"`
}

var dispEventOnErr atomic.Int64

func (e *dispEvent) Name() string { return e.name }
func (e *dispEvent) Id() string   { return e.EventID }
func (e *dispEvent) OnErr(context.Context, error, string) {
	dispEventOnErr.Add(1)
}

// dispRegistry returns an mdRegistry that decodes name into a dispEvent.
func dispRegistry(name string) *mdRegistry {
	r := newMdRegistry()
	r.Register(name, func() event.Event { return &dispEvent{name: name} }, 1)
	return r
}

// dispErrCounter counts OnErr calls and keeps the last error.
type dispErrCounter struct {
	calls atomic.Int64
	last  atomic.Value // error
}

func (c *dispErrCounter) onErr(_ context.Context, _ event.Event, err error, _ string) {
	c.calls.Add(1)
	c.last.Store(&err)
}

func (c *dispErrCounter) lastErr() error {
	if p, ok := c.last.Load().(*error); ok {
		return *p
	}
	return nil
}

// dispEmit emits a dispEvent through bus and returns the recorded payload.
func dispEmit(t *testing.T, bus *Bus, js *mdJS, name string) []byte {
	t.Helper()
	if err := bus.Emit(context.Background(), &dispEvent{name: name}, event.WithID("id-disp")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return js.payload(t, 0).data
}

func TestJetStreamBus_FinalDeliveryTerminalDeadLetteredThenTermed(t *testing.T) {
	const name = "disp.order"
	dispEventOnErr.Store(0)
	js := &mdJS{}
	dlq := event.NewMemoryDLQ()
	var errs dispErrCounter
	bus := newMdBus(js, dispRegistry(name), BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
	payload := dispEmit(t, bus, js, name)

	msg := &mdMsg{data: payload, numDelivered: 3}
	policy := deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		return dispPermanent{}
	}, policy)(msg)

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	rec := recs[0]
	if string(rec.Raw) != string(payload) {
		t.Errorf("record Raw = %q, want the emitted payload %q", rec.Raw, payload)
	}
	if rec.Delivery.Count != 3 || !rec.Delivery.Final() {
		t.Errorf("record Delivery = %+v, want Count 3 and Final", rec.Delivery)
	}
	if rec.Delivery.StreamSeq != 7 {
		t.Errorf("record StreamSeq = %d, want 7", rec.Delivery.StreamSeq)
	}

	got := msg.snapshot()
	if want := []string{"dead_lettered: terminal"}; len(got.termReasons) != 1 || got.termReasons[0] != want[0] {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 || got.plainTerms != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if n := errs.calls.Load(); n != 1 {
		t.Errorf("bus OnErr calls = %d, want 1", n)
	}
	if !errors.Is(errs.lastErr(), event.ErrDeadLettered) {
		t.Errorf("bus OnErr error = %v, want it to match event.ErrDeadLettered", errs.lastErr())
	}
	if n := dispEventOnErr.Load(); n != 1 {
		t.Errorf("event OnErr calls = %d, want 1 (D-16)", n)
	}
}

// --- handler-error disposition matrix ---

// dispInvoker is an event.Invoker that returns err without calling next, which
// stands for a chain-level invoker deciding the outcome.
type dispInvoker struct{ err error }

func (i dispInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	return i.err
}

// dispFailingPublisher is a DLQPublisher whose Publish always fails.
type dispFailingPublisher struct{ calls atomic.Int64 }

func (p *dispFailingPublisher) Publish(context.Context, event.DLQRecord) error {
	p.calls.Add(1)
	return errors.New("disp: broker down")
}

const dispEventName = "disp.matrix"

var dispBackOff = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}

// dispRun feeds one message to the bus handler. The handler returns err (or the
// chain invoker does when the bus was built with a dispInvoker).
func dispRun(t *testing.T, ctx context.Context, bus *Bus, js *mdJS, delivered uint64, policy deliveryPolicy, err error) *mdMsg {
	t.Helper()
	msg := &mdMsg{data: dispEmit(t, bus, js, dispEventName), numDelivered: delivered}
	bus.msgHandler(ctx, "h", func(context.Context, event.Event) error { return err }, policy)(msg)
	if got := msg.snapshot(); got.plainTerms != 0 {
		t.Fatalf("plain Term called %d times, the bus must only use TermWithReason", got.plainTerms)
	}
	return msg
}

func dispBus(opts BusOptions) (*Bus, *mdJS) {
	js := &mdJS{}
	return newMdBus(js, dispRegistry(dispEventName), opts), js
}

// dispWantDelay asserts exactly one NakWithDelay of want and nothing else.
func dispWantDelay(t *testing.T, msg *mdMsg, want time.Duration) {
	t.Helper()
	got := msg.snapshot()
	if len(got.nakDelays) != 1 || got.nakDelays[0] != want {
		t.Errorf("nakDelays = %v, want [%v]", got.nakDelays, want)
	}
	if got.acks != 0 || got.naks != 0 || len(got.termReasons) != 0 {
		t.Errorf("snapshot = %+v, want only a NakWithDelay", got)
	}
}

func TestJetStreamBus_NonFinalTransientUsesBackOff(t *testing.T) {
	var errs dispErrCounter
	boom := errors.New("boom")
	policy := deliveryPolicy{maxDeliver: 4, backOff: dispBackOff}
	for _, tc := range []struct {
		delivered uint64
		want      time.Duration
	}{{1, 10 * time.Millisecond}, {2, 20 * time.Millisecond}, {3, 40 * time.Millisecond}} {
		bus, js := dispBus(BusOptions{DLQPublisher: event.NewMemoryDLQ(), OnErr: errs.onErr})
		msg := dispRun(t, context.Background(), bus, js, tc.delivered, policy, boom)
		dispWantDelay(t, msg, tc.want)
	}
	if n := errs.calls.Load(); n != 0 {
		t.Errorf("OnErr calls = %d, want 0 on intermediate deliveries (D-16)", n)
	}
}

func TestJetStreamBus_BackOffClampedToLast(t *testing.T) {
	bus, js := dispBus(BusOptions{DLQPublisher: event.NewMemoryDLQ()})
	policy := deliveryPolicy{maxDeliver: 4, backOff: dispBackOff[:2]}
	msg := dispRun(t, context.Background(), bus, js, 3, policy, errors.New("boom"))
	dispWantDelay(t, msg, 20*time.Millisecond)
}

func TestJetStreamBus_UnlimitedDeliveriesNeverTerm(t *testing.T) {
	var errs dispErrCounter
	dlq := event.NewMemoryDLQ()
	bus, js := dispBus(BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
	policy := deliveryPolicy{maxDeliver: 0, backOff: dispBackOff[:1]}
	msg := dispRun(t, context.Background(), bus, js, 9, policy, errors.New("boom"))
	dispWantDelay(t, msg, 10*time.Millisecond)
	if dlq.Len() != 0 || errs.calls.Load() != 0 {
		t.Errorf("records=%d OnErr=%d, want 0 and 0: unlimited deliveries are never final", dlq.Len(), errs.calls.Load())
	}
}

func TestJetStreamBus_FlowControlDelays(t *testing.T) {
	const circuit, rate = 111 * time.Millisecond, 222 * time.Millisecond
	policy := deliveryPolicy{maxDeliver: 4, backOff: dispBackOff}
	for _, tc := range []struct {
		name string
		err  error
		want time.Duration
	}{
		{"circuit open", fmt.Errorf("wrapped: %w", event.ErrCircuitOpen), circuit},
		{"rate limited", fmt.Errorf("wrapped: %w", event.ErrRateLimited), rate},
		{"in progress uses the BackOff schedule", fmt.Errorf("wrapped: %w", event.ErrInProgress), 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			js := &mdJS{}
			bus := NewBus(js, dispRegistry(dispEventName), BusOptions{
				Invoker:          invoker.NewChain(),
				CircuitOpenDelay: circuit,
				RateLimitDelay:   rate,
				DLQPublisher:     event.NewMemoryDLQ(),
			})
			msg := dispRun(t, context.Background(), bus, js, 2, policy, tc.err)
			dispWantDelay(t, msg, tc.want)
		})
	}
}

func TestJetStreamBus_FinalRateLimitedDeadLettered(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	bus, js := dispBus(BusOptions{DLQPublisher: dlq})
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}
	msg := dispRun(t, context.Background(), bus, js, 3, policy, event.ErrRateLimited)

	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Reason != event.ReasonRateLimited {
		t.Fatalf("records = %+v, want one with reason rate_limited", recs)
	}
	got := msg.snapshot()
	if len(got.termReasons) != 1 || got.termReasons[0] != "dead_lettered: rate_limited" {
		t.Errorf("termReasons = %q, want [dead_lettered: rate_limited]", got.termReasons)
	}
	if got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
}

func TestJetStreamBus_DuplicateAcked(t *testing.T) {
	var errs dispErrCounter
	dlq := event.NewMemoryDLQ()
	bus, js := dispBus(BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}
	msg := dispRun(t, context.Background(), bus, js, 3, policy, fmt.Errorf("seen: %w", event.ErrDuplicate))

	got := msg.snapshot()
	if got.acks != 1 || got.naks != 0 || len(got.nakDelays) != 0 || len(got.termReasons) != 0 {
		t.Errorf("snapshot = %+v, want a single Ack", got)
	}
	if errs.calls.Load() != 0 || dlq.Len() != 0 {
		t.Errorf("OnErr=%d records=%d, want 0 and 0", errs.calls.Load(), dlq.Len())
	}
}

func TestJetStreamBus_ChainDeadLetteredAcked(t *testing.T) {
	var errs dispErrCounter
	dlq := event.NewMemoryDLQ()
	chainErr := &event.DeadLetteredError{Cause: errors.New("boom"), Reason: event.ReasonRetriesExhausted}
	bus, js := dispBus(BusOptions{Invoker: dispInvoker{err: chainErr}, DLQPublisher: dlq, OnErr: errs.onErr})
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}
	msg := dispRun(t, context.Background(), bus, js, 2, policy, nil)

	got := msg.snapshot()
	if got.acks != 1 || got.naks != 0 || len(got.nakDelays) != 0 || len(got.termReasons) != 0 {
		t.Errorf("snapshot = %+v, want a single Ack, never a Term", got)
	}
	if dlq.Len() != 0 {
		t.Errorf("bus publisher records = %d, want 0: the chain already dead-lettered it", dlq.Len())
	}
	if n := errs.calls.Load(); n != 1 {
		t.Errorf("OnErr calls = %d, want 1", n)
	}
}

func TestJetStreamBus_ChainDeadLetterFailedNaks(t *testing.T) {
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}
	for _, tc := range []struct {
		delivered uint64
		want      time.Duration
		onErr     int64
	}{{1, 10 * time.Millisecond, 0}, {3, 20 * time.Millisecond, 1}} {
		var errs dispErrCounter
		dlq := event.NewMemoryDLQ()
		chainErr := &event.DeadLetterFailedError{
			Cause:      errors.New("boom"),
			Reason:     event.ReasonRetriesExhausted,
			PublishErr: errors.New("broker down"),
		}
		bus, js := dispBus(BusOptions{Invoker: dispInvoker{err: chainErr}, DLQPublisher: dlq, OnErr: errs.onErr})
		msg := dispRun(t, context.Background(), bus, js, tc.delivered, policy, nil)
		dispWantDelay(t, msg, tc.want)
		if n := errs.calls.Load(); n != tc.onErr {
			t.Errorf("delivery %d: OnErr calls = %d, want %d", tc.delivered, n, tc.onErr)
		}
		if dlq.Len() != 0 {
			t.Errorf("delivery %d: bus publisher records = %d, want 0", tc.delivered, dlq.Len())
		}
	}
}

func TestJetStreamBus_BackstopPublishFailureNeverTerms(t *testing.T) {
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}

	t.Run("non-final", func(t *testing.T) {
		var errs dispErrCounter
		pub := &dispFailingPublisher{}
		bus, js := dispBus(BusOptions{DLQPublisher: pub, OnErr: errs.onErr})
		msg := dispRun(t, context.Background(), bus, js, 1, policy, dispPermanent{})
		if n := pub.calls.Load(); n != 1 {
			t.Errorf("Publish calls = %d, want 1", n)
		}
		dispWantDelay(t, msg, 10*time.Millisecond)
		if errs.calls.Load() != 0 {
			t.Errorf("OnErr calls = %d, want 0 on a non-final delivery", errs.calls.Load())
		}
	})

	t.Run("final", func(t *testing.T) {
		var errs dispErrCounter
		pub := &dispFailingPublisher{}
		bus, js := dispBus(BusOptions{DLQPublisher: pub, OnErr: errs.onErr})
		msg := dispRun(t, context.Background(), bus, js, 3, policy, dispPermanent{})
		if n := pub.calls.Load(); n != 3 {
			t.Errorf("Publish calls = %d, want 3 (bounded in-process retry)", n)
		}
		dispWantDelay(t, msg, 20*time.Millisecond)
		if n := errs.calls.Load(); n != 1 {
			t.Fatalf("OnErr calls = %d, want 1", n)
		}
		if !errors.Is(errs.lastErr(), event.ErrDeadLetterFailed) {
			t.Errorf("OnErr error = %v, want it to match event.ErrDeadLetterFailed", errs.lastErr())
		}
	})
}

func TestJetStreamBus_NoPublisherDropped(t *testing.T) {
	var errs dispErrCounter
	bus, js := dispBus(BusOptions{OnErr: errs.onErr})
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}
	failure := &dispPermanentPtr{}
	msg := dispRun(t, context.Background(), bus, js, 1, policy, failure)

	got := msg.snapshot()
	if len(got.termReasons) != 1 || got.termReasons[0] != "dropped: terminal" {
		t.Errorf("termReasons = %q, want [dropped: terminal]", got.termReasons)
	}
	if got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if errs.lastErr() != error(failure) {
		t.Errorf("OnErr error = %v, want the identical handler error", errs.lastErr())
	}
}

// dispPermanentPtr is a pointer-identity terminal error.
type dispPermanentPtr struct{}

func (*dispPermanentPtr) Error() string  { return "disp: permanent failure (pointer)" }
func (*dispPermanentPtr) Terminal() bool { return true }

func TestJetStreamBus_InterruptedPlainNak(t *testing.T) {
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}
	for _, tc := range []struct {
		delivered uint64
		onErr     int64
	}{{1, 0}, {3, 1}} {
		var errs dispErrCounter
		dlq := event.NewMemoryDLQ()
		bus, js := dispBus(BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		msg := dispRun(t, ctx, bus, js, tc.delivered, policy, ctx.Err())

		got := msg.snapshot()
		if got.naks != 1 || got.acks != 0 || len(got.nakDelays) != 0 || len(got.termReasons) != 0 {
			t.Errorf("delivery %d: snapshot = %+v, want a single plain Nak", tc.delivered, got)
		}
		if dlq.Len() != 0 {
			t.Errorf("delivery %d: records = %d, want 0: an interruption is never dead-lettered", tc.delivered, dlq.Len())
		}
		if n := errs.calls.Load(); n != tc.onErr {
			t.Errorf("delivery %d: OnErr calls = %d, want %d", tc.delivered, n, tc.onErr)
		}
	}
}

func TestJetStreamBus_HandlerTimeoutIsTransient(t *testing.T) {
	bus, js := dispBus(BusOptions{DLQPublisher: event.NewMemoryDLQ()})
	policy := deliveryPolicy{maxDeliver: 4, backOff: dispBackOff}
	msg := dispRun(t, context.Background(), bus, js, 2, policy, context.DeadlineExceeded)
	dispWantDelay(t, msg, 20*time.Millisecond)
}

// --- decode failures ---

// dispDecodeRegistry is an event.Registry whose Decode always fails with err.
type dispDecodeRegistry struct{ err *event.DecodeError }

func (dispDecodeRegistry) Register(string, event.Factory, int) {}
func (dispDecodeRegistry) Encode(context.Context, event.Event) ([]byte, error) {
	return []byte("{}"), nil
}
func (r dispDecodeRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	return ctx, nil, r.err
}

// dispDecodeRun feeds the bytes "raw-bytes" to a bus whose registry fails with
// cause, and reports whether the handler ran and what OnErr saw.
type dispDecodeResult struct {
	msg        *mdMsg
	handlerRan bool
	onErrCalls int64
	onErrEvt   atomic.Value // holds *event.Event
}

func dispDecodeRun(t *testing.T, cause error, pub event.DLQPublisher, delivered uint64, policy deliveryPolicy) *dispDecodeResult {
	t.Helper()
	res := &dispDecodeResult{msg: &mdMsg{data: []byte("raw-bytes"), numDelivered: delivered, subject: "s.decode"}}
	bus := newMdBus(&mdJS{}, dispDecodeRegistry{err: &event.DecodeError{Err: cause}}, BusOptions{
		DLQPublisher: pub,
		OnErr: func(_ context.Context, evt event.Event, _ error, _ string) {
			atomic.AddInt64(&res.onErrCalls, 1)
			res.onErrEvt.Store(&evt)
		},
	})
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		res.handlerRan = true
		return nil
	}, policy)(res.msg)
	if got := res.msg.snapshot(); got.plainTerms != 0 {
		t.Fatalf("plain Term called %d times, the bus must only use TermWithReason", got.plainTerms)
	}
	return res
}

func (r *dispDecodeResult) onErrEvent() event.Event {
	if p, ok := r.onErrEvt.Load().(*event.Event); ok {
		return *p
	}
	return nil
}

func TestJetStreamBus_DecodeFailureDeadLetteredBeforeTerm(t *testing.T) {
	for _, tc := range []struct {
		cause  error
		reason string
	}{
		{event.ErrMalformedEnvelope, event.ReasonDecodeMalformed},
		{event.ErrUnknownEvent, event.ReasonDecodeUnknownEvent},
		{event.ErrMissingUpcaster, event.ReasonDecodeMissingUpcaster},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			dlq := event.NewMemoryDLQ()
			res := dispDecodeRun(t, tc.cause, dlq, 1, deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]})

			recs := dlq.Records()
			if len(recs) != 1 {
				t.Fatalf("DLQ records = %d, want 1", len(recs))
			}
			if string(recs[0].Raw) != "raw-bytes" || recs[0].Evt != nil || recs[0].Reason != tc.reason {
				t.Errorf("record = {Raw:%q Evt:%v Reason:%q}, want raw bytes, nil event, reason %s",
					recs[0].Raw, recs[0].Evt, recs[0].Reason, tc.reason)
			}
			got := res.msg.snapshot()
			if want := "dead_lettered: " + tc.reason; len(got.termReasons) != 1 || got.termReasons[0] != want {
				t.Errorf("termReasons = %q, want [%s]", got.termReasons, want)
			}
			if got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 {
				t.Errorf("snapshot = %+v, want only a TermWithReason", got)
			}
			if n := atomic.LoadInt64(&res.onErrCalls); n != 1 || res.onErrEvent() != nil {
				t.Errorf("OnErr calls=%d evt=%v, want one call with a nil event", n, res.onErrEvent())
			}
			if res.handlerRan {
				t.Error("handler ran for an undecodable message")
			}
		})
	}
}

func TestJetStreamBus_UnknownVersionWaitsForFinal(t *testing.T) {
	policy := deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]}

	t.Run("not final", func(t *testing.T) {
		dlq := event.NewMemoryDLQ()
		res := dispDecodeRun(t, event.ErrUnknownVersion, dlq, 1, policy)
		dispWantDelay(t, res.msg, 10*time.Millisecond)
		if dlq.Len() != 0 || atomic.LoadInt64(&res.onErrCalls) != 0 {
			t.Errorf("records=%d OnErr=%d, want 0 and 0 while a redelivery follows", dlq.Len(), atomic.LoadInt64(&res.onErrCalls))
		}
	})

	t.Run("final", func(t *testing.T) {
		dlq := event.NewMemoryDLQ()
		res := dispDecodeRun(t, event.ErrUnknownVersion, dlq, 3, policy)
		recs := dlq.Records()
		if len(recs) != 1 || recs[0].Reason != event.ReasonUnknownVersion {
			t.Fatalf("records = %+v, want one with reason unknown_version", recs)
		}
		got := res.msg.snapshot()
		if len(got.termReasons) != 1 || got.termReasons[0] != "dead_lettered: unknown_version" {
			t.Errorf("termReasons = %q, want [dead_lettered: unknown_version]", got.termReasons)
		}
		if got.naks != 0 || len(got.nakDelays) != 0 {
			t.Errorf("snapshot = %+v, want only a TermWithReason", got)
		}
	})
}

func TestJetStreamBus_DecodePublishFailureNeverTerms(t *testing.T) {
	pub := &dispFailingPublisher{}
	res := dispDecodeRun(t, event.ErrMalformedEnvelope, pub, 1, deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]})
	dispWantDelay(t, res.msg, 10*time.Millisecond)
	if n := pub.calls.Load(); n != 1 {
		t.Errorf("Publish calls = %d, want 1", n)
	}
}

func TestJetStreamBus_MetadataErrorTreatedAsFirstDelivery(t *testing.T) {
	var (
		got  event.Delivery
		ok   bool
		seen int
	)
	bus, js := dispBus(BusOptions{})
	msg := &mdMsg{data: dispEmit(t, bus, js, dispEventName), metaErr: errors.New("no metadata"), subject: "s.meta"}
	bus.msgHandler(context.Background(), "h", func(ctx context.Context, _ event.Event) error {
		got, ok = event.DeliveryFromContext(ctx)
		seen++
		return nil
	}, deliveryPolicy{maxDeliver: 3})(msg)

	if seen != 1 || !ok {
		t.Fatalf("handler runs=%d delivery ok=%v, want 1 and true", seen, ok)
	}
	if got.Count != 1 || got.Max != 3 || got.Subject != "s.meta" {
		t.Errorf("Delivery = %+v, want Count 1, Max 3, Subject s.meta", got)
	}
	if snap := msg.snapshot(); snap.acks != 1 {
		t.Errorf("snapshot = %+v, want a single Ack", snap)
	}
}
