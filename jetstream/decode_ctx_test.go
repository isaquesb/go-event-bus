package jetstream

import (
	"context"
	"strings"
	"testing"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/trace"
)

// dcTraceID and dcSpanID are the remote span context a dcRegistry puts in the
// ctx it returns, as the json Registry does once an envelope carries a trace.
var (
	dcTraceID = trace.TraceID{0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c}
	dcSpanID  = trace.SpanID{0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31}
)

// dcRegistry is an event.Registry whose Decode fails with an unknown-event
// DecodeError. With withCtx it returns, like the json Registry once the envelope
// parsed, a ctx carrying the Metadata and a remote span; otherwise it returns a
// nil ctx.
type dcRegistry struct{ withCtx bool }

func (dcRegistry) Register(string, event.Factory, int) {}

func (dcRegistry) Encode(context.Context, event.Event) ([]byte, error) {
	return []byte("{}"), nil
}

func (r dcRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	derr := &event.DecodeError{Name: "dc.order", Version: 1, Err: event.ErrUnknownEvent}
	if !r.withCtx {
		return nil, nil, derr
	}
	ctx = event.WithMetadata(ctx, event.Metadata{
		ID:            "evt-42",
		CorrelationID: "corr-7",
		Name:          "dc.order",
		Version:       1,
	})
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    dcTraceID,
		SpanID:     dcSpanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, sc), nil, derr
}

// dcRun feeds one undecodable message on its final delivery (3 of 3) to a bus
// whose DLQ publisher is the real NewDLQ over the capturing dlqtJS, and returns
// the backend, the message and the Observer recording.
func dcRun(t *testing.T, withCtx bool) (*dlqtJS, *mdMsg, *joRecorder) {
	t.Helper()
	backend := &dlqtJS{}
	rec := &joRecorder{}
	bus := newMdBus(&mdJS{}, dcRegistry{withCtx: withCtx}, BusOptions{
		DLQPublisher: NewDLQ(backend, "dlq."),
		Observer:     rec,
	})
	msg := &mdMsg{data: []byte("raw-bytes"), numDelivered: 3, subject: "dc.order"}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		t.Error("handler ran for a message that failed to decode")
		return nil
	}, deliveryPolicy{maxDeliver: 3, backOff: dispBackOff[:2]})(msg)
	return backend, msg, rec
}

func TestJetStreamBus_DecodeFailureKeepsDecodeContext(t *testing.T) {
	t.Run("decode_ctx", func(t *testing.T) {
		backend, msg, rec := dcRun(t, true)

		fails := rec.failures()
		if len(fails) != 1 {
			t.Fatalf("Failure calls = %d, want 1", len(fails))
		}
		f := fails[0]
		if f.Metadata.ID != "evt-42" || f.Metadata.CorrelationID != "corr-7" {
			t.Errorf("Failure Metadata id/correlation = %q/%q, want evt-42/corr-7", f.Metadata.ID, f.Metadata.CorrelationID)
		}
		if f.Reason != event.ReasonDecodeUnknownEvent {
			t.Errorf("Failure reason = %q, want %q", f.Reason, event.ReasonDecodeUnknownEvent)
		}

		if backend.calls() != 1 {
			t.Fatalf("DLQ publishes = %d, want 1", backend.calls())
		}
		h := backend.msg(0).Header
		if got := h.Get(event.HeaderDLQEventID); got != "evt-42" {
			t.Errorf("%s = %q, want evt-42", event.HeaderDLQEventID, got)
		}
		if got := h.Get(event.HeaderDLQEventName); got != "dc.order" {
			t.Errorf("%s = %q, want dc.order", event.HeaderDLQEventName, got)
		}
		tp := h.Get(event.HeaderDLQTraceparent)
		if tp == "" {
			t.Fatalf("%s is empty, want the traceparent of the Decode span", event.HeaderDLQTraceparent)
		}
		if !strings.Contains(tp, dcTraceID.String()) {
			t.Errorf("%s = %q, want it to carry trace id %s", event.HeaderDLQTraceparent, tp, dcTraceID)
		}

		// The settlement is unchanged: the DLQ publish precedes the Term.
		got := msg.snapshot()
		if want := "dead_lettered: " + event.ReasonDecodeUnknownEvent; len(got.termReasons) != 1 || got.termReasons[0] != want {
			t.Errorf("termReasons = %q, want [%s]", got.termReasons, want)
		}
		if got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 {
			t.Errorf("snapshot = %+v, want only a TermWithReason", got)
		}
	})

	t.Run("nil_decode_ctx_falls_back", func(t *testing.T) {
		backend, msg, rec := dcRun(t, false)

		fails := rec.failures()
		if len(fails) != 1 {
			t.Fatalf("Failure calls = %d, want 1", len(fails))
		}
		if fails[0].Metadata.ID != "" || fails[0].Metadata.CorrelationID != "" {
			t.Errorf("Failure Metadata = %+v, want it empty", fails[0].Metadata)
		}
		if backend.calls() != 1 {
			t.Fatalf("DLQ publishes = %d, want 1", backend.calls())
		}
		h := backend.msg(0).Header
		if _, ok := h[event.HeaderDLQTraceparent]; ok {
			t.Errorf("%s present without a Decode span: %v", event.HeaderDLQTraceparent, h)
		}
		if want := "dead_lettered: " + event.ReasonDecodeUnknownEvent; len(msg.snapshot().termReasons) != 1 || msg.snapshot().termReasons[0] != want {
			t.Errorf("termReasons = %q, want [%s]", msg.snapshot().termReasons, want)
		}
		if n := len(rec.internalErrors()); n != 0 {
			t.Errorf("InternalError calls = %d, want 0 (no panic on a nil Decode ctx)", n)
		}
	})
}
