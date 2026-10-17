package nats

import (
	"context"
	"errors"
	"testing"

	"github.com/isaquesb/go-event-bus"
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

// dcCheckFailure asserts the single Failure of a decode failure carries the
// Metadata of the parsed envelope.
func dcCheckFailure(t *testing.T, rec *noRecorder) {
	t.Helper()
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
}

// dcCheckRecord asserts the dead-letter record keeps the event identity.
func dcCheckRecord(t *testing.T, dlq *event.MemoryDLQ) {
	t.Helper()
	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	if recs[0].EventID != "evt-42" || recs[0].EventName != "dc.order" {
		t.Errorf("DLQ record EventID/EventName = %q/%q, want evt-42/dc.order", recs[0].EventID, recs[0].EventName)
	}
	if recs[0].Reason != event.ReasonDecodeUnknownEvent {
		t.Errorf("DLQ record Reason = %q, want %q", recs[0].Reason, event.ReasonDecodeUnknownEvent)
	}
}

func TestNatsSubscribe_DecodeFailureKeepsDecodeContext(t *testing.T) {
	t.Run("decode_ctx", func(t *testing.T) {
		conn := newFsLoopConn()
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := fsBus(conn, dcRegistry{withCtx: true}, dlq, rec, nil)

		if err := bus.Subscribe(context.Background(), "dc.order", func(context.Context, event.Event) error {
			t.Error("handler ran for a message that failed to decode")
			return nil
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		conn.deliver(t, "dc.order", []byte("raw"))

		dcCheckFailure(t, rec)
		dcCheckRecord(t, dlq)
	})

	t.Run("nil_decode_ctx_falls_back", func(t *testing.T) {
		conn := newFsLoopConn()
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := fsBus(conn, dcRegistry{}, dlq, rec, nil)

		if err := bus.Subscribe(context.Background(), "dc.order", func(context.Context, event.Event) error {
			return nil
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		conn.deliver(t, "dc.order", []byte("raw"))

		fails := rec.failures()
		if len(fails) != 1 {
			t.Fatalf("Failure calls = %d, want 1", len(fails))
		}
		if fails[0].Metadata.ID != "" || fails[0].Metadata.CorrelationID != "" {
			t.Errorf("Failure Metadata = %+v, want it empty", fails[0].Metadata)
		}
		if dlq.Len() != 1 {
			t.Errorf("DLQ records = %d, want 1", dlq.Len())
		}
		if n := len(rec.internalErrors()); n != 0 {
			t.Errorf("InternalError calls = %d, want 0 (no panic on a nil Decode ctx)", n)
		}
	})
}

func TestNatsSync_DecodeFailureKeepsDecodeContext(t *testing.T) {
	t.Run("decode_ctx", func(t *testing.T) {
		conn := newFsLoopConn()
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := fsBus(conn, dcRegistry{withCtx: true}, dlq, rec, nil)

		if err := bus.SubscribeSync(context.Background(), "dc.get", func(context.Context, event.Event) error {
			t.Error("handler ran for a message that failed to decode")
			return nil
		}); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}
		_, err := bus.EmitSync(context.Background(), &mdEvent{name: "dc.get"})
		if !errors.Is(err, event.ErrUnknownEvent) {
			t.Errorf("EmitSync error = %v, want it to match event.ErrUnknownEvent", err)
		}
		if v := conn.lastReply(t).Header.Get(HeaderError); v != event.ReasonDecodeUnknownEvent {
			t.Errorf("%s = %q, want %q", HeaderError, v, event.ReasonDecodeUnknownEvent)
		}

		dcCheckFailure(t, rec)
		dcCheckRecord(t, dlq)
	})

	t.Run("nil_decode_ctx_falls_back", func(t *testing.T) {
		conn := newFsLoopConn()
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := fsBus(conn, dcRegistry{}, dlq, rec, nil)

		if err := bus.SubscribeSync(context.Background(), "dc.get", func(context.Context, event.Event) error {
			return nil
		}); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}
		_, err := bus.EmitSync(context.Background(), &mdEvent{name: "dc.get"})
		if err == nil {
			t.Fatal("EmitSync error = nil, want the decode failure")
		}
		if v := conn.lastReply(t).Header.Get(HeaderError); v != event.ReasonDecodeUnknownEvent {
			t.Errorf("%s = %q, want %q", HeaderError, v, event.ReasonDecodeUnknownEvent)
		}
		fails := rec.failures()
		if len(fails) != 1 || fails[0].Metadata.ID != "" {
			t.Errorf("Failures = %+v, want one with empty Metadata", fails)
		}
		if n := len(rec.internalErrors()); n != 0 {
			t.Errorf("InternalError calls = %d, want 0", n)
		}
	})
}
