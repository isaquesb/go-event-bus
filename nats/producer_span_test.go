package nats

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
)

// psSpan is one span a psTracer started.
type psSpan struct {
	name  string
	kind  trace.SpanKind
	attrs map[attribute.Key]attribute.Value
}

// psTracerProvider is a recording fake tracer provider built from the API
// packages only (no SDK): its tracers record the name, kind and attributes of
// every started span and delegate the span itself to the noop provider.
type psTracerProvider struct {
	embedded.TracerProvider

	mu    sync.Mutex
	spans []psSpan
}

func (p *psTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return &psTracer{tp: p}
}

func (p *psTracerProvider) started() []psSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]psSpan(nil), p.spans...)
}

type psTracer struct {
	embedded.Tracer
	tp *psTracerProvider
}

func (t *psTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range cfg.Attributes() {
		attrs[kv.Key] = kv.Value
	}
	t.tp.mu.Lock()
	t.tp.spans = append(t.tp.spans, psSpan{name: name, kind: cfg.SpanKind(), attrs: attrs})
	t.tp.mu.Unlock()
	return noop.NewTracerProvider().Tracer("").Start(ctx, name, opts...)
}

// psSecret is the payload of the events under test; no span attribute may
// carry it.
const psSecret = "ps-secret-payload"

// psWantAttrs checks the five semconv attributes of a publish span and that no
// other attribute, and no payload, is present.
func psWantAttrs(t *testing.T, sp psSpan, destination, messageID string) {
	t.Helper()
	want := map[attribute.Key]string{
		"messaging.system":           "nats",
		"messaging.operation.name":   "publish",
		"messaging.operation.type":   "send",
		"messaging.destination.name": destination,
		"messaging.message.id":       messageID,
	}
	if len(sp.attrs) != len(want) {
		t.Errorf("span has %d attributes (%v), want %d", len(sp.attrs), sp.attrs, len(want))
	}
	for k, v := range want {
		got, ok := sp.attrs[k]
		if !ok {
			t.Errorf("attribute %s missing", k)
			continue
		}
		if got.AsString() != v {
			t.Errorf("attribute %s = %q, want %q", k, got.AsString(), v)
		}
	}
	for k, v := range sp.attrs {
		if strings.Contains(v.Emit(), psSecret) {
			t.Errorf("attribute %s carries payload bytes: %q", k, v.Emit())
		}
	}
}

func TestNatsEmit_PublishSpanSemconv(t *testing.T) {
	tp := &psTracerProvider{}
	conn := newRbConn()
	bus := &Bus{nc: conn, reg: newMdRegistry("ps.order"), timeout: time.Second, tp: tp}

	err := bus.Emit(context.Background(),
		&mdEvent{name: "ps.order", EventID: "e-1", Payload: psSecret},
		event.WithSubject("orders.created"))
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	spans := tp.started()
	if len(spans) != 1 {
		t.Fatalf("spans started = %d, want 1", len(spans))
	}
	if spans[0].name != "publish orders.created" {
		t.Errorf("span name = %q, want %q", spans[0].name, "publish orders.created")
	}
	if spans[0].kind != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want Producer", spans[0].kind)
	}
	psWantAttrs(t, spans[0], "orders.created", "e-1")
}

func TestNatsEmitSync_PublishSpanSemconv(t *testing.T) {
	tp := &psTracerProvider{}
	conn := newRbConn()
	bus := &Bus{nc: conn, reg: newMdRegistry("ps.quote"), timeout: time.Second, tp: tp}
	if err := bus.SubscribeSync(context.Background(), "ps.quote", func(context.Context, event.Event) error {
		return nil
	}); err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	if _, err := bus.EmitSync(context.Background(), &mdEvent{name: "ps.quote", EventID: "q-1", Payload: psSecret}); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	spans := tp.started()
	if len(spans) != 1 {
		t.Fatalf("spans started = %d, want 1", len(spans))
	}
	if spans[0].name != "publish ps.quote" {
		t.Errorf("span name = %q, want %q", spans[0].name, "publish ps.quote")
	}
	if spans[0].kind != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want Producer", spans[0].kind)
	}
	psWantAttrs(t, spans[0], "ps.quote", "q-1")
}

func TestNatsEmit_GlobalTracerProviderFallback(t *testing.T) {
	tp := &psTracerProvider{}
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	bus := &Bus{nc: newRbConn(), reg: newMdRegistry("ps.order"), timeout: time.Second}
	if err := bus.Emit(context.Background(), &mdEvent{name: "ps.order", EventID: "g-1"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	spans := tp.started()
	if len(spans) != 1 || spans[0].name != "publish ps.order" {
		t.Fatalf("global provider spans = %+v, want one named %q", spans, "publish ps.order")
	}
}

// psFailReplyConn fails every reply publish.
type psFailReplyConn struct{ *rbConn }

func (psFailReplyConn) PublishMsg(*natsgo.Msg) error { return errors.New("ps: reply down") }

// psEncodeFailRegistry decodes through the wrapped registry and fails every
// encode.
type psEncodeFailRegistry struct{ *mdRegistry }

func (psEncodeFailRegistry) Encode(context.Context, event.Event) ([]byte, error) {
	return nil, errors.New("ps: encode down")
}

func (psEncodeFailRegistry) EncodeWith(context.Context, event.Event, event.Metadata) ([]byte, error) {
	return nil, errors.New("ps: encode down")
}

func TestNatsReply_InternalErrorSignals(t *testing.T) {
	ok := func(context.Context, event.Event) error { return nil }

	t.Run("reply publish fails", func(t *testing.T) {
		rec := &noRecorder{}
		reg := newMdRegistry("ps.quote")
		conn := psFailReplyConn{newRbConn()}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, observer: rec}
		if err := bus.SubscribeSync(context.Background(), "ps.quote", ok, event.WithHandlerName("h")); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		conn.callback(t, "ps.quote")(&natsgo.Msg{
			Subject: "ps.quote", Data: fsRequestBytes(t, reg, "ps.quote"), Reply: rbInbox,
		})

		got := rec.internalErrors()
		if len(got) != 1 {
			t.Fatalf("InternalError calls = %d (%+v), want 1", len(got), got)
		}
		if got[0].Component != event.ComponentTransport || got[0].Op != event.OpReply {
			t.Errorf("Component/Op = %q/%q, want %q/%q", got[0].Component, got[0].Op, event.ComponentTransport, event.OpReply)
		}
		if got[0].Handler != "h" || got[0].Subject != "ps.quote" {
			t.Errorf("Handler/Subject = %q/%q, want h/ps.quote", got[0].Handler, got[0].Subject)
		}
		if got[0].Err == nil || !strings.Contains(got[0].Err.Error(), "reply down") {
			t.Errorf("Err = %v, want the publish error", got[0].Err)
		}
	})

	t.Run("reply encode fails", func(t *testing.T) {
		rec := &noRecorder{}
		inner := newMdRegistry("ps.quote")
		conn := newRbConn()
		bus := &Bus{nc: conn, reg: psEncodeFailRegistry{inner}, timeout: time.Second, observer: rec}
		if err := bus.SubscribeSync(context.Background(), "ps.quote", ok, event.WithHandlerName("h")); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		conn.callback(t, "ps.quote")(&natsgo.Msg{
			Subject: "ps.quote", Data: fsRequestBytes(t, inner, "ps.quote"), Reply: rbInbox,
		})

		got := rec.internalErrors()
		if len(got) != 1 {
			t.Fatalf("InternalError calls = %d (%+v), want 1", len(got), got)
		}
		if got[0].Component != event.ComponentTransport || got[0].Op != event.OpEncodeReply {
			t.Errorf("Component/Op = %q/%q, want %q/%q", got[0].Component, got[0].Op, event.ComponentTransport, event.OpEncodeReply)
		}
		if got[0].Handler != "h" || got[0].Subject != "ps.quote" {
			t.Errorf("Handler/Subject = %q/%q, want h/ps.quote", got[0].Handler, got[0].Subject)
		}
	})
}
