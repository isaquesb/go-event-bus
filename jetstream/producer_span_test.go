package jetstream

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
)

// jpSpan is one span a jpTracer started: its name, kind and every attribute set
// at start or later through SetAttributes.
type jpSpan struct {
	name  string
	kind  trace.SpanKind
	attrs map[attribute.Key]attribute.Value
	ended bool
	code  codes.Code
}

// jpTracerProvider is a recording fake tracer provider built from the API
// packages only (no SDK, so jetstream/go.mod stays unchanged): its tracers
// record the spans they start and delegate the span itself to the noop provider.
type jpTracerProvider struct {
	embedded.TracerProvider

	mu    sync.Mutex
	spans []*jpSpan
}

func (p *jpTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return &jpTracer{tp: p}
}

// started returns a copy of the recorded spans.
func (p *jpTracerProvider) started() []jpSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]jpSpan, len(p.spans))
	for i, sp := range p.spans {
		cp := *sp
		cp.attrs = make(map[attribute.Key]attribute.Value, len(sp.attrs))
		for k, v := range sp.attrs {
			cp.attrs[k] = v
		}
		out[i] = cp
	}
	return out
}

type jpTracer struct {
	embedded.Tracer
	tp *jpTracerProvider
}

func (t *jpTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	rec := &jpSpan{name: name, kind: cfg.SpanKind(), attrs: map[attribute.Key]attribute.Value{}}
	for _, kv := range cfg.Attributes() {
		rec.attrs[kv.Key] = kv.Value
	}
	t.tp.mu.Lock()
	t.tp.spans = append(t.tp.spans, rec)
	t.tp.mu.Unlock()

	_, inner := noop.NewTracerProvider().Tracer("").Start(ctx, name, opts...)
	sp := &jpRecordingSpan{Span: inner, tp: t.tp, rec: rec}
	return trace.ContextWithSpan(ctx, sp), sp
}

// jpRecordingSpan records SetAttributes, SetStatus and End into its jpSpan and
// delegates everything else to the noop span.
type jpRecordingSpan struct {
	trace.Span
	tp  *jpTracerProvider
	rec *jpSpan
}

func (s *jpRecordingSpan) SetAttributes(kv ...attribute.KeyValue) {
	s.tp.mu.Lock()
	defer s.tp.mu.Unlock()
	for _, a := range kv {
		s.rec.attrs[a.Key] = a.Value
	}
}

func (s *jpRecordingSpan) SetStatus(code codes.Code, _ string) {
	s.tp.mu.Lock()
	s.rec.code = code
	s.tp.mu.Unlock()
}

func (s *jpRecordingSpan) End(...trace.SpanEndOption) {
	s.tp.mu.Lock()
	s.rec.ended = true
	s.tp.mu.Unlock()
}

// jpSecret is the payload of the events under test; no span attribute may carry
// it.
const jpSecret = "jp-secret-payload"

// jpWantAttrs checks the five semconv attributes of a publish span plus the
// publish attempt count, and that nothing else and no payload is present.
func jpWantAttrs(t *testing.T, sp jpSpan, destination, messageID string) {
	t.Helper()
	want := map[attribute.Key]string{
		"messaging.system":           "nats",
		"messaging.operation.name":   "publish",
		"messaging.operation.type":   "send",
		"messaging.destination.name": destination,
		"messaging.message.id":       messageID,
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
	if got, ok := sp.attrs["eventbus.publish.attempts"]; !ok || got.AsInt64() != 1 {
		t.Errorf("attribute eventbus.publish.attempts = %v (present %v), want 1", got, ok)
	}
	if len(sp.attrs) != len(want)+1 {
		t.Errorf("span has %d attributes (%v), want %d: the semconv five and the attempt count", len(sp.attrs), sp.attrs, len(want)+1)
	}
	for k, v := range sp.attrs {
		if strings.Contains(v.String(), jpSecret) {
			t.Errorf("attribute %s carries payload bytes: %q", k, v.String())
		}
	}
}

func TestJetStreamEmit_PublishSpanSemconv(t *testing.T) {
	tp := &jpTracerProvider{}
	js := &mdJS{}
	bus := newMdBus(js, newMdRegistry("jp.order"), BusOptions{TracerProvider: tp})

	err := bus.Emit(context.Background(),
		&mdEvent{name: "jp.order", EventID: "e-1", Payload: jpSecret},
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
	if !spans[0].ended {
		t.Error("span was not ended")
	}
	jpWantAttrs(t, spans[0], "orders.created", "e-1")
}

func TestJetStreamEmit_GlobalTracerProviderFallback(t *testing.T) {
	tp := &jpTracerProvider{}
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	// Built before the global provider is read: it is resolved at call time.
	bus := newMdBus(&mdJS{}, newMdRegistry("jp.order"), BusOptions{})
	if err := bus.Emit(context.Background(), &mdEvent{name: "jp.order", EventID: "g-1"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	spans := tp.started()
	if len(spans) != 1 || spans[0].name != "publish jp.order" {
		t.Fatalf("global provider spans = %+v, want one named %q", spans, "publish jp.order")
	}
}

// jpInternal returns the single InternalError the recorder holds, failing the
// test otherwise.
func jpInternal(t *testing.T, rec *joRecorder) event.InternalErrorInfo {
	t.Helper()
	got := rec.internalErrors()
	if len(got) != 1 {
		t.Fatalf("InternalError calls = %d (%+v), want 1", len(got), got)
	}
	return got[0]
}

func TestJetStreamConsume_InternalErrorSignal(t *testing.T) {
	rec := &joRecorder{}
	bus := newMdBus(&mdJS{}, newMdRegistry("jp.order"), BusOptions{Observer: rec})

	bus.consumeErr(context.Background(), "h", "jp.order")(nil, errors.New("jp: heartbeat missed"))

	got := jpInternal(t, rec)
	if got.Component != event.ComponentTransport || got.Op != event.OpConsume {
		t.Errorf("Component/Op = %q/%q, want %q/%q", got.Component, got.Op, event.ComponentTransport, event.OpConsume)
	}
	if got.Handler != "h" || got.Subject != "jp.order" {
		t.Errorf("Handler/Subject = %q/%q, want h/jp.order", got.Handler, got.Subject)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "heartbeat missed") {
		t.Errorf("Err = %v, want the consume error", got.Err)
	}
}

// jpFailMsg is an mdMsg whose broker calls are recorded and then fail with the
// scripted error.
type jpFailMsg struct {
	*mdMsg
	ackErr, nakErr, nakDelayErr, termErr error
}

func (m *jpFailMsg) Ack() error {
	_ = m.mdMsg.Ack()
	return m.ackErr
}

func (m *jpFailMsg) Nak() error {
	_ = m.mdMsg.Nak()
	return m.nakErr
}

func (m *jpFailMsg) NakWithDelay(d time.Duration) error {
	_ = m.mdMsg.NakWithDelay(d)
	return m.nakDelayErr
}

func (m *jpFailMsg) TermWithReason(reason string) error {
	_ = m.mdMsg.TermWithReason(reason)
	return m.termErr
}

// jpEmit emits a jp.order event through bus and returns the recorded payload.
func jpEmit(t *testing.T, bus *Bus, js *mdJS) []byte {
	t.Helper()
	if err := bus.Emit(context.Background(), &mdEvent{name: "jp.order"}, event.WithID("id-jp")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return js.payload(t, 0).data
}

func TestJetStreamApply_AckCallInternalErrors(t *testing.T) {
	brokerDown := errors.New("jp: broker down")
	policy := deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name      string
		ctx       context.Context
		delivered uint64
		handler   error
		msg       func(*mdMsg) *jpFailMsg
		wantOp    string
	}{
		{
			name: "Ack", ctx: context.Background(), delivered: 1, handler: nil,
			msg:    func(m *mdMsg) *jpFailMsg { return &jpFailMsg{mdMsg: m, ackErr: brokerDown} },
			wantOp: event.OpAck,
		},
		{
			name: "Nak", ctx: cancelled, delivered: 1, handler: cancelled.Err(),
			msg:    func(m *mdMsg) *jpFailMsg { return &jpFailMsg{mdMsg: m, nakErr: brokerDown} },
			wantOp: event.OpNak,
		},
		{
			name: "NakWithDelay", ctx: context.Background(), delivered: 1, handler: errors.New("jp: transient"),
			msg:    func(m *mdMsg) *jpFailMsg { return &jpFailMsg{mdMsg: m, nakDelayErr: brokerDown} },
			wantOp: event.OpNak,
		},
		{
			name: "TermWithReason", ctx: context.Background(), delivered: 3, handler: &joTerminal{msg: "jp: terminal"},
			msg:    func(m *mdMsg) *jpFailMsg { return &jpFailMsg{mdMsg: m, termErr: brokerDown} },
			wantOp: event.OpTerm,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			js := &mdJS{}
			rec := &joRecorder{}
			bus := newMdBus(js, newMdRegistry("jp.order"), BusOptions{Observer: rec})
			payload := jpEmit(t, bus, js)

			msg := tc.msg(&mdMsg{data: payload, numDelivered: tc.delivered, subject: "s.jp"})
			bus.msgHandler(tc.ctx, "h", func(context.Context, event.Event) error { return tc.handler }, policy)(msg)

			got := jpInternal(t, rec)
			if got.Component != event.ComponentTransport || got.Op != tc.wantOp {
				t.Errorf("Component/Op = %q/%q, want %q/%q", got.Component, got.Op, event.ComponentTransport, tc.wantOp)
			}
			if got.Handler != "h" || got.Subject != "s.jp" {
				t.Errorf("Handler/Subject = %q/%q, want h/s.jp", got.Handler, got.Subject)
			}
			if !errors.Is(got.Err, brokerDown) {
				t.Errorf("Err = %v, want the broker error", got.Err)
			}
		})
	}
}

func TestJetStreamHandBack_NakInternalError(t *testing.T) {
	js := &mdJS{}
	rec := &joRecorder{}
	bus := newMdBus(js, newMdRegistry("jp.order"), BusOptions{Observer: rec})
	payload := jpEmit(t, bus, js)

	if err := bus.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	brokerDown := errors.New("jp: broker down")
	msg := &jpFailMsg{mdMsg: &mdMsg{data: payload, numDelivered: 1, subject: "s.jp"}, nakErr: brokerDown}
	ran := false
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		ran = true
		return nil
	}, deliveryPolicy{maxDeliver: 3})(msg)

	if ran {
		t.Error("handler ran for a message delivered while closing")
	}
	got := jpInternal(t, rec)
	if got.Component != event.ComponentTransport || got.Op != event.OpNak {
		t.Errorf("Component/Op = %q/%q, want %q/%q", got.Component, got.Op, event.ComponentTransport, event.OpNak)
	}
	if got.Handler != "h" || got.Subject != "s.jp" {
		t.Errorf("Handler/Subject = %q/%q, want h/s.jp", got.Handler, got.Subject)
	}
	if !errors.Is(got.Err, brokerDown) {
		t.Errorf("Err = %v, want the broker error", got.Err)
	}
}
