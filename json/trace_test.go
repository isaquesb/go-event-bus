package json_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	trTraceHex = "0102030405060708090a0b0c0d0e0f10"
	trSpanHex  = "0102030405060708"
)

func trRegistry(opts ...eventjson.Option) *eventjson.Registry {
	reg := eventjson.NewRegistry(opts...)
	reg.Register("simple.event", func() event.Event { return &SimpleEvent{} }, 1)
	return reg
}

// trSpanCtx returns ctx holding a valid span context with the given flags and
// tracestate (empty for none).
func trSpanCtx(t *testing.T, ctx context.Context, flags trace.TraceFlags, tracestate string) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(trTraceHex)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	sid, err := trace.SpanIDFromHex(trSpanHex)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	cfg := trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: flags}
	if tracestate != "" {
		ts, err := trace.ParseTraceState(tracestate)
		if err != nil {
			t.Fatalf("tracestate: %v", err)
		}
		cfg.TraceState = ts
	}
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(cfg))
}

func trEnvelope(t *testing.T, data []byte) event.Envelope {
	t.Helper()
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return env
}

func trEncode(t *testing.T, reg *eventjson.Registry, ctx context.Context) []byte {
	t.Helper()
	data, err := reg.Encode(ctx, &SimpleEvent{Message: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return data
}

func trDecode(t *testing.T, reg *eventjson.Registry, ctx context.Context, data []byte) context.Context {
	t.Helper()
	out, _, err := reg.Decode(ctx, data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func trRawEnvelope(t *testing.T, env event.Envelope) []byte {
	t.Helper()
	env.Name = "simple.event"
	env.Version = 1
	env.Timestamp = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	env.Payload = json.RawMessage(`{"message":"x"}`)
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func TestTrace_SampledProducerStaysSampled(t *testing.T) {
	reg := trRegistry()
	data := trEncode(t, reg, trSpanCtx(t, context.Background(), trace.FlagsSampled, ""))

	env := trEnvelope(t, data)
	if tp := env.Headers["traceparent"]; !strings.HasSuffix(tp, "-01") {
		t.Fatalf("traceparent = %q, want a sampled (-01) value", tp)
	}

	sc := trace.SpanContextFromContext(trDecode(t, reg, context.Background(), data))
	if !sc.IsValid() || !sc.IsRemote() || !sc.IsSampled() {
		t.Fatalf("decoded span = valid %v remote %v sampled %v, want all true", sc.IsValid(), sc.IsRemote(), sc.IsSampled())
	}
	if sc.TraceID().String() != trTraceHex || sc.SpanID().String() != trSpanHex {
		t.Errorf("decoded ids = %s/%s, want %s/%s", sc.TraceID(), sc.SpanID(), trTraceHex, trSpanHex)
	}
}

func TestTrace_UnsampledProducerNotForceSampled(t *testing.T) {
	reg := trRegistry()
	data := trEncode(t, reg, trSpanCtx(t, context.Background(), 0, ""))

	env := trEnvelope(t, data)
	if tp := env.Headers["traceparent"]; !strings.HasSuffix(tp, "-00") {
		t.Fatalf("traceparent = %q, want an unsampled (-00) value", tp)
	}
	if env.TraceID != trTraceHex {
		t.Errorf("legacy trace_id = %q, want it dual-written", env.TraceID)
	}

	sc := trace.SpanContextFromContext(trDecode(t, reg, context.Background(), data))
	if !sc.IsValid() || !sc.IsRemote() {
		t.Fatalf("decoded span = valid %v remote %v, want both true", sc.IsValid(), sc.IsRemote())
	}
	if sc.IsSampled() {
		t.Errorf("an unsampled producer was upgraded to sampled (D-15)")
	}
}

func TestTrace_TracestateAndBaggageSurvive(t *testing.T) {
	reg := trRegistry()
	member, err := baggage.NewMember("k", "v")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage: %v", err)
	}
	ctx := trSpanCtx(t, baggage.ContextWithBaggage(context.Background(), bag), trace.FlagsSampled, "vendor=abc")
	data := trEncode(t, reg, ctx)

	env := trEnvelope(t, data)
	if env.Headers["tracestate"] != "vendor=abc" {
		t.Errorf("tracestate header = %q", env.Headers["tracestate"])
	}
	if env.Headers["baggage"] == "" {
		t.Errorf("baggage header missing: %v", env.Headers)
	}

	out := trDecode(t, reg, context.Background(), data)
	if got := trace.SpanContextFromContext(out).TraceState().Get("vendor"); got != "abc" {
		t.Errorf("decoded tracestate vendor = %q, want abc", got)
	}
	if got := baggage.FromContext(out).Member("k").Value(); got != "v" {
		t.Errorf("decoded baggage k = %q, want v", got)
	}
}

func TestTrace_NoSpanWritesNoTraceparent(t *testing.T) {
	reg := trRegistry()
	data := trEncode(t, reg, context.Background())

	if strings.Contains(string(data), `"headers"`) {
		t.Errorf("envelope has a headers key without span, baggage or user headers: %s", data)
	}
	if env := trEnvelope(t, data); env.Headers != nil || env.TraceID != "" {
		t.Errorf("headers %v / trace_id %q, want none", env.Headers, env.TraceID)
	}
}

func TestTrace_SpanWithoutUserHeadersDoesNotPanic(t *testing.T) {
	reg := trRegistry()
	ctx := trSpanCtx(t, context.Background(), trace.FlagsSampled, "")

	data, err := reg.EncodeWith(ctx, &SimpleEvent{Message: "x"}, event.Metadata{ID: "id-1"})
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	if tp := trEnvelope(t, data).Headers["traceparent"]; tp == "" {
		t.Errorf("traceparent missing for a valid span with nil headers")
	}

	// The plain Encode path too.
	if tp := trEnvelope(t, trEncode(t, reg, ctx)).Headers["traceparent"]; tp == "" {
		t.Errorf("traceparent missing from Encode")
	}
}

func TestTrace_LegacyOnlyEnvelopeIsSampledRemote(t *testing.T) {
	reg := trRegistry()
	data := trRawEnvelope(t, event.Envelope{TraceID: trTraceHex, SpanID: trSpanHex})

	sc := trace.SpanContextFromContext(trDecode(t, reg, context.Background(), data))
	if !sc.IsValid() || !sc.IsRemote() || !sc.IsSampled() {
		t.Fatalf("legacy span = valid %v remote %v sampled %v, want all true (D-16)", sc.IsValid(), sc.IsRemote(), sc.IsSampled())
	}
	if sc.TraceID().String() != trTraceHex || sc.SpanID().String() != trSpanHex {
		t.Errorf("legacy ids = %s/%s", sc.TraceID(), sc.SpanID())
	}
}

func TestTrace_MalformedTraceparentFallsBackToLegacy(t *testing.T) {
	reg := trRegistry()
	data := trRawEnvelope(t, event.Envelope{
		Headers: map[string]string{"traceparent": "garbage"},
		TraceID: trTraceHex,
		SpanID:  trSpanHex,
	})

	sc := trace.SpanContextFromContext(trDecode(t, reg, context.Background(), data))
	if !sc.IsValid() || !sc.IsRemote() || !sc.IsSampled() {
		t.Fatalf("fallback span = valid %v remote %v sampled %v, want all true", sc.IsValid(), sc.IsRemote(), sc.IsSampled())
	}
	if sc.TraceID().String() != trTraceHex || sc.SpanID().String() != trSpanHex {
		t.Errorf("fallback ids = %s/%s, want the legacy ids", sc.TraceID(), sc.SpanID())
	}
}

func TestTrace_LocalSpanNotMistakenForExtract(t *testing.T) {
	reg := trRegistry()
	data := trRawEnvelope(t, event.Envelope{Headers: map[string]string{"traceparent": "garbage"}})

	local := trSpanCtx(t, context.Background(), trace.FlagsSampled, "")
	sc := trace.SpanContextFromContext(trDecode(t, reg, local, data))
	if sc.IsRemote() {
		t.Errorf("the local span was reported as a remote extracted one")
	}
}

func TestTrace_UserReservedHeadersAreReplaced(t *testing.T) {
	reg := trRegistry()
	forged := "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
	md := event.Metadata{
		ID:      "id-1",
		Headers: map[string]string{"traceparent": forged, "tracestate": "evil=1", "baggage": "x=y", "tenant": "acme"},
	}

	// No span: the forged keys are removed and the rest is kept, with no error.
	data, err := reg.EncodeWith(context.Background(), &SimpleEvent{Message: "x"}, md)
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	env := trEnvelope(t, data)
	for _, k := range []string{"traceparent", "tracestate", "baggage"} {
		if v, ok := env.Headers[k]; ok {
			t.Errorf("forged %s = %q reached the wire", k, v)
		}
	}
	if env.Headers["tenant"] != "acme" {
		t.Errorf("tenant header lost: %v", env.Headers)
	}
	if len(md.Headers) != 4 {
		t.Errorf("the caller headers map was mutated: %v", md.Headers)
	}

	// With a span: the propagator's value wins.
	ctx := trSpanCtx(t, context.Background(), trace.FlagsSampled, "")
	data, err = reg.EncodeWith(ctx, &SimpleEvent{Message: "x"}, md)
	if err != nil {
		t.Fatalf("encode with span: %v", err)
	}
	env = trEnvelope(t, data)
	want := "00-" + trTraceHex + "-" + trSpanHex + "-01"
	if env.Headers["traceparent"] != want {
		t.Errorf("traceparent = %q, want %q", env.Headers["traceparent"], want)
	}
	if env.Headers["tracestate"] == "evil=1" || env.Headers["baggage"] == "x=y" {
		t.Errorf("forged tracestate/baggage survived: %v", env.Headers)
	}
	if env.Headers["tenant"] != "acme" {
		t.Errorf("tenant header lost: %v", env.Headers)
	}
}

func TestTrace_WithPropagatorReplacesDefault(t *testing.T) {
	member, err := baggage.NewMember("k", "v")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage: %v", err)
	}
	ctx := trSpanCtx(t, baggage.ContextWithBaggage(context.Background(), bag), trace.FlagsSampled, "")

	reg := trRegistry(eventjson.WithPropagator(propagation.TraceContext{}))
	env := trEnvelope(t, trEncode(t, reg, ctx))
	if env.Headers["traceparent"] == "" {
		t.Errorf("traceparent missing: %v", env.Headers)
	}
	if _, ok := env.Headers["baggage"]; ok {
		t.Errorf("baggage written although the propagator is trace-context only: %v", env.Headers)
	}

	// The default writes both.
	def := trEnvelope(t, trEncode(t, trRegistry(), ctx))
	if def.Headers["baggage"] == "" {
		t.Errorf("default propagator did not write baggage: %v", def.Headers)
	}

	// A nil propagator keeps the default.
	nilp := trEnvelope(t, trEncode(t, trRegistry(eventjson.WithPropagator(nil)), ctx))
	if nilp.Headers["baggage"] == "" {
		t.Errorf("nil propagator did not keep the default: %v", nilp.Headers)
	}
}

func TestTrace_DualWriteMatchesTraceparent(t *testing.T) {
	reg := trRegistry()
	env := trEnvelope(t, trEncode(t, reg, trSpanCtx(t, context.Background(), trace.FlagsSampled, "")))

	parts := strings.Split(env.Headers["traceparent"], "-")
	if len(parts) != 4 {
		t.Fatalf("traceparent = %q", env.Headers["traceparent"])
	}
	if env.TraceID != parts[1] || env.SpanID != parts[2] {
		t.Errorf("trace_id/span_id = %s/%s, traceparent ids = %s/%s", env.TraceID, env.SpanID, parts[1], parts[2])
	}
}
