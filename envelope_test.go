package event_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

// Real bytes produced by the v0.3.0 json.Registry.Encode.
const (
	wireV030Traced   = `{"name":"user.created","version":1,"ts":"2026-10-13T15:32:18.140707127Z","trace_id":"0102030405060708090a0b0c0d0e0f10","span_id":"0102030405060708","payload":{"user_id":"user-123","name":"John"}}`
	wireV030Untraced = `{"name":"user.created","version":1,"ts":"2026-10-13T15:32:18.140787873Z","payload":{"user_id":"user-123","name":"John"}}`
)

func TestEnvelope_V030BytesRoundTripUnchanged(t *testing.T) {
	for name, in := range map[string]string{"traced": wireV030Traced, "untraced": wireV030Untraced} {
		t.Run(name, func(t *testing.T) {
			var env event.Envelope
			if err := json.Unmarshal([]byte(in), &env); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			out, err := json.Marshal(env)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !bytes.Equal(out, []byte(in)) {
				t.Fatalf("bytes changed:\n got %s\nwant %s", out, in)
			}
		})
	}
}

func TestEnvelope_V030BytesHaveEmptyNewFields(t *testing.T) {
	for _, in := range []string{wireV030Traced, wireV030Untraced} {
		var env event.Envelope
		if err := json.Unmarshal([]byte(in), &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if env.ID != "" || env.CorrelationID != "" || env.CausationID != "" {
			t.Fatalf("new id fields not empty: %+v", env)
		}
		if env.Headers != nil {
			t.Fatalf("Headers = %v, want nil", env.Headers)
		}
	}
}

func TestEnvelope_NewFieldsOmittedWhenEmpty(t *testing.T) {
	env := event.Envelope{
		Name:      "user.created",
		Version:   1,
		Timestamp: time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC),
		TraceID:   "0102030405060708090a0b0c0d0e0f10",
		SpanID:    "0102030405060708",
		Payload:   json.RawMessage(`{}`),
	}
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"id"`, "correlation_id", "causation_id", `"headers"`} {
		if strings.Contains(string(out), key) {
			t.Fatalf("%s present in %s", key, out)
		}
	}
}

func TestEnvelope_NewFieldsRoundTrip(t *testing.T) {
	in := event.Envelope{
		Name:          "user.created",
		Version:       2,
		Timestamp:     time.Date(2026, 10, 13, 15, 0, 0, 0, time.UTC),
		ID:            "0199c0de-0000-7000-8000-000000000001",
		CorrelationID: "0199c0de-0000-7000-8000-000000000000",
		CausationID:   "0199c0de-0000-7000-8000-0000000000aa",
		Headers:       map[string]string{"tenant": "acme", "traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"},
		TraceID:       "0102030405060708090a0b0c0d0e0f10",
		SpanID:        "0102030405060708",
		Payload:       json.RawMessage(`{"a":1}`),
	}
	out, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	last := -1
	for _, key := range []string{`"name"`, `"version"`, `"ts"`, `"id"`, `"correlation_id"`, `"causation_id"`, `"headers"`, `"trace_id"`, `"span_id"`, `"payload"`} {
		idx := bytes.Index(out, []byte(key))
		if idx < 0 || idx <= last {
			t.Fatalf("key %s at %d out of order (previous %d) in %s", key, idx, last, out)
		}
		last = idx
	}
	var got event.Envelope
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != in.ID || got.CorrelationID != in.CorrelationID || got.CausationID != in.CausationID ||
		got.Name != in.Name || got.Version != in.Version || !got.Timestamp.Equal(in.Timestamp) ||
		got.TraceID != in.TraceID || got.SpanID != in.SpanID || string(got.Payload) != string(in.Payload) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, in)
	}
	if len(got.Headers) != 2 || got.Headers["tenant"] != "acme" || got.Headers["traceparent"] != in.Headers["traceparent"] {
		t.Fatalf("headers mismatch: %v", got.Headers)
	}
}

func TestEnvelope_TolerantDecode(t *testing.T) {
	var env event.Envelope
	if err := json.Unmarshal([]byte(`{"name":"x","version":1,"ts":"2026-10-13T00:00:00Z","future_key":{"a":[1,2]},"payload":{}}`), &env); err != nil {
		t.Fatalf("unknown extra key: %v", err)
	}
	if env.Name != "x" {
		t.Fatalf("Name = %q", env.Name)
	}
	env = event.Envelope{}
	if err := json.Unmarshal([]byte(`{"name":"x","payload":{}}`), &env); err != nil {
		t.Fatalf("missing version: %v", err)
	}
	if env.Version != 0 {
		t.Fatalf("Version = %d, want 0", env.Version)
	}
}
