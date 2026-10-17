//go:build integration

package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"go.opentelemetry.io/otel/trace"
)

// oxTraceID and oxSpanID are the fixed ids of the span oxSpanInvoker puts in
// the context, so a test can compute the traceparent it expects.
const (
	oxTraceID = "0102030405060708090a0b0c0d0e0f10"
	oxSpanID  = "1112131415161718"
)

// oxFixedTraceparent is the W3C traceparent of the fixed, sampled span context.
const oxFixedTraceparent = "00-" + oxTraceID + "-" + oxSpanID + "-01"

// oxSpanContext is the fixed valid, sampled span context.
func oxSpanContext(t testing.TB) trace.SpanContext {
	t.Helper()
	tid, err := trace.TraceIDFromHex(oxTraceID)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	sid, err := trace.SpanIDFromHex(oxSpanID)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
}

// oxSpanInvoker stands in for the telemetry TracerInvoker without the SDK: it
// puts the fixed span context in the ctx it passes to next, which is the ctx the
// chain DLQ invoker publishes with.
type oxSpanInvoker struct{ sc trace.SpanContext }

func (i oxSpanInvoker) Invoke(ctx context.Context, _ event.Event, _ string, next func(context.Context) error) error {
	return next(trace.ContextWithSpanContext(ctx, i.sc))
}

// oxReplayed is what the handler of the history test saw on a successful run.
type oxReplayed struct {
	headers map[string][]string
	count   int
}

// TestIntegration_HistoryTraceparentAndReplayHeaders is the tracer of the
// failure observability path on a real server: a message whose retries are
// exhausted on its final delivery lands in the DLQ stream with the original
// envelope bytes, a bounded retry history and the traceparent of the span where
// it died; the Replayer then publishes it back with the Eventbus-Replay-*
// headers, which the original consumer receives in Delivery.Headers (OBS-04,
// D-08, D-16, D-17).
func TestIntegration_HistoryTraceparentAndReplayHeaders(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "ox")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	dlq := NewDLQ(js, prefix)
	rec := &joRecorder{}
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			oxSpanInvoker{sc: oxSpanContext(t)},
			invoker.NewDLQ(dlq),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 7, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}),
		),
		DLQPublisher:     dlq,
		Observer:         rec,
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var healthy atomic.Bool
	replayed := make(chan oxReplayed, 4)
	handler := func(hctx context.Context, _ event.Event) error {
		if !healthy.Load() {
			return errors.New("db down")
		}
		d, _ := event.DeliveryFromContext(hctx)
		replayed <- oxReplayed{headers: d.Headers, count: d.Count}
		return nil
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(2),
		event.WithBackOff([]time.Duration{100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(ctx, &itOrder{ID: "o-ox-1"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	itWaitStreamMsgs(t, js, dlqStream, 1)

	orig := itStreamMsg(t, js, stream, 1)
	dead := itStreamMsg(t, js, dlqStream, 1)
	if !bytes.Equal(orig.Data, dead.Data) {
		t.Fatalf("DLQ data is not byte-identical to the original\n orig: %s\n dlq:  %s", orig.Data, dead.Data)
	}

	hist := dead.Header.Get(event.HeaderDLQHistory)
	if hist == "" {
		t.Fatalf("DLQ record has no %s header; headers: %v", event.HeaderDLQHistory, dead.Header)
	}
	if len(hist) > event.HistoryHeaderLimit {
		t.Errorf("%s is %d bytes, want at most %d", event.HeaderDLQHistory, len(hist), event.HistoryHeaderLimit)
	}
	var entries []struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal([]byte(hist), &entries); err != nil {
		t.Fatalf("%s is not valid JSON: %v\n%s", event.HeaderDLQHistory, err, hist)
	}
	if len(entries) != 5 {
		t.Fatalf("%s holds %d attempts, want the last 5 of 7: %s", event.HeaderDLQHistory, len(entries), hist)
	}
	for i, e := range entries {
		if want := i + 3; e.N != want {
			t.Errorf("history entry %d has n = %d, want %d (attempts 3..7): %s", i, e.N, want, hist)
		}
	}
	if got := dead.Header.Get(event.HeaderDLQHistoryTruncated); got != "" {
		t.Errorf("%s = %q, want it absent: five short attempts fit", event.HeaderDLQHistoryTruncated, got)
	}
	if got := dead.Header.Get(event.HeaderDLQTraceparent); got != oxFixedTraceparent {
		t.Errorf("%s = %q, want %q", event.HeaderDLQTraceparent, got, oxFixedTraceparent)
	}

	// One Failure for the terminal outcome, one HandlerDone per delivery.
	deadline := time.Now().Add(5 * time.Second)
	for (len(rec.failures()) < 1 || len(rec.handlerDones()) < 2) && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	fails := rec.failures()
	if len(fails) != 1 {
		t.Fatalf("Failure called %d times, want exactly 1", len(fails))
	}
	if fails[0].Outcome != event.OutcomeDeadLettered {
		t.Errorf("Failure outcome = %q, want %q", fails[0].Outcome, event.OutcomeDeadLettered)
	}
	if len(fails[0].History) != 5 {
		t.Errorf("Failure history length = %d, want 5", len(fails[0].History))
	}
	if fails[0].Delivery.Count != 2 {
		t.Errorf("Failure delivery count = %d, want 2", fails[0].Delivery.Count)
	}
	if dones := rec.handlerDones(); len(dones) != 2 {
		t.Errorf("HandlerDone called %d times, want once per delivery (2)", len(dones))
	}

	// Replay: the handler is healthy again and receives the replayed message
	// with the three Eventbus-Replay-* headers.
	healthy.Store(true)
	res, err := NewReplayer(js).Replay(ctx, ReplayOptions{FromStream: dlqStream})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Fatalf("Replay result = %+v, want 1 replayed", res)
	}

	select {
	case got := <-replayed:
		if v := oxFirst(got.headers[event.HeaderReplayFailedTraceparent]); v != oxFixedTraceparent {
			t.Errorf("%s = %q, want %q (headers %v)", event.HeaderReplayFailedTraceparent, v, oxFixedTraceparent, got.headers)
		}
		if v := oxFirst(got.headers[event.HeaderReplayDLQSequence]); v != "1" {
			t.Errorf("%s = %q, want 1 (headers %v)", event.HeaderReplayDLQSequence, v, got.headers)
		}
		if got.count != 1 {
			t.Errorf("replayed message delivery count = %d, want 1: it is a new stream message", got.count)
		}
		for k := range got.headers {
			if strings.HasPrefix(k, event.HeaderDLQPrefix) {
				t.Errorf("replayed message still carries %s", k)
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the replayed message did not reach the original consumer within 15s")
	}
}

// oxFirst returns the first value of vs, "" when it is empty.
func oxFirst(vs []string) string {
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}
