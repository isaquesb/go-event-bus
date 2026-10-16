//go:build integration

package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// termAdvisorySubject is the MSG_TERMINATED advisory subject of one consumer.
func termAdvisorySubject(stream, consumer string) string {
	return "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED." + stream + "." + consumer
}

// maxDeliveriesAdvisorySubject is the MAX_DELIVERIES advisory subject of one
// consumer.
func maxDeliveriesAdvisorySubject(stream, consumer string) string {
	return "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + stream + "." + consumer
}

// TestIntegration_ChainDeadLetterVerbatimRecord is the tracer of the failure
// semantics: with the documented chain (DLQ outside Retry) a terminal handler
// error is dead-lettered once into a DLQ stream whose message data is
// byte-identical to the original stream message, carries the Eventbus-Dlq-*
// failure headers, and the original message is Acked (FAIL-01, FAIL-04,
// FAIL-06, D-09).
func TestIntegration_ChainDeadLetterVerbatimRecord(t *testing.T) {
	nc, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	dlq := NewDLQ(js, nil, prefix)
	onErr := make(chan error, 8)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewDLQ(dlq, nil),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 2, BaseDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond}, nil),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		OnErr: func(_ context.Context, _ event.Event, err error, _ string) {
			onErr <- err
		},
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The chain dead-letters, so the backstop must never terminate this message.
	terminated := itAdvisories(t, nc, termAdvisorySubject(stream, consumer))

	handler := func(context.Context, event.Event) error {
		return invoker.PermanentError{Err: errors.New("boom")}
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{100 * time.Millisecond, 100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(ctx, &itOrder{ID: "o-1"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	itWaitStreamMsgs(t, js, dlqStream, 1)

	orig := itStreamMsg(t, js, stream, 1)
	rec := itStreamMsg(t, js, dlqStream, 1)
	if !bytes.Equal(orig.Data, rec.Data) {
		t.Fatalf("DLQ data is not byte-identical to the original\n orig: %s\n dlq:  %s", orig.Data, rec.Data)
	}
	if want := prefix + subject; rec.Subject != want {
		t.Errorf("DLQ subject = %q, want %q", rec.Subject, want)
	}

	var env event.Envelope
	if err := json.Unmarshal(rec.Data, &env); err != nil {
		t.Fatalf("DLQ data is not an envelope: %v", err)
	}
	if env.ID != "o-1" {
		t.Errorf("envelope id = %q, want o-1", env.ID)
	}

	for header, want := range map[string]string{
		event.HeaderDLQReason:          event.ReasonTerminal,
		event.HeaderDLQOriginalSubject: subject,
		event.HeaderDLQHandler:         consumer,
		event.HeaderDLQDeliveryCount:   "1",
		event.HeaderDLQAttempts:        "1",
	} {
		if got := rec.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if got := rec.Header.Get(event.HeaderDLQEventID); got == "" || got != env.ID {
		t.Errorf("%s = %q, want the envelope id %q", event.HeaderDLQEventID, got, env.ID)
	}

	// Acked: the ack floor advanced and nothing is pending. Term would also move
	// the floor, so the MSG_TERMINATED advisory is asserted absent below.
	waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.AckFloor.Consumer == 1 && ci.NumAckPending == 0
	})

	select {
	case err := <-onErr:
		if !errors.Is(err, event.ErrDeadLettered) {
			t.Errorf("OnErr error = %v, want it to match event.ErrDeadLettered", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnErr was not called within 5s")
	}

	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-onErr:
		t.Errorf("OnErr called a second time: %v", err)
	default:
	}
	select {
	case data := <-terminated:
		t.Errorf("MSG_TERMINATED advisory raised although the chain dead-lettered and Acked: %s", data)
	default:
	}
	if n := itStreamMsgs(t, js, dlqStream); n != 1 {
		t.Errorf("DLQ stream holds %d messages, want exactly 1", n)
	}
}

// TestIntegration_TermWithReasonOnlyAfterDLQPublish proves on the real server
// that the transport backstop dead-letters first and terminates second: with an
// empty chain a terminal error is published by the bus DLQPublisher and only
// then TermWithReason raises a MSG_TERMINATED advisory carrying the reason
// "dead_lettered: terminal" (FAIL-05, D-27, D-31).
func TestIntegration_TermWithReasonOnlyAfterDLQPublish(t *testing.T) {
	nc, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker:          invoker.NewChain(),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     NewDLQ(js, nil, prefix),
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	terminated := itAdvisories(t, nc, termAdvisorySubject(stream, consumer))

	handler := func(context.Context, event.Event) error {
		return invoker.PermanentError{Err: errors.New("boom")}
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{100 * time.Millisecond, 100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(ctx, &itOrder{ID: "o-2"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	adv := itNextAdvisory(t, terminated, 5*time.Second)
	if !strings.Contains(adv.Reason, "dead_lettered: terminal") {
		t.Errorf("MSG_TERMINATED reason = %q, want it to contain %q", adv.Reason, "dead_lettered: terminal")
	}
	if adv.Deliveries != 1 {
		t.Errorf("MSG_TERMINATED deliveries = %d, want 1", adv.Deliveries)
	}

	// The advisory is raised by the Term, which only follows a successful
	// publish: the record must already be in the DLQ stream.
	itWaitStreamMsgs(t, js, dlqStream, 1)
	orig := itStreamMsg(t, js, stream, 1)
	rec := itStreamMsg(t, js, dlqStream, 1)
	if !bytes.Equal(orig.Data, rec.Data) {
		t.Fatalf("DLQ data is not byte-identical to the original\n orig: %s\n dlq:  %s", orig.Data, rec.Data)
	}
	if got := rec.Header.Get(event.HeaderDLQReason); got != event.ReasonTerminal {
		t.Errorf("%s = %q, want %q", event.HeaderDLQReason, got, event.ReasonTerminal)
	}
	if n := itStreamMsgs(t, js, dlqStream); n != 1 {
		t.Errorf("DLQ stream holds %d messages, want exactly 1", n)
	}
}

// TestIntegration_DLQPublishFailureNakNeverTerm proves that when the DLQ cannot
// be written the message is never terminated: it is redelivered up to
// MaxDeliver, no MSG_TERMINATED advisory is raised, the server reports
// MAX_DELIVERIES, and the final delivery reports dlq_publish_failed once
// (FAIL-05, D-26, D-31).
func TestIntegration_DLQPublishFailureNakNeverTerm(t *testing.T) {
	nc, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	consumer := uniqueName(t, "C")

	// No stream captures this prefix, so every DLQ publish fails.
	dlq := NewDLQ(js, nil, uniqueName(t, "nostream")+".")
	onErr := make(chan error, 8)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker:          invoker.NewChain(),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		OnErr: func(_ context.Context, _ event.Event, err error, _ string) {
			onErr <- err
		},
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	terminated := itAdvisories(t, nc, termAdvisorySubject(stream, consumer))
	maxDeliveries := itAdvisories(t, nc, maxDeliveriesAdvisorySubject(stream, consumer))

	counts := make(chan int, 16)
	handler := func(hctx context.Context, _ event.Event) error {
		if d, ok := event.DeliveryFromContext(hctx); ok {
			counts <- d.Count
		}
		return invoker.PermanentError{Err: errors.New("boom")}
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{100 * time.Millisecond, 100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(ctx, &itOrder{ID: "o-3"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	for want := 1; want <= 3; want++ {
		select {
		case got := <-counts:
			if got != want {
				t.Fatalf("handler delivery count = %d, want %d", got, want)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("handler did not see delivery %d within 20s", want)
		}
	}

	// The final delivery reports the lost dead-letter once. It runs after the
	// handler (bounded in-process DLQ retries), so wait for it.
	select {
	case err := <-onErr:
		if !errors.Is(err, event.ErrDeadLetterFailed) {
			t.Errorf("OnErr error = %v, want it to match event.ErrDeadLetterFailed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnErr was not called on the final delivery")
	}

	// The server abandons the message after MaxDeliver and says so.
	itNextAdvisory(t, maxDeliveries, 10*time.Second)

	time.Sleep(500 * time.Millisecond)
	select {
	case data := <-terminated:
		t.Fatalf("MSG_TERMINATED advisory raised although the DLQ publish never succeeded: %s", data)
	default:
	}
	select {
	case n := <-counts:
		t.Errorf("handler ran a 4th time (delivery %d), MaxDeliver is 3", n)
	default:
	}
	select {
	case err := <-onErr:
		t.Errorf("OnErr called more than once: %v", err)
	default:
	}
}

// itCall is one handler invocation of the BackOff test.
type itCall struct {
	count int
	at    time.Time
}

// TestIntegration_FinalDeliveryDeadLetterWithBackOff proves that a transient
// failure with Retry in the chain is NakWithDelayed on deliveries 1..N-1, with
// a redelivery gap of at least the BackOff entry, and dead-lettered exactly
// once on delivery N = MaxDeliver with reason retries_exhausted (FAIL-02, D-01,
// D-04). Timing assertions are lower bounds only.
func TestIntegration_FinalDeliveryDeadLetterWithBackOff(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	const backOff = 150 * time.Millisecond
	dlq := NewDLQ(js, nil, prefix)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewDLQ(dlq, nil),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 2, BaseDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond}, nil),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu         sync.Mutex
		calls      []itCall
		dlqAtFinal = int64(-1)
	)
	handler := func(hctx context.Context, _ event.Event) error {
		d, _ := event.DeliveryFromContext(hctx)
		mu.Lock()
		first := d.Count == 3 && dlqAtFinal < 0
		calls = append(calls, itCall{count: d.Count, at: time.Now()})
		mu.Unlock()
		if first {
			// Nothing may be dead-lettered before the final delivery runs.
			n, err := itStreamMsgsErr(js, dlqStream)
			if err != nil {
				n = 1 << 32
			}
			mu.Lock()
			dlqAtFinal = int64(n)
			mu.Unlock()
		}
		return errors.New("transient")
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{backOff, backOff}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(ctx, &itOrder{ID: "o-4"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	itWaitStreamMsgs(t, js, dlqStream, 1)
	// The consumer sequence counts deliveries (3), the stream sequence counts
	// messages (1): the one message is Acked once the stream floor reaches 1.
	waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.AckFloor.Stream == 1 && ci.NumAckPending == 0
	})

	mu.Lock()
	got := append([]itCall(nil), calls...)
	atFinal := dlqAtFinal
	mu.Unlock()

	wantCounts := []int{1, 1, 2, 2, 3, 3}
	if len(got) != len(wantCounts) {
		t.Fatalf("handler ran %d times, want %d (2 attempts x 3 deliveries): %+v", len(got), len(wantCounts), got)
	}
	for i, w := range wantCounts {
		if got[i].count != w {
			t.Fatalf("call %d saw delivery count %d, want %d (all: %+v)", i, got[i].count, w, got)
		}
	}
	// Gap from the last call of delivery n to the first call of delivery n+1:
	// a lower bound, the server adds slack of its own.
	for _, n := range []int{1, 2} {
		last, first := got[2*n-1].at, got[2*n].at
		if gap := first.Sub(last); gap < backOff {
			t.Errorf("redelivery gap after delivery %d = %s, want at least %s", n, gap, backOff)
		}
	}
	if atFinal != 0 {
		t.Errorf("DLQ stream held %d messages when delivery 3 started, want 0", atFinal)
	}

	if n := itStreamMsgs(t, js, dlqStream); n != 1 {
		t.Fatalf("DLQ stream holds %d messages, want exactly 1", n)
	}
	rec := itStreamMsg(t, js, dlqStream, 1)
	for header, want := range map[string]string{
		event.HeaderDLQReason:        event.ReasonRetriesExhausted,
		event.HeaderDLQDeliveryCount: "3",
		event.HeaderDLQAttempts:      "2",
	} {
		if got := rec.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if orig := itStreamMsg(t, js, stream, 1); !bytes.Equal(orig.Data, rec.Data) {
		t.Errorf("DLQ data is not byte-identical to the original\n orig: %s\n dlq:  %s", orig.Data, rec.Data)
	}
}

// TestIntegration_InProgressRecordNotAcked proves that a redelivered message
// that meets a processing idempotency record (a crashed or still running
// execution) is not Acked as a duplicate: it is redelivered, then dead-lettered
// by the transport backstop with reason in_progress on the final delivery, and
// the handler never runs (FAIL-09, D-21, D-27).
func TestIntegration_InProgressRecordNotAcked(t *testing.T) {
	nc, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	// The key is the event id plus the handler name, which defaults to the
	// consumer name.
	store := invoker.NewMemoryIdempotencyStore()
	if err := store.Put(context.Background(), "o-9:"+consumer, invoker.IdempotencyRecord{
		Status:    invoker.StatusProcessing,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed idempotency store: %v", err)
	}

	dlq := NewDLQ(js, nil, prefix)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewIdempotency(store, invoker.IdempotencyConfig{ProcessingTTL: time.Hour}, nil),
			invoker.NewDLQ(dlq, nil),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 1}, nil),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	terminated := itAdvisories(t, nc, termAdvisorySubject(stream, consumer))

	var ran atomic.Int32
	handler := func(context.Context, event.Event) error {
		ran.Add(1)
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
	if err := bus.Emit(ctx, &itOrder{ID: "o-9"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	adv := itNextAdvisory(t, terminated, 10*time.Second)
	if !strings.Contains(adv.Reason, "dead_lettered: in_progress") {
		t.Errorf("MSG_TERMINATED reason = %q, want it to contain %q", adv.Reason, "dead_lettered: in_progress")
	}
	// Delivery 1 was redelivered, never Acked: the Term is on delivery 2.
	if adv.Deliveries != 2 {
		t.Errorf("MSG_TERMINATED deliveries = %d, want 2", adv.Deliveries)
	}

	itWaitStreamMsgs(t, js, dlqStream, 1)
	rec := itStreamMsg(t, js, dlqStream, 1)
	for header, want := range map[string]string{
		event.HeaderDLQReason:        event.ReasonInProgress,
		event.HeaderDLQDeliveryCount: "2",
		event.HeaderDLQAttempts:      "0",
	} {
		if got := rec.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if n := ran.Load(); n != 0 {
		t.Errorf("handler ran %d times, want 0: an in-progress record must not execute", n)
	}
	if n := itStreamMsgs(t, js, dlqStream); n != 1 {
		t.Errorf("DLQ stream holds %d messages, want exactly 1", n)
	}
}

// itEmptyBodyReport is one OnErr report of the empty-body test.
type itEmptyBodyReport struct {
	evt event.Event
	err error
}

// TestIntegration_EmptyBodyDecodeFailureDeadLettered proves on the real server
// that an undecodable message with an empty body is dead-lettered with an empty
// body instead of being lost at MaxDeliver (08-REVIEW CR-02): the DLQ record is
// stored first, TermWithReason follows after one delivery, and a Replay sends
// the record back so the consumer dead-letters it a second time (FAIL-05,
// FAIL-08, D-14, D-18, D-20, PD-2).
func TestIntegration_EmptyBodyDecodeFailureDeadLettered(t *testing.T) {
	nc, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	reports := make(chan itEmptyBodyReport, 8)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker:          invoker.NewChain(),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     NewDLQ(js, nil, prefix),
		OnErr: func(_ context.Context, evt event.Event, err error, _ string) {
			reports <- itEmptyBodyReport{evt: evt, err: err}
		},
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	terminated := itAdvisories(t, nc, termAdvisorySubject(stream, consumer))

	var handled atomic.Int64
	handler := func(context.Context, event.Event) error {
		handled.Add(1)
		return nil
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{100 * time.Millisecond, 100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	nextReport := func() itEmptyBodyReport {
		t.Helper()
		select {
		case r := <-reports:
			if r.evt != nil {
				t.Errorf("OnErr event = %v, want nil", r.evt)
			}
			if !errors.Is(r.err, event.ErrDeadLettered) || !errors.Is(r.err, event.ErrMalformedEnvelope) {
				t.Errorf("OnErr error = %v, want ErrDeadLettered and ErrMalformedEnvelope", r.err)
			}
			return r
		case <-time.After(5 * time.Second):
			t.Fatal("OnErr was not called within 5s")
			return itEmptyBodyReport{}
		}
	}

	// The poison message is published directly: a zero-length body, user header.
	if _, err := js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Header:  nats.Header{"tenant": []string{"acme"}},
	}, jetstream.WithMsgID("empty-1")); err != nil {
		t.Fatalf("publish empty message: %v", err)
	}

	// First dead letter: D-14 dead-letters a malformed message at once.
	adv := itNextAdvisory(t, terminated, 5*time.Second)
	if !strings.Contains(adv.Reason, "dead_lettered: decode_malformed") {
		t.Errorf("MSG_TERMINATED reason = %q, want it to contain %q", adv.Reason, "dead_lettered: decode_malformed")
	}
	if adv.Deliveries != 1 {
		t.Errorf("MSG_TERMINATED deliveries = %d, want 1", adv.Deliveries)
	}
	// The advisory is raised by the Term, which only follows a stored record.
	itWaitStreamMsgs(t, js, dlqStream, 1)
	orig := itStreamMsg(t, js, stream, 1)
	rec := itStreamMsg(t, js, dlqStream, 1)
	if len(orig.Data) != 0 || len(rec.Data) != 0 {
		t.Errorf("data lengths orig=%d dlq=%d, want 0 and 0", len(orig.Data), len(rec.Data))
	}
	for k, want := range map[string]string{
		event.HeaderDLQReason:           event.ReasonDecodeMalformed,
		event.HeaderDLQOriginalSubject:  subject,
		event.HeaderDLQOriginalStream:   stream,
		event.HeaderDLQOriginalSequence: "1",
		event.HeaderDLQOriginalMsgID:    "empty-1",
		"tenant":                        "acme",
	} {
		if got := rec.Header.Get(k); got != want {
			t.Errorf("DLQ header %s = %q, want %q", k, got, want)
		}
	}
	if rec.Header.Get(event.HeaderDLQCause) == "" {
		t.Errorf("DLQ header %s is empty, want the decode cause", event.HeaderDLQCause)
	}
	if got := rec.Header.Get("Nats-Msg-Id"); got != "" {
		t.Errorf("DLQ record Nats-Msg-Id = %q, want it stripped", got)
	}
	nextReport()

	// Replay (PD-2). Limit 1 keeps the run from chasing the second dead letter
	// that the still-subscribed consumer is about to write.
	res, err := NewReplayer(js).Replay(ctx, ReplayOptions{FromStream: dlqStream, Limit: 1})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res != (ReplayResult{Replayed: 1, Failed: 0}) {
		t.Fatalf("Replay result = %+v, want {Replayed:1 Failed:0}", res)
	}
	itWaitStreamMsgs(t, js, stream, 2)
	again := itStreamMsg(t, js, stream, 2)
	if len(again.Data) != 0 {
		t.Errorf("replayed data = %q, want a zero-length body", again.Data)
	}
	if got := again.Header.Get("tenant"); got != "acme" {
		t.Errorf("replayed tenant header = %q, want acme", got)
	}
	if got, want := again.Header.Get("Nats-Msg-Id"), "replay:"+dlqStream+":1"; got != want {
		t.Errorf("replayed Nats-Msg-Id = %q, want %q", got, want)
	}
	for k := range again.Header {
		if strings.HasPrefix(strings.ToLower(k), "eventbus-dlq-") {
			t.Errorf("header %q leaked into the replay", k)
		}
	}

	// Second dead letter: the replay did not drop the record.
	adv = itNextAdvisory(t, terminated, 5*time.Second)
	if !strings.Contains(adv.Reason, "dead_lettered: decode_malformed") {
		t.Errorf("second MSG_TERMINATED reason = %q, want it to contain %q", adv.Reason, "dead_lettered: decode_malformed")
	}
	itWaitStreamMsgs(t, js, dlqStream, 2)
	rec2 := itStreamMsg(t, js, dlqStream, 2)
	if len(rec2.Data) != 0 {
		t.Errorf("second DLQ data = %q, want a zero-length body", rec2.Data)
	}
	if got := rec2.Header.Get(event.HeaderDLQOriginalSequence); got != "2" {
		t.Errorf("second DLQ %s = %q, want 2", event.HeaderDLQOriginalSequence, got)
	}
	if got, want := rec2.Header.Get(event.HeaderDLQOriginalMsgID), "replay:"+dlqStream+":1"; got != want {
		t.Errorf("second DLQ %s = %q, want %q", event.HeaderDLQOriginalMsgID, got, want)
	}
	nextReport()

	if n := itStreamMsgs(t, js, dlqStream); n != 2 {
		t.Errorf("DLQ stream holds %d messages, want exactly 2", n)
	}
	if n := handled.Load(); n != 0 {
		t.Errorf("handler ran %d times for an undecodable message, want 0", n)
	}
}
