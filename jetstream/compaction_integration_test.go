//go:build integration

package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
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

// Real-server proofs for the JetStream log-compaction guide (DOC-02, D-09).
// Every claim of the guide cites one of these tests, so none of it rests on
// memory. The library adds no compaction API (D-08): the tests pin what the
// server does with a MaxMsgsPerSubject=1 stream and how the existing Bus,
// DLQ and Replayer behave on top of it.
//
// Every stream, consumer and subject name comes from uniqueName and every
// time-based check polls with a deadline: subject delete markers expire lazily
// (a 2s MaxAge still held the message at 3.5s when this was probed), so a fixed
// sleep must never decide an assertion.

// cpSink records the ids of the events a handler received, in order.
type cpSink struct {
	mu  sync.Mutex
	got []string
}

// handler is the event.HandleFn that records the id of every event.
func (s *cpSink) handler() event.HandleFn {
	return func(_ context.Context, evt event.Event) error {
		o, ok := evt.(*itOrder)
		if !ok {
			return nil
		}
		s.mu.Lock()
		s.got = append(s.got, o.ID)
		s.mu.Unlock()
		return nil
	}
}

// ids returns a copy of the ids received so far.
func (s *cpSink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.got)
}

// cpPoll polls cond every 25ms until it holds, and fails the test with desc
// after d. It is the only way these tests wait for the server.
func cpPoll(t testing.TB, d time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %s waiting for %s", d, desc)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// cpBus builds a Bus over js with the itRegistry and a MemoryDLQ. The returned
// Bus is closed on cleanup.
func cpBus(t testing.TB, js jetstream.JetStream, obs event.Observer) *Bus {
	t.Helper()
	bus := NewBus(js, itRegistry(), BusOptions{
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     event.NewMemoryDLQ(),
		Observer:         obs,
	})
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

// cpEmit publishes version id on the entity subject through the Bus. The id is
// the Nats-Msg-Id, so every version needs its own.
func cpEmit(ctx context.Context, t testing.TB, bus *Bus, subject, id string) {
	t.Helper()
	if err := bus.Emit(ctx, &itOrder{ID: id}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit %s on %s: %v", id, subject, err)
	}
}

// cpStreamInfo returns the stream state.
func cpStreamInfo(t testing.TB, js jetstream.JetStream, stream string) *jetstream.StreamInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream %s info: %v", stream, err)
	}
	return info
}

// cpEnvelopeID decodes the id of an envelope body.
func cpEnvelopeID(t testing.TB, data []byte) string {
	t.Helper()
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("decode envelope %q: %v", data, err)
	}
	return env.ID
}

// cpExpectIDs polls until sink holds at least len(want) ids, then waits a
// grace period and requires the ids to equal want exactly: nothing arrives
// after the expected ones.
func cpExpectIDs(t testing.TB, sink *cpSink, grace time.Duration, want ...string) {
	t.Helper()
	cpPoll(t, 10*time.Second, "handler to receive "+joinIDs(want), func() bool {
		return len(sink.ids()) >= len(want)
	})
	time.Sleep(grace) // a grace window for extras, never the decision to proceed
	if got := sink.ids(); !slices.Equal(got, want) {
		t.Fatalf("handler received %v, want exactly %v", got, want)
	}
}

func joinIDs(ids []string) string {
	b, _ := json.Marshal(ids)
	return string(b)
}

// TestIntegration_CompactionKeepsLatestPerEntity is the tracer of the
// compaction guide: on a stream with MaxMsgsPerSubject=1 only the latest
// message of each entity subject survives (C1); a Bus subscription on the
// wildcard entity subject sees one message per entity, the latest, in
// stream-sequence order (C2); versions published while no consumer fetches are
// lost except the latest (C3); and a DeliverLastPerSubject consumer yields the
// same per-entity snapshot (C4) (DOC-02, D-09).
func TestIntegration_CompactionKeepsLatestPerEntity(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	entity := func(n string) string { return base + ".user." + n }
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:          []string{base + ".user.>"},
		MaxMsgsPerSubject: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// C1: user.1 is written three times, the others once, interleaved.
	bus := cpBus(t, js, event.NopObserver{})
	cpEmit(ctx, t, bus, entity("1"), "u1-v1")
	cpEmit(ctx, t, bus, entity("2"), "u2-v1")
	cpEmit(ctx, t, bus, entity("1"), "u1-v2")
	cpEmit(ctx, t, bus, entity("3"), "u3-v1")
	cpEmit(ctx, t, bus, entity("1"), "u1-v3")

	st := cpStreamInfo(t, js, stream).State
	if st.Msgs != 3 || st.FirstSeq != 2 || st.LastSeq != 5 || st.NumSubjects != 3 {
		t.Fatalf("C1: stream state msgs=%d first=%d last=%d subjects=%d, want msgs=3 first=2 last=5 subjects=3",
			st.Msgs, st.FirstSeq, st.LastSeq, st.NumSubjects)
	}

	// C2: a fresh durable consumer on the wildcard. Bus.Subscribe sets the
	// consumer FilterSubject to the subscribed subject verbatim. The delivery
	// order is the stream-sequence order (user.2, user.3, user.1), not the order
	// in which the entities were first created.
	consumer := uniqueName(t, "C")
	subscribe := func(b *Bus, sink *cpSink) {
		t.Helper()
		if err := b.Subscribe(ctx, base+".user.>", sink.handler(),
			event.WithStream(stream),
			event.WithConsumer(consumer),
			event.WithMaxDeliver(3),
			event.WithBackOff([]time.Duration{time.Second, time.Second}),
		); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	first := &cpSink{}
	subscribe(bus, first)
	cpExpectIDs(t, first, 500*time.Millisecond, "u2-v1", "u3-v1", "u1-v3")

	// C3: the consumer is stopped, two more versions of user.1 are published.
	// Version 4 is compacted away before any consumer fetches it.
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bus2 := cpBus(t, js, event.NopObserver{})
	cpEmit(ctx, t, bus2, entity("1"), "u1-v4")
	cpEmit(ctx, t, bus2, entity("1"), "u1-v5")

	second := &cpSink{}
	subscribe(bus2, second)
	cpExpectIDs(t, second, 500*time.Millisecond, "u1-v5")

	// C4: DeliverLastPerSubject, the right policy to rebuild current state, sees
	// the latest message of every entity and nothing else.
	cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
	defer ccancel()
	snap, err := js.CreateOrUpdateConsumer(cctx, stream, jetstream.ConsumerConfig{
		Name:          uniqueName(t, "snap"),
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		FilterSubject: base + ".user.>",
	})
	if err != nil {
		t.Fatalf("create DeliverLastPerSubject consumer: %v", err)
	}
	batch, err := snap.Fetch(3, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	snapshot := map[string]string{}
	for msg := range batch.Messages() {
		snapshot[msg.Subject()] = cpEnvelopeID(t, msg.Data())
	}
	if err := batch.Error(); err != nil {
		t.Fatalf("Fetch batch: %v", err)
	}
	want := map[string]string{entity("1"): "u1-v5", entity("2"): "u2-v1", entity("3"): "u3-v1"}
	if len(snapshot) != len(want) {
		t.Fatalf("C4: snapshot %v, want %v", snapshot, want)
	}
	for subject, id := range want {
		if snapshot[subject] != id {
			t.Errorf("C4: snapshot of %s = %q, want %q", subject, snapshot[subject], id)
		}
	}
}

// cpLastMsg returns the stream's last message for subject.
func cpLastMsg(t testing.TB, js jetstream.JetStream, stream, subject string) *jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	m, err := s.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		t.Fatalf("last message of %s on stream %s: %v", subject, stream, err)
	}
	return m
}

// cpCalls counts handler calls per event id.
type cpCalls struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *cpCalls) add(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[id]++
	return c.n[id]
}

func (c *cpCalls) get(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[id]
}

// TestIntegration_CompactionInFlightMessageNeverDeadLettered proves C5: a
// message that is delivered but not yet acknowledged and is then compacted away
// by a newer message of the same entity is never redelivered, so it can never
// reach its final delivery and is never dead-lettered. Its failure vanishes:
// the DLQ stream stays empty, the consumer has nothing pending and the Observer
// reports no Failure for it (DOC-02, D-09, D-11).
func TestIntegration_CompactionInFlightMessageNeverDeadLettered(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	subject := base + ".user.1"
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:          []string{base + ".user.>"},
		MaxMsgsPerSubject: 1,
	})
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	dlq := NewDLQ(js, prefix)
	rec := &joRecorder{}
	bus := NewBus(js, itRegistry(), BusOptions{
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		Observer:         rec,
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// v1 fails with a transient error, but only after v2 was published: it is
	// delivered, unacknowledged and in flight when it is compacted away.
	calls := &cpCalls{}
	v1Started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ context.Context, evt event.Event) error {
		o, ok := evt.(*itOrder)
		if !ok {
			return nil
		}
		if calls.add(o.ID) == 1 && o.ID == "u1-v1" {
			close(v1Started)
			<-release
			return errors.New("transient: the dependency is down")
		}
		return nil
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{time.Second, time.Second}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cpEmit(ctx, t, bus, subject, "u1-v1")
	select {
	case <-v1Started:
	case <-time.After(10 * time.Second):
		t.Fatal("v1 was not delivered within 10s")
	}
	cpEmit(ctx, t, bus, subject, "u1-v2")
	st := cpStreamInfo(t, js, stream).State
	if st.Msgs != 1 || st.FirstSeq != 2 {
		t.Fatalf("stream state msgs=%d first=%d, want v1 compacted away (msgs=1 first=2)", st.Msgs, st.FirstSeq)
	}
	close(release) // v1 now fails, Nak-ed although its message no longer exists

	cpPoll(t, 10*time.Second, "v2 to be handled", func() bool { return calls.get("u1-v2") == 1 })

	// Negative assertion over two backoff periods: the failed v1 must stay gone.
	// A redelivery of v1 would arrive after the 1s backoff, so a 2.5s window
	// covers two periods.
	for end := time.Now().Add(2500 * time.Millisecond); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		if n := calls.get("u1-v1"); n != 1 {
			t.Fatalf("v1 handled %d times, want exactly 1: a compacted in-flight message is never redelivered", n)
		}
	}
	if n := calls.get("u1-v2"); n != 1 {
		t.Errorf("v2 handled %d times, want exactly 1", n)
	}
	if n := itStreamMsgs(t, js, dlqStream); n != 0 {
		t.Errorf("DLQ stream holds %d messages, want 0: the failure of a compacted message is never dead-lettered", n)
	}
	if n := len(rec.deadLetters()); n != 0 {
		t.Errorf("Observer.DeadLettered called %d times, want 0", n)
	}
	if n := rec.failureCount(); n != 0 {
		t.Errorf("Observer.Failure called %d times, want 0: the failure of a compacted message vanishes", n)
	}
	waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.NumAckPending == 0 && ci.NumPending == 0
	})
}

// TestIntegration_CompactionCompactedOriginalStillReplays proves the replay
// interplay: a DLQ record whose original message was compacted away still
// replays. The Replayer republishes the verbatim snapshot to the original
// subject and the original consumer handles it, but on a MaxMsgsPerSubject=1
// stream the replayed, older snapshot becomes the latest message of the entity
// and evicts the newer state (DOC-02, D-09, D-11, T-11-12).
func TestIntegration_CompactionCompactedOriginalStillReplays(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	subject := base + ".user.1"
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:          []string{base + ".user.>"},
		MaxMsgsPerSubject: 1,
	})
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	dlq := NewDLQ(js, prefix)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewDLQ(dlq),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 1}),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		Observer:         event.NopObserver{},
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// v1 fails terminally while failV1 is set, so it is dead-lettered on its
	// first delivery; the test clears the flag before the replay.
	var failV1 atomic.Bool
	failV1.Store(true)
	calls := &cpCalls{}
	handler := func(_ context.Context, evt event.Event) error {
		o, ok := evt.(*itOrder)
		if !ok {
			return nil
		}
		calls.add(o.ID)
		if o.ID == "u1-v1" && failV1.Load() {
			return invoker.PermanentError{Err: errors.New("boom")}
		}
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

	cpEmit(ctx, t, bus, subject, "u1-v1")
	itWaitStreamMsgs(t, js, dlqStream, 1)
	orig := itStreamMsg(t, js, stream, 1).Data
	if got := cpEnvelopeID(t, orig); got != "u1-v1" {
		t.Fatalf("original message id = %q, want u1-v1", got)
	}

	// v2 compacts v1 out of the source stream: only the DLQ record still has it.
	cpEmit(ctx, t, bus, subject, "u1-v2")
	cpPoll(t, 10*time.Second, "v2 to be handled", func() bool { return calls.get("u1-v2") == 1 })
	st := cpStreamInfo(t, js, stream).State
	if st.Msgs != 1 || st.FirstSeq != 2 {
		t.Fatalf("source stream msgs=%d first=%d, want v1 compacted away (msgs=1 first=2)", st.Msgs, st.FirstSeq)
	}
	if got := cpEnvelopeID(t, cpLastMsg(t, js, stream, subject).Data); got != "u1-v2" {
		t.Fatalf("latest message of the entity = %q, want u1-v2", got)
	}
	if rec := itStreamMsg(t, js, dlqStream, 1); !bytes.Equal(rec.Data, orig) {
		t.Fatalf("DLQ record is not the verbatim original\n orig: %s\n dlq:  %s", orig, rec.Data)
	}

	// Replay although the original is gone.
	failV1.Store(false)
	res, err := NewReplayer(js).Replay(ctx, ReplayOptions{FromStream: dlqStream})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Fatalf("Replay result = %+v, want Replayed 1 Failed 0", res)
	}

	// The original consumer handles the verbatim snapshot (second call of v1).
	cpPoll(t, 10*time.Second, "the original consumer to handle the replayed v1", func() bool {
		return calls.get("u1-v1") == 2
	})

	// The replay is a new write: the stale snapshot is now the latest message
	// of the entity and the newer v2 state was evicted.
	last := cpLastMsg(t, js, stream, subject)
	if !bytes.Equal(last.Data, orig) {
		t.Errorf("latest message after the replay is not the verbatim v1\n want: %s\n got:  %s", orig, last.Data)
	}
	if got, want := last.Header.Get(jetstream.MsgIDHeader), "u1-v1:replay:1"; got != want {
		t.Errorf("replayed %s = %q, want %q", jetstream.MsgIDHeader, got, want)
	}
	st = cpStreamInfo(t, js, stream).State
	if st.Msgs != 1 || st.LastSeq != 3 {
		t.Errorf("source stream msgs=%d last=%d after the replay, want msgs=1 last=3: v2 was evicted by the stale snapshot",
			st.Msgs, st.LastSeq)
	}
}

// TestIntegration_CompactionIndependentOfMsgIDDedup proves C6: the Nats-Msg-Id
// duplicate window is independent of compaction. Re-publishing the id of a
// message that compaction already removed is still reported as a duplicate, is
// not stored and does not resurrect the old state (DOC-02, D-09, D-11).
func TestIntegration_CompactionIndependentOfMsgIDDedup(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	subject := base + ".user.1"
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:          []string{base + ".user.>"},
		MaxMsgsPerSubject: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	publish := func(id, body string) *jetstream.PubAck {
		t.Helper()
		ack, err := js.PublishMsg(ctx, &nats.Msg{
			Subject: subject,
			Data:    []byte(body),
			Header:  nats.Header{jetstream.MsgIDHeader: {id}},
		})
		if err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
		return ack
	}

	if ack := publish("X", "v1"); ack.Duplicate || ack.Sequence != 1 {
		t.Fatalf("first publish of X: %+v, want a new message at sequence 1", ack)
	}
	if ack := publish("Y", "v2"); ack.Duplicate || ack.Sequence != 2 {
		t.Fatalf("publish of Y: %+v, want a new message at sequence 2", ack)
	}
	st := cpStreamInfo(t, js, stream).State
	if st.Msgs != 1 || st.FirstSeq != 2 {
		t.Fatalf("stream msgs=%d first=%d, want X compacted away (msgs=1 first=2)", st.Msgs, st.FirstSeq)
	}

	// X is gone from the stream, yet its id is still inside the duplicate window.
	ack := publish("X", "v1")
	if !ack.Duplicate {
		t.Errorf("republishing X: %+v, want Duplicate true although X was compacted away", ack)
	}
	if ack.Sequence != 1 {
		t.Errorf("duplicate reports sequence %d, want 1 (the sequence X was stored at)", ack.Sequence)
	}
	st = cpStreamInfo(t, js, stream).State
	if st.Msgs != 1 || st.FirstSeq != 2 || st.LastSeq != 2 {
		t.Errorf("stream msgs=%d first=%d last=%d after the duplicate, want unchanged (msgs=1 first=2 last=2)",
			st.Msgs, st.FirstSeq, st.LastSeq)
	}
	if got := string(cpLastMsg(t, js, stream, subject).Data); got != "v2" {
		t.Errorf("latest message = %q, want v2", got)
	}
}

// TestIntegration_CompactionDLQStreamKeepsOnlyLatestFailure proves why the DLQ
// stream must never be compacted: the DLQ publisher stores a record on prefix
// plus the original subject, so a MaxMsgsPerSubject=1 DLQ stream keeps only the
// newest failure of an entity and silently drops the earlier ones. Never
// compact the DLQ stream (DOC-02, D-09, D-11).
func TestIntegration_CompactionDLQStreamKeepsOnlyLatestFailure(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	subject := base + ".user.1"
	// The source stream is NOT compacted, so both failures reach the handler.
	stream := newStream(t, js, base+".user.>")

	dlqBase := uniqueName(t, "dlq")
	prefix := dlqBase + "."
	dlqStream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:          []string{dlqBase + ".>"},
		MaxMsgsPerSubject: 1, // the mistake the guide forbids
	})
	consumer := uniqueName(t, "C")

	dlq := NewDLQ(js, prefix)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewDLQ(dlq),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 1}),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		Observer:         event.NopObserver{},
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	calls := &cpCalls{}
	handler := func(_ context.Context, evt event.Event) error {
		if o, ok := evt.(*itOrder); ok {
			calls.add(o.ID)
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

	// Two different failures of the same entity, one after the other so the
	// stream order is deterministic.
	cpEmit(ctx, t, bus, subject, "u1-a")
	cpPoll(t, 10*time.Second, "the first failure to be dead-lettered", func() bool {
		return cpStreamInfo(t, js, dlqStream).State.LastSeq == 1
	})
	cpEmit(ctx, t, bus, subject, "u1-b")
	cpPoll(t, 10*time.Second, "the second failure to be dead-lettered", func() bool {
		return cpStreamInfo(t, js, dlqStream).State.LastSeq == 2
	})

	st := cpStreamInfo(t, js, dlqStream).State
	if st.Msgs != 1 || st.FirstSeq != 2 {
		t.Errorf("DLQ stream msgs=%d first=%d, want only the newest failure (msgs=1 first=2)", st.Msgs, st.FirstSeq)
	}
	rec := cpLastMsg(t, js, dlqStream, prefix+subject)
	if got := rec.Header.Get(event.HeaderDLQEventID); got != "u1-b" {
		t.Errorf("remaining DLQ record is for event %q, want u1-b: the failure of u1-a was lost", got)
	}
	if n := calls.get("u1-a"); n != 1 {
		t.Errorf("u1-a handled %d times, want 1", n)
	}
}

// TestIntegration_CompactionDiscardNewPerSubjectRejects proves C7: with
// Discard DiscardNew and DiscardNewPerSubject the stream rejects a second
// message on a subject that is at its MaxMsgsPerSubject limit instead of
// evicting the old one. That is a "first write wins" log, the opposite of
// compaction, and the publisher sees an error (DOC-02, D-08, D-09).
func TestIntegration_CompactionDiscardNewPerSubjectRejects(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	subject := base + ".user.1"
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:             []string{base + ".user.>"},
		MaxMsgsPerSubject:    1,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := js.Publish(ctx, subject, []byte("v1")); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	_, err := js.Publish(ctx, subject, []byte("v2"))
	if err == nil {
		t.Fatal("second publish on the same subject succeeded, want it rejected by DiscardNewPerSubject")
	}
	if !strings.Contains(err.Error(), "maximum messages per subject exceeded") {
		t.Errorf("second publish error = %q, want it to mention %q", err, "maximum messages per subject exceeded")
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode != 10077 {
			t.Errorf("API error code = %d, want 10077", apiErr.ErrorCode)
		}
	} else {
		t.Errorf("second publish error %T is not a *jetstream.APIError", err)
	}

	st := cpStreamInfo(t, js, stream).State
	if st.Msgs != 1 || st.LastSeq != 1 {
		t.Errorf("stream msgs=%d last=%d, want the rejected v2 not stored (msgs=1 last=1)", st.Msgs, st.LastSeq)
	}
	if got := string(cpLastMsg(t, js, stream, subject).Data); got != "v1" {
		t.Errorf("latest message = %q, want v1: DiscardNew keeps the first write", got)
	}
}

// cpTryLastMsg is cpLastMsg that reports a missing message as an error.
func cpTryLastMsg(js jetstream.JetStream, stream, subject string) (*jetstream.RawStreamMsg, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return nil, err
	}
	return s.GetLastMsgForSubject(ctx, subject)
}

// TestIntegration_CompactionDeleteMarkerReachesConsumerAsEmptyBody proves the
// marker hazard of the guide. With AllowMsgTTL and SubjectDeleteMarkerTTL, when
// the last message of a subject expires by MaxAge the server inserts an
// empty-body marker on the same subject with headers Nats-Marker-Reason MaxAge,
// Nats-Rollup sub and Nats-TTL (C8). A Bus consumer on the subject receives it
// as an ordinary message and dead-letters it as decode_malformed (C9). Eviction
// by MaxMsgsPerSubject inserts no marker (C10, the limit case) (DOC-02, D-08).
//
// Expiry is lazy: a MaxAge of 2s still held the message at 3.5s when probed, so
// the marker is polled for up to 30s and the elapsed time is logged, never slept
// on.
func TestIntegration_CompactionDeleteMarkerReachesConsumerAsEmptyBody(t *testing.T) {
	_, js := connect(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// C10: a limit eviction inserts no marker. This uses its own stream without
	// MaxAge, so no expiry can race the check and add a marker of its own.
	limBase := uniqueName(t, "cp")
	limSubject := limBase + ".user.1"
	limStream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:               []string{limBase + ".user.>"},
		MaxMsgsPerSubject:      1,
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: 5 * time.Second,
	})
	for _, body := range []string{"v1", "v2"} {
		if _, err := js.Publish(ctx, limSubject, []byte(body)); err != nil {
			t.Fatalf("C10: publish %s: %v", body, err)
		}
	}
	// A marker would take a stream sequence of its own; look for one for a while.
	for end := time.Now().Add(time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		st := cpStreamInfo(t, js, limStream).State
		if st.Msgs != 1 || st.FirstSeq != 2 || st.LastSeq != 2 {
			t.Fatalf("C10: stream msgs=%d first=%d last=%d, want v1 evicted with no marker (msgs=1 first=2 last=2)",
				st.Msgs, st.FirstSeq, st.LastSeq)
		}
	}
	last := cpLastMsg(t, js, limStream, limSubject)
	if string(last.Data) != "v2" || last.Header.Get("Nats-Marker-Reason") != "" {
		t.Errorf("C10: latest message = %q with marker reason %q, want v2 and no marker",
			last.Data, last.Header.Get("Nats-Marker-Reason"))
	}

	// C8: the marker of a MaxAge expiry.
	base := uniqueName(t, "cp")
	subject := base + ".user.1"
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:               []string{base + ".user.>"},
		MaxMsgsPerSubject:      1,
		MaxAge:                 3 * time.Second,
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: 5 * time.Second,
	})

	// C9: a Bus consumer is already subscribed when the marker appears, so it
	// receives the marker live, within the marker's own 5s lifetime.
	rec := &joRecorder{}
	dlq := event.NewMemoryDLQ()
	bus := NewBus(js, itRegistry(), BusOptions{
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		Observer:         rec,
	})
	t.Cleanup(func() { _ = bus.Close() })
	sink := &cpSink{}
	if err := bus.Subscribe(ctx, subject, sink.handler(),
		event.WithStream(stream),
		event.WithConsumer(uniqueName(t, "C")),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{time.Second, time.Second}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cpEmit(ctx, t, bus, subject, "u1-v1")
	published := time.Now()
	cpPoll(t, 10*time.Second, "the real message to be handled", func() bool { return len(sink.ids()) == 1 })

	var marker *jetstream.RawStreamMsg
	cpPoll(t, 30*time.Second, "the MaxAge delete marker to appear in the stream", func() bool {
		m, err := cpTryLastMsg(js, stream, subject)
		if err != nil || len(m.Data) != 0 || m.Header.Get("Nats-Marker-Reason") == "" {
			return false
		}
		marker = m
		return true
	})
	t.Logf("observed: MaxAge 3s marker (TTL 5s) appeared %s after the publish", time.Since(published).Round(100*time.Millisecond))

	if len(marker.Data) != 0 {
		t.Errorf("C8: marker body = %q, want empty", marker.Data)
	}
	if got := marker.Header.Get("Nats-Marker-Reason"); got != "MaxAge" {
		t.Errorf("C8: Nats-Marker-Reason = %q, want MaxAge", got)
	}
	if got := marker.Header.Get("Nats-Rollup"); got != "sub" {
		t.Errorf("C8: Nats-Rollup = %q, want sub", got)
	}
	if got := marker.Header.Get("Nats-TTL"); got == "" {
		t.Error("C8: Nats-TTL header is missing")
	} else {
		t.Logf("observed: Nats-TTL = %q", got)
	}
	if marker.Subject != subject {
		t.Errorf("C8: marker subject = %q, want the entity subject %q", marker.Subject, subject)
	}

	// C9: the Bus consumer got the marker as a message with an empty body and
	// dead-lettered it as a malformed envelope; the handler never saw it.
	cpPoll(t, 15*time.Second, "the Bus to dead-letter the marker", func() bool { return dlq.Len() == 1 })
	rows := dlq.Records()
	if got := rows[0].Reason; got != event.ReasonDecodeMalformed {
		t.Errorf("C9: dead-letter reason = %q, want %q", got, event.ReasonDecodeMalformed)
	}
	if len(rows[0].Raw) != 0 {
		t.Errorf("C9: dead-lettered body = %q, want empty", rows[0].Raw)
	}
	if got := nats.Header(rows[0].Headers).Get("Nats-Marker-Reason"); got != "MaxAge" {
		t.Errorf("C9: dead-lettered Nats-Marker-Reason = %q, want MaxAge", got)
	}
	cpPoll(t, 5*time.Second, "the decode failure to be reported", func() bool { return len(rec.decodeFailures()) == 1 })
	if df := rec.decodeFailures()[0]; !errors.Is(df.Err, event.ErrMalformedEnvelope) {
		t.Errorf("C9: DecodeFailed error = %v, want it to match event.ErrMalformedEnvelope", df.Err)
	}
	if got := sink.ids(); !slices.Equal(got, []string{"u1-v1"}) {
		t.Errorf("C9: handler received %v, want only the real message u1-v1", got)
	}
}

// TestIntegration_CompactionKVBucketIsAStream proves C12 and C13: a KV bucket
// is a stream KV_<bucket> on $KV.<bucket>.> whose MaxMsgsPerSubject equals the
// History, so it is a compacted log the library can read as one. Delete appends
// an empty KV-Operation DEL marker and Purge a KV-Operation PURGE marker that
// rolls up every earlier revision of the key (DOC-02, D-12).
func TestIntegration_CompactionKVBucketIsAStream(t *testing.T) {
	_, js := connect(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bucket := uniqueName(t, "kv")
	const history = 3
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: history, Storage: jetstream.MemoryStorage})
	if err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_ = js.DeleteKeyValue(dctx, bucket)
	})

	// C12: the bucket is a stream, and History is its per-subject limit.
	stream := "KV_" + bucket
	cfg := cpStreamInfo(t, js, stream).Config
	if want := []string{"$KV." + bucket + ".>"}; !slices.Equal(cfg.Subjects, want) {
		t.Errorf("C12: subjects = %v, want %v", cfg.Subjects, want)
	}
	if cfg.MaxMsgsPerSubject != history {
		t.Errorf("C12: MaxMsgsPerSubject = %d, want the History %d", cfg.MaxMsgsPerSubject, history)
	}
	if !cfg.DenyDelete || !cfg.AllowRollup {
		t.Errorf("C12: DenyDelete=%v AllowRollup=%v, want both true", cfg.DenyDelete, cfg.AllowRollup)
	}
	if cfg.Discard != jetstream.DiscardNew || cfg.DiscardNewPerSubject {
		t.Errorf("C12: Discard=%v DiscardNewPerSubject=%v, want DiscardNew without the per-subject flag: the history limit still evicts old revisions",
			cfg.Discard, cfg.DiscardNewPerSubject)
	}

	// C13, Delete: three revisions fill the history; Delete appends a marker
	// and the history limit evicts the oldest revision.
	keySubject := "$KV." + bucket + ".k"
	for _, v := range []string{"v1", "v2", "v3"} {
		if _, err := kv.PutString(ctx, "k", v); err != nil {
			t.Fatalf("Put %s: %v", v, err)
		}
	}
	if st := cpStreamInfo(t, js, stream).State; st.Msgs != history {
		t.Fatalf("C13: stream holds %d messages after three Puts, want %d", st.Msgs, history)
	}
	if err := kv.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	del := cpLastMsg(t, js, stream, keySubject)
	if len(del.Data) != 0 {
		t.Errorf("C13: delete marker body = %q, want empty", del.Data)
	}
	if got := del.Header.Get("KV-Operation"); got != "DEL" {
		t.Errorf("C13: KV-Operation = %q, want DEL", got)
	}
	if st := cpStreamInfo(t, js, stream).State; st.Msgs != history || st.FirstSeq != 2 {
		t.Errorf("C13: after Delete stream msgs=%d first=%d, want the history limit to evict v1 (msgs=%d first=2)",
			st.Msgs, st.FirstSeq, history)
	}
	if _, err := kv.Get(ctx, "k"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Errorf("C13: Get after Delete = %v, want ErrKeyNotFound", err)
	}

	// C13, Purge: the marker rolls up every earlier revision of the key.
	if _, err := kv.PutString(ctx, "k", "v4"); err != nil {
		t.Fatalf("Put v4: %v", err)
	}
	if err := kv.Purge(ctx, "k"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	purge := cpLastMsg(t, js, stream, keySubject)
	if got := purge.Header.Get("KV-Operation"); got != "PURGE" {
		t.Errorf("C13: KV-Operation = %q, want PURGE", got)
	}
	if got := purge.Header.Get("Nats-Rollup"); got != "sub" {
		t.Errorf("C13: Nats-Rollup = %q, want sub", got)
	}
	if st := cpStreamInfo(t, js, stream).State; st.Msgs != 1 || st.FirstSeq != st.LastSeq {
		t.Errorf("C13: after Purge stream msgs=%d first=%d last=%d, want the marker alone", st.Msgs, st.FirstSeq, st.LastSeq)
	}
	if _, err := kv.Get(ctx, "k"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Errorf("C13: Get after Purge = %v, want ErrKeyNotFound", err)
	}
}
