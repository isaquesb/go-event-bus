package event_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/isaquesb/go-event-bus"
)

func TestTruncateHeader(t *testing.T) {
	t.Run("short value unchanged", func(t *testing.T) {
		if got := event.TruncateHeader("hello", 1024); got != "hello" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("long ascii is capped with the marker", func(t *testing.T) {
		got := event.TruncateHeader(strings.Repeat("a", 5000), 1024)
		if len(got) > 1024 || !strings.HasSuffix(got, event.TruncationMarker) {
			t.Errorf("len %d, suffix ok %v", len(got), strings.HasSuffix(got, event.TruncationMarker))
		}
	})
	t.Run("never splits a rune", func(t *testing.T) {
		got := event.TruncateHeader(strings.Repeat("é", 600), 1024)
		if !utf8.ValidString(got) || len(got) > 1024 {
			t.Errorf("valid %v, len %d", utf8.ValidString(got), len(got))
		}
		// A budget that lands inside a rune must cut before it.
		got = event.TruncateHeader(strings.Repeat("é", 600), 1023)
		if !utf8.ValidString(got) || len(got) > 1023 {
			t.Errorf("odd budget: valid %v, len %d", utf8.ValidString(got), len(got))
		}
	})
	t.Run("control characters become spaces", func(t *testing.T) {
		got := event.TruncateHeader("a\r\nb\tc\x00d\x7fe", 1024)
		if got != "a  b c d e" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("invalid utf-8 is repaired", func(t *testing.T) {
		got := event.TruncateHeader("ok\xff\xfeend", 1024)
		if !utf8.ValidString(got) || !strings.HasPrefix(got, "ok") || !strings.HasSuffix(got, "end") {
			t.Errorf("got %q", got)
		}
	})
	t.Run("tiny budget", func(t *testing.T) {
		got := event.TruncateHeader("abcdefghij", 4)
		if len(got) > 4 {
			t.Errorf("len %d, want at most 4", len(got))
		}
	})
}

func fsFullRecord() event.DLQRecord {
	return event.DLQRecord{
		Evt:       &fsEvent{},
		EventID:   "evt-1",
		EventName: "orders.created",
		Cause:     errors.New("boom"),
		Reason:    event.ReasonTerminal,
		Handler:   "h",
		Subject:   "orders.created",
		Attempts:  2,
		Delivery: event.Delivery{
			Count: 3, Max: 5, Subject: "orders.created", Stream: "ORDERS", StreamSeq: 77,
			PublishedAt: time.Date(2026, 10, 16, 1, 2, 3, 0, time.UTC),
		},
		Execution: event.Execution{Handler: "h", StartedAt: time.Date(2026, 10, 16, 1, 2, 0, 0, time.UTC)},
		FailedAt:  time.Date(2026, 10, 16, 1, 2, 4, 5, time.UTC),
	}
}

func TestFailureHeaders(t *testing.T) {
	t.Run("fully populated", func(t *testing.T) {
		got := fsFullRecord().FailureHeaders()
		want := map[string]string{
			event.HeaderDLQCause:            "boom",
			event.HeaderDLQReason:           "terminal",
			event.HeaderDLQAttempts:         "2",
			event.HeaderDLQDeliveryCount:    "3",
			event.HeaderDLQHandler:          "h",
			event.HeaderDLQOriginalSubject:  "orders.created",
			event.HeaderDLQFailedAt:         "2026-10-16T01:02:04.000000005Z",
			event.HeaderDLQFirstAttemptAt:   "2026-10-16T01:02:00Z",
			event.HeaderDLQEventID:          "evt-1",
			event.HeaderDLQEventName:        "orders.created",
			event.HeaderDLQOriginalStream:   "ORDERS",
			event.HeaderDLQOriginalSequence: "77",
			event.HeaderDLQPublishedAt:      "2026-10-16T01:02:03Z",
		}
		if len(got) != len(want) {
			t.Errorf("got %d headers, want %d: %v", len(got), len(want), got)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
		}
		for k := range got {
			if !strings.HasPrefix(k, event.HeaderDLQPrefix) {
				t.Errorf("header %q lacks the %q prefix", k, event.HeaderDLQPrefix)
			}
		}
	})
	t.Run("zero record omits unknown values", func(t *testing.T) {
		got := event.DLQRecord{}.FailureHeaders()
		for _, k := range []string{
			event.HeaderDLQFirstAttemptAt, event.HeaderDLQEventID, event.HeaderDLQOriginalStream,
			event.HeaderDLQOriginalSequence, event.HeaderDLQPublishedAt, event.HeaderDLQFailedAt,
			event.HeaderDLQCause, event.HeaderDLQHandler,
		} {
			if _, ok := got[k]; ok {
				t.Errorf("zero record carries %s", k)
			}
		}
		if got[event.HeaderDLQAttempts] != "0" {
			t.Errorf("Attempts = %q, want 0 (the handler never ran)", got[event.HeaderDLQAttempts])
		}
	})
	t.Run("long cause is bounded", func(t *testing.T) {
		rec := fsFullRecord()
		rec.Cause = errors.New(strings.Repeat("x", 5000))
		if n := len(rec.FailureHeaders()[event.HeaderDLQCause]); n > event.DLQHeaderValueLimit || n == 0 {
			t.Errorf("cause header is %d bytes, want 1..%d", n, event.DLQHeaderValueLimit)
		}
	})
	t.Run("cause with control characters is sanitized", func(t *testing.T) {
		rec := fsFullRecord()
		rec.Cause = errors.New("line1\r\nInjected: yes")
		got := rec.FailureHeaders()[event.HeaderDLQCause]
		if got == "" || strings.ContainsAny(got, "\r\n") {
			t.Errorf("cause header %q is empty or still has CR/LF", got)
		}
	})
}

// fsPublisher counts calls and fails according to failUntil: calls numbered
// up to failUntil fail, later ones succeed. A negative failUntil always fails.
type fsPublisher struct {
	calls     atomic.Int32
	failUntil int32
}

func (p *fsPublisher) Publish(context.Context, event.DLQRecord) error {
	n := p.calls.Add(1)
	if p.failUntil < 0 || n <= p.failUntil {
		return errors.New("broker down")
	}
	return nil
}

func TestPublishDeadLetter_RetriesOnFinalDelivery(t *testing.T) {
	t.Run("final delivery tries three times", func(t *testing.T) {
		p := &fsPublisher{failUntil: -1}
		start := time.Now()
		err := event.PublishDeadLetter(context.Background(), p, event.DLQRecord{Delivery: event.Delivery{Count: 1, Max: 1}})
		if err == nil || p.calls.Load() != 3 {
			t.Fatalf("err %v, calls %d, want an error after 3 calls", err, p.calls.Load())
		}
		if el := time.Since(start); el < 250*time.Millisecond {
			t.Errorf("elapsed %v, want at least 250ms of backoff", el)
		}
	})
	t.Run("non-final delivery tries once", func(t *testing.T) {
		p := &fsPublisher{failUntil: -1}
		err := event.PublishDeadLetter(context.Background(), p, event.DLQRecord{Delivery: event.Delivery{Count: 1, Max: 3}})
		if err == nil || p.calls.Load() != 1 {
			t.Fatalf("err %v, calls %d, want an error after 1 call", err, p.calls.Load())
		}
	})
	t.Run("success on the second call", func(t *testing.T) {
		p := &fsPublisher{failUntil: 1}
		if err := event.PublishDeadLetter(context.Background(), p, event.DLQRecord{Delivery: event.Delivery{Count: 1, Max: 1}}); err != nil {
			t.Fatalf("err %v, want nil", err)
		}
		if p.calls.Load() != 2 {
			t.Errorf("calls %d, want 2", p.calls.Load())
		}
	})
	t.Run("cancelled ctx stops after the first call", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p := &fsPublisher{failUntil: -1}
		if err := event.PublishDeadLetter(ctx, p, event.DLQRecord{Delivery: event.Delivery{Count: 1, Max: 1}}); err == nil {
			t.Fatal("want the publish error")
		}
		if p.calls.Load() != 1 {
			t.Errorf("calls %d, want 1", p.calls.Load())
		}
	})
}

func TestMemoryDLQ_Concurrent(t *testing.T) {
	m := event.NewMemoryDLQ()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Publish(context.Background(), event.DLQRecord{Raw: []byte("x"), Handler: "h" + strconv.Itoa(i)})
		}()
	}
	wg.Wait()
	if m.Len() != 50 {
		t.Fatalf("Len = %d, want 50", m.Len())
	}
	recs := m.Records()
	recs[0].Handler = "mutated"
	recs[0].Raw[0] = 'z'
	again := m.Records()
	if again[0].Handler == "mutated" || again[0].Raw[0] != 'x' {
		t.Error("mutating a Records() copy changed the store")
	}

	var zero event.MemoryDLQ
	if err := zero.Publish(context.Background(), event.DLQRecord{}); err != nil || zero.Len() != 1 {
		t.Errorf("zero value MemoryDLQ not usable: err %v, len %d", err, zero.Len())
	}
}
