package event_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

type hhEvent struct{}

func (hhEvent) Name() string { return "hh.order" }

// hhPublisher fails every call and counts them.
type hhPublisher struct {
	calls int
	err   error
}

func (p *hhPublisher) Publish(context.Context, event.DLQRecord) error {
	p.calls++
	return p.err
}

func hhHistory() []event.Attempt {
	base := time.Date(2026, 10, 17, 7, 0, 0, 0, time.UTC)
	return []event.Attempt{
		event.NewAttempt(1, errors.New("db down"), event.AttemptTransient, base, 120*time.Millisecond, 10*time.Millisecond),
		event.NewAttempt(2, errors.New("db down"), event.AttemptTransient, base.Add(time.Second), 80*time.Millisecond, 20*time.Millisecond),
		event.NewAttempt(3, errors.New("deadline"), event.AttemptTimeout, base.Add(2*time.Second), time.Second, 0),
	}
}

func TestDLQRecord_HistoryToHeaderAndSignal(t *testing.T) {
	rec := &obsRecorder{}
	raw := make([]byte, 64)
	for i := range raw {
		raw[i] = byte('a' + i%26)
	}
	ctx := event.WithObserver(context.Background(), rec)
	ctx = event.WithDelivery(ctx, event.Delivery{Count: 3, Max: 3, Subject: "hh.subject", Raw: raw})
	ctx = event.WithMetadata(ctx, event.Metadata{ID: "h-1"})

	cause := &event.FailureError{
		Cause:    errors.New("db down"),
		Reason:   event.ReasonRetriesExhausted,
		Attempts: 3,
		History:  hhHistory(),
	}
	r := event.NewDLQRecord(ctx, hhEvent{}, "h", cause)
	if len(r.History) != 3 {
		t.Fatalf("len(History) = %d, want 3", len(r.History))
	}
	cause.History[0].N = 99
	if r.History[0].N != 1 {
		t.Fatalf("record History aliases the FailureError slice: N = %d", r.History[0].N)
	}

	h := r.FailureHeaders()
	var got []map[string]any
	if err := json.Unmarshal([]byte(h[event.HeaderDLQHistory]), &got); err != nil {
		t.Fatalf("history header is not valid JSON: %v (%q)", err, h[event.HeaderDLQHistory])
	}
	if len(got) != 3 {
		t.Fatalf("history entries = %d, want 3", len(got))
	}
	wantKeys := []string{"at", "class", "dur_ms", "err", "n"}
	wantDur := []float64{120, 80, 1000}
	for i, e := range got {
		keys := make([]string, 0, len(e))
		for k := range e {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) != len(wantKeys) {
			t.Fatalf("entry %d keys = %v, want %v", i, keys, wantKeys)
		}
		for j := range keys {
			if keys[j] != wantKeys[j] {
				t.Fatalf("entry %d keys = %v, want %v", i, keys, wantKeys)
			}
		}
		if e["n"] != float64(i+1) {
			t.Errorf("entry %d n = %v, want %d", i, e["n"], i+1)
		}
		if e["dur_ms"] != wantDur[i] {
			t.Errorf("entry %d dur_ms = %v, want %v", i, e["dur_ms"], wantDur[i])
		}
	}
	if got[2]["class"] != "timeout" {
		t.Errorf("class of third = %v, want timeout", got[2]["class"])
	}
	if _, perr := time.Parse(time.RFC3339Nano, got[0]["at"].(string)); perr != nil {
		t.Errorf("at is not RFC3339Nano: %v", got[0]["at"])
	}
	if _, ok := h[event.HeaderDLQHistoryTruncated]; ok {
		t.Errorf("unexpected %s header", event.HeaderDLQHistoryTruncated)
	}

	mem := event.NewMemoryDLQ()
	if err := event.PublishDeadLetter(ctx, mem, r); err != nil {
		t.Fatalf("PublishDeadLetter: %v", err)
	}
	dl := rec.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLettered signals = %d, want 1", len(dl))
	}
	d := dl[0]
	if d.Handler != "h" || d.Event != "hh.order" || d.Reason != event.ReasonRetriesExhausted ||
		d.Attempts != 3 || d.Size != 64 {
		t.Errorf("DeadLetterInfo = %+v", d)
	}
	if d.Delivery.Raw != nil {
		t.Errorf("DeadLetterInfo.Delivery.Raw = %d bytes, want nil", len(d.Delivery.Raw))
	}
	if n := len(rec.Calls()); n != 1 {
		t.Errorf("observer calls = %v, want only DeadLettered", rec.Calls())
	}
	stored := mem.Records()
	if len(stored) != 1 || string(stored[0].Raw) != string(raw) || len(stored[0].History) != 3 {
		t.Errorf("stored record changed: raw=%d history=%d", len(stored[0].Raw), len(stored[0].History))
	}
}

func TestPublishDeadLetter_FailureSignalOnce(t *testing.T) {
	cases := []struct {
		name      string
		delivery  event.Delivery
		wantCalls int
		wantFinal bool
	}{
		{"final", event.Delivery{Count: 3, Max: 3, Raw: []byte("payload")}, 3, true},
		{"not final", event.Delivery{Count: 1, Max: 3, Raw: []byte("payload")}, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &obsRecorder{}
			ctx := event.WithObserver(context.Background(), rec)
			ctx = event.WithDelivery(ctx, tc.delivery)
			pub := &hhPublisher{err: errors.New("broker down")}
			r := event.NewDLQRecord(ctx, hhEvent{}, "h", errors.New("boom"))
			if err := event.PublishDeadLetter(ctx, pub, r); err == nil {
				t.Fatal("PublishDeadLetter = nil, want error")
			}
			if pub.calls != tc.wantCalls {
				t.Errorf("publisher calls = %d, want %d", pub.calls, tc.wantCalls)
			}
			var failed []event.DLQPublishFailure
			for _, c := range rec.All() {
				if c.Method == "DLQPublishFailed" {
					failed = append(failed, c.Payload.(event.DLQPublishFailure))
				}
			}
			if len(failed) != 1 {
				t.Fatalf("DLQPublishFailed signals = %d, want 1", len(failed))
			}
			f := failed[0]
			if f.Final != tc.wantFinal || f.Delivery.Raw != nil || f.Err == nil || f.Handler != "h" {
				t.Errorf("DLQPublishFailure = %+v", f)
			}
			if n := len(rec.DeadLetters()); n != 0 {
				t.Errorf("DeadLettered signals = %d, want 0", n)
			}
		})
	}
}

func TestFailureHeaders_NoHistoryNoHeader(t *testing.T) {
	r := event.NewDLQRecord(context.Background(), hhEvent{}, "h", errors.New("boom"))
	h := r.FailureHeaders()
	if _, ok := h[event.HeaderDLQHistory]; ok {
		t.Errorf("unexpected %s header", event.HeaderDLQHistory)
	}
	if _, ok := h[event.HeaderDLQHistoryTruncated]; ok {
		t.Errorf("unexpected %s header", event.HeaderDLQHistoryTruncated)
	}
}
