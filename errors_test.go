package event_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	event "github.com/isaquesb/go-event-bus"
)

var wireSentinels = []error{
	event.ErrUnknownEvent,
	event.ErrMalformedEnvelope,
	event.ErrMissingUpcaster,
	event.ErrUnknownVersion,
}

func TestDecodeError_IsSentinel(t *testing.T) {
	for _, s := range wireSentinels {
		de := &event.DecodeError{Name: "user.created", Version: 1, Err: s}
		if !errors.Is(de, s) {
			t.Errorf("errors.Is(DecodeError, %v) = false", s)
		}
	}
}

func TestDecodeError_IsCauseThroughMultiWrap(t *testing.T) {
	cause := errors.New("invalid character 'x'")
	de := &event.DecodeError{Err: fmt.Errorf("%w: %w", event.ErrMalformedEnvelope, cause)}
	if !errors.Is(de, event.ErrMalformedEnvelope) {
		t.Error("sentinel not matched")
	}
	if !errors.Is(de, cause) {
		t.Error("cause not matched")
	}
	if errors.Is(de, event.ErrUnknownEvent) {
		t.Error("unrelated sentinel matched")
	}
}

func TestDecodeError_AsThroughTransportWrap(t *testing.T) {
	de := &event.DecodeError{Name: "user.created", Version: 3, Err: event.ErrUnknownVersion}
	wrapped := fmt.Errorf("nats: %w", de)
	var got *event.DecodeError
	if !errors.As(wrapped, &got) {
		t.Fatal("errors.As did not find *DecodeError")
	}
	if got.Name != "user.created" || got.Version != 3 {
		t.Fatalf("got %+v", got)
	}
	if !errors.Is(wrapped, event.ErrUnknownVersion) {
		t.Error("sentinel not matched through wrap")
	}
}

func TestDecodeError_ErrorText(t *testing.T) {
	de := &event.DecodeError{Name: "user.created", Version: 3, Err: event.ErrUnknownVersion}
	msg := de.Error()
	if !strings.Contains(msg, "user.created") || !strings.Contains(msg, "v3") || !strings.Contains(msg, event.ErrUnknownVersion.Error()) {
		t.Errorf("Error() = %q", msg)
	}
	noName := (&event.DecodeError{Err: event.ErrMalformedEnvelope}).Error()
	if noName != "event: decode: event: malformed envelope" {
		t.Errorf("Error() = %q", noName)
	}
}

func TestDecodeError_NilErrDoesNotPanic(t *testing.T) {
	de := &event.DecodeError{Name: "x", Version: 1}
	if !strings.Contains(de.Error(), "unknown error") {
		t.Errorf("Error() = %q", de.Error())
	}
	if de.Unwrap() != nil {
		t.Error("Unwrap() != nil")
	}
	if !strings.Contains((&event.DecodeError{}).Error(), "unknown error") {
		t.Error("zero value text")
	}
}

func TestSentinels_AreDistinct(t *testing.T) {
	for i, a := range wireSentinels {
		for j, b := range wireSentinels {
			if i != j && errors.Is(a, b) {
				t.Errorf("%v matches %v", a, b)
			}
		}
	}
}
