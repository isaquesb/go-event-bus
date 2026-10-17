package event

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"time"
)

// HistoryHeaderLimit is the byte cap of the Eventbus-Dlq-History header value.
// It is larger than DLQHeaderValueLimit because the value holds several
// attempts.
const HistoryHeaderLimit = 2048

// historyEntry is the wire form of one Attempt. The keys are wire format once
// released. Attempt.Delay is deliberately not on the wire.
type historyEntry struct {
	N     int          `json:"n"`
	Err   string       `json:"err"`
	Class AttemptClass `json:"class"`
	At    string       `json:"at"`
	DurMS int64        `json:"dur_ms"`
}

// EncodeHistoryHeader encodes the retry history as the compact JSON array that
// is the value of the Eventbus-Dlq-History header:
//
//	[{"n":1,"err":"…","class":"transient","at":"2026-10-17T07:00:00Z","dur_ms":120},…]
//
// Each entry has exactly the keys n, err, class, at (RFC3339Nano, UTC) and
// dur_ms (whole milliseconds). The result is at most capBytes bytes long and is
// always valid JSON. When the whole history does not fit, the oldest attempts
// are dropped until it does and the second result is true. When even the
// newest attempt alone does not fit, its err text is shortened further, and as
// a last resort the value is "[]". An empty history gives "" and false.
//
// It is a pure function shared by every broker DLQ publisher through
// DLQRecord.FailureHeaders. Text is escaped by encoding/json, never
// concatenated, so hostile error text cannot break out of its string.
func EncodeHistoryHeader(history []Attempt, capBytes int) (string, bool) {
	if len(history) == 0 {
		return "", false
	}
	if capBytes < 2 {
		capBytes = 2
	}
	parts := make([][]byte, len(history))
	for i, a := range history {
		parts[i] = marshalHistoryEntry(a, sanitizeHistoryErr(a.Err))
	}
	// Walk from the newest entry back while the array still fits.
	total := 2
	first := len(parts)
	for i := len(parts) - 1; i >= 0; i-- {
		need := len(parts[i])
		if first != len(parts) {
			need++ // separating comma
		}
		if total+need > capBytes {
			break
		}
		total += need
		first = i
	}
	if first == len(parts) {
		// Not even the newest attempt fits: shorten its err text, and as a
		// last resort send an empty array.
		newest := history[len(history)-1]
		if e, ok := shrinkHistoryEntry(newest, capBytes-2); ok {
			return "[" + string(e) + "]", true
		}
		return "[]", true
	}
	return "[" + string(bytes.Join(parts[first:], []byte(","))) + "]", first > 0
}

// sanitizeHistoryErr makes err text safe for a header: invalid UTF-8 and
// control characters are replaced like TruncateHeader does, without a length
// cut.
func sanitizeHistoryErr(s string) string {
	return TruncateHeader(s, math.MaxInt)
}

// shrinkHistoryEntry encodes a with the longest err that keeps the entry within
// budget bytes. It reports false when even an empty err does not fit.
func shrinkHistoryEntry(a Attempt, budget int) ([]byte, bool) {
	empty := marshalHistoryEntry(a, "")
	if len(empty) > budget {
		return nil, false
	}
	errText := sanitizeHistoryErr(a.Err)
	room := budget - len(empty)
	if room > len(errText) {
		room = len(errText)
	}
	for room > 0 {
		e := marshalHistoryEntry(a, TruncateHeader(errText, room))
		if len(e) <= budget {
			return e, true
		}
		// JSON escaping made the text longer than its byte count; cut by the
		// overshoot (at least one byte) and retry.
		room -= len(e) - budget
	}
	return empty, true
}

// marshalHistoryEntry encodes one attempt with errText as its err field, using
// an encoder with HTML escaping off so < > & stay readable.
func marshalHistoryEntry(a Attempt, errText string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(historyEntry{
		N:     a.N,
		Err:   errText,
		Class: a.Class,
		At:    a.Started.UTC().Format(time.RFC3339Nano),
		DurMS: a.Duration.Milliseconds(),
	})
	return []byte(strings.TrimSuffix(buf.String(), "\n"))
}
