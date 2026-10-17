package event_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	event "github.com/isaquesb/go-event-bus"
)

type hhWire struct {
	N     int    `json:"n"`
	Err   string `json:"err"`
	Class string `json:"class"`
	At    string `json:"at"`
	DurMS int64  `json:"dur_ms"`
}

// hhAttempts builds n attempts numbered 1..n whose err texts are errText.
func hhAttempts(n int, errText string) []event.Attempt {
	base := time.Date(2026, 10, 17, 7, 0, 0, 0, time.UTC)
	out := make([]event.Attempt, n)
	for i := range out {
		out[i] = event.Attempt{
			N:        i + 1,
			Err:      errText,
			Class:    event.AttemptTransient,
			Started:  base.Add(time.Duration(i) * time.Second),
			Duration: 120 * time.Millisecond,
		}
	}
	return out
}

func hhDecode(t *testing.T, v string) []hhWire {
	t.Helper()
	if !json.Valid([]byte(v)) {
		t.Fatalf("not valid JSON: %q", v)
	}
	var out []hhWire
	if err := json.Unmarshal([]byte(v), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestEncodeHistoryHeader_AllFit(t *testing.T) {
	v, truncated := event.EncodeHistoryHeader(hhAttempts(3, "db down"), event.HistoryHeaderLimit)
	if truncated {
		t.Error("truncated = true, want false")
	}
	got := hhDecode(t, v)
	if len(got) != 3 || got[0].N != 1 || got[2].N != 3 {
		t.Fatalf("entries = %+v", got)
	}
	if got[0].Err != "db down" || got[0].Class != "transient" || got[0].DurMS != 120 {
		t.Errorf("entry 0 = %+v", got[0])
	}
	if got[1].At != "2026-10-17T07:00:01Z" {
		t.Errorf("at = %q, want RFC3339Nano UTC", got[1].At)
	}
	if v, tr := event.EncodeHistoryHeader(nil, 100); v != "" || tr {
		t.Errorf("empty history = %q, %v; want \"\", false", v, tr)
	}
}

func TestEncodeHistoryHeader_DropsOldestFirst(t *testing.T) {
	hist := hhAttempts(20, strings.Repeat("e", 256))
	v, truncated := event.EncodeHistoryHeader(hist, event.HistoryHeaderLimit)
	if !truncated {
		t.Error("truncated = false, want true")
	}
	if len(v) > event.HistoryHeaderLimit {
		t.Errorf("len = %d, want <= %d", len(v), event.HistoryHeaderLimit)
	}
	got := hhDecode(t, v)
	if len(got) == 0 || len(got) >= 20 {
		t.Fatalf("kept %d entries, want some but not all", len(got))
	}
	if got[len(got)-1].N != 20 {
		t.Errorf("newest kept n = %d, want 20", got[len(got)-1].N)
	}
	for i := 1; i < len(got); i++ {
		if got[i].N != got[i-1].N+1 {
			t.Fatalf("n not contiguous and increasing: %+v", got)
		}
	}
}

func TestEncodeHistoryHeader_ShrinksNewest(t *testing.T) {
	hist := hhAttempts(1, strings.Repeat("x", 500))
	v, truncated := event.EncodeHistoryHeader(hist, 128)
	if !truncated {
		t.Error("truncated = false, want true")
	}
	if len(v) > 128 {
		t.Errorf("len = %d, want <= 128", len(v))
	}
	got := hhDecode(t, v)
	if len(got) != 1 {
		t.Fatalf("entries = %d, want 1", len(got))
	}
	e := got[0]
	if e.N != 1 || e.Class != "transient" || e.At == "" || e.DurMS != 120 {
		t.Errorf("entry lost fields: %+v", e)
	}
	if e.Err == "" || len(e.Err) >= 500 || !strings.HasSuffix(e.Err, event.TruncationMarker) {
		t.Errorf("err = %q, want shortened and ending with the marker", e.Err)
	}
}

func TestEncodeHistoryHeader_TinyCapEmptyArray(t *testing.T) {
	v, truncated := event.EncodeHistoryHeader(hhAttempts(2, "boom"), 10)
	if v != "[]" || !truncated {
		t.Errorf("got %q, %v; want \"[]\", true", v, truncated)
	}
	v, truncated = event.EncodeHistoryHeader(hhAttempts(2, "boom"), 0)
	if v != "[]" || !truncated {
		t.Errorf("cap 0: got %q, %v; want \"[]\", true", v, truncated)
	}
}

func TestEncodeHistoryHeader_EscapesHostileText(t *testing.T) {
	hostile := "quote\" back\\slash\nnew\ttab\x00nul \xff\xfe bad \U0001F600 emoji\r\n"
	v, _ := event.EncodeHistoryHeader(hhAttempts(2, hostile), event.HistoryHeaderLimit)
	if strings.ContainsAny(v, "\n\r\t\x00") {
		t.Errorf("raw control characters in header value: %q", v)
	}
	got := hhDecode(t, v)
	if len(got) != 2 {
		t.Fatalf("entries = %d, want 2", len(got))
	}
	for _, e := range got {
		if strings.ContainsAny(e.Err, "\n\r") {
			t.Errorf("decoded err holds a raw newline: %q", e.Err)
		}
		if !utf8.ValidString(e.Err) {
			t.Errorf("decoded err is not valid UTF-8: %q", e.Err)
		}
		if !strings.Contains(e.Err, "\U0001F600") || !strings.Contains(e.Err, "quote\"") {
			t.Errorf("decoded err lost content: %q", e.Err)
		}
	}
}

func TestEncodeHistoryHeader_NoHTMLEscaping(t *testing.T) {
	v, _ := event.EncodeHistoryHeader(hhAttempts(1, "a<b>&c"), event.HistoryHeaderLimit)
	if !strings.Contains(v, "a<b>&c") {
		t.Errorf("value escaped <, > or &: %q", v)
	}
}

func FuzzEncodeHistoryHeader(f *testing.F) {
	f.Add("", "", "", 2048)
	f.Add("quote\" back\\ nl\n tab\t nul\x00", "plain", "\r\n", 256)
	f.Add(strings.Repeat("y", 3000), "short", strings.Repeat("z", 3000), 2048)
	f.Add("\xff\xfe\xfd", "ok", "\xc3", 128)
	f.Add("\U0001F600\U0001F600\U0001F600", "  ", "<&>", 90)
	f.Add("boom", "boom", "boom", 2)
	f.Add("boom", "boom", "boom", 75)
	f.Fuzz(func(t *testing.T, a, b, c string, capBytes int) {
		if capBytes < 2 || capBytes > 1<<20 {
			t.Skip()
		}
		base := time.Date(2026, 10, 17, 7, 0, 0, 0, time.UTC)
		var hist []event.Attempt
		for i, s := range []string{a, b, c} {
			hist = append(hist, event.NewAttempt(i+1, errors.New(s), event.AttemptTransient,
				base.Add(time.Duration(i)*time.Second), 5*time.Millisecond, 0))
		}
		v, _ := event.EncodeHistoryHeader(hist, capBytes)
		if v == "" {
			t.Fatal("non-empty history gave an empty value")
		}
		if len(v) > capBytes {
			t.Fatalf("len %d exceeds cap %d: %q", len(v), capBytes, v)
		}
		if !json.Valid([]byte(v)) {
			t.Fatalf("invalid JSON: %q", v)
		}
		var got []struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal([]byte(v), &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(got) == 0 {
			return
		}
		if got[len(got)-1].N != len(hist) {
			t.Fatalf("newest kept n = %d, want %d", got[len(got)-1].N, len(hist))
		}
		for i := 1; i < len(got); i++ {
			if got[i].N != got[i-1].N+1 {
				t.Fatalf("not a contiguous suffix: %+v", got)
			}
		}
	})
}
