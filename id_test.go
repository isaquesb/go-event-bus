package event

import (
	"encoding/hex"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

var idRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// fakeClock is a settable clock for the generator.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newTestGen(t time.Time) (*uuidv7Gen, *fakeClock) {
	c := &fakeClock{t: t}
	return &uuidv7Gen{now: c.now}, c
}

func TestNewID_Format(t *testing.T) {
	for range 1000 {
		if id := NewID(); !idRe.MatchString(id) {
			t.Fatalf("bad id %q", id)
		}
	}
}

func TestNewID_TimestampPrefix(t *testing.T) {
	at := time.UnixMilli(1_760_000_000_123)
	g, _ := newTestGen(at)
	id := g.next()
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", "")[:12])
	if err != nil {
		t.Fatal(err)
	}
	var ms int64
	for _, b := range raw {
		ms = ms<<8 | int64(b)
	}
	if ms != at.UnixMilli() {
		t.Errorf("timestamp %d, want %d", ms, at.UnixMilli())
	}
}

func TestNewID_StrictlyIncreasing(t *testing.T) {
	g := &uuidv7Gen{now: time.Now}
	prev := g.next()
	for i := range 100000 {
		id := g.next()
		if id <= prev {
			t.Fatalf("id %d %q not after %q", i, id, prev)
		}
		prev = id
	}
}

func TestNewID_FrozenClock(t *testing.T) {
	at := time.UnixMilli(1_760_000_000_000)
	g, _ := newTestGen(at)
	first := g.next()
	prev := first
	for i := range 3000 {
		id := g.next()
		if id <= prev {
			t.Fatalf("id %d %q not after %q", i, id, prev)
		}
		prev = id
	}
	if first[:13] != prev[:13] {
		t.Errorf("timestamp prefix moved within headroom: %q vs %q", first[:13], prev[:13])
	}
}

func TestNewID_ClockRollback(t *testing.T) {
	g, c := newTestGen(time.UnixMilli(1_760_000_000_000))
	var last string
	for range 10 {
		last = g.next()
	}
	c.t = c.t.Add(-5 * time.Millisecond)
	for i := range 100 {
		id := g.next()
		if id <= last {
			t.Fatalf("after rollback id %d %q not after %q", i, id, last)
		}
		last = id
	}
}

func TestNewID_CounterRollover(t *testing.T) {
	at := time.UnixMilli(1_760_000_000_000)
	g, _ := newTestGen(at)
	prev := g.next()
	for i := range 10000 {
		id := g.next()
		if id <= prev {
			t.Fatalf("id %d %q not after %q", i, id, prev)
		}
		prev = id
	}
	if g.lastMS <= at.UnixMilli() {
		t.Errorf("lastMS %d did not advance past %d (last id %s)", g.lastMS, at.UnixMilli(), prev)
	}
}

func TestNewID_ConcurrentUnique(t *testing.T) {
	const workers, each = 16, 5000
	out := make(chan []string, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]string, each)
			for i := range ids {
				ids[i] = NewID()
			}
			out <- ids
		}()
	}
	wg.Wait()
	close(out)
	seen := make(map[string]struct{}, workers*each)
	for ids := range out {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate id %q", id)
			}
			seen[id] = struct{}{}
		}
	}
	if len(seen) != workers*each {
		t.Errorf("got %d ids, want %d", len(seen), workers*each)
	}
}
