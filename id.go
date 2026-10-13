package event

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// uuidv7Gen mints RFC 9562 UUIDv7 values. It follows Method 1 (fixed
// bit-length dedicated counter): a 12-bit counter in rand_a right after the
// 48-bit millisecond timestamp keeps ids from one generator strictly
// increasing as strings, including within one millisecond, across a clock
// rollback and across a counter rollover.
type uuidv7Gen struct {
	mu     sync.Mutex
	lastMS int64
	seq    uint16 // 12 bits
	now    func() time.Time
}

var defaultIDGen = &uuidv7Gen{now: time.Now}

// NewID returns a new lowercase RFC 9562 UUIDv7 string.
//
// Ids are built only from the standard library with crypto/rand. Ids from the
// same process are strictly increasing as strings. Ids from different
// processes are unique but only ordered by their millisecond prefix.
func NewID() string {
	return defaultIDGen.next()
}

func (g *uuidv7Gen) next() string {
	var b [16]byte
	_, _ = rand.Read(b[:])                            // never errors since Go 1.24
	seed := (uint16(b[6])<<8 | uint16(b[7])) & 0x07FF // top counter bit clear: 2048 ids of headroom

	g.mu.Lock()
	if ms := g.now().UnixMilli(); ms > g.lastMS {
		g.lastMS = ms
		g.seq = seed
	} else {
		// Same millisecond, or the clock moved back: keep lastMS and count up.
		g.seq++
		if g.seq > 0x0FFF {
			// Counter exhausted: borrow the next millisecond and reseed.
			g.lastMS++
			g.seq = seed
		}
	}
	ts, seq := uint64(g.lastMS), g.seq
	g.mu.Unlock()

	b[0], b[1], b[2], b[3], b[4], b[5] = byte(ts>>40), byte(ts>>32), byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts)
	b[6] = 0x70 | byte(seq>>8)  // version 7 + counter high 4 bits
	b[7] = byte(seq)            // counter low 8 bits
	b[8] = (b[8] & 0x3F) | 0x80 // variant 10
	return formatUUID(b)
}

func formatUUID(b [16]byte) string {
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
