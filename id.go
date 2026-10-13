package event

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NewID returns a new lowercase RFC 9562 UUIDv7 string.
//
// The first 48 bits are the Unix millisecond timestamp, the version nibble is
// 7, the variant bits are 10 and every other bit comes from crypto/rand.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // never errors since Go 1.24
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2], b[3], b[4], b[5] = byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
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
