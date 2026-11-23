package redisadapter

import (
	"encoding/binary"
	"errors"
	"math"
)

// maxDepth bounds the nesting of a broadcast, written or read, so that no
// message can exhaust the stack of the server that decodes it.
const maxDepth = 512

var (
	errTruncated   = errors.New("redisadapter: truncated MessagePack value")
	errTrailing    = errors.New("redisadapter: bytes after the MessagePack value")
	errTooDeep     = errors.New("redisadapter: value nested too deeply")
	errTooLarge    = errors.New("redisadapter: value too large for MessagePack")
	errUnsupported = errors.New("redisadapter: unsupported MessagePack type")
	errAttachment  = errors.New("redisadapter: attachment without its placeholder")
	errJSON        = errors.New("redisadapter: packet data is not JSON")
)

// family describes the headers of one kind of sized MessagePack value. A
// length below short is packed into the fix byte. The byte wide announces a
// 16-bit length and the one after it a 32-bit length; with narrow, the one
// before it announces an 8-bit length.
type family struct {
	fix    byte
	short  int
	narrow bool
	wide   byte
}

var (
	text       = family{fix: 0xa0, short: 32, narrow: true, wide: 0xda}
	blob       = family{narrow: true, wide: 0xc5}
	array      = family{fix: 0x90, short: 16, wide: 0xdc}
	dictionary = family{fix: 0x80, short: 16, wide: 0xde}
)

// header appends the shortest header of a value of n bytes or elements.
func (f family) header(dst []byte, n int) []byte {
	switch {
	case n < f.short:
		return append(dst, f.fix|byte(n))
	case f.narrow && n <= math.MaxUint8:
		return append(dst, f.wide-1, byte(n))
	case n <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(dst, f.wide), uint16(n))
	}
	return binary.BigEndian.AppendUint32(append(dst, f.wide+1), uint32(n))
}

// appendText appends value as a MessagePack string.
func appendText[T string | []byte](dst []byte, value T) []byte {
	return append(text.header(dst, len(value)), value...)
}

// appendTexts appends values as a MessagePack array of strings.
func appendTexts(dst []byte, values []string) []byte {
	dst = array.header(dst, len(values))
	for _, value := range values {
		dst = appendText(dst, value)
	}
	return dst
}
