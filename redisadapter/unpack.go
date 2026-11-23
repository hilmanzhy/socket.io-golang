package redisadapter

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// kind is the type of a MessagePack value.
type kind uint8

const (
	kindNil kind = iota
	kindBool
	kindInt
	kindUint
	kindFloat
	kindText
	kindBlob
	kindArray
	kindMap
	kindExt
)

// token is the head of a MessagePack value. size is the length in bytes of a
// string, a binary value or an extension, or the number of elements of an
// array or pairs of a map. bits is the value of a boolean or a number, or the
// type of an extension.
type token struct {
	kind kind
	size int
	bits uint64
}

// reader walks a MessagePack value without building it in memory. It turns
// the parts a caller asks for into JSON text and passes over the others.
// slots holds the offset in out of the placeholder of each attachment.
type reader struct {
	src         []byte
	out         []byte
	attachments [][]byte
	slots       []int
	depth       int
}

// next reads the head of the next value. What follows it in the input is the
// body of a string, a binary value or an extension, or the elements of an
// array or a map.
func (r *reader) next() (token, error) {
	head, err := r.take(1)
	if err != nil {
		return token{}, err
	}
	switch b := head[0]; {
	case b < 0x80:
		return token{kind: kindInt, bits: uint64(b)}, nil
	case b < 0x90:
		return token{kind: kindMap, size: int(b & 0x0f)}, nil
	case b < 0xa0:
		return token{kind: kindArray, size: int(b & 0x0f)}, nil
	case b < 0xc0:
		return token{kind: kindText, size: int(b & 0x1f)}, nil
	case b >= 0xe0:
		return token{kind: kindInt, bits: uint64(int64(int8(b)))}, nil
	case b == 0xc0:
		return token{kind: kindNil}, nil
	case b == 0xc2, b == 0xc3:
		return token{kind: kindBool, bits: uint64(b & 1)}, nil
	case b >= 0xc4 && b <= 0xc6:
		return r.sized(kindBlob, 1<<(b-0xc4))
	case b >= 0xc7 && b <= 0xc9:
		return r.typed(r.sized(kindExt, 1<<(b-0xc7)))
	case b == 0xca:
		bits, err := r.uint(4)
		return token{kind: kindFloat, bits: math.Float64bits(float64(math.Float32frombits(uint32(bits))))}, err
	case b == 0xcb:
		bits, err := r.uint(8)
		return token{kind: kindFloat, bits: bits}, err
	case b >= 0xcc && b <= 0xcf:
		bits, err := r.uint(1 << (b - 0xcc))
		return token{kind: kindUint, bits: bits}, err
	case b >= 0xd0 && b <= 0xd3:
		width := 1 << (b - 0xd0)
		bits, err := r.uint(width)
		shift := 64 - 8*width
		return token{kind: kindInt, bits: uint64(int64(bits<<shift) >> shift)}, err
	case b >= 0xd4 && b <= 0xd8:
		return r.typed(token{kind: kindExt, size: 1 << (b - 0xd4)}, nil)
	case b >= 0xd9 && b <= 0xdb:
		return r.sized(kindText, 1<<(b-0xd9))
	case b == 0xdc, b == 0xdd:
		return r.sized(kindArray, 2<<(b-0xdc))
	case b == 0xde, b == 0xdf:
		return r.sized(kindMap, 2<<(b-0xde))
	}
	return token{}, fmt.Errorf("%w: 0x%02x", errUnsupported, head[0])
}

// take returns the next n bytes of the input. The result aliases the input
// and has no spare capacity, so that appending to it never writes over what
// follows.
func (r *reader) take(n int) ([]byte, error) {
	if n > len(r.src) {
		return nil, errTruncated
	}
	taken := r.src[:n:n]
	r.src = r.src[n:]
	return taken, nil
}

func (r *reader) uint(width int) (uint64, error) {
	raw, err := r.take(width)
	if err != nil {
		return 0, err
	}
	var value uint64
	for _, b := range raw {
		value = value<<8 | uint64(b)
	}
	return value, nil
}

// sized reads the head of a value whose size is written on width bytes. A
// value needs at least one byte per byte or element it announces, so a size
// beyond the remaining input is a truncation.
func (r *reader) sized(k kind, width int) (token, error) {
	n, err := r.uint(width)
	if err == nil && n > uint64(len(r.src)) {
		err = errTruncated
	}
	return token{kind: k, size: int(n)}, err
}

// typed reads the type of the extension t.
func (r *reader) typed(t token, err error) (token, error) {
	if err != nil {
		return t, err
	}
	tag, err := r.take(1)
	if err != nil {
		return t, err
	}
	t.bits = uint64(tag[0])
	return t, nil
}

// skip passes over the next value.
func (r *reader) skip() error {
	t, err := r.next()
	if err != nil {
		return err
	}
	_, _, err = r.drop(t)
	return err
}

// drop passes over the rest of the value that starts with t, and returns the
// number of bin values in it and the bytes they hold. It counts the values
// still to come instead of recursing, so that nesting costs nothing.
func (r *reader) drop(t token) (blobs, held int, err error) {
	for pending := 1; ; {
		switch t.kind {
		case kindBlob:
			blobs, held = blobs+1, held+t.size
			fallthrough
		case kindText, kindExt:
			if _, err := r.take(t.size); err != nil {
				return 0, 0, err
			}
		case kindArray:
			pending += t.size
		case kindMap:
			pending += 2 * t.size
		}
		if pending--; pending == 0 {
			return blobs, held, nil
		}
		if t, err = r.next(); err != nil {
			return 0, 0, err
		}
	}
}

// value appends to out the JSON text of the next value, the one
// JSON.stringify writes for what notepack.io decodes. A bin value becomes a
// Socket.IO placeholder, and its bytes, which alias the input, an attachment.
func (r *reader) value() error {
	t, err := r.next()
	if err != nil {
		return err
	}
	switch t.kind {
	case kindNil:
		r.out = append(r.out, "null"...)
	case kindBool:
		r.out = strconv.AppendBool(r.out, t.bits == 1)
	case kindInt:
		r.out = strconv.AppendInt(r.out, int64(t.bits), 10)
	case kindUint:
		r.out = strconv.AppendUint(r.out, t.bits, 10)
	case kindFloat:
		r.out = appendFloat(r.out, math.Float64frombits(t.bits))
	case kindText:
		raw, err := r.take(t.size)
		if err != nil {
			return err
		}
		r.out = appendQuoted(r.out, raw)
	case kindBlob:
		raw, err := r.take(t.size)
		if err != nil {
			return err
		}
		r.slots = append(r.slots, len(r.out))
		r.out = strconv.AppendInt(append(r.out, placeholder...), int64(len(r.attachments)), 10)
		r.out = append(r.out, '}')
		r.attachments = append(r.attachments, raw)
	case kindArray:
		return r.array(t.size)
	case kindMap:
		return r.object(t.size)
	case kindExt:
		return r.date(t)
	}
	return nil
}

// array appends the JSON text of an array whose n elements come next.
func (r *reader) array(n int) error {
	if err := r.enter(); err != nil {
		return err
	}
	r.out = append(r.out, '[')
	for i := range n {
		if i > 0 {
			r.out = append(r.out, ',')
		}
		if err := r.value(); err != nil {
			return err
		}
	}
	r.out = append(r.out, ']')
	r.depth--
	return nil
}

// object appends the JSON text of a map whose n pairs come next. Its keys
// must be strings.
func (r *reader) object(n int) error {
	if err := r.enter(); err != nil {
		return err
	}
	r.out = append(r.out, '{')
	for i := range n {
		if i > 0 {
			r.out = append(r.out, ',')
		}
		key, err := r.next()
		if err != nil {
			return err
		}
		if key.kind != kindText {
			return fmt.Errorf("%w: map key that is not a string", errUnsupported)
		}
		name, err := r.take(key.size)
		if err != nil {
			return err
		}
		r.out = append(appendQuoted(r.out, name), ':')
		if err := r.value(); err != nil {
			return err
		}
	}
	r.out = append(r.out, '}')
	r.depth--
	return nil
}

func (r *reader) enter() error {
	if r.depth++; r.depth > maxDepth {
		return errTooDeep
	}
	return nil
}

// date appends the JSON text of the extension t. The only extension
// notepack.io writes is the timestamp, its form of a JavaScript Date.
func (r *reader) date(t token) error {
	body, err := r.take(t.size)
	if err != nil {
		return err
	}
	var seconds, nanos float64
	switch {
	case int8(t.bits) == -1 && len(body) == 4:
		seconds = float64(binary.BigEndian.Uint32(body))
	case int8(t.bits) == -1 && len(body) == 8:
		packed := binary.BigEndian.Uint64(body)
		seconds, nanos = float64(packed&(1<<34-1)), float64(packed>>34)
	case int8(t.bits) == -1 && len(body) == 12:
		seconds, nanos = float64(int64(binary.BigEndian.Uint64(body[4:]))), float64(binary.BigEndian.Uint32(body))
	default:
		return fmt.Errorf("%w: extension %d", errUnsupported, int8(t.bits))
	}
	r.out = appendDate(r.out, float64(seconds*1e3)+float64(nanos/1e6))
	return nil
}

// appendDate appends what JSON.stringify writes for the Date that is ms
// milliseconds after the epoch, computed as JavaScript computes it: an
// ISO 8601 string, with a signed six-digit year outside the years 0 to 9999,
// or null beyond the range of a Date.
func appendDate(dst []byte, ms float64) []byte {
	ms = math.Trunc(ms)
	if math.Abs(ms) > 8.64e15 {
		return append(dst, "null"...)
	}
	moment := time.UnixMilli(int64(ms)).UTC()
	dst = append(dst, '"')
	if year := moment.Year(); year < 0 || year > 9999 {
		dst = moment.AppendFormat(fmt.Appendf(dst, "%+07d", year), "-01-02T15:04:05.000Z")
	} else {
		dst = moment.AppendFormat(dst, "2006-01-02T15:04:05.000Z")
	}
	return append(dst, '"')
}

// appendFloat appends a float64 the way JSON.stringify writes it: a zero has
// no sign, and NaN and the infinities, which JSON has no text for, are null.
func appendFloat(dst []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(dst, "null"...)
	}
	abs := math.Abs(f)
	if abs == 0 {
		return append(dst, '0')
	}
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.AppendFloat(dst, f, 'f', -1, 64)
	}
	dst = strconv.AppendFloat(dst, f, 'e', -1, 64)
	if n := len(dst); n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
		dst = append(dst[:n-2], dst[n-1])
	}
	return dst
}

// appendQuoted appends raw as a JSON string, with the escapes JSON.stringify
// writes. A byte that is not UTF-8 becomes the replacement character.
func appendQuoted(dst, raw []byte) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(raw); {
		c := raw[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(raw[i:])
			if r == utf8.RuneError && size == 1 {
				dst = append(append(dst, raw[start:i]...), `\ufffd`...)
				start = i + 1
			}
			i += size
			continue
		}
		if c >= 0x20 && c != '"' && c != '\\' {
			i++
			continue
		}
		dst = append(dst, raw[start:i]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0x0f])
		}
		i++
		start = i
	}
	return append(append(dst, raw[start:]...), '"')
}
