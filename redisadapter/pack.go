package redisadapter

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// placeholder opens the object the parser writes in place of an attachment.
var placeholder = []byte(`{"_placeholder":true,"num":`)

// appendJSON appends to dst the MessagePack form of the JSON text src, the
// way notepack.io writes the value JavaScript reads from that text: every
// header is the shortest one, the members of an object keep their order, and
// a number without a fraction is an integer however it is spelled. depth is
// the number of arrays and maps the value already sits in.
//
// The placeholders of the attachments are replaced by the attachments they
// stand for, as bin values: that is how the Node.js servers hold binary
// arguments. slots holds the offset in src of the placeholder of each
// attachment, in the order of the attachments. Only the object at one of
// those offsets is a placeholder: an object of the same bytes that the
// application passed as an argument stays a map. A packet whose offsets do
// not lead to the placeholders of its attachments, one after the other, is
// refused.
//
// The text is never decoded into a tree. One pass counts the elements of
// every array and object, a second one writes.
func appendJSON(dst, src []byte, attachments [][]byte, slots []int, depth int) ([]byte, error) {
	if uint64(len(src)) > math.MaxUint32 {
		return dst, errTooLarge
	}
	sizes, err := measure(src, maxDepth-depth)
	if err != nil {
		return dst, err
	}
	if !json.Valid(src) {
		return dst, errJSON
	}
	if len(slots) != len(attachments) {
		return dst, errAttachment
	}
	dst = slices.Grow(dst, len(src))
	var scratch []byte
	opened, next := 0, 0
	for i := 0; i < len(src); i++ {
		switch c := src[i]; {
		case c == '[':
			dst = array.header(dst, int(sizes[opened]))
			opened++
		case c == '{':
			if next < len(slots) && slots[next] == i {
				length, raw := placeholderLength(src[i:], next), attachments[next]
				if length == 0 {
					return dst, errAttachment
				}
				if uint64(len(raw)) > math.MaxUint32 {
					return dst, errTooLarge
				}
				dst = append(blob.header(dst, len(raw)), raw...)
				next++
				i += length - 1
			} else {
				dst = dictionary.header(dst, int(sizes[opened]))
			}
			opened++
		case c == '"':
			end, escaped := endOfString(src, i)
			body := src[i+1 : end]
			if escaped || !utf8.Valid(body) {
				scratch = appendUnquoted(scratch[:0], body)
				body = scratch
			}
			dst = appendText(dst, body)
			i = end
		case c == 't':
			dst = append(dst, 0xc3)
			i += len("true") - 1
		case c == 'f':
			dst = append(dst, 0xc2)
			i += len("false") - 1
		case c == 'n':
			dst = append(dst, 0xc0)
			i += len("null") - 1
		case c == '-' || c >= '0' && c <= '9':
			end := endOfNumber(src, i)
			dst = appendNumber(dst, src[i:end])
			i = end - 1
		}
	}
	if next != len(slots) {
		return dst, errAttachment
	}
	return dst, nil
}

// measure returns the number of elements of every array and object of the
// JSON text src, in the order they open. It refuses a text that nests more
// than limit of them. The brackets of the text, those inside its strings
// included, bound the number of entries, so the list is allocated once.
func measure(src []byte, limit int) ([]uint32, error) {
	sizes := make([]uint32, 0, bytes.Count(src, []byte{'['})+bytes.Count(src, []byte{'{'}))
	var open []int
	empty := false
	for i := 0; i < len(src); i++ {
		switch c := src[i]; c {
		case ' ', '\t', '\n', '\r':
		case '"':
			i, _ = endOfString(src, i)
			empty = false
		case '[', '{':
			if len(open) >= limit {
				return nil, errTooDeep
			}
			open = append(open, len(sizes))
			sizes = append(sizes, 0)
			empty = true
		case ']', '}', ',':
			if len(open) == 0 {
				return nil, errJSON
			}
			if !empty {
				sizes[open[len(open)-1]]++
			}
			if c != ',' {
				open = open[:len(open)-1]
			}
			empty = false
		default:
			empty = false
		}
	}
	return sizes, nil
}

// endOfString returns the index of the quote that closes the JSON string
// opened at start, and whether the string holds an escape.
func endOfString(src []byte, start int) (end int, escaped bool) {
	for end = start + 1; end < len(src); end++ {
		switch src[end] {
		case '\\':
			escaped = true
			end++
		case '"':
			return end, escaped
		}
	}
	return len(src), escaped
}

// endOfNumber returns the index after the JSON number that starts at start.
func endOfNumber(src []byte, start int) int {
	end := start + 1
	for end < len(src) && strings.IndexByte("+-.eE0123456789", src[end]) >= 0 {
		end++
	}
	return end
}

// placeholderLength returns the length of the placeholder that src starts
// with, or zero when src does not start with the placeholder of the
// attachment of that number.
func placeholderLength(src []byte, number int) int {
	rest, found := bytes.CutPrefix(src, placeholder)
	if !found {
		return 0
	}
	var buffer [20]byte
	digits := strconv.AppendInt(buffer[:0], int64(number), 10)
	if rest, found = bytes.CutPrefix(rest, digits); !found || len(rest) == 0 || rest[0] != '}' {
		return 0
	}
	return len(placeholder) + len(digits) + 1
}

// appendUnquoted appends the text of a JSON string, given without its quotes,
// the way encoding/json decodes it: a lone surrogate and a byte that is not
// UTF-8 both become the replacement character.
func appendUnquoted(dst, body []byte) []byte {
	for i := 0; i < len(body); {
		if body[i] != '\\' {
			r, size := utf8.DecodeRune(body[i:])
			dst = utf8.AppendRune(dst, r)
			i += size
			continue
		}
		escape := body[i+1]
		i += 2
		switch escape {
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			r := hex4(body[i:])
			i += 4
			if utf16.IsSurrogate(r) {
				low := rune(0)
				if len(body)-i >= 6 && body[i] == '\\' && body[i+1] == 'u' {
					low = hex4(body[i+2:])
				}
				if r = utf16.DecodeRune(r, low); r != utf8.RuneError {
					i += 6
				}
			}
			dst = utf8.AppendRune(dst, r)
		default:
			dst = append(dst, escape)
		}
	}
	return dst
}

// hex4 reads the four hexadecimal digits of a \u escape.
func hex4(digits []byte) rune {
	var r rune
	for _, c := range digits[:4] {
		switch {
		case c >= 'a':
			c -= 'a' - 10
		case c >= 'A':
			c -= 'A' - 10
		default:
			c -= '0'
		}
		r = r<<4 | rune(c)
	}
	return r
}

// appendNumber appends a JSON number the way notepack.io writes the number
// JavaScript reads from it: the shortest integer when it has no fraction, a
// float64 otherwise. An integer too large for 64 bits is written as a
// float64, which keeps its value where notepack.io would not.
func appendNumber(dst, literal []byte) []byte {
	const two63 = 1 << 63
	number := string(literal)
	if !strings.ContainsAny(number, ".eE") {
		if i, err := strconv.ParseInt(number, 10, 64); err == nil {
			return appendInt(dst, i)
		}
		if u, err := strconv.ParseUint(number, 10, 64); err == nil {
			return binary.BigEndian.AppendUint64(append(dst, 0xcf), u)
		}
	}
	f, _ := strconv.ParseFloat(number, 64)
	switch {
	case f != math.Trunc(f) || math.IsInf(f, 0):
	case f >= -two63 && f < two63:
		return appendInt(dst, int64(f))
	case f > 0 && f < 2*two63:
		return binary.BigEndian.AppendUint64(append(dst, 0xcf), uint64(f))
	}
	return binary.BigEndian.AppendUint64(append(dst, 0xcb), math.Float64bits(f))
}

// appendInt appends an integer in its shortest MessagePack form.
func appendInt(dst []byte, i int64) []byte {
	switch {
	case i >= -0x20 && i < 0x80:
		return append(dst, byte(i))
	case i >= 0 && i <= math.MaxUint8:
		return append(dst, 0xcc, byte(i))
	case i >= 0 && i <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(dst, 0xcd), uint16(i))
	case i >= 0 && i <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(dst, 0xce), uint32(i))
	case i >= 0:
		return binary.BigEndian.AppendUint64(append(dst, 0xcf), uint64(i))
	case i >= math.MinInt8:
		return append(dst, 0xd0, byte(i))
	case i >= math.MinInt16:
		return binary.BigEndian.AppendUint16(append(dst, 0xd1), uint16(i))
	case i >= math.MinInt32:
		return binary.BigEndian.AppendUint32(append(dst, 0xd2), uint32(i))
	}
	return binary.BigEndian.AppendUint64(append(dst, 0xd3), uint64(i))
}
