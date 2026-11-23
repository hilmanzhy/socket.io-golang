package redisadapter

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// golden is one row of a table: a JSON text, the attachments its placeholders
// name, and the MessagePack bytes of the same value in hexadecimal.
type golden struct {
	name        string
	json        string
	attachments [][]byte
	hex         string
}

type stringHeaderGolden struct {
	name  string
	unit  string
	count int
	hex   string
}

type headerGolden struct {
	name string
	n    int
	hex  string
}

type stringListGolden struct {
	name   string
	values []string
	hex    string
}

// The tables below hold what notepack.io 3.0.1 writes for the listed values
// and what JSON.stringify writes for what it reads back, which is what the
// Node.js adapter of Socket.IO does. The members of every object keep the
// order of the text. The rows that notepack.io cannot produce, or reads
// differently, say so in the comment of their table and follow the
// MessagePack specification. The tables of strings, binary values, lists and
// maps hold only the header of the value, because the body is a long
// repetition that the tests rebuild: the bytes of a string, 0xab for a binary
// value, 0xc0 for a list element, and a key of the form k00000 followed by
// 0xc0 for a map entry.

// unhex decodes a hexadecimal literal of a golden table.
func unhex(t testing.TB, s string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex literal %q: %v", s, err)
	}
	return raw
}

// excerpt shows 24 bytes of b around the offset at.
func excerpt(b []byte, at int) string {
	start := min(max(at-4, 0), len(b))
	end := min(start+24, len(b))
	return fmt.Sprintf("[%d:%d] %x", start, end, b[start:end])
}

func requireBytes(t testing.TB, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	offset := 0
	for offset < len(got) && offset < len(want) && got[offset] == want[offset] {
		offset++
	}
	t.Fatalf("got %d bytes, want %d bytes, first difference at offset %d\n got: %s\nwant: %s",
		len(got), len(want), offset, excerpt(got, offset), excerpt(want, offset))
}

// requireText compares a JSON text with the one expected and shows where they
// first differ.
func requireText(t testing.TB, got []byte, want string) {
	t.Helper()
	if string(got) == want {
		return
	}
	offset := 0
	for offset < len(got) && offset < len(want) && got[offset] == want[offset] {
		offset++
	}
	from := max(offset-20, 0)
	t.Fatalf("got %d bytes, want %d bytes, first difference at offset %d\n got: ...%q\nwant: ...%q",
		len(got), len(want), offset,
		got[from:min(offset+40, len(got))], want[from:min(offset+40, len(want))])
}

func brief(v any) string {
	text := fmt.Sprintf("%#v", v)
	if len(text) > 240 {
		return text[:240] + "..."
	}
	return text
}

func requireEqual(t testing.TB, what string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\n got: %s\nwant: %s", what, brief(got), brief(want))
	}
}

// requireAttachments compares attachments by length and content, so that a nil
// list and an empty one are the same.
func requireAttachments(t testing.TB, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d attachments, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("attachment %d = %s, want %s", i, brief(got[i]), brief(want[i]))
		}
	}
}

// decodeValue reads one whole MessagePack value the way the decoder of a
// broadcast reads the data of a packet, and refuses what follows it.
func decodeValue(src []byte) ([]byte, [][]byte, error) {
	r := reader{src: src}
	if err := r.value(); err != nil {
		return nil, nil, err
	}
	if len(r.src) > 0 {
		return nil, nil, errTrailing
	}
	return r.out, r.attachments, nil
}

// slotsOf returns the offset in text of the placeholder of each of the first
// count attachments, every one looked for after the one before it: the offsets
// the parser records for a packet whose arguments hold no look-alike. It stops
// at the first placeholder that is missing.
func slotsOf(text string, count int) []int {
	var slots []int
	from := 0
	for number := range count {
		at := strings.Index(text[from:], placeholderText(number))
		if at < 0 {
			break
		}
		slots = append(slots, from+at)
		from += at + len(placeholderText(number))
	}
	return slots
}

// canonicalJSON reads a JSON text into a tree in which two texts that mean the
// same number are equal, and a number beyond the range of a float64 is null,
// as the codec writes it.
func canonicalJSON(text []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return canonicalNumbers(value), nil
}

func canonicalNumbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		f, _ := strconv.ParseFloat(string(v), 64)
		if math.IsInf(f, 0) {
			return nil
		}
		return f
	case []any:
		for i := range v {
			v[i] = canonicalNumbers(v[i])
		}
	case map[string]any:
		for key := range v {
			v[key] = canonicalNumbers(v[key])
		}
	}
	return value
}

func dictionaryKey(i int) string {
	return fmt.Sprintf("k%05d", i)
}

func dictionaryBody(n int) string {
	var body strings.Builder
	for i := range n {
		body.WriteString("a6" + hex.EncodeToString([]byte(dictionaryKey(i))) + "c0")
	}
	return body.String()
}

func listText(n int) string {
	return "[" + strings.TrimSuffix(strings.Repeat("null,", n), ",") + "]"
}

func dictionaryText(n int) string {
	members := make([]string, n)
	for i := range n {
		members[i] = fmt.Sprintf("%q:null", dictionaryKey(i))
	}
	return "{" + strings.Join(members, ",") + "}"
}

func stringValue(tc stringHeaderGolden) string {
	return strings.Repeat(tc.unit, tc.count)
}

// nestedText is a JSON text of n arrays, or of n objects, the innermost one
// empty.
func nestedText(n int, objects bool) string {
	if objects {
		return strings.Repeat(`{"a":`, n-1) + "{}" + strings.Repeat("}", n-1)
	}
	return strings.Repeat("[", n) + strings.Repeat("]", n)
}

// nestedBytes is the MessagePack value that nestedText holds.
func nestedBytes(n int, objects bool) []byte {
	if objects {
		return append(bytes.Repeat([]byte{0x81, 0xa1, 'a'}, n-1), 0x80)
	}
	return append(bytes.Repeat([]byte{0x91}, n-1), 0x90)
}

// The mp functions build MessagePack values by hand, from the specification
// alone, so that a test can write a message no encoder of this package would.

func mpSized(n int, fix byte, short int, narrow, wide byte) []byte {
	switch {
	case n < short:
		return []byte{fix | byte(n)}
	case narrow != 0 && n <= math.MaxUint8:
		return []byte{narrow, byte(n)}
	case n <= math.MaxUint16:
		return binary.BigEndian.AppendUint16([]byte{wide}, uint16(n))
	}
	return binary.BigEndian.AppendUint32([]byte{wide + 1}, uint32(n))
}

func mpText(s string) []byte {
	return append(mpSized(len(s), 0xa0, 32, 0xd9, 0xda), s...)
}

func mpBin(b []byte) []byte {
	return append(mpSized(len(b), 0, 0, 0xc4, 0xc5), b...)
}

func mpArrayHead(n int) []byte {
	return mpSized(n, 0x90, 16, 0, 0xdc)
}

func mpMapHead(n int) []byte {
	return mpSized(n, 0x80, 16, 0, 0xde)
}

func mpArray(items ...[]byte) []byte {
	return slices.Concat(mpArrayHead(len(items)), slices.Concat(items...))
}

// mpMember is the key and the value of one entry of a map.
func mpMember(key string, value []byte) []byte {
	return append(mpText(key), value...)
}

func mpMap(members ...[]byte) []byte {
	return slices.Concat(mpMapHead(len(members)), slices.Concat(members...))
}

// mpNested is the data of an event, ["e", ...], that holds n containers in
// all, the outer array included.
func mpNested(n int) []byte {
	if n == 1 {
		return mpArray(mpText("e"))
	}
	return slices.Concat(mpArrayHead(2), mpText("e"), bytes.Repeat([]byte{0x91}, n-2), []byte{0x90})
}

// textNested is the JSON text that mpNested holds.
func textNested(n int) string {
	if n == 1 {
		return `["e"]`
	}
	return `["e",` + strings.Repeat("[", n-1) + strings.Repeat("]", n-1) + "]"
}

func allocatedBytes(runs int, run func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		run()
	}
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// numberGoldens are numbers in the spelling JSON.stringify writes, both ways.
var numberGoldens = []golden{
	{"int 0", "0", nil, "00"},
	{"int 1", "1", nil, "01"},
	{"int 127", "127", nil, "7f"},
	{"int 128", "128", nil, "cc80"},
	{"int 255", "255", nil, "ccff"},
	{"int 256", "256", nil, "cd0100"},
	{"int 65535", "65535", nil, "cdffff"},
	{"int 65536", "65536", nil, "ce00010000"},
	{"int 4294967295", "4294967295", nil, "ceffffffff"},
	{"int 4294967296", "4294967296", nil, "cf0000000100000000"},
	{"int 9007199254740991", "9007199254740991", nil, "cf001fffffffffffff"},
	{"int -1", "-1", nil, "ff"},
	{"int -32", "-32", nil, "e0"},
	{"int -33", "-33", nil, "d0df"},
	{"int -128", "-128", nil, "d080"},
	{"int -129", "-129", nil, "d1ff7f"},
	{"int -32768", "-32768", nil, "d18000"},
	{"int -32769", "-32769", nil, "d2ffff7fff"},
	{"int -2147483648", "-2147483648", nil, "d280000000"},
	{"int -2147483649", "-2147483649", nil, "d3ffffffff7fffffff"},
	{"int -9007199254740991", "-9007199254740991", nil, "d3ffe0000000000001"},
	{"float 1.5", "1.5", nil, "cb3ff8000000000000"},
	{"float -0.25", "-0.25", nil, "cbbfd0000000000000"},
	{"float 3.14159", "3.14159", nil, "cb400921f9f01b866e"},
	{"float 0.1", "0.1", nil, "cb3fb999999999999a"},
	{"float 1e-7", "1e-7", nil, "cb3e7ad7f29abcaf48"},
	{"float 0.0025", "0.0025", nil, "cb3f647ae147ae147b"},
	{"float 123456.789", "123456.789", nil, "cb40fe240c9fbe76c9"},
	{"float -1.5e-10", "-1.5e-10", nil, "cbbde49da7e361ce4c"},
	{"float 2147483648.5", "2147483648.5", nil, "cb41e0000000100000"},
	{"float -2147483649.5", "-2147483649.5", nil, "cbc1e0000000300000"},
	{"float 0.000001", "0.000001", nil, "cb3eb0c6f7a0b5ed8d"},
	{"float 9.999999e-7", "9.999999e-7", nil, "cb3eb0c6f784902b25"},
	{"float 5e-324", "5e-324", nil, "cb0000000000000001"},
	{"float 0.5", "0.5", nil, "cb3fe0000000000000"},
	{"float 1.7976931348623157e-300", "1.7976931348623157e-300", nil, "cb01b3432f0cb68e61"},
	{"float -4.5e-300", "-4.5e-300", nil, "cb81c81be3bb5811c4"},
}

// integralGoldens are numbers without a fraction, however they are spelled: notepack.io writes the shortest integer.
var integralGoldens = []golden{
	{"integer 1.0", "1.0", nil, "01"},
	{"integer 1e3", "1e3", nil, "cd03e8"},
	{"integer 1.5e2", "1.5e2", nil, "cc96"},
	{"integer -2e1", "-2e1", nil, "ec"},
	{"integer 1E2", "1E2", nil, "64"},
	{"integer 100.0", "100.0", nil, "64"},
	{"integer 0.5e1", "0.5e1", nil, "05"},
	{"integer -0", "-0", nil, "00"},
	{"integer -0.0", "-0.0", nil, "00"},
	{"integer 0e0", "0e0", nil, "00"},
	{"integer 0.0e5", "0.0e5", nil, "00"},
	{"integer 1E0", "1E0", nil, "01"},
	{"integer 10e-1", "10e-1", nil, "01"},
	{"integer 0.1e1", "0.1e1", nil, "01"},
	{"integer 1e1", "1e1", nil, "0a"},
	{"integer 2.0e0", "2.0e0", nil, "02"},
	{"integer 255.0", "255.0", nil, "ccff"},
	{"integer 256.0", "256.0", nil, "cd0100"},
	{"integer 65535.0", "65535.0", nil, "cdffff"},
	{"integer 4294967296.0", "4294967296.0", nil, "cf0000000100000000"},
	{"integer -128.0", "-128.0", nil, "d080"},
	{"integer -129.0", "-129.0", nil, "d1ff7f"},
	{"integer -1e10", "-1e10", nil, "d3fffffffdabf41c00"},
	{"integer 9007199254740992.0", "9007199254740992.0", nil, "cf0020000000000000"},
	{"integer 9007199254740992", "9007199254740992", nil, "cf0020000000000000"},
	{"integer 1e19", "1e19", nil, "cf8ac7230489e80000"},
	{"integer 10000000000000000000", "10000000000000000000", nil, "cf8ac7230489e80000"},
	{"integer 9.223372036854775808e18", "9.223372036854775808e18", nil, "cf8000000000000000"},
	{"integer 9223372036854775808", "9223372036854775808", nil, "cf8000000000000000"},
	{"integer 1.8446744073709548e19", "1.8446744073709548e19", nil, "cffffffffffffff000"},
	{"integer 18446744073709549568", "18446744073709549568", nil, "cffffffffffffff800"},
	{"integer -9.223372036854775808e18", "-9.223372036854775808e18", nil, "d38000000000000000"},
	{"integer -9223372036854775808", "-9223372036854775808", nil, "d38000000000000000"},
	{"integer -9223372036854775809", "-9223372036854775809", nil, "d38000000000000000"},
	{"integer 1e-400", "1e-400", nil, "00"},
	{"integer 1E+2", "1E+2", nil, "64"},
}

// floatGoldens are numbers with a fraction or beyond the range of a float64, however they are spelled.
var floatGoldens = []golden{
	{"float 2.5e-3", "2.5e-3", nil, "cb3f647ae147ae147b"},
	{"float 2.5E-3", "2.5E-3", nil, "cb3f647ae147ae147b"},
	{"float 25e-4", "25e-4", nil, "cb3f647ae147ae147b"},
	{"float 1E-7", "1E-7", nil, "cb3e7ad7f29abcaf48"},
	{"float -1.5E-10", "-1.5E-10", nil, "cbbde49da7e361ce4c"},
	{"float 5e-1", "5e-1", nil, "cb3fe0000000000000"},
	{"float 1.25e+0", "1.25e+0", nil, "cb3ff4000000000000"},
	{"float 1e400", "1e400", nil, "cb7ff0000000000000"},
	{"float -1e400", "-1e400", nil, "cbfff0000000000000"},
	{"float 5e-324", "5e-324", nil, "cb0000000000000001"},
	{"float 4.9e-324", "4.9e-324", nil, "cb0000000000000001"},
	{"float 1.5E0", "1.5E0", nil, "cb3ff8000000000000"},
	{"float -0.5", "-0.5", nil, "cbbfe0000000000000"},
}

// numberSpecGoldens are numbers that a JavaScript number cannot hold exactly. The bytes keep the digits where the specification has a form for them; notepack.io rounds to a double and, from 2^64 up, wraps. It reads these bytes back to the nearest double.
var numberSpecGoldens = []golden{
	{"int64 max", "9223372036854775807", nil, "cf7fffffffffffffff"},
	{"uint64 max", "18446744073709551615", nil, "cfffffffffffffffff"},
	{"uint64 above 2^53 keeps its digits", "9007199254740993", nil, "cf0020000000000001"},
	{"beyond uint64 is a float64", "18446744073709551616", nil, "cb43f0000000000000"},
	{"beyond uint64 with an exponent is a float64", "1e21", nil, "cb444b1ae4d6e2ef50"},
	{"largest float64", "1.7976931348623157e308", nil, "cb7fefffffffffffff"},
	{"below int64 is a float64", "-9223372036854777856", nil, "cbc3e0000000000001"},
	{"far below int64 is a float64", "-1e300", nil, "cbfe37e43c8800759c"},
	{"integral float beyond uint64 is a float64", "36893488147419103232.0", nil, "cb4400000000000000"},
}

// integerDecodeGoldens are integers in forms other than the shortest, and the ones beyond 2^53, which a JavaScript number cannot hold and the text keeps exactly.
var integerDecodeGoldens = []golden{
	{"uint64 above int64", "9223372036854775808", nil, "cf8000000000000000"},
	{"uint64 max", "18446744073709551615", nil, "cfffffffffffffffff"},
	{"int64 max", "9223372036854775807", nil, "cf7fffffffffffffff"},
	{"int64 min", "-9223372036854775808", nil, "d38000000000000000"},
	{"uint64 above 2^53 keeps its digits", "9007199254740993", nil, "cf0020000000000001"},
	{"uint8 holding a small value", "5", nil, "cc05"},
	{"uint16 holding a small value", "5", nil, "cd0005"},
	{"uint32 holding a small value", "5", nil, "ce00000005"},
	{"uint64 holding a small value", "5", nil, "cf0000000000000005"},
	{"int8 holding a small value", "5", nil, "d005"},
	{"int16 holding a small value", "5", nil, "d10005"},
	{"int32 holding a small value", "5", nil, "d200000005"},
	{"int64 holding a small value", "5", nil, "d30000000000000005"},
	{"int8 minus one", "-1", nil, "d0ff"},
	{"int16 minus one", "-1", nil, "d1ffff"},
	{"int32 minus one", "-1", nil, "d2ffffffff"},
	{"int64 minus one", "-1", nil, "d3ffffffffffffffff"},
	{"negative fixint minimum", "-32", nil, "e0"},
	{"positive fixint maximum", "127", nil, "7f"},
}

// floatDecodeGoldens are float64 values and the text JSON.stringify writes for them.
var floatDecodeGoldens = []golden{
	{"float64 1e21", "1e+21", nil, "cb444b1ae4d6e2ef50"},
	{"float64 1.5e300", "1.5e+300", nil, "cb7e41eb2d66005835"},
	{"float64 max", "1.7976931348623157e+308", nil, "cb7fefffffffffffff"},
	{"float64 beyond 2^64", "123456789012345680000", nil, "cb441ac53a7e04bcda"},
	{"float64 below 1e21", "123456789012345680", nil, "cb437b69b4ba630f35"},
	{"float64 just below 1e21", "999999999999999900000", nil, "cb444b1ae4d6e2ef4f"},
	{"float64 1e-6", "0.000001", nil, "cb3eb0c6f7a0b5ed8d"},
	{"float64 just below 1e-6", "9.99999e-7", nil, "cb3eb0c6f6873c557a"},
	{"float64 1e-7", "1e-7", nil, "cb3e7ad7f29abcaf48"},
	{"float64 min subnormal", "5e-324", nil, "cb0000000000000001"},
	{"float64 min normal", "2.2250738585072014e-308", nil, "cb0010000000000000"},
	{"float64 negative 1e21", "-1e+21", nil, "cbc44b1ae4d6e2ef50"},
	{"float64 negative small", "-1.5e-10", nil, "cbbde49da7e361ce4c"},
	{"float64 half", "0.5", nil, "cb3fe0000000000000"},
	{"float64 integral", "3", nil, "cb4008000000000000"},
	{"float64 integral large", "4294967296", nil, "cb41f0000000000000"},
	{"float64 negative integral", "-2", nil, "cbc000000000000000"},
	{"float64 2^63", "9223372036854776000", nil, "cb43e0000000000000"},
	{"float64 pi", "3.141592653589793", nil, "cb400921fb54442d18"},
	{"float64 0.1", "0.1", nil, "cb3fb999999999999a"},
	{"float64 1e-10", "1e-10", nil, "cb3ddb7cdfd9d7bdbb"},
	{"float64 1.234e-20", "1.234e-20", nil, "cb3bcd231282b687e3"},
	{"float64 1e20", "100000000000000000000", nil, "cb4415af1d78b58c40"},
	{"float64 123456789.123456789", "123456789.12345679", nil, "cb419d6f34547e6b75"},
}

// negativeZeroGoldens is a float64 negative zero, which JSON.stringify writes as 0.
var negativeZeroGoldens = []golden{
	{"float64 negative zero", "0", nil, "cb8000000000000000"},
}

// floatWidthGoldens are float32 values, widened to float64, and the values that have no JSON text: JSON.stringify writes null for those.
var floatWidthGoldens = []golden{
	{"float32 1.5", "1.5", nil, "ca3fc00000"},
	{"float32 zero", "0", nil, "ca00000000"},
	{"float32 0.1 widened", "0.10000000149011612", nil, "ca3dcccccd"},
	{"float32 max", "3.4028234663852886e+38", nil, "ca7f7fffff"},
	{"float32 positive infinity", "null", nil, "ca7f800000"},
	{"float32 negative infinity", "null", nil, "caff800000"},
	{"float32 NaN", "null", nil, "ca7fc00000"},
	{"float32 min subnormal", "1.401298464324817e-45", nil, "ca00000001"},
	{"float32 negative", "-1", nil, "cabf800000"},
	{"float64 positive infinity", "null", nil, "cb7ff0000000000000"},
	{"float64 negative infinity", "null", nil, "cbfff0000000000000"},
	{"float64 NaN", "null", nil, "cb7ff8000000000000"},
	{"float64 zero", "0", nil, "cb0000000000000000"},
}

// stringGoldens are strings in the spelling JSON.stringify writes, both ways.
var stringGoldens = []golden{
	{"string empty", "\"\"", nil, "a0"},
	{"string ascii", "\"hello\"", nil, "a568656c6c6f"},
	{"string quote and backslash", "\"a\\\"b\\\\c\"", nil, "a56122625c63"},
	{"string control characters", "\"\\u0000\\u0001\\b\\t\\n\\u000b\\f\\r\\u001f\"", nil, "a9000108090a0b0c0d1f"},
	{"string every control character", "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000b\\f\\r\\u000e\\u000f\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f\"", nil, "d920000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"},
	{"string delete is kept", "\"a\u007fb\"", nil, "a3617f62"},
	{"string two byte rune", "\"é\"", nil, "a2c3a9"},
	{"string three byte runes", "\"日本\"", nil, "a6e697a5e69cac"},
	{"string four byte rune", "\"😀\"", nil, "a4f09f9880"},
	{"string line and paragraph separators stay raw", "\"a\u2028b\u2029c\"", nil, "a961e280a862e280a963"},
	{"string markup stays raw", "\"<b>&amp;</b>\"", nil, "ac3c623e26616d703b3c2f623e"},
	{"string slash stays raw", "\"a/b/c\"", nil, "a5612f622f63"},
	{"string noncharacters", "\"\ufffe\uffff\"", nil, "a6efbfbeefbfbf"},
	{"string replacement character", "\"\ufffd\"", nil, "a3efbfbd"},
	{"string byte order mark", "\"\ufeff\"", nil, "a3efbbbf"},
	{"string private use", "\"\ue000\"", nil, "a3ee8080"},
	{"string quote only", "\"\\\"\"", nil, "a122"},
	{"string backslash only", "\"\\\\\"", nil, "a15c"},
	{"string escape lookalikes", "\"\\\\n\\\\u0041\"", nil, "a85c6e5c7530303431"},
}

// stringSpellingGoldens are strings spelled with escapes. The bytes are those of the string JavaScript reads.
var stringSpellingGoldens = []golden{
	{"string escaped two byte rune", "\"\\u00e9\"", nil, "a2c3a9"},
	{"string escaped ascii", "\"\\u0041\"", nil, "a141"},
	{"string escaped slash", "\"a\\/b\"", nil, "a3612f62"},
	{"string surrogate pair", "\"\\ud83d\\ude00\"", nil, "a4f09f9880"},
	{"string surrogate pair in capitals", "\"\\uD83D\\uDE00\"", nil, "a4f09f9880"},
	{"string escaped line separator", "\"\\u2028\"", nil, "a3e280a8"},
	{"string escaped markup", "\"\\u003cb\\u003e\"", nil, "a33c623e"},
	{"string escaped nul", "\"a\\u0000b\"", nil, "a3610062"},
	{"string escaped controls", "\"\\b\\f\\n\\r\\t\"", nil, "a5080c0a0d09"},
	{"string escaped quote", "\"\\\"\"", nil, "a122"},
	{"string escaped backslash", "\"\\\\\"", nil, "a15c"},
	{"string capital hex digits", "\"\\u00E9\"", nil, "a2c3a9"},
	{"string escape after raw text", "\"abc\\ndef\"", nil, "a76162630a646566"},
	{"string pair after raw text", "\"x\\ud83d\\ude00y\"", nil, "a678f09f988079"},
	{"string many escapes", "\"\\u0041\\u0042\\u0043\\t\\u00e9\\u65e5\"", nil, "a941424309c3a9e697a5"},
	{"string escaped in a key", "{\"\\u0061\\n\":1}", nil, "81a2610a01"},
	{"string escaped noncharacter", "\"\\uffff\"", nil, "a3efbfbf"},
	{"string escaped replacement", "\"\\ufffd\"", nil, "a3efbfbd"},
	{"string escaped delete", "\"\\u007f\"", nil, "a17f"},
	{"string escaped three byte", "\"\\u65e5\\u672c\"", nil, "a6e697a5e69cac"},
}

var stringHeaderGoldens = []stringHeaderGolden{
	{"empty", "a", 0, "a0"},
	{"one byte", "a", 1, "a1"},
	{"30 bytes", "a", 30, "be"},
	{"31 bytes", "a", 31, "bf"},
	{"32 bytes", "a", 32, "d920"},
	{"255 bytes", "a", 255, "d9ff"},
	{"256 bytes", "a", 256, "da0100"},
	{"65535 bytes", "a", 65535, "daffff"},
	{"65536 bytes", "a", 65536, "db00010000"},
	{"2-byte runes 30 bytes", "é", 15, "be"},
	{"2-byte runes 32 bytes", "é", 16, "d920"},
	{"2-byte runes 254 bytes", "é", 127, "d9fe"},
	{"2-byte runes 256 bytes", "é", 128, "da0100"},
	{"3-byte runes 65535 bytes", "日", 21845, "daffff"},
	{"3-byte runes 65538 bytes", "日", 21846, "db00010002"},
	{"4-byte runes 32 bytes", "😀", 8, "d920"},
}

var binaryHeaderGoldens = []headerGolden{
	{"bin 0", 0, "c400"},
	{"bin 1", 1, "c401"},
	{"bin 3", 3, "c403"},
	{"bin 255", 255, "c4ff"},
	{"bin 256", 256, "c50100"},
	{"bin 65535", 65535, "c5ffff"},
	{"bin 65536", 65536, "c600010000"},
}

var arrayHeaderGoldens = []headerGolden{
	{"array of 0", 0, "90"},
	{"array of 1", 1, "91"},
	{"array of 15", 15, "9f"},
	{"array of 16", 16, "dc0010"},
	{"array of 255", 255, "dc00ff"},
	{"array of 256", 256, "dc0100"},
	{"array of 65535", 65535, "dcffff"},
	{"array of 65536", 65536, "dd00010000"},
}

var mapHeaderGoldens = []headerGolden{
	{"map of 0", 0, "80"},
	{"map of 1", 1, "81"},
	{"map of 15", 15, "8f"},
	{"map of 16", 16, "de0010"},
	{"map of 255", 255, "de00ff"},
	{"map of 256", 256, "de0100"},
	{"map of 65535", 65535, "deffff"},
	{"map of 65536", 65536, "df00010000"},
}

var stringListGoldens = []stringListGolden{
	{"empty", []string{}, "90"},
	{"two", []string{"a", "bb"}, "92a161a26262"},
	{"fifteen", []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14"}, "9fa27330a27331a27332a27333a27334a27335a27336a27337a27338a27339a3733130a3733131a3733132a3733133a3733134"},
	{"sixteen", []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14", "s15"}, "dc0010a27330a27331a27332a27333a27334a27335a27336a27337a27338a27339a3733130a3733131a3733132a3733133a3733134a3733135"},
	{"unicode", []string{"é", "日本", "😀"}, "93a2c3a9a6e697a5e69caca4f09f9880"},
	{"empty strings", []string{"", "", ""}, "93a0a0a0"},
}

// compositeGoldens hold objects whose members are not in sorted order: they keep the order of the text, both ways.
var compositeGoldens = []golden{
	{"unsorted members", "{\"z\":1,\"a\":{\"y\":[true,null,\"x\"],\"b\":1.5},\"m\":[]}", nil, "83a17a01a16182a17993c3c0a178a162cb3ff8000000000000a16d90"},
	{"unicode keys out of byte order", "{\"é\":1.5,\"日\":\"本\",\"A\":1,\"_\":[true,false,null],\"a\":{\"x\":\"y\"}}", nil, "85a2c3a9cb3ff8000000000000a3e697a5a3e69caca14101a15f93c3c2c0a16181a178a179"},
	{"nested unsorted objects", "{\"b\":{\"d\":1,\"c\":2},\"a\":{\"f\":1,\"e\":2}}", nil, "82a16282a16401a16302a16182a16601a16502"},
	{"array of arrays", "[[1,2],[],[[300]],[\"a\"]]", nil, "94920102909191cd012c91a161"},
	{"every integer family", "[0,127,128,255,256,65535,65536,4294967295,4294967296,-1,-32,-33,-128,-129,-32768,-32769,-2147483648,-2147483649]", nil, "dc0012007fcc80ccffcd0100cdffffce00010000ceffffffffcf0000000100000000ffe0d0dfd080d1ff7fd18000d2ffff7fffd280000000d3ffffffff7fffffff"},
	{"scalars", "[null,true,false,\"\",0,0.5]", nil, "96c0c3c2a000cb3fe0000000000000"},
	{"deep object in array", "{\"data\":{\"nested\":{\"deep\":[1,[2,[3,[4]]]]}}}", nil, "81a46461746181a66e657374656481a4646565709201920292039104"},
	{"event payload", "[\"news\",{\"zeta\":1,\"alpha\":{\"y\":2,\"b\":3}},70000,-3,4294967296]", nil, "95a46e65777382a47a65746101a5616c70686182a17902a16203ce00011170fdcf0000000100000000"},
	{"empty containers", "{\"a\":{},\"b\":[],\"c\":[{}],\"d\":[[]]}", nil, "84a16180a16290a1639180a1649190"},
	{"duplicate looking keys differ by case", "{\"b\":1,\"B\":2,\"a\":3,\"A\":4}", nil, "84a16201a14202a16103a14104"},
}

// binaryGoldens are values with binary parts. The text holds a placeholder where the bin value is, in reading order, both ways.
var binaryGoldens = []golden{
	{"one bin of three bytes", "[{\"_placeholder\":true,\"num\":0}]", [][]byte{{1, 2, 3}}, "91c403010203"},
	{"empty bin", "[{\"_placeholder\":true,\"num\":0}]", [][]byte{{}}, "91c400"},
	{"two bins in an array", "[{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "92c40101c40102"},
	{"bin as an object value", "{\"k\":{\"_placeholder\":true,\"num\":0}}", [][]byte{{1}}, "81a16bc40101"},
	{"bins in reading order", "[{\"_placeholder\":true,\"num\":0},{\"a\":[{\"_placeholder\":true,\"num\":1},{\"_placeholder\":true,\"num\":2}]},{\"_placeholder\":true,\"num\":3}]", [][]byte{{1}, {2}, {3}, {4}}, "93c4010181a16192c40102c40103c40104"},
	{"bin at the top level", "{\"_placeholder\":true,\"num\":0}", [][]byte{{9, 9}}, "c4020909"},
	{"twelve bins need two digits", "[{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1},{\"_placeholder\":true,\"num\":2},{\"_placeholder\":true,\"num\":3},{\"_placeholder\":true,\"num\":4},{\"_placeholder\":true,\"num\":5},{\"_placeholder\":true,\"num\":6},{\"_placeholder\":true,\"num\":7},{\"_placeholder\":true,\"num\":8},{\"_placeholder\":true,\"num\":9},{\"_placeholder\":true,\"num\":10},{\"_placeholder\":true,\"num\":11}]", [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}}, "9cc40100c40101c40102c40103c40104c40105c40106c40107c40108c40109c4010ac4010b"},
	{"bin with text around it", "[\"file\",\"x\",{\"_placeholder\":true,\"num\":0},\"y\",1.5,{\"n\":{\"_placeholder\":true,\"num\":1}}]", [][]byte{{7}, {8, 8}}, "96a466696c65a178c40107a179cb3ff800000000000081a16ec4020808"},
	{"bin of 256 bytes", "[{\"_placeholder\":true,\"num\":0}]", [][]byte{{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}}, "91c5010007070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707"},
	{"bin of 65536 bytes", "[{\"_placeholder\":true,\"num\":0}]", [][]byte{bytes.Repeat([]byte{5}, 65536)}, "91c600010000" + strings.Repeat("05", 65536)},
}

// placeholderReuseGoldens repeat a placeholder or write one out of turn. With
// the offsets of slotsOf an attachment is written once, at the offset of its
// turn, and every other object of that shape stays a map. The text reads back
// the same, because a bin value is given the number of its turn again.
var placeholderReuseGoldens = []golden{
	{"one attachment named twice", "[{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":0}]", [][]byte{{1}}, "92c4010182ac5f706c616365686f6c646572c3a36e756d00"},
	{"second placeholder past the attachments", "[{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":3}]", [][]byte{{1}}, "92c4010182ac5f706c616365686f6c646572c3a36e756d03"},
	{"placeholder before its turn", "[{\"_placeholder\":true,\"num\":1},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756d01c40101c40102"},
	{"placeholder after its turn", "[{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1},{\"_placeholder\":true,\"num\":0}]", [][]byte{{1}, {2}}, "93c40101c4010282ac5f706c616365686f6c646572c3a36e756d00"},
}

// notPlaceholderGoldens are objects that look like a placeholder and are not
// one, ahead of the two real ones: they stay maps.
var notPlaceholderGoldens = []golden{
	{"keys swapped", "[{\"num\":0,\"_placeholder\":true},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382a36e756d00ac5f706c616365686f6c646572c3c40101c40102"},
	{"space after the first colon", "[{\"_placeholder\": true,\"num\":0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756d00c40101c40102"},
	{"space after the second colon", "[{\"_placeholder\":true,\"num\": 0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756d00c40101c40102"},
	{"space before the closing brace", "[{\"_placeholder\":true,\"num\":0 },{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756d00c40101c40102"},
	{"extra member", "[{\"_placeholder\":true,\"num\":0,\"x\":1},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9383ac5f706c616365686f6c646572c3a36e756d00a17801c40101c40102"},
	{"member before", "[{\"x\":1,\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9383a17801ac5f706c616365686f6c646572c3a36e756d00c40101c40102"},
	{"negative number", "[{\"_placeholder\":true,\"num\":-1},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756dffc40101c40102"},
	{"number with a fraction", "[{\"_placeholder\":true,\"num\":1.0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756d01c40101c40102"},
	{"number with an exponent", "[{\"_placeholder\":true,\"num\":1e0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756d01c40101c40102"},
	{"number as text", "[{\"_placeholder\":true,\"num\":\"0\"},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756da130c40101c40102"},
	{"number missing", "[{\"_placeholder\":true},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9381ac5f706c616365686f6c646572c3c40101c40102"},
	{"flag false", "[{\"_placeholder\":false,\"num\":0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c2a36e756d00c40101c40102"},
	{"flag as text", "[{\"_placeholder\":\"true\",\"num\":0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572a474727565a36e756d00c40101c40102"},
	{"flag one", "[{\"_placeholder\":1,\"num\":0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c64657201a36e756d00c40101c40102"},
	{"number only", "[{\"num\":0},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9381a36e756d00c40101c40102"},
	{"empty object", "[{},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9380c40101c40102"},
	{"number null", "[{\"_placeholder\":true,\"num\":null},{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1}]", [][]byte{{1}, {2}}, "9382ac5f706c616365686f6c646572c3a36e756dc0c40101c40102"},
}

var plainPlaceholderGoldens = []golden{
	{"canonical form", "[{\"_placeholder\":true,\"num\":0}]", nil, "9182ac5f706c616365686f6c646572c3a36e756d00"},
	{"canonical form with a larger number", "[{\"_placeholder\":true,\"num\":5}]", nil, "9182ac5f706c616365686f6c646572c3a36e756d05"},
	{"canonical form with many digits", "[{\"_placeholder\":true,\"num\":123456}]", nil, "9182ac5f706c616365686f6c646572c3a36e756dce0001e240"},
}

// textPlaceholderGoldens hold the text of a placeholder inside a string, ahead
// of a real one: a string is never a placeholder.
var textPlaceholderGoldens = []golden{
	{"placeholder inside a string", "[\"{\\\"_placeholder\\\":true,\\\"num\\\":0}\",{\"_placeholder\":true,\"num\":0}]", [][]byte{{1}}, "92bd7b225f706c616365686f6c646572223a747275652c226e756d223a307dc40101"},
	{"placeholder in a key", "[{\"{\\\"_placeholder\\\":true,\\\"num\\\":0}\":1},{\"_placeholder\":true,\"num\":0}]", [][]byte{{1}}, "9281bd7b225f706c616365686f6c646572223a747275652c226e756d223a307d01c40101"},
}

// timestampGoldens are timestamps and the text JSON.stringify writes for the Date that notepack.io reads from them.
var timestampGoldens = []golden{
	{"timestamp 32 epoch", "\"1970-01-01T00:00:00.000Z\"", nil, "d6ff00000000"},
	{"timestamp 32", "\"2023-11-14T22:13:20.000Z\"", nil, "d6ff6553f100"},
	{"timestamp 32 max", "\"2106-02-07T06:28:15.000Z\"", nil, "d6ffffffffff"},
	{"timestamp 64 milliseconds", "\"2023-11-14T22:13:20.123Z\"", nil, "d7ff1d5353006553f100"},
	{"timestamp 64 min second step", "\"2106-02-07T06:28:16.000Z\"", nil, "d7ff0000000100000000"},
	{"timestamp 64 max", "\"2514-05-30T01:53:03.999Z\"", nil, "d7ffee2e1f03ffffffff"},
	{"timestamp 96 before epoch", "\"1969-12-31T23:59:59.999Z\"", nil, "c70cff3b8b87c0ffffffffffffffff"},
	{"timestamp 96 year 1", "\"0001-01-01T00:00:00.000Z\"", nil, "c70cff00000000fffffff1886e0900"},
	{"timestamp 96 after 34 bits", "\"2514-05-30T01:53:04.000Z\"", nil, "c70cff000000000000000400000000"},
	{"timestamp 96 year 9999", "\"9999-12-31T23:59:59.999Z\"", nil, "c70cff3b8b87c00000003afff4417f"},
	{"timestamp 96 year 10000", "\"+010000-01-01T00:00:00.000Z\"", nil, "c70cff000000000000003afff44180"},
	{"timestamp 96 year 275760", "\"+275760-09-13T00:00:00.000Z\"", nil, "c70cff00000000000007dba8218000"},
	{"timestamp 96 year -271821", "\"-271821-04-20T00:00:00.000Z\"", nil, "c70cff00000000fffff82457de8000"},
	{"timestamp 96 year -1", "\"-000001-01-01T00:00:00.000Z\"", nil, "c70cff00000000fffffff184aa5080"},
	{"timestamp 96 year 0", "\"0000-01-01T00:00:00.000Z\"", nil, "c70cff00000000fffffff1868b8400"},
	{"timestamp 96 year 100000", "\"+100000-01-01T00:00:00.000Z\"", nil, "c70cff00000000000002d044a2eb00"},
	{"timestamp 96 year -100000", "\"-099999-01-01T00:00:00.000Z\"", nil, "c70cff00000000fffffd12ca56a200"},
	{"timestamp 32 leap day", "\"2000-02-29T00:00:00.000Z\"", nil, "d6ff38bb0c00"},
	{"timestamp 32 last second of 2016", "\"2016-12-31T23:59:59.000Z\"", nil, "d6ff5868467f"},
	{"timestamp 64 with nanoseconds", "\"2023-11-14T22:13:20.123Z\"", nil, "d7ff1d6f34546553f100"},
	{"timestamp 64 with nanoseconds that round up to the next millisecond", "\"2023-11-14T22:13:20.001Z\"", nil, "d7ff003d08fc6553f100"},
	{"timestamp 64 with the largest nanoseconds", "\"2023-11-14T22:13:21.000Z\"", nil, "d7ffee6b27fc6553f100"},
	{"timestamp 96 with nanoseconds", "\"2023-11-14T22:13:20.123Z\"", nil, "c70cff075bcd15000000006553f100"},
	{"timestamp 96 before the epoch with nanoseconds", "\"1969-12-31T23:59:59.001Z\"", nil, "c70cff000f3e58ffffffffffffffff"},
	{"timestamp 96 before the epoch by half a millisecond", "\"1970-01-01T00:00:00.000Z\"", nil, "c70cff3b9328e0ffffffffffffffff"},
	{"timestamp 96 sub-millisecond nanoseconds after the epoch", "\"1970-01-01T00:00:00.000Z\"", nil, "c70cff000003e70000000000000000"},
	{"timestamp 96 just beyond the Date range", "null", nil, "c70cff00000000000007dba8218001"},
	{"timestamp 96 far beyond the Date range", "null", nil, "c70cff000000007ffffffffffffc00"},
	{"timestamp 96 far before the Date range", "null", nil, "c70cff000000008000000000000400"},
	{"timestamp 96 on the last millisecond of the Date range", "\"+275760-09-13T00:00:00.000Z\"", nil, "c70cff00000000000007dba8218000"},
	{"timestamp 96 on the first millisecond of the Date range", "\"-271821-04-20T00:00:00.000Z\"", nil, "c70cff00000000fffff82457de8000"},
	{"timestamp 96 nanoseconds that round up to the end of the Date range", "\"+275760-09-13T00:00:00.000Z\"", nil, "c70cff3b9ac9ff000007dba8217fff"},
	{"timestamp 96 half a millisecond past the Date range rounds back", "\"+275760-09-13T00:00:00.000Z\"", nil, "c70cff0007a120000007dba8218000"},
	{"timestamp 32 as ext8 of length 4", "\"2023-11-14T12:11:44.000Z\"", nil, "c704ff65536400"},
	{"timestamp 64 as ext8 of length 8", "\"2023-11-14T12:11:44.000Z\"", nil, "c708ff0000000065536400"},
	{"timestamp 32 as ext16", "\"2023-11-14T12:11:44.000Z\"", nil, "c80004ff65536400"},
	{"timestamp 96 as ext16", "\"2023-11-14T12:11:44.000Z\"", nil, "c8000cff000000000000000065536400"},
	{"timestamp 32 as ext32", "\"2023-11-14T12:11:44.000Z\"", nil, "c900000004ff65536400"},
	{"timestamp 32 as fixext4", "\"2023-11-14T22:13:20.000Z\"", nil, "d6ff6553f100"},
	{"timestamp 32 maximum", "\"2106-02-07T06:28:15.000Z\"", nil, "d6ffffffffff"},
}

// decodeGoldens are values in forms notepack.io does not write, and the text JSON.stringify writes for what it reads from them.
var decodeGoldens = []golden{
	{"nil", "null", nil, "c0"},
	{"false", "false", nil, "c2"},
	{"true", "true", nil, "c3"},
	{"str8 holding one byte", "\"a\"", nil, "d90161"},
	{"str16 holding one byte", "\"a\"", nil, "da000161"},
	{"str32 holding one byte", "\"a\"", nil, "db0000000161"},
	{"str8 holding nothing", "\"\"", nil, "d900"},
	{"empty array", "[]", nil, "90"},
	{"array16 holding nothing", "[]", nil, "dc0000"},
	{"array32 holding nothing", "[]", nil, "dd00000000"},
	{"fixarray of two", "[1,2]", nil, "920102"},
	{"array16 of two", "[1,2]", nil, "dc00020102"},
	{"array32 of two", "[1,2]", nil, "dd000000020102"},
	{"empty map", "{}", nil, "80"},
	{"map16 holding nothing", "{}", nil, "de0000"},
	{"map32 holding nothing", "{}", nil, "df00000000"},
	{"fixmap of one", "{\"a\":1}", nil, "81a16101"},
	{"map16 of one", "{\"a\":1}", nil, "de0001a16101"},
	{"map32 of one", "{\"a\":1}", nil, "df00000001a16101"},
	{"map key as str8", "{\"a\":1}", nil, "81d9016101"},
	{"map key as str16", "{\"a\":1}", nil, "81da00016101"},
	{"bin16 holding three bytes", "{\"_placeholder\":true,\"num\":0}", [][]byte{{1, 2, 3}}, "c50003010203"},
	{"bin32 holding three bytes", "{\"_placeholder\":true,\"num\":0}", [][]byte{{1, 2, 3}}, "c600000003010203"},
	{"bin16 holding nothing", "{\"_placeholder\":true,\"num\":0}", [][]byte{{}}, "c50000"},
	{"bin32 holding nothing", "{\"_placeholder\":true,\"num\":0}", [][]byte{{}}, "c600000000"},
	{"timestamp 32 as fixext4", "\"1970-01-01T00:00:00.000Z\"", nil, "d6ff00000000"},
	{"timestamp 64 as fixext8", "\"1970-01-01T00:00:00.000Z\"", nil, "d7ff0000000000000000"},
	{"timestamp 96 as ext8", "\"1970-01-01T00:00:00.000Z\"", nil, "c70cff000000000000000000000000"},
	{"array with a nil and a map", "[null,{\"a\":1}]", nil, "92c081a16101"},
}

var unsupportedGoldens = []struct{ name, hex string }{
	{"fixext1 of type 1", "d40100"},
	{"fixext1 holding undefined for notepack.io", "d40000"},
	{"fixext1 of the timestamp type", "d4ff00"},
	{"fixext2 of the timestamp type", "d5ff0000"},
	{"fixext4 of type 1", "d60100000000"},
	{"fixext4 of type 0", "d60000000000"},
	{"fixext8 of type 5", "d7050000000000000000"},
	{"fixext8 of type 2", "d7020000000000000000"},
	{"fixext16 of the timestamp type", "d8ff00000000000000000000000000000000"},
	{"ext8 of type 5 and length 12", "c70c05000000000000000000000000"},
	{"ext8 of the timestamp type and length 3", "c703ff000000"},
	{"ext8 of the timestamp type and length 0", "c700ff"},
	{"ext8 of the timestamp type and length 5", "c705ff0000000000"},
	{"ext8 of the timestamp type and length 13", "c70dff00000000000000000000000000"},
	{"ext8 of type 7 and length 3", "c70307aabbcc"},
	{"ext16 of type 7", "c8000307aabbcc"},
	{"ext32 of type 7", "c90000000307aabbcc"},
	{"ext16 of the timestamp type and length 5", "c80005ff0000000000"},
	{"never used byte 0xc1", "c1"},
}

// goldenGroup is a named table of golden rows.
type goldenGroup struct {
	name string
	rows []golden
}

var encodeGroups = []goldenGroup{
	{name: "number", rows: numberGoldens},
	{name: "integral", rows: integralGoldens},
	{name: "float", rows: floatGoldens},
	{name: "number beyond JavaScript", rows: numberSpecGoldens},
	{name: "string", rows: stringGoldens},
	{name: "string spelling", rows: stringSpellingGoldens},
	{name: "composite", rows: compositeGoldens},
	{name: "binary", rows: binaryGoldens},
	{name: "placeholder reuse", rows: placeholderReuseGoldens},
	{name: "not a placeholder", rows: notPlaceholderGoldens},
	{name: "placeholder without attachments", rows: plainPlaceholderGoldens},
	{name: "placeholder in text", rows: textPlaceholderGoldens},
}

var decodeGroups = []goldenGroup{
	{name: "number", rows: numberGoldens},
	{name: "integer", rows: integerDecodeGoldens},
	{name: "float", rows: floatDecodeGoldens},
	{name: "negative zero", rows: negativeZeroGoldens},
	{name: "float width", rows: floatWidthGoldens},
	{name: "string", rows: stringGoldens},
	{name: "composite", rows: compositeGoldens},
	{name: "binary", rows: binaryGoldens},
	{name: "placeholder without attachments", rows: plainPlaceholderGoldens},
	{name: "placeholder reuse", rows: placeholderReuseGoldens},
	{name: "placeholder in text", rows: textPlaceholderGoldens},
	{name: "timestamp", rows: timestampGoldens},
	{name: "other forms", rows: decodeGoldens},
}

func TestAppendJSONMatchesNotepack(t *testing.T) {
	t.Parallel()
	for _, group := range encodeGroups {
		for _, tc := range group.rows {
			t.Run(group.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				got, err := appendJSON(nil, []byte(tc.json), tc.attachments, slotsOf(tc.json, len(tc.attachments)), 0)
				if err != nil {
					t.Fatalf("appendJSON(%s) failed: %v", brief(tc.json), err)
				}
				requireBytes(t, got, unhex(t, tc.hex))
			})
		}
	}
}

func TestDecodeMatchesJSONStringify(t *testing.T) {
	t.Parallel()
	for _, group := range decodeGroups {
		for _, tc := range group.rows {
			t.Run(group.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				out, attachments, err := decodeValue(unhex(t, tc.hex))
				if err != nil {
					t.Fatalf("decoding %.64s failed: %v", tc.hex, err)
				}
				requireText(t, out, tc.json)
				requireAttachments(t, attachments, tc.attachments)
			})
		}
	}
}

func TestDecodedAttachmentsAliasTheInput(t *testing.T) {
	t.Parallel()
	src := unhex(t, "92c40301020391c0")
	_, attachments, err := decodeValue(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(attachments) != 1 || len(attachments[0]) != 3 {
		t.Fatalf("attachments = %v", attachments)
	}
	if &attachments[0][0] != &src[3] {
		t.Error("the attachment is a copy, want a slice of the input")
	}
}

func TestTextHeaders(t *testing.T) {
	t.Parallel()
	for _, tc := range stringHeaderGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value := stringValue(tc)
			want := append(unhex(t, tc.hex), value...)
			requireBytes(t, appendText(nil, value), want)
			requireBytes(t, appendText(nil, []byte(value)), want)
			encoded, err := appendJSON(nil, []byte(`"`+value+`"`), nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			requireBytes(t, encoded, want)
			out, _, err := decodeValue(want)
			if err != nil {
				t.Fatal(err)
			}
			requireText(t, out, `"`+value+`"`)
		})
	}
}

func TestBinaryHeaders(t *testing.T) {
	t.Parallel()
	for _, tc := range binaryHeaderGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := unhex(t, tc.hex)
			requireBytes(t, blob.header(nil, tc.n), header)
			body := bytes.Repeat([]byte{0xab}, tc.n)
			want := slices.Concat([]byte{0x91}, header, body)
			encoded, err := appendJSON(nil, []byte(`[{"_placeholder":true,"num":0}]`), [][]byte{body}, []int{1}, 0)
			if err != nil {
				t.Fatal(err)
			}
			requireBytes(t, encoded, want)
			out, attachments, err := decodeValue(want)
			if err != nil {
				t.Fatal(err)
			}
			requireText(t, out, `[{"_placeholder":true,"num":0}]`)
			requireAttachments(t, attachments, [][]byte{body})
		})
	}
}

func TestArrayHeaders(t *testing.T) {
	t.Parallel()
	for _, tc := range arrayHeaderGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := unhex(t, tc.hex)
			requireBytes(t, array.header(nil, tc.n), header)
			want := slices.Concat(header, bytes.Repeat([]byte{0xc0}, tc.n))
			encoded, err := appendJSON(nil, []byte(listText(tc.n)), nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			requireBytes(t, encoded, want)
			out, _, err := decodeValue(want)
			if err != nil {
				t.Fatal(err)
			}
			requireText(t, out, listText(tc.n))
		})
	}
}

func TestMapHeaders(t *testing.T) {
	t.Parallel()
	for _, tc := range mapHeaderGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := unhex(t, tc.hex)
			requireBytes(t, dictionary.header(nil, tc.n), header)
			want := append(header, unhex(t, dictionaryBody(tc.n))...)
			encoded, err := appendJSON(nil, []byte(dictionaryText(tc.n)), nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			requireBytes(t, encoded, want)
			out, _, err := decodeValue(want)
			if err != nil {
				t.Fatal(err)
			}
			requireText(t, out, dictionaryText(tc.n))
		})
	}
}

func TestAppendTextsMatchesNotepack(t *testing.T) {
	t.Parallel()
	for _, tc := range stringListGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := unhex(t, tc.hex)
			requireBytes(t, appendTexts(nil, tc.values), want)
			requireBytes(t, appendTexts([]byte{0xff}, tc.values), append([]byte{0xff}, want...))
		})
	}
}

func TestAppendJSONKeepsWhatIsAlreadyWritten(t *testing.T) {
	t.Parallel()
	got, err := appendJSON([]byte{0xff, 0xfe}, []byte(`[1,"a"]`), nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	requireBytes(t, got, []byte{0xff, 0xfe, 0x92, 0x01, 0xa1, 'a'})
}

func TestDecodeUnsupported(t *testing.T) {
	t.Parallel()
	for _, tc := range unsupportedGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeValue(unhex(t, tc.hex))
			if !errors.Is(err, errUnsupported) {
				t.Fatalf("err = %v, want errUnsupported", err)
			}
		})
	}
}

func TestDecodeRejectsKeysThatAreNotStrings(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, hex string }{
		{"integer key", "8101c0"},
		{"negative integer key", "81ffc0"},
		{"unsigned key", "81cc05c0"},
		{"nil key", "81c0c0"},
		{"true key", "81c3c0"},
		{"float key", "81cb3ff0000000000000c0"},
		{"bin key", "81c40101c0"},
		{"array key", "8191 01c0"},
		{"map key", "8180c0"},
		{"timestamp key", "81d6ff00000000c0"},
		{"second key", "82a161c001c0"},
		{"key of a map in an array", "9181 01c0"},
		{"key of a map in a map", "81a16181 01c0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeValue(unhex(t, strings.ReplaceAll(tc.hex, " ", "")))
			if !errors.Is(err, errUnsupported) {
				t.Fatalf("err = %v, want errUnsupported", err)
			}
		})
	}
}

func TestDecodeReportsEveryPrefixAsTruncated(t *testing.T) {
	t.Parallel()
	for _, group := range decodeGroups {
		for _, tc := range group.rows {
			raw := unhex(t, tc.hex)
			if len(raw) > 1024 {
				continue
			}
			t.Run(group.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				for n := range len(raw) {
					if _, _, err := decodeValue(raw[:n]); !errors.Is(err, errTruncated) {
						t.Fatalf("the first %d of %d bytes: err = %v, want errTruncated", n, len(raw), err)
					}
				}
			})
		}
	}
}

func TestDecodeRefusesBytesAfterTheValue(t *testing.T) {
	t.Parallel()
	for _, group := range decodeGroups {
		for _, tc := range group.rows {
			t.Run(group.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				for _, extra := range []string{"00", "c0", "9100"} {
					raw := append(unhex(t, tc.hex), unhex(t, extra)...)
					if _, _, err := decodeValue(raw); !errors.Is(err, errTrailing) {
						t.Fatalf("followed by %s: err = %v, want errTrailing", extra, err)
					}
				}
			})
		}
	}
}

func TestDecodeOfNothingIsTruncated(t *testing.T) {
	t.Parallel()
	for _, src := range [][]byte{nil, {}} {
		if _, _, err := decodeValue(src); !errors.Is(err, errTruncated) {
			t.Errorf("decodeValue(%#v) error = %v, want errTruncated", src, err)
		}
	}
}

func TestDecodeFailsFastOnLengthsBeyondTheInput(t *testing.T) {
	tests := []struct{ name, hex string }{
		{"array32 of four billion", "ddffffffff"},
		{"array32 of two billion", "dd7fffffff"},
		{"array16 without elements", "dcffff"},
		{"map32 of four billion", "dfffffffff"},
		{"map16 without pairs", "deffff"},
		{"str32 of four billion", "dbffffffff"},
		{"str16 without bytes", "daffff"},
		{"str8 without bytes", "d9ff"},
		{"bin32 of four billion", "c6ffffffff"},
		{"bin32 of two billion", "c67fffffff"},
		{"bin16 without bytes", "c5ffff"},
		{"ext32 of four billion", "c9ffffffff07"},
		{"ext16 without bytes", "c8ffff07"},
		{"array in an array", "91ddffffffff"},
		{"map value", "81a161ddffffffff"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := unhex(t, tc.hex)
			const runs = 100
			allocated := allocatedBytes(runs, func() {
				if _, _, err := decodeValue(raw); !errors.Is(err, errTruncated) {
					t.Errorf("err = %v, want errTruncated", err)
				}
			})
			if allocated > 1<<16 {
				t.Errorf("%d bytes allocated in %d runs, want almost none", allocated, runs)
			}
		})
	}
}

func TestDepthLimit(t *testing.T) {
	t.Parallel()
	for _, objects := range []bool{false, true} {
		shape := "arrays"
		if objects {
			shape = "objects"
		}
		for _, depth := range []int{0, 1, 2, 100} {
			limit := maxDepth - depth
			t.Run(fmt.Sprintf("%s starting at depth %d", shape, depth), func(t *testing.T) {
				t.Parallel()
				text, raw := nestedText(limit, objects), nestedBytes(limit, objects)
				encoded, err := appendJSON(nil, []byte(text), nil, nil, depth)
				if err != nil {
					t.Fatalf("encoding %d containers: %v", limit, err)
				}
				requireBytes(t, encoded, raw)
				r := reader{src: raw, depth: depth}
				if err := r.value(); err != nil {
					t.Fatalf("decoding %d containers: %v", limit, err)
				}
				requireText(t, r.out, text)
				text, raw = nestedText(limit+1, objects), nestedBytes(limit+1, objects)
				if _, err := appendJSON(nil, []byte(text), nil, nil, depth); !errors.Is(err, errTooDeep) {
					t.Errorf("encoding %d containers: err = %v, want errTooDeep", limit+1, err)
				}
				r = reader{src: raw, depth: depth}
				if err := r.value(); !errors.Is(err, errTooDeep) {
					t.Errorf("decoding %d containers: err = %v, want errTooDeep", limit+1, err)
				}
			})
		}
	}
}

func TestDepthLimitHoldsForVeryDeepValues(t *testing.T) {
	t.Parallel()
	for _, objects := range []bool{false, true} {
		const levels = 100_000
		if _, err := appendJSON(nil, []byte(nestedText(levels, objects)), nil, nil, 0); !errors.Is(err, errTooDeep) {
			t.Errorf("encoding %d containers (objects %t): err = %v, want errTooDeep", levels, objects, err)
		}
		if _, _, err := decodeValue(nestedBytes(levels, objects)); !errors.Is(err, errTooDeep) {
			t.Errorf("decoding %d containers (objects %t): err = %v, want errTooDeep", levels, objects, err)
		}
	}
}

func TestSkippingNeedsNoDepth(t *testing.T) {
	t.Parallel()
	const levels = 100_000
	for _, objects := range []bool{false, true} {
		raw := nestedBytes(levels, objects)
		r := reader{src: raw}
		if err := r.skip(); err != nil {
			t.Errorf("skipping %d containers (objects %t): %v", levels, objects, err)
		}
		if len(r.src) != 0 {
			t.Errorf("%d bytes left after skipping (objects %t)", len(r.src), objects)
		}
	}
}

func TestAppendJSONRejects(t *testing.T) {
	t.Parallel()
	one := [][]byte{{1}}
	two := [][]byte{{1}, {2}}
	tests := []struct {
		name        string
		text        string
		attachments [][]byte
		want        error
	}{
		{"nothing", ``, nil, errJSON},
		{"blank", ` `, nil, errJSON},
		{"half a literal", `nul`, nil, errJSON},
		{"half of true", `tru`, nil, errJSON},
		{"trailing comma in an array", `[1,]`, nil, errJSON},
		{"trailing comma in an object", `{"a":1,}`, nil, errJSON},
		{"missing comma", `[1 2]`, nil, errJSON},
		{"unclosed array", `[`, nil, errJSON},
		{"unclosed array after an element", `[1,2`, nil, errJSON},
		{"unclosed object", `{`, nil, errJSON},
		{"closing bracket alone", `]`, nil, errJSON},
		{"closing brace alone", `}`, nil, errJSON},
		{"comma alone", `,`, nil, errJSON},
		{"extra closing brace", `{"a":1}}`, nil, errJSON},
		{"member without a value", `{"a"}`, nil, errJSON},
		{"leading zero", `01`, nil, errJSON},
		{"trailing dot", `1.`, nil, errJSON},
		{"leading dot", `.5`, nil, errJSON},
		{"leading plus", `+1`, nil, errJSON},
		{"lonely minus", `-`, nil, errJSON},
		{"empty exponent", `1e`, nil, errJSON},
		{"NaN", `NaN`, nil, errJSON},
		{"single quotes", `'a'`, nil, errJSON},
		{"unclosed string", `"a`, nil, errJSON},
		{"unknown escape", `"\x"`, nil, errJSON},
		{"short unicode escape", `"\u12"`, nil, errJSON},
		{"raw newline in a string", "[\"a\nb\"]", nil, errJSON},
		{"two values", `[1] [2]`, nil, errJSON},
		{"text after the value", `["a"] x`, nil, errJSON},
		{"placeholder with a leading zero", `[{"_placeholder":true,"num":01}]`, one, errJSON},
		{"placeholder past the attachments", `[{"_placeholder":true,"num":1}]`, one, errAttachment},
		{"placeholder far past the attachments", `[{"_placeholder":true,"num":99}]`, one, errAttachment},
		{"placeholder number beyond an integer", `[{"_placeholder":true,"num":99999999999999999999}]`, one, errAttachment},
		{"placeholders out of order", `[{"_placeholder":true,"num":1},{"_placeholder":true,"num":0}]`, two, errAttachment},
		{"attachment left unnamed", `[{"_placeholder":true,"num":0}]`, two, errAttachment},
		{"attachment named by a look-alike only", `[{"num":0,"_placeholder":true}]`, one, errAttachment},
		{"attachment named inside a string only", `["{\"_placeholder\":true,\"num\":0}"]`, one, errAttachment},
		{"attachments without any placeholder", `["a",1]`, two, errAttachment},
		{"arrays too deep", nestedText(maxDepth+1, false), nil, errTooDeep},
		{"objects too deep", nestedText(maxDepth+1, true), nil, errTooDeep},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := appendJSON(nil, []byte(tc.text), tc.attachments, slotsOf(tc.text, len(tc.attachments)), 0); !errors.Is(err, tc.want) {
				t.Fatalf("appendJSON(%s) error = %v, want %v", brief(tc.text), err, tc.want)
			}
		})
	}
}

var unquotedBodies = []string{
	``,
	`plain`,
	`\"`,
	`\\`,
	`\/`,
	`\b\f\n\r\t`,
	`\u0041`,
	`\u00e9\u20ac`,
	`\u00E9\u20AC`,
	`\ud83d\ude00`,
	`\uD83D\uDE00`,
	`\ud800\udc00`,
	`\udbff\udfff`,
	`\ud83d`,
	`\ude00`,
	`\ud83dx`,
	`\ud83dA`,
	`\ud83d\ud83d\ude00`,
	`\ude00\ud83d`,
	`\ud83d\n`,
	`\u0000`,
	`a\u0000b`,
	`\u2028\u2029`,
	`\\n\\u0041`,
	`a\nb\"c\\d`,
	"\xff",
	"a\xffb\\n",
	"\xc3",
	"\xe2\x82",
	"\xed\xa0\x80",
	"\xf4\x90\x80\x80",
	"\xc0\x80",
	"\xf0\x9f\x98",
	"\u2028\u2029",
	"\u007f",
	"\ufeff",
	"\ufffd",
	"é日😀",
	"é\\n日\\u00e9😀\xff",
}

func TestAppendUnquotedMatchesEncodingJSON(t *testing.T) {
	t.Parallel()
	for _, body := range unquotedBodies {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			t.Parallel()
			requireUnquotedAsEncodingJSON(t, []byte(body))
		})
	}
}

func requireUnquotedAsEncodingJSON(t testing.TB, body []byte) {
	t.Helper()
	literal := slices.Concat([]byte{'"'}, body, []byte{'"'})
	if !json.Valid(literal) {
		t.Fatalf("%q is not a JSON string", literal)
	}
	var want string
	if err := json.Unmarshal(literal, &want); err != nil {
		t.Fatalf("encoding/json refuses %q: %v", literal, err)
	}
	if got := appendUnquoted(nil, body); string(got) != want {
		t.Fatalf("appendUnquoted(%q)\n got: %q\nwant: %q", body, got, want)
	}
	end, escaped := endOfString(literal, 0)
	if end != len(literal)-1 {
		t.Errorf("endOfString(%q) = %d, want %d", literal, end, len(literal)-1)
	}
	if escaped != bytes.Contains(body, []byte{'\\'}) {
		t.Errorf("endOfString(%q) reports escaped = %t", literal, escaped)
	}
	prefix := []byte("xyz")
	requireBytes(t, appendUnquoted(prefix, body), append([]byte("xyz"), want...))
}

func FuzzAppendUnquoted(f *testing.F) {
	for _, body := range unquotedBodies {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if !json.Valid(slices.Concat([]byte{'"'}, body, []byte{'"'})) {
			t.Skip()
		}
		requireUnquotedAsEncodingJSON(t, body)
	})
}

var decodeErrors = []error{errTruncated, errTrailing, errTooDeep, errUnsupported}

func FuzzDecodeValue(f *testing.F) {
	for _, group := range decodeGroups {
		for _, tc := range group.rows {
			if raw := unhex(f, tc.hex); len(raw) <= 4096 {
				f.Add(raw)
			}
		}
	}
	for _, tc := range unsupportedGoldens {
		f.Add(unhex(f, tc.hex))
	}
	f.Add(nestedBytes(maxDepth, false))
	f.Add(nestedBytes(maxDepth+1, true))
	f.Fuzz(func(t *testing.T, src []byte) {
		out, attachments, err := decodeValue(src)
		if err != nil {
			if !slices.ContainsFunc(decodeErrors, func(known error) bool { return errors.Is(err, known) }) {
				t.Fatalf("decodeValue(%x) failed with an error that is none of the known ones: %v", src, err)
			}
			return
		}
		if !utf8.Valid(out) || !json.Valid(out) {
			t.Fatalf("decodeValue(%x) = %q, which is not JSON", src, out)
		}
		for i, attachment := range attachments {
			if len(attachment) > len(src) {
				t.Fatalf("attachment %d holds %d bytes of an input of %d", i, len(attachment), len(src))
			}
		}
	})
}

// arrayOf is the JSON text of an array of n copies of element.
func arrayOf(element string, n int) []byte {
	return []byte("[" + strings.TrimSuffix(strings.Repeat(element+",", n), ",") + "]")
}

// leastAllocations is the smallest number of allocations that run makes over
// several measurements. The runtime counts the allocations of every goroutine,
// the collector's among them, and it collects more often when the buffers are
// larger: the collector is off while run is measured, and called in between.
func leastAllocations(run func()) float64 {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	least := math.Inf(1)
	for range 7 {
		runtime.GC()
		least = min(least, testing.AllocsPerRun(10, run))
	}
	return least
}

// skipUnderRace skips a test that is about the number of allocations alone: the
// race detector allocates on its own, and it runs the code some forty times
// slower than it runs without it.
func skipUnderRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("the race detector changes the number of allocations")
	}
}

func appendJSONAllocations(t *testing.T, src []byte, attachments [][]byte) float64 {
	t.Helper()
	slots := slotsOf(string(src), len(attachments))
	return leastAllocations(func() {
		if _, err := appendJSON(nil, src, attachments, slots, 0); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAppendJSONAllocationsStayConstant(t *testing.T) {
	skipUnderRace(t)
	tests := []struct {
		name        string
		element     string
		attachments [][]byte
	}{
		{"small integers", "1", nil},
		{"integers of every width", "70000", nil},
		{"negative integers", "-300", nil},
		{"strings", `"abc"`, nil},
		{"strings with escapes", `"a\nb\u00e9"`, nil},
		{"strings with invalid UTF-8", "\"a\xffb\"", nil},
		{"literals", "null", nil},
		{"empty arrays", "[]", nil},
		{"empty objects", "{}", nil},
		{"placeholders", `{"_placeholder":true,"num":0}`, [][]byte{{1, 2, 3}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			small := appendJSONAllocations(t, arrayOf(tc.element, 1<<10), tc.attachments)
			large := appendJSONAllocations(t, arrayOf(tc.element, 1<<16), tc.attachments)
			t.Logf("allocations at 1<<10 elements: %v, at 1<<16: %v", small, large)
			if large != small {
				t.Errorf("allocations grow with the array: %v for %d elements, %v for %d", small, 1<<10, large, 1<<16)
			}
			if large > 4 {
				t.Errorf("%v allocations, want a few", large)
			}
		})
	}
}

// A number with a fraction takes nine bytes, more than its text does, so an
// array of them outgrows the room that the size of the text reserves.
func TestAppendJSONAllocationsOfFloatsGrowSlowly(t *testing.T) {
	skipUnderRace(t)
	small := appendJSONAllocations(t, arrayOf("1.5", 1<<10), nil)
	large := appendJSONAllocations(t, arrayOf("1.5", 1<<16), nil)
	t.Logf("allocations at 1<<10 elements: %v, at 1<<16: %v", small, large)
	requireSlowGrowth(t, small, large, 4)
}

func TestPlaceholderLength(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		text   string
		number int
		want   int
	}{
		{"the placeholder alone", placeholderText(0), 0, len(placeholderText(0))},
		{"the placeholder with text after it", placeholderText(0) + `,1]`, 0, len(placeholderText(0))},
		{"a number of two digits", placeholderText(12), 12, len(placeholderText(12))},
		{"a number that starts with the one asked for", placeholderText(12), 1, 0},
		{"a number that the one asked for starts with", placeholderText(1), 12, 0},
		{"another number", placeholderText(1), 0, 0},
		{"cut before the closing brace", `{"_placeholder":true,"num":0`, 0, 0},
		{"cut before the number", `{"_placeholder":true,"num":`, 0, 0},
		{"a space before the closing brace", `{"_placeholder":true,"num":0 }`, 0, 0},
		{"another member", `{"_placeholder":true,"num":0,"x":1}`, 0, 0},
		{"no number", `{"_placeholder":true}`, 0, 0},
		{"the keys the other way round", `{"num":0,"_placeholder":true}`, 0, 0},
		{"no text", ``, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := placeholderLength([]byte(tc.text), tc.number); got != tc.want {
				t.Errorf("placeholderLength(%s, %d) = %d, want %d", tc.text, tc.number, got, tc.want)
			}
		})
	}
}

func TestAppendJSONWritesOnlyTheObjectAtAnOffsetAsBinary(t *testing.T) {
	t.Parallel()
	look := placeholderText(0)
	flag, zero := mpMember("_placeholder", []byte{0xc3}), mpMember("num", []byte{0})
	object, body := mpMap(flag, zero), mpBin([]byte{9})
	tests := []struct {
		name  string
		text  string
		slots []int
		want  []byte
	}{
		{"the look-alike first", "[" + look + "," + look + "]", []int{2 + len(look)}, mpArray(object, body)},
		{"the look-alike last", "[" + look + "," + look + "]", []int{1}, mpArray(body, object)},
		{"a look-alike on both sides", "[" + look + "," + look + "," + look + "]", []int{2 + len(look)}, mpArray(object, body, object)},
		{"the look-alike under a key", `{"a":` + look + `,"b":` + look + `}`, []int{10 + len(look)}, mpMap(mpMember("a", object), mpMember("b", body))},
		{"the look-alike in an array of its own", "[[" + look + "]," + look + "]", []int{4 + len(look)}, mpArray(mpArray(object), body)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := appendJSON(nil, []byte(tc.text), [][]byte{{9}}, tc.slots, 0)
			if err != nil {
				t.Fatal(err)
			}
			requireBytes(t, got, tc.want)
			r := reader{src: got}
			if err := r.value(); err != nil {
				t.Fatal(err)
			}
			requireText(t, r.out, tc.text)
			if !slices.Equal(r.slots, tc.slots) {
				t.Errorf("offsets read back = %v, want %v", r.slots, tc.slots)
			}
		})
	}
}

func TestAppendJSONRejectsOffsetsThatLeadToNoPlaceholder(t *testing.T) {
	t.Parallel()
	one, two := [][]byte{{1}}, [][]byte{{1}, {2}}
	first, second := placeholderText(0), placeholderText(1)
	single, pair := "["+first+"]", "["+first+","+second+"]"
	tests := []struct {
		name        string
		text        string
		attachments [][]byte
		slots       []int
	}{
		{"no offset for an attachment", single, one, nil},
		{"an offset without an attachment", single, nil, []int{1}},
		{"fewer offsets than attachments", pair, two, []int{1}},
		{"more offsets than attachments", pair, one, []int{1, 2 + len(first)}},
		{"an offset at the bracket of the array", single, one, []int{0}},
		{"an offset inside the placeholder", single, one, []int{2}},
		{"an offset before the text", single, one, []int{-1}},
		{"an offset past the text", single, one, []int{len(single)}},
		{"an offset at another object", `[{"a":1},` + first + `]`, one, []int{1}},
		{"an offset at the placeholder of another attachment", "[" + second + "]", one, []int{1}},
		{"an offset at a placeholder with a space", `[{"_placeholder":true,"num":0 }]`, one, []int{1}},
		{"an offset at a placeholder with another member", `[{"_placeholder":true,"num":0,"x":1}]`, one, []int{1}},
		{"an offset at a brace inside a string", `["{",` + first + `]`, one, []int{2}},
		{"the offsets the other way round", pair, two, []int{2 + len(first), 1}},
		{"the same offset twice", pair, two, []int{1, 1}},
		{"the second offset at the first placeholder again", "[" + first + "," + first + "]", two, []int{1, 2 + len(first)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := appendJSON(nil, []byte(tc.text), tc.attachments, tc.slots, 0); !errors.Is(err, errAttachment) {
				t.Fatalf("appendJSON(%s) with the offsets %v: error = %v, want errAttachment", brief(tc.text), tc.slots, err)
			}
		})
	}
}

func TestDecodeRecordsTheOffsetOfEveryPlaceholder(t *testing.T) {
	t.Parallel()
	for _, tc := range binaryGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := reader{src: unhex(t, tc.hex)}
			if err := r.value(); err != nil {
				t.Fatal(err)
			}
			if want := slotsOf(string(r.out), len(tc.attachments)); !slices.Equal(r.slots, want) {
				t.Errorf("offsets = %v, want %v", r.slots, want)
			}
		})
	}
}
