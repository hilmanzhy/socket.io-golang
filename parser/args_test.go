package parser_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/nuriontech/socket.io-golang/parser"
)

func TestArgsLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wire        string
		attachments [][]byte
		want        int
	}{
		{"event without arguments", `2["foo"]`, nil, 0},
		{"event with one argument", `2["foo","a"]`, nil, 1},
		{"event with null arguments", `2["foo",null,null,null]`, nil, 3},
		{"binary event", `52-["foo",` + ph0 + `,"x",` + ph1 + `]`, blobs(2), 3},
		{"binary event without argument", `51-["foo"]`, blobs(1), 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, args := mustEvent(t, tc.wire, tc.attachments...)
			if args.Len() != tc.want {
				t.Fatalf("Len() = %d, want %d", args.Len(), tc.want)
			}
		})
	}
}

func TestArgsOutOfRangeIndexes(t *testing.T) {
	t.Parallel()
	_, textArgs := mustEvent(t, `2["foo","a",1]`)
	_, binaryArgs := mustEvent(t, `51-["foo","a",`+ph0+`]`, []byte{1})
	_, noArgs := mustEvent(t, `2["foo"]`)
	ackArgs := mustAck(t, `3["x"]`)
	zero := parser.Args{}
	cases := map[string]parser.Args{
		"text arguments":   textArgs,
		"binary arguments": binaryArgs,
		"no arguments":     noArgs,
		"ack arguments":    ackArgs,
		"zero value":       zero,
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, i := range []int{-1, -2, -100, math.MinInt, math.MinInt32, args.Len(), args.Len() + 1, 99, math.MaxInt32, math.MaxInt} {
				if args.IsBinary(i) {
					t.Errorf("IsBinary(%d) = true", i)
				}
				if got := args.Bytes(i); got != nil {
					t.Errorf("Bytes(%d) = %v, want nil", i, got)
				}
				if got := args.Raw(i); got != nil {
					t.Errorf("Raw(%d) = %q, want nil", i, got)
				}
			}
		})
	}
}

func TestArgsAccessors(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `51-["upload","a.txt",`+ph0+`,7,null,{"k":[1]}]`, []byte{9, 8})
	tests := []struct {
		index  int
		binary bool
		bytes  []byte
		raw    string
	}{
		{0, false, nil, `"a.txt"`},
		{1, true, []byte{9, 8}, ``},
		{2, false, nil, `7`},
		{3, false, nil, `null`},
		{4, false, nil, `{"k":[1]}`},
	}
	if args.Len() != len(tests) {
		t.Fatalf("Len() = %d, want %d", args.Len(), len(tests))
	}
	for _, tc := range tests {
		if got := args.IsBinary(tc.index); got != tc.binary {
			t.Errorf("IsBinary(%d) = %t, want %t", tc.index, got, tc.binary)
		}
		if got := args.Bytes(tc.index); !bytes.Equal(got, tc.bytes) || (tc.bytes == nil) != (got == nil) {
			t.Errorf("Bytes(%d) = %v, want %v", tc.index, got, tc.bytes)
		}
		got := args.Raw(tc.index)
		if string(got) != tc.raw || (tc.binary && got != nil) {
			t.Errorf("Raw(%d) = %q, want %q", tc.index, got, tc.raw)
		}
	}
}

func TestArgsWithoutAttachments(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `2["foo","a",{"x":1}]`)
	for i := range args.Len() {
		if args.IsBinary(i) || args.Bytes(i) != nil || args.Raw(i) == nil {
			t.Errorf("argument %d: IsBinary=%t Bytes=%v Raw=%q", i, args.IsBinary(i), args.Bytes(i), args.Raw(i))
		}
	}
	if got := string(args.Raw(1)); got != `{"x":1}` {
		t.Errorf("Raw(1) = %q", got)
	}
}

func TestArgsPlaceholderNumbersSelectAttachments(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `53-["ev",`+ph2+`,`+ph0+`,`+ph1+`]`, []byte("a"), []byte("b"), []byte("c"))
	for i, want := range []string{"c", "a", "b"} {
		if !args.IsBinary(i) || string(args.Bytes(i)) != want {
			t.Errorf("argument %d = %q (binary %t), want %q", i, args.Bytes(i), args.IsBinary(i), want)
		}
	}
}

func TestArgsPlaceholdersAreIgnoredWithoutAttachments(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `2["ev",`+ph0+`]`)
	if args.IsBinary(0) || args.Bytes(0) != nil || string(args.Raw(0)) != ph0 {
		t.Fatalf("IsBinary=%t Bytes=%v Raw=%q", args.IsBinary(0), args.Bytes(0), args.Raw(0))
	}
}

func TestArgsPlaceholderFlaggedFalseIsPlainJSON(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `51-["ev",{"_placeholder":false,"num":0},`+ph0+`]`, []byte{1})
	if args.IsBinary(0) || !args.IsBinary(1) {
		t.Fatalf("IsBinary = %t, %t; want false, true", args.IsBinary(0), args.IsBinary(1))
	}
	var plain struct {
		Placeholder bool `json:"_placeholder"`
		Num         int  `json:"num"`
	}
	if err := args.Bind(&plain); err != nil || plain.Placeholder || plain.Num != 0 {
		t.Fatalf("Bind = %+v, %v", plain, err)
	}
}

func TestArgsEmptyAndNilAttachmentsAreStillBinary(t *testing.T) {
	t.Parallel()
	for name, blob := range map[string][]byte{"empty": {}, "nil": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, args := mustEvent(t, `51-["ev",`+ph0+`]`, blob)
			if !args.IsBinary(0) || args.Raw(0) != nil || len(args.Bytes(0)) != 0 {
				t.Fatalf("IsBinary=%t Raw=%q Bytes=%v", args.IsBinary(0), args.Raw(0), args.Bytes(0))
			}
			got := []byte{1}
			if err := args.Bind(&got); err != nil || len(got) != 0 {
				t.Fatalf("Bind = %v, %v", got, err)
			}
		})
	}
}

func TestArgsNullIsNotBinary(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `51-["ev",null,`+ph0+`]`, []byte{1})
	if args.IsBinary(0) || string(args.Raw(0)) != "null" || !args.IsBinary(1) {
		t.Fatalf("IsBinary = %t, %t; Raw(0) = %q", args.IsBinary(0), args.IsBinary(1), args.Raw(0))
	}
	got := any("unset")
	if err := args.Bind(&got); err != nil || got != nil {
		t.Fatalf("Bind = %v, %v", got, err)
	}
}
