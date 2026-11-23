package parser_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

func TestArgsBindTypedTargets(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `2["ev","text",42,1.5,true,{"name":"n","tags":["a","b"]},[1,2,3],null,{"x":1},"raw"]`)
	var (
		text   string
		whole  int
		ratio  float64
		flag   bool
		object struct {
			Name string   `json:"name"`
			Tags []string `json:"tags"`
		}
		list    []int
		nothing = new(int)
		generic any
		raw     json.RawMessage
	)
	if err := args.Bind(&text, &whole, &ratio, &flag, &object, &list, &nothing, &generic, &raw); err != nil {
		t.Fatal(err)
	}
	if text != "text" || whole != 42 || ratio != 1.5 || !flag {
		t.Errorf("scalars = %q %d %v %t", text, whole, ratio, flag)
	}
	if object.Name != "n" || !reflect.DeepEqual(object.Tags, []string{"a", "b"}) {
		t.Errorf("object = %+v", object)
	}
	if !reflect.DeepEqual(list, []int{1, 2, 3}) || nothing != nil {
		t.Errorf("list = %v, nothing = %v", list, nothing)
	}
	if want := map[string]any{"x": float64(1)}; !reflect.DeepEqual(generic, want) {
		t.Errorf("generic = %#v, want %#v", generic, want)
	}
	if string(raw) != `"raw"` {
		t.Errorf("raw = %q", raw)
	}
}

func TestArgsBindBinaryTargets(t *testing.T) {
	t.Parallel()
	want := []byte{1, 2, 3}
	_, args := mustEvent(t, `51-["ev",`+ph0+`]`, want)
	t.Run("byte slice", func(t *testing.T) {
		t.Parallel()
		var got []byte
		if err := args.Bind(&got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Bind = %v, %v", got, err)
		}
	})
	t.Run("any", func(t *testing.T) {
		t.Parallel()
		var got any
		if err := args.Bind(&got); err != nil {
			t.Fatal(err)
		}
		blob, ok := got.([]byte)
		if !ok || !bytes.Equal(blob, want) {
			t.Fatalf("Bind = %#v, want %v", got, want)
		}
	})
}

func TestArgsBindBinaryToOtherTypeFails(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `51-["ev",`+ph0+`]`, []byte{1, 2, 3})
	targets := map[string]any{
		"string":      new(string),
		"int":         new(int),
		"map":         new(map[string]any),
		"raw message": new(json.RawMessage),
		"struct":      new(struct{ A int }),
		"strings":     new([]string),
		"nil":         nil,
		"non pointer": []byte{},
	}
	for name, target := range targets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := args.Bind(target); err == nil {
				t.Fatal("Bind succeeded, want an error")
			}
			if v := reflect.ValueOf(target); v.Kind() == reflect.Pointer && !v.Elem().IsZero() {
				t.Fatalf("target modified: %v", v.Elem())
			}
		})
	}
}

func TestArgsBindMixedArguments(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `52-["ev","a",`+ph1+`,3,`+ph0+`]`, []byte{10}, []byte{20})
	var (
		text   string
		first  []byte
		number int
		second any
	)
	if err := args.Bind(&text, &first, &number, &second); err != nil {
		t.Fatal(err)
	}
	if text != "a" || !bytes.Equal(first, []byte{20}) || number != 3 || !bytes.Equal(second.([]byte), []byte{10}) {
		t.Fatalf("bound %q %v %d %v", text, first, number, second)
	}
}

func TestArgsBindTypeMismatch(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `2["ev","text",1]`)
	var n int
	err := args.Bind(&n)
	var mismatch *json.UnmarshalTypeError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want *json.UnmarshalTypeError", err)
	}
	var text string
	if err := args.Bind(&text, &text); err == nil {
		t.Fatal("binding a number into a string succeeded")
	}
}

func TestArgsBindFewerArgumentsThanTargets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wire string
		atts [][]byte
		want [3]string
	}{
		{"none", `2["ev"]`, nil, [3]string{"s0", "s1", "s2"}},
		{"one", `2["ev","only"]`, nil, [3]string{"only", "s1", "s2"}},
		{"two", `2["ev","a","b"]`, nil, [3]string{"a", "b", "s2"}},
		{"binary then none", `51-["ev",` + ph0 + `]`, [][]byte{[]byte("blob")}, [3]string{"blob", "s1", "s2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, args := mustEvent(t, tc.wire, tc.atts...)
			got := [3]string{"s0", "s1", "s2"}
			var first any = got[0]
			if args.IsBinary(0) {
				if err := args.Bind(&first, &got[1], &got[2]); err != nil {
					t.Fatal(err)
				}
				got[0] = string(first.([]byte))
			} else if err := args.Bind(&got[0], &got[1], &got[2]); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("targets = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestArgsBindMoreArgumentsThanTargets(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `2["ev",1,2,3]`)
	var first int
	if err := args.Bind(&first); err != nil || first != 1 {
		t.Fatalf("Bind = %d, %v", first, err)
	}
	if err := args.Bind(); err != nil {
		t.Fatalf("Bind() = %v", err)
	}
	var empty parser.Args
	untouched := 5
	if err := empty.Bind(&untouched); err != nil || untouched != 5 {
		t.Fatalf("zero Args Bind = %d, %v", untouched, err)
	}
}

func TestArgsBindInvalidTargetsDoNotPanic(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `2["ev","text"]`)
	for name, target := range map[string]any{"nil": nil, "value": "text", "nil pointer": (*string)(nil), "int": 3} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var invalid *json.InvalidUnmarshalError
			if err := args.Bind(target); !errors.As(err, &invalid) {
				t.Fatalf("error = %v, want *json.InvalidUnmarshalError", err)
			}
		})
	}
}

func TestArgsBindNestedBinary(t *testing.T) {
	t.Parallel()
	t.Run("object field", func(t *testing.T) {
		t.Parallel()
		type upload struct {
			Name string `json:"name"`
			Data []byte `json:"data"`
			Size int64  `json:"size"`
		}
		_, args := mustEvent(t, `51-["upload",{"name":"a.png","data":`+ph0+`,"size":9007199254740993}]`, []byte{1, 2, 3})
		if args.IsBinary(0) {
			t.Fatal("an object holding a placeholder is not a binary argument")
		}
		var got upload
		if err := args.Bind(&got); err != nil {
			t.Fatal(err)
		}
		if got.Name != "a.png" || !bytes.Equal(got.Data, []byte{1, 2, 3}) || got.Size != 9007199254740993 {
			t.Fatalf("bound %+v", got)
		}
	})
	t.Run("array of binaries", func(t *testing.T) {
		t.Parallel()
		_, args := mustEvent(t, `52-["ev",[`+ph0+`,`+ph1+`]]`, []byte{1}, []byte{2, 3})
		var got [][]byte
		if err := args.Bind(&got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, [][]byte{{1}, {2, 3}}) {
			t.Fatalf("bound %v", got)
		}
	})
	t.Run("deeply nested", func(t *testing.T) {
		t.Parallel()
		type tree struct {
			A struct {
				B []struct {
					C []byte `json:"c"`
				} `json:"b"`
			} `json:"a"`
		}
		_, args := mustEvent(t, `51-["ev",{"a":{"b":[{"c":`+ph0+`}]}}]`, []byte("deep"))
		var got tree
		if err := args.Bind(&got); err != nil {
			t.Fatal(err)
		}
		if len(got.A.B) != 1 || string(got.A.B[0].C) != "deep" {
			t.Fatalf("bound %+v", got)
		}
	})
	t.Run("top level and nested together", func(t *testing.T) {
		t.Parallel()
		_, args := mustEvent(t, `52-["ev",`+ph1+`,{"data":`+ph0+`}]`, []byte("nested"), []byte("top"))
		var (
			top    []byte
			nested holder
		)
		if err := args.Bind(&top, &nested); err != nil {
			t.Fatal(err)
		}
		if string(top) != "top" || string(nested.Data) != "nested" {
			t.Fatalf("bound %q %+v", top, nested)
		}
	})
	t.Run("ack argument", func(t *testing.T) {
		t.Parallel()
		args := mustAck(t, `61-3[{"data":`+ph0+`}]`, []byte{7})
		var got holder
		if err := args.Bind(&got); err != nil || !bytes.Equal(got.Data, []byte{7}) {
			t.Fatalf("bound %+v, %v", got, err)
		}
	})
	t.Run("empty attachment", func(t *testing.T) {
		t.Parallel()
		_, args := mustEvent(t, `51-["ev",{"data":`+ph0+`}]`, []byte{})
		got := holder{Data: []byte{1}}
		if err := args.Bind(&got); err != nil || len(got.Data) != 0 {
			t.Fatalf("bound %+v, %v", got, err)
		}
	})
	t.Run("numbers keep their precision", func(t *testing.T) {
		t.Parallel()
		_, args := mustEvent(t, `51-["ev",{"id":18446744073709551615,"f":1.25e2,"data":`+ph0+`}]`, []byte{1})
		var got struct {
			ID   uint64  `json:"id"`
			F    float64 `json:"f"`
			Data []byte  `json:"data"`
		}
		if err := args.Bind(&got); err != nil || got.ID != math.MaxUint64 || got.F != 125 {
			t.Fatalf("bound %+v, %v", got, err)
		}
	})
}
