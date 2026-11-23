package parser_test

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func TestArgsPlaceholderIsTheObjectWhoseFlagIsTrue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		argument string
		blob     int
	}{
		{"the parser's own spelling", ph1, 1},
		{"members in another order", `{"num":1,"_placeholder":true}`, 1},
		{"spaces between the members", "{ \"_placeholder\" : true ,\n\t\"num\" : 1 }", 1},
		{"the last flag is true", `{"_placeholder":false,"_placeholder":true,"num":1}`, 1},
		{"the last num counts", `{"num":7,"_placeholder":true,"num":"x","num":1}`, 1},
		{"minus zero", `{"_placeholder":true,"num":-0}`, 0},
		{"other members", `{"type":"Buffer","_placeholder":true,"data":[1,{"x":2}],"num":1,"z":null}`, 1},
		{"a placeholder inside it is not looked into", `{"_placeholder":true,"num":1,"x":{"_placeholder":true,"num":99}}`, 1},
		{"a placeholder of the same attachment inside it", `{"_placeholder":true,"x":[` + ph1 + `],"num":1}`, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			attachments := blobs(2)
			for _, wire := range []string{`52-["ev",` + tc.argument + `]`, `52-[ "ev" , ` + tc.argument + ` ]`} {
				_, args := mustEvent(t, wire, attachments...)
				if !args.IsBinary(0) || !bytes.Equal(args.Bytes(0), attachments[tc.blob]) {
					t.Errorf("%s: IsBinary=%t Bytes=%v Raw=%s, want attachment %d", wire, args.IsBinary(0), args.Bytes(0), args.Raw(0), tc.blob)
				}
			}
			_, args := mustEvent(t, `52-["ev",{"a":[`+tc.argument+`]}]`, attachments...)
			want := `{"a":["` + base64.StdEncoding.EncodeToString(attachments[tc.blob]) + `"]}`
			if args.IsBinary(0) || string(args.Raw(0)) != want {
				t.Errorf("nested: IsBinary=%t Raw=%s, want %s", args.IsBinary(0), args.Raw(0), want)
			}
		})
	}
}

func TestArgsObjectWithoutATrueFlagIsPlainJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		argument string
	}{
		{"the last flag is false", `{"_placeholder":true,"_placeholder":false,"num":0}`},
		{"flag that is a string", `{"_placeholder":"true","num":0}`},
		{"flag that is a number", `{"_placeholder":1,"num":0}`},
		{"flag that is null", `{"_placeholder":null,"num":0}`},
		{"flag that is an array", `{"_placeholder":[true],"num":0}`},
		{"flag that is an object", `{"_placeholder":{"_placeholder":false},"num":0}`},
		{"the name as a value", `{"k":"_placeholder","v":true,"num":0}`},
		{"the names in an array", `["_placeholder",true,"num",0]`},
		{"the name in a string", `"{\"_placeholder\":true,\"num\":0} \"_placeholder\""`},
		{"a name written with an escape", `{"\u005fplaceholder":true,"num":0,"k":"_placeholder"}`},
		{"a name in another case", `{"_Placeholder":true,"NUM":0,"k":"_placeholder"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, wire := range []string{`51-["ev",` + tc.argument + `]`, `51-["ev",{"a":[` + tc.argument + `]}]`} {
				_, args := mustEvent(t, wire, []byte{1})
				want := wire[len(`51-["ev",`) : len(wire)-1]
				if args.IsBinary(0) || string(args.Raw(0)) != want {
					t.Errorf("%s: IsBinary=%t Raw=%s, want the argument as written", wire, args.IsBinary(0), args.Raw(0))
				}
			}
		})
	}
}

func TestArgsNestedPlaceholderInsideAPlaceholderTakesNoAttachment(t *testing.T) {
	t.Parallel()
	_, args := mustEvent(t, `52-["ev",{"a":{"_placeholder":true,"num":0,"x":`+ph1+`},"b":`+ph1+`}]`, blobs(2)...)
	if got, want := string(args.Raw(0)), `{"a":"AAE=","b":"AQI="}`; got != want {
		t.Fatalf("Raw(0) = %s, want %s", got, want)
	}
}
