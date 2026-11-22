package socketio_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	socketio "github.com/nuriontech/socket.io-golang"
	"github.com/nuriontech/socket.io-golang/parser"
)

func TestEventDelivery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, namespace string }{{"main", "/"}, {"custom", "/chat"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			events := make(chan *socketio.Event, 4)
			p := watch(server.Of(tc.namespace), func(s *socketio.Socket) {
				s.On("greet", func(e *socketio.Event) { events <- e })
			})
			c := dial(t, ts)
			c.connect(tc.namespace, "")
			s := p.socket(t)
			c.send(pkt('2', tc.namespace, `["greet","bob",3]`))
			e := receive(t, events)
			var name string
			var count int
			if err := e.Bind(&name, &count); err != nil {
				t.Fatalf("Bind: %v", err)
			}
			if e.Name != "greet" || e.Socket != s || e.Args.Len() != 2 || name != "bob" || count != 3 || e.WantsAck() {
				t.Errorf("event = name %q socket %v args %d wantsAck %v bound (%q, %d)", e.Name, e.Socket == s, e.Args.Len(), e.WantsAck(), name, count)
			}
		})
	}
}

func TestEventBind(t *testing.T) {
	t.Parallel()
	type bound struct {
		text  string
		count int
		err   error
	}
	results := make(chan bound, 1)
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("bind", func(e *socketio.Event) {
			r := bound{text: "unset", count: -1}
			r.err = e.Bind(&r.text, &r.count)
			results <- r
		})
	})
	c, _ := f.join(t)
	for _, tc := range []struct {
		name    string
		frame   string
		want    bound
		wantErr bool
	}{
		{"both arguments", `42["bind","a",2]`, bound{"a", 2, nil}, false},
		{"missing last argument", `42["bind","a"]`, bound{"a", -1, nil}, false},
		{"no arguments", `42["bind"]`, bound{"unset", -1, nil}, false},
		{"extra arguments", `42["bind","a",2,true,null]`, bound{"a", 2, nil}, false},
		{"first has the wrong type", `42["bind",1,2]`, bound{"unset", -1, nil}, true},
		{"second has the wrong type", `42["bind","a","b"]`, bound{"a", -1, nil}, true},
	} {
		c.send(tc.frame)
		got := receive(t, results)
		if (got.err != nil) != tc.wantErr || got.text != tc.want.text || got.count != tc.want.count {
			t.Errorf("%s: bound (%q, %d, err %v), want (%q, %d, err %v)", tc.name, got.text, got.count, got.err, tc.want.text, tc.want.count, tc.wantErr)
		}
	}
}

func TestOnReplacesThePreviousHandler(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("who", func(e *socketio.Event) { e.Ack("first") })
		s.On("who", func(e *socketio.Event) { e.Ack("second") })
	})
	c, s := f.join(t)
	c.send(`421["who"]`)
	c.expect(`431["second"]`)
	s.On("who", func(e *socketio.Event) { e.Ack("third") })
	c.send(`422["who"]`)
	c.expect(`432["third"]`)
	c.barrier(s)
}

func TestEventWithoutHandlerIsIgnored(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	c.send(`42["nobody"]`)
	c.send(`4211["nobody","with","args"]`)
	c.ping("/")
	c.barrier(s)
}

func TestSocketMiddleware(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var seen []string
	note := func(entry string) {
		mu.Lock()
		seen = append(seen, entry)
		mu.Unlock()
	}
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.Use(func(e *socketio.Event) error {
			if e.Name == "ping" {
				return nil
			}
			note("first:" + e.Name)
			if e.Name == "blocked" {
				return errors.New("not allowed")
			}
			return nil
		})
		s.Use(func(e *socketio.Event) error {
			if e.Name != "ping" {
				note("second:" + e.Name)
			}
			return nil
		})
		s.On("blocked", func(e *socketio.Event) {
			note("handler:blocked")
			e.Ack()
		})
		s.On("allowed", func(e *socketio.Event) {
			note("handler:allowed")
			e.Ack()
		})
	})
	c, _ := f.join(t)
	c.send(`421["blocked"]`)
	c.send(`422["allowed"]`)
	c.send(`42["unhandled"]`)
	c.expect(`432[]`)
	c.ping("/")
	want := []string{
		"first:blocked",
		"first:allowed", "second:allowed", "handler:allowed",
		"first:unhandled", "second:unhandled",
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(seen, want) {
		t.Errorf("middleware and handler calls = %v, want %v", seen, want)
	}
}

func TestBinaryEventReceived(t *testing.T) {
	t.Parallel()
	type upload struct {
		File []byte `json:"file"`
		Name string `json:"name"`
	}
	cases := []struct {
		name      string
		namespace string
		frame     string
		blobs     [][]byte
		binary    []bool
		check     func(t *testing.T, e *socketio.Event)
	}{
		{
			name: "single attachment", namespace: "/",
			frame: `451-["upload",{"_placeholder":true,"num":0}]`,
			blobs: [][]byte{{1, 2, 3}}, binary: []bool{true},
			check: func(t *testing.T, e *socketio.Event) {
				var data []byte
				if err := e.Bind(&data); err != nil || !slices.Equal(data, []byte{1, 2, 3}) {
					t.Errorf("Bind([]byte) = %v, %v", data, err)
				}
			},
		},
		{
			name: "text and two attachments", namespace: "/",
			frame: `452-["upload","text",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1}]`,
			blobs: [][]byte{{1}, {2, 3}}, binary: []bool{false, true, true},
			check: func(t *testing.T, e *socketio.Event) {
				var text string
				var first, second []byte
				if err := e.Bind(&text, &first, &second); err != nil || text != "text" || !slices.Equal(first, []byte{1}) || !slices.Equal(second, []byte{2, 3}) {
					t.Errorf("Bind = %q %v %v, %v", text, first, second, err)
				}
			},
		},
		{
			name: "placeholders in reverse order", namespace: "/",
			frame: `452-["upload",{"_placeholder":true,"num":1},{"_placeholder":true,"num":0}]`,
			blobs: [][]byte{{1}, {2}}, binary: []bool{true, true},
			check: func(t *testing.T, e *socketio.Event) {
				if !slices.Equal(e.Args.Bytes(0), []byte{2}) || !slices.Equal(e.Args.Bytes(1), []byte{1}) {
					t.Errorf("Bytes = %v %v, want [2] [1]", e.Args.Bytes(0), e.Args.Bytes(1))
				}
			},
		},
		{
			name: "attachment nested in an object", namespace: "/",
			frame: `451-["upload",{"file":{"_placeholder":true,"num":0},"name":"a.txt"}]`,
			blobs: [][]byte{{9, 8}}, binary: []bool{false},
			check: func(t *testing.T, e *socketio.Event) {
				var got upload
				if err := e.Bind(&got); err != nil || !slices.Equal(got.File, []byte{9, 8}) || got.Name != "a.txt" {
					t.Errorf("Bind(struct) = %+v, %v", got, err)
				}
			},
		},
		{
			name: "custom namespace", namespace: "/chat",
			frame: `451-/chat,["upload",{"_placeholder":true,"num":0}]`,
			blobs: [][]byte{{7}}, binary: []bool{true},
			check: func(t *testing.T, e *socketio.Event) {
				if e.Socket.Namespace().Name() != "/chat" || !slices.Equal(e.Args.Bytes(0), []byte{7}) {
					t.Errorf("event in %s with bytes %v", e.Socket.Namespace().Name(), e.Args.Bytes(0))
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			events := make(chan *socketio.Event, 1)
			p := watch(server.Of(tc.namespace), func(s *socketio.Socket) {
				s.On("upload", func(e *socketio.Event) { events <- e })
			})
			c := dial(t, ts)
			c.connect(tc.namespace, "")
			p.socket(t)
			c.send(tc.frame)
			for _, blob := range tc.blobs {
				c.sendBinary(blob)
			}
			e := receive(t, events)
			if e.Name != "upload" || e.Args.Len() != len(tc.binary) {
				t.Fatalf("event %q with %d arguments, want upload with %d", e.Name, e.Args.Len(), len(tc.binary))
			}
			for i, want := range tc.binary {
				if e.Args.IsBinary(i) != want {
					t.Errorf("IsBinary(%d) = %v, want %v", i, e.Args.IsBinary(i), want)
				}
			}
			tc.check(t, e)
		})
	}
}

func TestEmitEncoding(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		event  string
		args   []any
		text   string
		binary [][]byte
	}{
		{"no arguments", "hello", nil, `42["hello"]`, nil},
		{"scalars", "hello", []any{"a", 1, true, nil, 1.5}, `42["hello","a",1,true,null,1.5]`, nil},
		{"structured values", "hello", []any{map[string]int{"a": 1}, []int{1, 2}}, `42["hello",{"a":1},[1,2]]`, nil},
		{"one attachment", "file", []any{[]byte{1, 2, 3}}, `451-["file",{"_placeholder":true,"num":0}]`, [][]byte{{1, 2, 3}}},
		{"text between attachments", "mix", []any{"a", []byte{1}, 5, []byte{2, 3}}, `452-["mix","a",{"_placeholder":true,"num":0},5,{"_placeholder":true,"num":1}]`, [][]byte{{1}, {2, 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			c, s := f.join(t)
			if err := s.Emit(tc.event, tc.args...); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			c.expect(tc.text)
			for _, blob := range tc.binary {
				c.expectBinary(blob)
			}
			c.barrier(s)
		})
	}
}

func TestEmitInCustomNamespace(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	p := watch(server.Of("/chat"))
	c := dial(t, ts)
	c.connect("/chat", "")
	s := p.socket(t)
	s.Emit("hello", "x")
	c.expect(`42/chat,["hello","x"]`)
	s.Emit("file", []byte{4, 5})
	c.expect(`451-/chat,["file",{"_placeholder":true,"num":0}]`)
	c.expectBinary([]byte{4, 5})
	c.barrier(s)
}

func TestEmitRejectsReservedNames(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, name := range []string{"connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener"} {
		attempts := map[string]error{
			"Socket.Emit":             s.Emit(name, 1),
			"Namespace.Emit":          f.server.Emit(name),
			"BroadcastOperator.Emit":  s.Broadcast().Emit(name),
			"BroadcastOperator.To":    s.To("room").Emit(name),
			"Socket.EmitWithAck call": func() error { _, err := s.EmitWithAck(ctx, name); return err }(),
		}
		for call, err := range attempts {
			if !errors.Is(err, parser.ErrReservedEvent) {
				t.Errorf("%s(%q) = %v, want ErrReservedEvent", call, name, err)
			}
		}
	}
	c.barrier(s)
}

func TestEmitRejectsUnencodableArguments(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	if err := s.Emit("x", make(chan int)); err == nil {
		t.Error("Emit with a channel argument returned nil")
	}
	if err := f.server.Emit("x", func() {}); err == nil {
		t.Error("Namespace.Emit with a func argument returned nil")
	}
	c.barrier(s)
}
