//go:build interop

// Package interop drives the server with the official JavaScript client, over
// each transport. Run it with "make interop"; it needs Node.js and the
// dependencies of package.json.
package interop

import (
	"context"
	"maps"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

// reasons counts the disconnect reasons the server reported.
type reasons struct {
	mu   sync.Mutex
	seen map[socketio.Reason]int
}

func (r *reasons) record(reason socketio.Reason) {
	r.mu.Lock()
	r.seen[reason]++
	r.mu.Unlock()
}

func (r *reasons) has(reason socketio.Reason) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[reason] > 0
}

func (r *reasons) counts() map[socketio.Reason]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.seen)
}

func values(args socketio.Args) []any {
	list := make([]any, args.Len())
	for i := range list {
		if args.IsBinary(i) {
			list[i] = args.Bytes(i)
		} else {
			list[i] = args.Raw(i)
		}
	}
	return list
}

func newServer(t *testing.T, seen *reasons) *socketio.Server {
	server := socketio.New(socketio.Options{})
	server.OnConnect(func(socket *socketio.Socket) {
		socket.OnDisconnect(seen.record)
		socket.On("echo", func(event *socketio.Event) {
			if err := event.Ack(values(event.Args)...); err != nil {
				t.Errorf("echo: %v", err)
			}
		})
		socket.On("ask", func(event *socketio.Event) {
			var subject, answer string
			if err := event.Bind(&subject); err != nil {
				t.Errorf("ask: %v", err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reply, err := socket.EmitWithAck(ctx, "question", subject)
			if err == nil {
				err = reply.Bind(&answer)
			}
			if err != nil {
				t.Errorf("question: %v", err)
				return
			}
			socket.Emit("answered", answer)
		})
		socket.On("join", func(event *socketio.Event) {
			var room string
			if err := event.Bind(&room); err != nil {
				t.Errorf("join: %v", err)
				return
			}
			socket.Join(room)
			event.Ack(socket.Rooms())
		})
		socket.On("shout", func(event *socketio.Event) {
			var room, text string
			if err := event.Bind(&room, &text); err != nil {
				t.Errorf("shout: %v", err)
				return
			}
			server.To(room).Emit("shouted", text)
		})
		socket.On("whisper", func(event *socketio.Event) {
			var room, text string
			if err := event.Bind(&room, &text); err != nil {
				t.Errorf("whisper: %v", err)
				return
			}
			socket.To(room).Emit("whispered", text)
		})
		socket.On("blast", func(*socketio.Event) {
			server.Emit("blob", []byte{1, 2, 3}, "blob")
		})
		socket.On("close", func(*socketio.Event) {
			socket.Disconnect(true)
		})
		socket.Emit("hello", socket.ID())
	})

	admin := server.Of("/admin")
	admin.Use(func(socket *socketio.Socket) error {
		var auth struct {
			Token string `json:"token"`
		}
		if err := socket.Auth(&auth); err != nil || auth.Token != "secret" {
			return &socketio.ConnectError{Message: "unauthorized", Data: map[string]int{"code": 401}}
		}
		return nil
	})
	admin.OnConnect(func(socket *socketio.Socket) {
		socket.OnDisconnect(seen.record)
		socket.On("kick", func(*socketio.Event) {
			socket.Disconnect(false)
		})
		socket.Emit("welcome", "admin")
	})
	return server
}

func TestJavaScriptClient(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	if _, err := os.Stat("node_modules/socket.io-client"); err != nil {
		t.Skip("run npm ci in test/interop first")
	}

	for _, transports := range []string{"polling", "websocket", "polling,websocket"} {
		t.Run(transports, func(t *testing.T) {
			seen := &reasons{seen: make(map[socketio.Reason]int)}
			server := newServer(t, seen)
			endpoint := httptest.NewServer(server)
			defer endpoint.Close()
			defer server.Close()

			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, node, "client.mjs", endpoint.URL, transports).CombinedOutput()
			t.Logf("\n%s", output)
			if err != nil {
				t.Fatalf("client failed: %v", err)
			}

			expected := []socketio.Reason{
				socketio.ReasonClientDisconnect,
				socketio.ReasonServerDisconnect,
				socketio.ReasonTransportClose,
			}
			deadline := time.Now().Add(5 * time.Second)
			for _, reason := range expected {
				for !seen.has(reason) {
					if time.Now().After(deadline) {
						t.Fatalf("no socket disconnected with %q; seen %v", reason, seen.counts())
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
}
