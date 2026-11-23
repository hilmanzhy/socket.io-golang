//go:build interop

package interop

import (
	"cmp"
	"context"
	"crypto/rand"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	socketio "github.com/hilmanzhy/socket.io-golang"
	"github.com/hilmanzhy/socket.io-golang/redisadapter"
)

// newClusterServer returns a server that its clients drive: the events emit,
// join, leave and kick run the broadcast of the same name on the sockets
// their first argument selects.
func newClusterServer(t *testing.T, adapter socketio.Adapter) *socketio.Server {
	server := socketio.New(socketio.Options{Adapter: adapter})
	server.OnConnect(func(socket *socketio.Socket) {
		socket.On("enter", func(event *socketio.Event) {
			var room string
			if err := event.Bind(&room); err != nil {
				t.Errorf("enter: %v", err)
				return
			}
			event.Ack(socket.Join(room) == nil)
		})
		socket.On("emit", func(event *socketio.Event) {
			var rooms, except []string
			var name string
			if err := event.Bind(&rooms, &except, &name); err != nil {
				t.Errorf("emit: %v", err)
				return
			}
			if err := server.To(rooms...).Except(except...).Emit(name, values(event.Args)[3:]...); err != nil {
				t.Errorf("emit %s: %v", name, err)
			}
		})
		socket.On("join", func(event *socketio.Event) {
			var targets, rooms []string
			if err := event.Bind(&targets, &rooms); err != nil {
				t.Errorf("join: %v", err)
				return
			}
			event.Ack(server.To(targets...).Join(rooms...) == nil)
		})
		socket.On("leave", func(event *socketio.Event) {
			var targets, rooms []string
			if err := event.Bind(&targets, &rooms); err != nil {
				t.Errorf("leave: %v", err)
				return
			}
			event.Ack(server.To(targets...).Leave(rooms...) == nil)
		})
		socket.On("kick", func(event *socketio.Event) {
			var targets []string
			var closeConnection bool
			if err := event.Bind(&targets, &closeConnection); err != nil {
				t.Errorf("kick: %v", err)
				return
			}
			event.Ack(server.To(targets...).Disconnect(closeConnection) == nil)
		})
	})
	return server
}

// TestNodeCluster puts this server and a Node.js server with
// @socket.io/redis-adapter behind one Redis, and checks that each applies the
// broadcasts, joins, leaves and disconnects of the other. It needs the Redis
// server at REDIS_ADDR, 127.0.0.1:6379 by default.
//
// The two servers share a random key. With INTEROP_DEFAULT_KEY set they both
// keep their default key instead, which is only safe on a Redis server that
// no other application uses.
func TestNodeCluster(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	if _, err := os.Stat("node_modules/@socket.io/redis-adapter"); err != nil {
		t.Skip("run npm ci in test/interop first")
	}
	address := cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379")
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	reachable, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := client.Ping(reachable).Err(); err != nil {
		t.Skipf("no Redis server at %s: %v", address, err)
	}

	key := "interop-" + rand.Text()
	if os.Getenv("INTEROP_DEFAULT_KEY") != "" {
		key = ""
	}
	server := newClusterServer(t, redisadapter.New(client, redisadapter.Options{
		Key:     key,
		OnError: func(err error) { t.Errorf("adapter: %v", err) },
	}))
	endpoint := httptest.NewServer(server)
	defer endpoint.Close()
	defer server.Close()

	scenario, stop := context.WithTimeout(t.Context(), time.Minute)
	defer stop()
	output, err := exec.CommandContext(scenario, node, "cluster.mjs", endpoint.URL, address, key).CombinedOutput()
	t.Logf("\n%s", output)
	if err != nil {
		t.Fatalf("scenario failed: %v", err)
	}
}
