package redisadapter_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	socketio "github.com/nuriontech/socket.io-golang"
	"github.com/nuriontech/socket.io-golang/engineio"
	"github.com/nuriontech/socket.io-golang/parser"
	"github.com/nuriontech/socket.io-golang/redisadapter"
)

const patience = 5 * time.Second

// redisClient returns a client for the server at REDIS_ADDR, or skips the
// test when there is none.
func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379")
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no Redis server at %s: %v", addr, err)
	}
	return client
}

// offlineClient returns a client whose connection attempts fail after wait
// has been closed, or when their context ends.
func offlineClient(t *testing.T, wait <-chan struct{}) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		MaxRetries: -1,
		Dialer: func(ctx context.Context, _, _ string) (net.Conn, error) {
			select {
			case <-wait:
				return nil, errors.New("redis is offline")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	t.Cleanup(func() { client.Close() })
	return client
}

func newAdapter(t *testing.T, client redis.UniversalClient, opts redisadapter.Options) *redisadapter.Adapter {
	t.Helper()
	adapter := redisadapter.New(client, opts)
	t.Cleanup(func() { adapter.Close() })
	return adapter
}

func subscribe(adapter *redisadapter.Adapter) <-chan socketio.Operation {
	received := make(chan socketio.Operation, 64)
	adapter.Subscribe(func(op socketio.Operation) { received <- op })
	return received
}

func newEmit(t *testing.T, event string) socketio.Operation {
	t.Helper()
	packet, err := parser.NewEvent("/", event, []any{"payload", []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	return socketio.Operation{Kind: socketio.OpEmit, Namespace: "/", Rooms: []string{"blue"}, Except: []string{"red"}, Packet: packet}
}

// publishUntil publishes op until one arrives on received: a subscription is
// established in the background, so the first publications may be missed.
func publishUntil(t *testing.T, adapter *redisadapter.Adapter, op socketio.Operation, received <-chan socketio.Operation) socketio.Operation {
	t.Helper()
	deadline := time.After(patience)
	for {
		if err := adapter.Publish(op); err != nil {
			t.Fatalf("publish: %v", err)
		}
		select {
		case got := <-received:
			return got
		case <-deadline:
			t.Fatal("the operation never arrived")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestPublishReachesTheOtherNodesOnly(t *testing.T) {
	client := redisClient(t)
	channel := "test-" + rand.Text()
	sender := newAdapter(t, client, redisadapter.Options{Channel: channel})
	receiver := newAdapter(t, client, redisadapter.Options{Channel: channel})
	stranger := newAdapter(t, client, redisadapter.Options{Channel: channel + "-other"})
	own, others, strangers := subscribe(sender), subscribe(receiver), subscribe(stranger)

	sent := newEmit(t, "news")
	got := publishUntil(t, sender, sent, others)

	if got.Kind != sent.Kind || got.Namespace != sent.Namespace {
		t.Errorf("got %+v, want %+v", got, sent)
	}
	if strings.Join(got.Rooms, ",") != "blue" || strings.Join(got.Except, ",") != "red" {
		t.Errorf("rooms %v except %v, want [blue] and [red]", got.Rooms, got.Except)
	}
	if got.Packet == nil || string(got.Packet.Encode()) != string(sent.Packet.Encode()) {
		t.Errorf("packet %+v, want %+v", got.Packet, sent.Packet)
	}
	if len(got.Packet.Attachments) != 1 || string(got.Packet.Attachments[0]) != "\x01\x02\x03" {
		t.Errorf("attachments %v, want [[1 2 3]]", got.Packet.Attachments)
	}
	select {
	case op := <-own:
		t.Errorf("the sender received its own operation: %+v", op)
	case op := <-strangers:
		t.Errorf("another channel received the operation: %+v", op)
	default:
	}
}

func TestUnreadableMessageIsReportedAndSkipped(t *testing.T) {
	client := redisClient(t)
	channel := "test-" + rand.Text()
	failures := make(chan error, 64)
	sender := newAdapter(t, client, redisadapter.Options{Channel: channel})
	receiver := newAdapter(t, client, redisadapter.Options{
		Channel: channel,
		OnError: func(err error) { failures <- err },
	})
	received := subscribe(receiver)

	publishUntil(t, sender, newEmit(t, "first"), received)
	if err := client.Publish(t.Context(), channel, "not json").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failures:
	case <-time.After(patience):
		t.Fatal("OnError was not called for an unreadable message")
	}
	publishUntil(t, sender, newEmit(t, "second"), received)
}

func TestClusterBroadcastOverRedis(t *testing.T) {
	client := redisClient(t)
	channel := "test-" + rand.Text()
	newServer := func() *socketio.Server {
		server := socketio.New(socketio.Options{
			Engine:  engineio.Options{PingInterval: time.Hour},
			Adapter: redisadapter.New(client, redisadapter.Options{Channel: channel}),
		})
		t.Cleanup(func() { server.Close() })
		return server
	}
	origin, remote := newServer(), newServer()
	endpoint := httptest.NewServer(remote)
	t.Cleanup(endpoint.Close)

	url := "ws" + strings.TrimPrefix(endpoint.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
	conn, _, err := websocket.DefaultDialer.DialContext(t.Context(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetReadDeadline(time.Now().Add(patience))
	frames := make(chan string, 64)
	go func() {
		defer close(frames)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			frames <- string(data)
		}
	}()
	expect := func(prefix string) {
		t.Helper()
		frame, open := <-frames
		if !open || !strings.HasPrefix(frame, prefix) {
			t.Fatalf("got frame %q, want prefix %q", frame, prefix)
		}
	}
	expect("0{")
	if err := conn.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		t.Fatal(err)
	}
	expect("40{")

	deadline := time.After(patience)
	for {
		if err := origin.Emit("news", "hello"); err != nil {
			t.Fatalf("emit: %v", err)
		}
		select {
		case frame := <-frames:
			if frame != `42["news","hello"]` {
				t.Fatalf("got frame %q", frame)
			}
			return
		case <-deadline:
			t.Fatal("the broadcast never reached the client of the other server")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestPublishFailsWhenTheBacklogIsFull(t *testing.T) {
	client := offlineClient(t, nil)
	adapter := newAdapter(t, client, redisadapter.Options{Backlog: 2, PublishTimeout: time.Minute})

	var err error
	for range 4 {
		if err = adapter.Publish(newEmit(t, "news")); err != nil {
			break
		}
	}
	if !errors.Is(err, redisadapter.ErrBacklogFull) {
		t.Fatalf("got %v, want ErrBacklogFull", err)
	}
}

func TestPublishFailureIsReported(t *testing.T) {
	offline := make(chan struct{})
	close(offline)
	failures := make(chan error, 1)
	adapter := newAdapter(t, offlineClient(t, offline), redisadapter.Options{
		OnError: func(err error) { failures <- err },
	})

	if err := adapter.Publish(newEmit(t, "news")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("OnError received a nil error")
		}
	case <-time.After(patience):
		t.Fatal("OnError was not called for a failed PUBLISH")
	}
}

func TestPublishAfterCloseFails(t *testing.T) {
	adapter := redisadapter.New(offlineClient(t, nil), redisadapter.Options{})
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Publish(newEmit(t, "news")); !errors.Is(err, redisadapter.ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
