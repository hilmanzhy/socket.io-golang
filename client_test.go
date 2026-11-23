package socketio_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

const (
	patience = 3 * time.Second
	quiet    = 80 * time.Millisecond
)

type frame struct {
	binary bool
	data   []byte
}

type client struct {
	t      *testing.T
	ws     *websocket.Conn
	frames chan frame
	done   chan struct{}
	stop   chan struct{}
	pong   bool

	mu  sync.Mutex
	err error
	end sync.Once
}

type dialOption func(*dialConfig)

type dialConfig struct {
	header http.Header
	query  url.Values
	pong   bool
}

func withHeader(key, value string) dialOption {
	return func(c *dialConfig) { c.header.Set(key, value) }
}

func withQuery(key, value string) dialOption {
	return func(c *dialConfig) { c.query.Set(key, value) }
}

func withoutPong() dialOption {
	return func(c *dialConfig) { c.pong = false }
}

func newServer(t *testing.T, opts socketio.Options) (*socketio.Server, *httptest.Server) {
	t.Helper()
	if opts.Engine.PingInterval == 0 {
		opts.Engine.PingInterval = time.Hour
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	server := socketio.New(opts)
	ts := httptest.NewServer(server)
	t.Cleanup(func() {
		server.Close()
		ts.Close()
	})
	return server, ts
}

func dial(t *testing.T, ts *httptest.Server, options ...dialOption) *client {
	t.Helper()
	config := &dialConfig{header: http.Header{}, query: url.Values{"EIO": {"4"}, "transport": {"websocket"}}, pong: true}
	for _, option := range options {
		option(config)
	}
	address := "ws" + strings.TrimPrefix(ts.URL, "http") + "/socket.io/?" + config.query.Encode()
	ws, _, err := websocket.DefaultDialer.Dial(address, config.header)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	c := &client{
		t:      t,
		ws:     ws,
		frames: make(chan frame, 1024),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		pong:   config.pong,
	}
	t.Cleanup(c.close)
	ws.SetReadDeadline(time.Now().Add(patience))
	kind, open, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read open packet: %v", err)
	}
	ws.SetReadDeadline(time.Time{})
	if kind != websocket.TextMessage || !bytes.HasPrefix(open, []byte("0{")) || !json.Valid(open[1:]) {
		t.Fatalf("first frame = %q, want an Engine.IO open packet", open)
	}
	go c.read()
	return c
}

func (c *client) read() {
	defer close(c.done)
	for {
		kind, data, err := c.ws.ReadMessage()
		if err != nil {
			c.mu.Lock()
			c.err = err
			c.mu.Unlock()
			return
		}
		if kind == websocket.TextMessage && string(data) == "2" {
			if c.pong {
				c.write(websocket.TextMessage, []byte("3"))
			}
			continue
		}
		select {
		case c.frames <- frame{binary: kind == websocket.BinaryMessage, data: data}:
		case <-c.stop:
			return
		}
	}
}

func (c *client) write(kind int, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ws.WriteMessage(kind, data)
}

func (c *client) close() {
	c.end.Do(func() {
		close(c.stop)
		c.mu.Lock()
		c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		c.mu.Unlock()
		c.ws.Close()
	})
}

// drop closes the TCP connection without a WebSocket close frame.
func (c *client) drop() {
	c.ws.UnderlyingConn().Close()
}

func (c *client) send(text string) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ws.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
		c.t.Fatalf("send %q: %v", text, err)
	}
}

func (c *client) sendBinary(data []byte) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
		c.t.Fatalf("send binary %v: %v", data, err)
	}
}

func (c *client) next() frame {
	c.t.Helper()
	select {
	case f := <-c.frames:
		return f
	default:
	}
	select {
	case f := <-c.frames:
		return f
	case <-c.done:
		select {
		case f := <-c.frames:
			return f
		default:
		}
		c.t.Fatalf("connection closed while waiting for a frame: %v", c.closeError())
	case <-time.After(patience):
		c.t.Fatalf("no frame within %v", patience)
	}
	return frame{}
}

func (c *client) closeError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *client) expect(text string) {
	c.t.Helper()
	f := c.next()
	if f.binary || string(f.data) != text {
		c.t.Fatalf("frame = %s %q, want text %q", kindName(f), f.data, text)
	}
}

func (c *client) expectBinary(data []byte) {
	c.t.Helper()
	f := c.next()
	if !f.binary || !bytes.Equal(f.data, data) {
		c.t.Fatalf("frame = %s %q, want binary %v", kindName(f), f.data, data)
	}
}

// expectSet reads len(texts) text frames and compares them regardless of order.
func (c *client) expectSet(texts ...string) {
	c.t.Helper()
	var got []string
	for range texts {
		f := c.next()
		if f.binary {
			c.t.Fatalf("got binary frame %v, want one of %q", f.data, texts)
		}
		got = append(got, string(f.data))
	}
	want := slices.Clone(texts)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		c.t.Fatalf("frames = %q, want %q", got, want)
	}
}

func (c *client) expectClosed() {
	c.t.Helper()
	select {
	case <-c.done:
	case <-time.After(patience):
		c.t.Fatalf("connection still open after %v", patience)
	}
}

func (c *client) expectOpen() {
	c.t.Helper()
	select {
	case <-c.done:
		c.t.Fatalf("connection closed: %v", c.closeError())
	default:
	}
}

// expectOpenFor fails if the connection closes within d.
func (c *client) expectOpenFor(d time.Duration) {
	c.t.Helper()
	select {
	case <-c.done:
		c.t.Fatalf("connection closed: %v", c.closeError())
	case <-time.After(d):
	}
}

func (c *client) expectSilence() {
	c.t.Helper()
	select {
	case f := <-c.frames:
		c.t.Fatalf("unexpected %s frame %q", kindName(f), f.data)
	case <-time.After(quiet):
	}
}

func kindName(f frame) string {
	if f.binary {
		return "binary"
	}
	return "text"
}

// pkt builds the text frame of a Socket.IO packet: the Engine.IO message
// prefix, the packet type, the namespace and body, which holds the optional
// ack id and the payload.
func pkt(kind byte, namespace, body string) string {
	return "4" + string(kind) + nsPrefix(namespace) + body
}

func nsPrefix(namespace string) string {
	if namespace == "/" {
		return ""
	}
	return namespace + ","
}

// connect joins a namespace and returns the sid the server answered with.
func (c *client) connect(namespace, auth string) string {
	c.t.Helper()
	c.send(pkt('0', namespace, auth))
	got := c.next()
	head, tail := pkt('0', namespace, `{"sid":"`), `"}`
	text := string(got.data)
	if got.binary || !strings.HasPrefix(text, head) || !strings.HasSuffix(text, tail) || len(text) <= len(head)+len(tail) {
		c.t.Fatalf("connect %s answered with %s %q, want %s<sid>%s", namespace, kindName(got), got.data, head, tail)
	}
	return text[len(head) : len(text)-len(tail)]
}

// ping sends an event the probe handler acknowledges and waits for the answer.
// Everything the server queued for this client before it answers would be
// read first, so it proves the connection is alive and nothing else is pending.
func (c *client) ping(namespace string) {
	c.t.Helper()
	c.send(pkt('2', namespace, `9999["ping"]`))
	c.expect(pkt('3', namespace, `9999[]`))
}

// barrier makes the server queue a marker event on socket and expects it next,
// which proves that nothing else was queued for the client before it.
func (c *client) barrier(socket *socketio.Socket) {
	c.t.Helper()
	if err := socket.Emit("barrier"); err != nil {
		c.t.Fatalf("emit barrier: %v", err)
	}
	c.expect(pkt('2', socket.Namespace().Name(), `["barrier"]`))
}

type probe struct {
	sockets chan *socketio.Socket
	reasons chan socketio.Reason
}

// watch records the sockets that connect to ns and the reasons they leave. Its
// sockets answer the "ping" event with an empty acknowledgement.
func watch(ns *socketio.Namespace, setup ...func(*socketio.Socket)) *probe {
	p := &probe{sockets: make(chan *socketio.Socket, 64), reasons: make(chan socketio.Reason, 64)}
	ns.OnConnect(func(s *socketio.Socket) {
		s.OnDisconnect(func(r socketio.Reason) { p.reasons <- r })
		s.On("ping", func(e *socketio.Event) { e.Ack() })
		for _, f := range setup {
			f(s)
		}
		p.sockets <- s
	})
	return p
}

func (p *probe) socket(t *testing.T) *socketio.Socket {
	t.Helper()
	return receive(t, p.sockets)
}

func (p *probe) reason(t *testing.T) socketio.Reason {
	t.Helper()
	return receive(t, p.reasons)
}

func (p *probe) noSocket(t *testing.T) {
	t.Helper()
	nothing(t, p.sockets)
}

func (p *probe) noReason(t *testing.T) {
	t.Helper()
	nothing(t, p.reasons)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(patience):
		t.Fatalf("nothing received within %v", patience)
	}
	var zero T
	return zero
}

func nothing[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected value %v", v)
	case <-time.After(quiet):
	}
}

type fixture struct {
	server *socketio.Server
	ts     *httptest.Server
	probe  *probe
}

func newFixture(t *testing.T, opts socketio.Options, setup ...func(*socketio.Socket)) *fixture {
	t.Helper()
	server, ts := newServer(t, opts)
	return &fixture{server: server, ts: ts, probe: watch(server.Namespace, setup...)}
}

// join dials a new client and connects it to the main namespace.
func (f *fixture) join(t *testing.T, options ...dialOption) (*client, *socketio.Socket) {
	t.Helper()
	c := dial(t, f.ts, options...)
	sid := c.connect("/", "")
	s := f.probe.socket(t)
	if s.ID() != sid {
		t.Fatalf("socket id = %q, sid sent to the client = %q", s.ID(), sid)
	}
	return c, s
}
