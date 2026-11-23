package engineio_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
)

const (
	wait         = 5 * time.Second
	settle       = 100 * time.Millisecond
	pollingQuery = "EIO=4&transport=polling"
	socketQuery  = "EIO=4&transport=websocket"
)

type recorder struct {
	session  *engineio.Session
	messages chan engineio.Message
	closed   chan engineio.CloseReason
	receive  func(*recorder, engineio.Message) error

	closeCalls       atomic.Int32
	inReceive        atomic.Int32
	overlapped       atomic.Bool
	lateReceive      atomic.Bool
	closedMidReceive atomic.Bool
}

func (r *recorder) Receive(m engineio.Message) error {
	if r.inReceive.Add(1) > 1 {
		r.overlapped.Store(true)
	}
	defer r.inReceive.Add(-1)
	if r.closeCalls.Load() > 0 {
		r.lateReceive.Store(true)
	}
	r.messages <- m
	if r.receive != nil {
		return r.receive(r, m)
	}
	return nil
}

func (r *recorder) Closed(reason engineio.CloseReason) {
	if r.inReceive.Load() != 0 {
		r.closedMidReceive.Store(true)
	}
	r.closeCalls.Add(1)
	select {
	case r.closed <- reason:
	default:
	}
}

func (r *recorder) message(t testing.TB) engineio.Message {
	t.Helper()
	return receive(t, r.messages, "a message")
}

func (r *recorder) text(t testing.TB) string {
	t.Helper()
	m := r.message(t)
	if m.Binary {
		t.Fatalf("got a binary message %v, want text", m.Data)
	}
	return string(m.Data)
}

func (r *recorder) reason(t testing.TB) engineio.CloseReason {
	t.Helper()
	return receive(t, r.closed, "Receiver.Closed")
}

func (r *recorder) ended(t testing.TB) {
	t.Helper()
	receive(t, r.session.Done(), "Session.Done")
}

func (r *recorder) open() bool {
	select {
	case <-r.session.Done():
		return false
	default:
		return true
	}
}

func (r *recorder) closedOnce(t testing.TB) {
	t.Helper()
	select {
	case again := <-r.closed:
		t.Fatalf("Closed called a second time with %q", again)
	case <-time.After(settle):
	}
	if n := r.closeCalls.Load(); n != 1 {
		t.Fatalf("Closed called %d times, want 1", n)
	}
}

func receive[T any](t testing.TB, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(wait):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

func quiet[T any](ch <-chan T, d time.Duration) bool {
	select {
	case <-ch:
		return false
	case <-time.After(d):
		return true
	}
}

func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func msg(s string) engineio.Message {
	return engineio.Message{Data: []byte(s)}
}

func blob(b ...byte) engineio.Message {
	return engineio.Message{Binary: true, Data: b}
}

func split(body string) []string {
	return strings.Split(body, "\x1e")
}

type openPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int64    `json:"pingInterval"`
	PingTimeout  int64    `json:"pingTimeout"`
	MaxPayload   int64    `json:"maxPayload"`
	raw          string
}

func parseOpen(t testing.TB, frame string) openPacket {
	t.Helper()
	if !strings.HasPrefix(frame, "0") {
		t.Fatalf("not an open packet: %q", frame)
	}
	var open openPacket
	if err := json.Unmarshal([]byte(frame[1:]), &open); err != nil {
		t.Fatalf("open packet %q: %v", frame, err)
	}
	if open.SID == "" {
		t.Fatalf("open packet without sid: %q", frame)
	}
	open.raw = frame
	return open
}

type failure struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func parseFailure(t testing.TB, r reply) failure {
	t.Helper()
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("failure Content-Type = %q, want application/json", ct)
	}
	var f failure
	if err := json.Unmarshal([]byte(r.body), &f); err != nil {
		t.Fatalf("failure body %q is not JSON: %v", r.body, err)
	}
	if f.Message == "" {
		t.Errorf("failure body %q has no message", r.body)
	}
	return f
}

type reply struct {
	status int
	body   string
	header http.Header
}

type call struct {
	cancel context.CancelFunc
	done   chan struct{}
	reply  reply
	err    error
}

func (c *call) wait(t testing.TB) reply {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(wait):
		t.Fatal("timed out waiting for the request to finish")
	}
	if c.err != nil {
		t.Fatalf("request failed: %v", c.err)
	}
	return c.reply
}

func (c *call) running(d time.Duration) bool {
	select {
	case <-c.done:
		return false
	case <-time.After(d):
		return true
	}
}

type harness struct {
	t        *testing.T
	server   *engineio.Server
	http     *httptest.Server
	client   *http.Client
	requests atomic.Int32
	accepts  atomic.Int32

	receive  func(*recorder, engineio.Message) error
	onAccept func(*engineio.Session)

	mu    sync.Mutex
	byID  map[string]*recorder
	order []*recorder
}

type option func(*harness)

func withReceive(fn func(*recorder, engineio.Message) error) option {
	return func(h *harness) { h.receive = fn }
}

func withEcho() option {
	return withReceive(func(r *recorder, m engineio.Message) error { return r.session.Send(m) })
}

func withAccept(fn func(*engineio.Session)) option {
	return func(h *harness) { h.onAccept = fn }
}

func newHarness(t *testing.T, opts engineio.Options, options ...option) *harness {
	t.Helper()
	h := &harness{t: t, byID: map[string]*recorder{}}
	for _, o := range options {
		o(h)
	}
	h.server = engineio.NewServer(opts, h.accept)
	h.http = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.requests.Add(1)
		h.server.ServeHTTP(w, r)
	}))
	h.client = &http.Client{Timeout: 3 * wait, Transport: &http.Transport{}}
	t.Cleanup(h.audit)
	t.Cleanup(h.http.Close)
	t.Cleanup(h.server.Close)
	t.Cleanup(h.client.CloseIdleConnections)
	return h
}

func (h *harness) accept(s *engineio.Session) engineio.Receiver {
	h.accepts.Add(1)
	r := &recorder{
		session:  s,
		messages: make(chan engineio.Message, 4096),
		closed:   make(chan engineio.CloseReason, 8),
		receive:  h.receive,
	}
	h.mu.Lock()
	h.byID[s.ID()] = r
	h.order = append(h.order, r)
	h.mu.Unlock()
	if h.onAccept != nil {
		h.onAccept(s)
	}
	return r
}

func (h *harness) audit() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.order {
		id := r.session.ID()
		if r.overlapped.Load() {
			h.t.Errorf("session %s: Receive ran concurrently", id)
		}
		if r.lateReceive.Load() {
			h.t.Errorf("session %s: Receive started after Closed", id)
		}
		if r.closedMidReceive.Load() {
			h.t.Errorf("session %s: Closed ran while Receive was in flight", id)
		}
		if n := r.closeCalls.Load(); n > 1 {
			h.t.Errorf("session %s: Closed called %d times", id, n)
		}
	}
}

func (h *harness) recorder(sid string) *recorder {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.byID[sid]
	if r == nil {
		h.t.Fatalf("no session %q was accepted", sid)
	}
	return r
}

func (h *harness) sole() *recorder {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.order) != 1 {
		h.t.Fatalf("%d sessions accepted, want 1", len(h.order))
	}
	return h.order[0]
}

func (h *harness) url(query string) string {
	return h.http.URL + "/engine.io/?" + query
}

func (h *harness) pollingURL(sid string) string {
	u := h.url(pollingQuery)
	if sid != "" {
		u += "&sid=" + sid
	}
	return u
}

func (h *harness) wsURL(query string) string {
	return "ws" + strings.TrimPrefix(h.http.URL, "http") + "/engine.io/?" + query
}

func (h *harness) send(ctx context.Context, method, target, body string, header http.Header) (reply, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(body))
	if err != nil {
		return reply{}, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return reply{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, body: string(data), header: resp.Header}, err
}

func (h *harness) request(method, target, body string, header http.Header) reply {
	h.t.Helper()
	r, err := h.send(context.Background(), method, target, body, header)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, target, err)
	}
	return r
}

func (h *harness) async(method, target, body string) *call {
	ctx, cancel := context.WithCancel(context.Background())
	c := &call{cancel: cancel, done: make(chan struct{})}
	h.t.Cleanup(cancel)
	go func() {
		defer close(c.done)
		c.reply, c.err = h.send(ctx, method, target, body, nil)
	}()
	return c
}

func (h *harness) poll(sid string) reply {
	h.t.Helper()
	return h.request(http.MethodGet, h.pollingURL(sid), "", nil)
}

func (h *harness) post(sid, body string) reply {
	h.t.Helper()
	return h.request(http.MethodPost, h.pollingURL(sid), body, nil)
}

// startPoll sends a GET in the background and returns once the server's
// handler has been entered for it.
func (h *harness) startPoll(sid string) *call {
	h.t.Helper()
	before := h.requests.Load()
	c := h.async(http.MethodGet, h.pollingURL(sid), "")
	eventually(h.t, "the poll to reach the server", func() bool { return h.requests.Load() > before })
	return c
}

func (h *harness) handshake() (string, openPacket) {
	h.t.Helper()
	r := h.poll("")
	if r.status != http.StatusOK {
		h.t.Fatalf("handshake status = %d, body %q", r.status, r.body)
	}
	open := parseOpen(h.t, split(r.body)[0])
	return open.SID, open
}

func (h *harness) open() (string, *recorder) {
	h.t.Helper()
	sid, _ := h.handshake()
	return sid, h.recorder(sid)
}
