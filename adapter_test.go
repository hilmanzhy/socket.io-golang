package socketio_test

import (
	"encoding/json"
	"sync"
	"testing"

	socketio "github.com/nuriontech/socket.io-golang"
)

// bus connects fake adapters the way a message broker would: an operation
// published by one adapter reaches every other adapter, never the sender.
type bus struct {
	mu       sync.Mutex
	adapters []*fakeAdapter
}

func (b *bus) adapter() *fakeAdapter {
	a := &fakeAdapter{bus: b}
	b.mu.Lock()
	b.adapters = append(b.adapters, a)
	b.mu.Unlock()
	return a
}

func (b *bus) peersOf(a *fakeAdapter) []*fakeAdapter {
	b.mu.Lock()
	defer b.mu.Unlock()
	var peers []*fakeAdapter
	for _, other := range b.adapters {
		if other != a {
			peers = append(peers, other)
		}
	}
	return peers
}

type fakeAdapter struct {
	bus *bus

	mu         sync.Mutex
	apply      func(socketio.Operation)
	subscribed int
	closed     int
	attempts   []socketio.Operation
	publishErr error
	closeErr   error
}

// Publish hands every other adapter its own JSON round trip of op, as a real
// transport would, so a receiver cannot alias the sender's data.
func (a *fakeAdapter) Publish(op socketio.Operation) error {
	a.mu.Lock()
	a.attempts = append(a.attempts, op)
	err := a.publishErr
	a.mu.Unlock()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	for _, peer := range a.bus.peersOf(a) {
		var copied socketio.Operation
		if err := json.Unmarshal(raw, &copied); err != nil {
			return err
		}
		peer.inject(copied)
	}
	return nil
}

func (a *fakeAdapter) Subscribe(apply func(socketio.Operation)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.apply = apply
	a.subscribed++
}

func (a *fakeAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed++
	return a.closeErr
}

// inject makes the server of a apply op as if another server had published it.
func (a *fakeAdapter) inject(op socketio.Operation) {
	a.mu.Lock()
	apply := a.apply
	a.mu.Unlock()
	if apply != nil {
		apply(op)
	}
}

func (a *fakeAdapter) failWith(err error) {
	a.mu.Lock()
	a.publishErr = err
	a.mu.Unlock()
}

func (a *fakeAdapter) closeWith(err error) {
	a.mu.Lock()
	a.closeErr = err
	a.mu.Unlock()
}

func (a *fakeAdapter) published() []socketio.Operation {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]socketio.Operation(nil), a.attempts...)
}

func (a *fakeAdapter) counts() (subscribed, closed int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.subscribed, a.closed
}

type node struct {
	*fixture
	adapter *fakeAdapter
}

// newCluster starts n servers that share one bus.
func newCluster(t *testing.T, n int) []*node {
	t.Helper()
	shared := &bus{}
	nodes := make([]*node, n)
	for i := range nodes {
		adapter := shared.adapter()
		nodes[i] = &node{fixture: newFixture(t, socketio.Options{Adapter: adapter}), adapter: adapter}
	}
	return nodes
}

func (n *node) expectPublished(t *testing.T, want int) {
	t.Helper()
	if got := len(n.adapter.published()); got != want {
		t.Errorf("%d operations published, want %d", got, want)
	}
}

func TestFakeAdapterBehavesLikeABus(t *testing.T) {
	t.Parallel()
	shared := &bus{}
	a, b, c := shared.adapter(), shared.adapter(), shared.adapter()
	received := map[*fakeAdapter][]socketio.Operation{}
	var mu sync.Mutex
	for _, adapter := range []*fakeAdapter{a, b, c} {
		adapter.Subscribe(func(op socketio.Operation) {
			mu.Lock()
			received[adapter] = append(received[adapter], op)
			mu.Unlock()
		})
	}
	op := socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"x"}, Targets: []string{"y"}}
	if err := a.Publish(op); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received[a]) != 0 || len(received[b]) != 1 || len(received[c]) != 1 {
		t.Fatalf("deliveries: sender %d, peers %d and %d, want 0, 1, 1", len(received[a]), len(received[b]), len(received[c]))
	}
	received[b][0].Rooms[0] = "changed"
	if received[c][0].Rooms[0] != "x" || op.Rooms[0] != "x" {
		t.Error("the peers share memory with each other or with the sender")
	}
}
