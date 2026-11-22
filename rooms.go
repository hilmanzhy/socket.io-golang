package socketio

import (
	"maps"
	"slices"
	"sync"
)

// registry holds the connected sockets of one namespace and the rooms they
// joined. A socket is connected exactly while it is in the registry.
type registry struct {
	mu      sync.RWMutex
	sockets map[string]*Socket
	members map[string]map[string]*Socket
	joined  map[string]map[string]struct{}
}

func newRegistry() *registry {
	return &registry{
		sockets: make(map[string]*Socket),
		members: make(map[string]map[string]*Socket),
		joined:  make(map[string]map[string]struct{}),
	}
}

// add connects a socket and puts it in the room named after its id.
func (r *registry) add(socket *Socket) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sockets[socket.id] = socket
	r.joined[socket.id] = make(map[string]struct{})
	r.enter(socket, socket.id)
}

// remove disconnects a socket and reports whether it was connected.
func (r *registry) remove(socket *Socket) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sockets[socket.id] != socket {
		return false
	}
	for room := range r.joined[socket.id] {
		r.exit(socket.id, room)
	}
	delete(r.sockets, socket.id)
	delete(r.joined, socket.id)
	return true
}

func (r *registry) has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, connected := r.sockets[id]
	return connected
}

func (r *registry) join(id string, rooms []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	socket, connected := r.sockets[id]
	if !connected {
		return
	}
	for _, room := range rooms {
		r.enter(socket, room)
	}
}

func (r *registry) leave(id string, rooms []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, room := range rooms {
		if _, in := r.joined[id][room]; in {
			r.exit(id, room)
		}
	}
}

func (r *registry) of(id string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.joined[id]))
}

// match returns the sockets that are in at least one of rooms, or every
// socket when rooms is empty, minus the ones in any of the except rooms.
func (r *registry) match(rooms, except []string) []*Socket {
	r.mu.RLock()
	defer r.mu.RUnlock()
	candidates := r.sockets
	if len(rooms) > 0 {
		candidates = r.union(rooms)
	}
	excluded := r.union(except)
	matched := make([]*Socket, 0, len(candidates))
	for id, socket := range candidates {
		if _, skip := excluded[id]; !skip {
			matched = append(matched, socket)
		}
	}
	return matched
}

func (r *registry) union(rooms []string) map[string]*Socket {
	switch len(rooms) {
	case 0:
		return nil
	case 1:
		return r.members[rooms[0]]
	}
	merged := make(map[string]*Socket)
	for _, room := range rooms {
		maps.Copy(merged, r.members[room])
	}
	return merged
}

func (r *registry) enter(socket *Socket, room string) {
	members, ok := r.members[room]
	if !ok {
		members = make(map[string]*Socket)
		r.members[room] = members
	}
	members[socket.id] = socket
	r.joined[socket.id][room] = struct{}{}
}

func (r *registry) exit(id, room string) {
	delete(r.joined[id], room)
	members := r.members[room]
	delete(members, id)
	if len(members) == 0 {
		delete(r.members, room)
	}
}
