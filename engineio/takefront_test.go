package engineio

import "testing"

func TestTakeFront(t *testing.T) {
	t.Parallel()
	messages := func(sizes ...int) []packet {
		queue := make([]packet, len(sizes))
		for i, size := range sizes {
			queue[i] = packet{kind: kindMessage, data: make([]byte, size)}
		}
		return queue
	}
	tests := []struct {
		name     string
		queue    []packet
		buffered int64
		limit    int64
		taken    int
		buffer   int64
	}{
		{"everything fits", messages(2, 2, 2), 6, 100, 3, 0},
		{"partial", messages(2, 2, 2, 2), 8, 11, 3, 2},
		{"oversized first packet goes alone", messages(50, 2), 52, 10, 1, 2},
		{"oversized last packet stays", messages(2, 2, 50), 54, 10, 2, 50},
		{"single oversized packet", messages(50), 50, 10, 1, 0},
		{"control packets are not buffered data", []packet{{kind: kindPing}, {kind: kindMessage, data: []byte("ab")}, {kind: kindMessage, data: []byte("cd")}}, 4, 5, 2, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &Session{queue: tt.queue, buffered: tt.buffered}
			want := append([]packet(nil), tt.queue...)
			batch := s.takeFront(tt.limit)
			if len(batch) != tt.taken || !samePackets(batch, want[:tt.taken]) {
				t.Fatalf("batch = %+v, want the first %d packets", batch, tt.taken)
			}
			if !samePackets(s.queue, want[tt.taken:]) {
				t.Errorf("queue = %+v, want the %d packets after the batch", s.queue, len(want)-tt.taken)
			}
			if s.buffered != tt.buffer {
				t.Errorf("buffered = %d, want %d", s.buffered, tt.buffer)
			}
		})
	}
}

func TestTakeFrontBatchSurvivesLaterQueueing(t *testing.T) {
	t.Parallel()
	s := &Session{
		queue:    []packet{{kind: kindMessage, data: []byte("a")}, {kind: kindMessage, data: []byte("b")}, {kind: kindMessage, data: []byte("c")}},
		buffered: 3,
	}
	batch := s.takeFront(2)
	if len(batch) != 1 {
		t.Fatalf("batch has %d packets, want 1", len(batch))
	}
	s.queue = append(s.queue, packet{kind: kindMessage, data: []byte("d")})
	if string(batch[0].data) != "a" {
		t.Errorf("batch changed to %q after a packet was queued", batch[0].data)
	}
	if len(s.queue) != 3 || string(s.queue[0].data) != "b" || string(s.queue[2].data) != "d" {
		t.Errorf("queue = %+v, want b, c, d", s.queue)
	}
}
