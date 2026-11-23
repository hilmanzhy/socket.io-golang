package engineio

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

// counter is a Receiver that counts the messages it receives.
type counter struct {
	received int
}

// Receive counts the message.
func (c *counter) Receive(Message) error {
	c.received++
	return nil
}

// Closed ignores the end of the session.
func (c *counter) Closed(CloseReason) {}

// TestPollingPostMemory checks that the memory a POST allocates on top of
// reading its body stays a small multiple of the body, however many packets the
// body holds. It reads the allocation of the whole process, so it must not run
// in parallel with other tests.
func TestPollingPostMemory(t *testing.T) {
	const size, multiple = 1 << 20, 8
	tests := []struct {
		name     string
		body     string
		status   int
		received int
	}{
		{"only separators", strings.Repeat("\x1e", size), http.StatusBadRequest, 0},
		{"empty messages", strings.Repeat("4\x1e", size/2-1) + "4", http.StatusOK, size / 2},
		{"empty messages then garbage", strings.Repeat("4\x1e", size/2-1) + "x", http.StatusBadRequest, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := &counter{}
			server := NewServer(Options{MaxPayload: 2 * size}, func(*Session) Receiver { return count })
			r := httptest.NewRequest(http.MethodPost, "/engine.io/?EIO=4&transport=polling", strings.NewReader(tt.body))
			session := newSession(server, r, Polling)
			session.receiver = count
			w := httptest.NewRecorder()
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			session.post(w, r)
			runtime.ReadMemStats(&after)
			if w.Code != tt.status {
				t.Errorf("status = %d, want %d", w.Code, tt.status)
			}
			if count.received != tt.received {
				t.Errorf("%d messages delivered, want %d", count.received, tt.received)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > multiple*size {
				t.Errorf("POST of %d bytes allocated %d bytes, want at most %d", len(tt.body), allocated, multiple*size)
			}
		})
	}
}
