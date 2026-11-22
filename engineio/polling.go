package engineio

import (
	"context"
	"errors"
	"io"
	"net/http"
)

var (
	errOverlap  = errors.New("engineio: overlapping polling request")
	errMismatch = errors.New("engineio: session no longer uses polling")
)

func (s *Session) servePolling(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.poll(w, r)
	case http.MethodPost:
		s.post(w, r)
	default:
		fail(w, http.StatusBadRequest, codeBadRequest)
	}
}

// poll answers a GET with the queued packets, waiting for the first one when
// the queue is empty.
func (s *Session) poll(w http.ResponseWriter, r *http.Request) {
	if !s.claim(w, &s.polling) {
		return
	}
	defer s.release(&s.polling)
	batch, ok := s.nextBatch(r.Context())
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Write(appendPayload(nil, batch))
}

// post consumes the packets a client sends in the body of a POST.
func (s *Session) post(w http.ResponseWriter, r *http.Request) {
	if !s.claim(w, &s.posting) {
		return
	}
	defer s.release(&s.posting)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.server.opts.MaxPayload))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		s.end(ReasonTransportError)
		fail(w, status, codeBadRequest)
		return
	}
	packets, err := decodePayload(body)
	if err != nil {
		s.end(ReasonParseError)
		fail(w, http.StatusBadRequest, codeBadRequest)
		return
	}
	for _, p := range packets {
		s.handle(p)
	}
	w.Header().Set("Content-Type", "text/html")
	io.WriteString(w, "ok")
}

// claim reserves one of the two polling slots, GET or POST, for a request. A
// second request for a busy slot is a protocol violation that ends the
// session.
func (s *Session) claim(w http.ResponseWriter, slot *bool) bool {
	err := s.reserve(slot)
	if err == nil {
		return true
	}
	if errors.Is(err, errOverlap) {
		s.end(ReasonTransportError)
	}
	fail(w, http.StatusBadRequest, codeBadRequest)
	return false
}

func (s *Session) reserve(slot *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.transport != Polling:
		return errMismatch
	case *slot:
		return errOverlap
	}
	*slot = true
	return nil
}

func (s *Session) release(slot *bool) {
	s.mu.Lock()
	*slot = false
	s.mu.Unlock()
}

func (s *Session) nextBatch(ctx context.Context) ([]packet, bool) {
	for {
		if batch := s.takeBatch(); batch != nil {
			return batch, true
		}
		select {
		case <-s.wake:
		case <-s.done:
		case <-ctx.Done():
			s.abandon()
			return nil, false
		}
	}
}

// takeBatch returns what a poll must answer right now, or nil when the poll
// has to wait. The close packet of a closing session travels alone, in the
// poll after the last messages: the JavaScript client acts on a close packet
// before the messages that share its payload.
func (s *Session) takeBatch() []packet {
	var batch []packet
	last := false
	s.mu.Lock()
	switch {
	case s.state == stateClosed:
		batch = []packet{{kind: s.farewell}}
	case s.transport != Polling:
		batch = []packet{{kind: kindNoop}}
	case len(s.queue) > 0:
		batch = s.drain(nil)
	case s.state == stateClosing:
		batch, last = []packet{{kind: kindClose}}, true
	case s.flushing:
		batch = []packet{{kind: kindNoop}}
	}
	s.mu.Unlock()
	if last {
		s.end(ReasonForcedClose)
	}
	return batch
}

// abandon ends a polling session whose client dropped the pending GET.
func (s *Session) abandon() {
	s.mu.Lock()
	polling := s.transport == Polling
	s.mu.Unlock()
	if polling {
		s.end(ReasonTransportError)
	}
}
