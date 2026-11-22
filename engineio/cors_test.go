package engineio_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/nuriontech/socket.io-golang/engineio"
)

const sameOriginMarker = "self"

func originCases() []struct {
	name   string
	origin string
	check  func(*http.Request) bool
	serve  bool
} {
	const allowed = "https://a.example"
	yes := func(*http.Request) bool { return true }
	no := func(*http.Request) bool { return false }
	return []struct {
		name   string
		origin string
		check  func(*http.Request) bool
		serve  bool
	}{
		{"no origin header", "", nil, true},
		{"same origin", sameOriginMarker, nil, true},
		{"cross origin without a checker", allowed, nil, false},
		{"cross origin refused", allowed, no, false},
		{"cross origin allowed", allowed, yes, true},
		{"AllowOrigins exact match", allowed, engineio.AllowOrigins(allowed), true},
		{"AllowOrigins one of several", "https://c.example", engineio.AllowOrigins("https://b.example", "https://c.example"), true},
		{"AllowOrigins other host", "https://b.example", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins other scheme", "http://a.example", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins other port", "https://a.example:8443", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins subdomain", "https://sub.a.example", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins longer host", "https://a.example.evil.example", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins trailing slash", "https://a.example/", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins null origin", "null", engineio.AllowOrigins(allowed), false},
		{"AllowOrigins empty list", allowed, engineio.AllowOrigins(), false},
	}
}

func originHeader(h *harness, origin string) http.Header {
	header := http.Header{}
	switch origin {
	case "":
	case sameOriginMarker:
		header.Set("Origin", h.http.URL)
	default:
		header.Set("Origin", origin)
	}
	return header
}

func TestCORSHandshake(t *testing.T) {
	t.Parallel()
	for _, tt := range originCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{CheckOrigin: tt.check})
			header := originHeader(h, tt.origin)
			r := h.request(http.MethodGet, h.pollingURL(""), "", header)
			cross := tt.origin != "" && tt.origin != sameOriginMarker
			if !tt.serve {
				if r.status != http.StatusForbidden {
					t.Fatalf("status = %d, want 403", r.status)
				}
				if f := parseFailure(t, r); f.Code != 4 {
					t.Errorf("code = %d, want 4 (forbidden)", f.Code)
				}
				if got := r.header.Get("Access-Control-Allow-Origin"); got != "" {
					t.Errorf("refused origin got Access-Control-Allow-Origin %q", got)
				}
				if n := h.accepts.Load(); n != 0 {
					t.Errorf("accept called %d times for a refused origin", n)
				}
				return
			}
			if r.status != http.StatusOK {
				t.Fatalf("status = %d, want 200", r.status)
			}
			if !cross {
				return
			}
			if got := r.header.Get("Access-Control-Allow-Origin"); got != tt.origin {
				t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, tt.origin)
			}
			if got := r.header.Get("Access-Control-Allow-Credentials"); got != "true" {
				t.Errorf("Access-Control-Allow-Credentials = %q, want true", got)
			}
			if got := strings.Join(r.header.Values("Vary"), ","); !strings.Contains(got, "Origin") {
				t.Errorf("Vary = %q, want it to include Origin", got)
			}
		})
	}
}

func TestCORSPreflight(t *testing.T) {
	t.Parallel()
	for _, tt := range originCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{CheckOrigin: tt.check})
			header := originHeader(h, tt.origin)
			header.Set("Access-Control-Request-Method", "POST")
			header.Set("Access-Control-Request-Headers", "content-type, x-custom")
			r := h.request(http.MethodOptions, h.pollingURL(""), "", header)
			if !tt.serve {
				if r.status != http.StatusForbidden {
					t.Fatalf("status = %d, want 403", r.status)
				}
				return
			}
			if r.status != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", r.status)
			}
			if tt.origin == "" || tt.origin == sameOriginMarker {
				return
			}
			if got := r.header.Get("Access-Control-Allow-Origin"); got != tt.origin {
				t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, tt.origin)
			}
			methods := r.header.Get("Access-Control-Allow-Methods")
			if !strings.Contains(methods, "GET") || !strings.Contains(methods, "POST") {
				t.Errorf("Access-Control-Allow-Methods = %q, want GET and POST", methods)
			}
			if got := r.header.Get("Access-Control-Allow-Headers"); got != "content-type, x-custom" {
				t.Errorf("Access-Control-Allow-Headers = %q, want the requested headers echoed", got)
			}
			if got := r.header.Get("Access-Control-Allow-Credentials"); got != "true" {
				t.Errorf("Access-Control-Allow-Credentials = %q, want true", got)
			}
			if r.body != "" {
				t.Errorf("preflight body = %q, want empty", r.body)
			}
		})
	}
}

func TestCORSHeadersOnEveryAnswer(t *testing.T) {
	t.Parallel()
	const origin = "https://a.example"
	h := newHarness(t, engineio.Options{CheckOrigin: engineio.AllowOrigins(origin)})
	header := http.Header{}
	header.Set("Origin", origin)
	r := h.request(http.MethodGet, h.pollingURL(""), "", header)
	sid := parseOpen(t, split(r.body)[0]).SID
	tests := []struct {
		name   string
		method string
		sid    string
		body   string
		status int
	}{
		{"post", http.MethodPost, sid, "4a", http.StatusOK},
		{"unknown session", http.MethodGet, "nope", "", http.StatusBadRequest},
		{"malformed post", http.MethodPost, sid, "abc", http.StatusBadRequest},
	}
	for _, tt := range tests {
		r := h.request(tt.method, h.pollingURL(tt.sid), tt.body, header)
		if r.status != tt.status {
			t.Errorf("%s: status = %d, want %d", tt.name, r.status, tt.status)
		}
		if got := r.header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("%s: Access-Control-Allow-Origin = %q, want %q", tt.name, got, origin)
		}
	}
}

func TestCORSWebSocket(t *testing.T) {
	t.Parallel()
	const allowed = "https://a.example"
	h := newHarness(t, engineio.Options{CheckOrigin: engineio.AllowOrigins(allowed)})
	tests := []struct {
		name   string
		origin string
		status int
	}{
		{"no origin", "", http.StatusSwitchingProtocols},
		{"same origin", sameOriginMarker, http.StatusSwitchingProtocols},
		{"allowed origin", allowed, http.StatusSwitchingProtocols},
		{"other origin", "https://evil.example", http.StatusForbidden},
	}
	for _, tt := range tests {
		if got := h.dialStatus(socketQuery, originHeader(h, tt.origin)); got != tt.status {
			t.Errorf("%s: status = %d, want %d", tt.name, got, tt.status)
		}
	}
}
