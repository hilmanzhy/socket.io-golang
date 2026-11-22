package socketio_test

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	socketio "github.com/nuriontech/socket.io-golang"
)

func TestSocketIdentity(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("chat"))
	first, second := dial(t, ts), dial(t, ts)
	sidMain := first.connect("/", "")
	main := pm.socket(t)
	sidChat := first.connect("/chat", "")
	chat := pc.socket(t)
	sidOther := second.connect("/", "")
	other := pm.socket(t)

	cases := []struct {
		name      string
		socket    *socketio.Socket
		sid       string
		namespace string
	}{
		{"main namespace", main, sidMain, "/"},
		{"custom namespace", chat, sidChat, "/chat"},
		{"another connection", other, sidOther, "/"},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.socket.ID(); got != tc.sid {
				t.Errorf("ID() = %q, the client was told %q", got, tc.sid)
			}
			if got := tc.socket.Namespace().Name(); got != tc.namespace {
				t.Errorf("Namespace().Name() = %q, want %q", got, tc.namespace)
			}
		})
		seen[tc.sid] = true
	}
	if len(seen) != len(cases) {
		t.Errorf("sockets share ids: %q, %q, %q", sidMain, sidChat, sidOther)
	}
}

func TestServerNamespaces(t *testing.T) {
	t.Parallel()
	server, _ := newServer(t, socketio.Options{})
	cases := []struct {
		name string
		got  *socketio.Namespace
		same *socketio.Namespace
		want string
	}{
		{"embedded main namespace", server.Namespace, server.Of("/"), "/"},
		{"name with a slash", server.Of("/x"), server.Of("/x"), "/x"},
		{"name without a slash", server.Of("x"), server.Of("/x"), "/x"},
		{"nested name", server.Of("/a/b"), server.Of("a/b"), "/a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.same {
				t.Error("Of returned another namespace for the same name")
			}
			if tc.got.Name() != tc.want {
				t.Errorf("Name() = %q, want %q", tc.got.Name(), tc.want)
			}
			if n := len(tc.got.Sockets()); n != 0 {
				t.Errorf("%d sockets in a namespace nobody joined", n)
			}
		})
	}
	if server.Of("/a") == server.Of("/b") {
		t.Error("Of returned the same namespace for two names")
	}
}

func TestSocketValues(t *testing.T) {
	t.Parallel()
	type user struct{ Name string }
	f := newFixture(t, socketio.Options{})
	_, s := f.join(t)
	_, other := f.join(t)
	ada := &user{"ada"}

	cases := []struct {
		name  string
		key   string
		value any
	}{
		{"string", "name", "ada"},
		{"integer", "count", 42},
		{"pointer", "user", ada},
		{"slice", "tags", []string{"a", "b"}},
		{"nil value", "nothing", nil},
		{"empty key", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s.Set(tc.key, tc.value)
			got, ok := s.Get(tc.key)
			if !ok || !reflect.DeepEqual(got, tc.value) {
				t.Errorf("Get(%q) = %v, %v, want %v, true", tc.key, got, ok, tc.value)
			}
			if _, ok := other.Get(tc.key); ok {
				t.Error("another socket sees the value")
			}
		})
	}
	if got, _ := s.Get("user"); got != ada {
		t.Error("Get returned another pointer than the one stored")
	}
	if got, ok := s.Get("absent"); ok || got != nil {
		t.Errorf("Get of a missing key = %v, %v, want nil, false", got, ok)
	}
	s.Set("name", "grace")
	if got, _ := s.Get("name"); got != "grace" {
		t.Errorf("Get after overwriting = %v, want grace", got)
	}
}

func TestSocketValuesAreSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	_, s := f.join(t)
	const writers = 16
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprint("key", i)
			for round := range 50 {
				s.Set(key, round)
				s.Get(key)
				s.Get("shared")
				s.Set("shared", i)
			}
		}()
	}
	wg.Wait()
	for i := range writers {
		if got, ok := s.Get(fmt.Sprint("key", i)); !ok || got != 49 {
			t.Errorf("key%d = %v, %v, want 49, true", i, got, ok)
		}
	}
}

func TestMiddlewareCanStoreValuesForTheHandlers(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	got := make(chan any, 1)
	server.Use(func(s *socketio.Socket) error {
		var auth struct{ User string }
		if err := s.Auth(&auth); err != nil {
			return errors.New("bad auth")
		}
		s.Set("user", auth.User)
		return nil
	})
	server.OnConnect(func(s *socketio.Socket) {
		user, _ := s.Get("user")
		got <- user
	})
	c := dial(t, ts)
	c.connect("/", `{"user":"ada"}`)
	if user := receive(t, got); user != "ada" {
		t.Errorf("handler read user %v, want ada", user)
	}
}

func TestSocketRequest(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("/chat"))
	first := dial(t, ts, withHeader("X-Trace", "t-1"), withHeader("Cookie", "session=abc"), withQuery("room", "lobby"))
	first.connect("/", "")
	main := pm.socket(t)
	first.connect("/chat", "")
	chat := pc.socket(t)
	second := dial(t, ts, withHeader("X-Trace", "t-2"))
	second.connect("/", "")
	other := pm.socket(t)

	cases := []struct {
		name   string
		socket *socketio.Socket
		trace  string
		room   string
		cookie string
	}{
		{"main namespace", main, "t-1", "lobby", "abc"},
		{"custom namespace of the same connection", chat, "t-1", "lobby", "abc"},
		{"another connection", other, "t-2", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.socket.Request()
			if r == nil {
				t.Fatal("Request() = nil")
			}
			if got := r.Header.Get("X-Trace"); got != tc.trace {
				t.Errorf("X-Trace header = %q, want %q", got, tc.trace)
			}
			if got := r.URL.Query().Get("room"); got != tc.room {
				t.Errorf("room query = %q, want %q", got, tc.room)
			}
			if got := r.URL.Query().Get("EIO"); got != "4" {
				t.Errorf("EIO query = %q, want 4", got)
			}
			cookie, err := r.Cookie("session")
			if tc.cookie == "" && err == nil || tc.cookie != "" && (err != nil || cookie.Value != tc.cookie) {
				t.Errorf("session cookie = %v, %v, want %q", cookie, err, tc.cookie)
			}
			if r.RemoteAddr == "" {
				t.Error("RemoteAddr is empty")
			}
		})
	}
}
