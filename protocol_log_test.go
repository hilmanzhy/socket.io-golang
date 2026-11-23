package socketio_test

import (
	"log/slog"
	"strings"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

func TestProtocolViolationsAreLoggedWithTheirCause(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		send  func(*client)
		cause string
	}{
		{"second connect to the same namespace", func(c *client) { c.send("40") }, "namespace joined twice"},
		{"connect error sent by the client", func(c *client) { c.send(`44{"message":"x"}`) }, "CONNECT_ERROR sent by a client"},
		{"unknown packet type", func(c *client) { c.send("47[]") }, "unknown packet type"},
		{"event name is a number", func(c *client) { c.send("42[1]") }, "event name must be a string"},
		{"reserved event name", func(c *client) { c.send(`42["connect"]`) }, "reserved event name"},
		{"too many arguments", func(c *client) { c.send(`42["e",1,2,3]`) }, "too many arguments"},
		{"binary frame without a packet", func(c *client) { c.sendBinary([]byte{1}) }, "binary frame without a packet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var logs syncBuffer
			f := newFixture(t, socketio.Options{MaxArguments: 3, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
			c, _ := f.join(t)
			c.ping("/")
			if got := logEntries(t, &logs); len(got) != 0 {
				t.Fatalf("%d log entries before the violation: %v", len(got), got)
			}
			tc.send(c)
			c.expectClosed()
			got := logEntries(t, &logs)
			if len(got) != 1 {
				t.Fatalf("%d log entries after the violation, want 1: %q", len(got), logs.String())
			}
			cause, _ := got[0]["error"].(string)
			if got[0]["msg"] != "socketio: malformed packet" || got[0]["level"] != slog.LevelWarn.String() ||
				!strings.Contains(cause, "parser: malformed packet") || !strings.Contains(cause, tc.cause) {
				t.Errorf("entry = %v, want a WARN entry %q whose error holds %q", got[0], "socketio: malformed packet", tc.cause)
			}
		})
	}
}
