// Command chat is a one-room chat built on socket.io-golang. Run it, then open
// http://localhost:3000 in two browser tabs.
package main

import (
	"context"
	_ "embed"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	socketio "github.com/nuriontech/socket.io-golang"
)

const (
	room         = "lobby"
	maxNameRunes = 24
	maxTextRunes = 500
)

//go:embed index.html
var page []byte

type message struct {
	From string `json:"from"`
	Text string `json:"text"`
}

func main() {
	server := socketio.New(socketio.Options{})
	server.Use(requireName)
	server.OnConnect(joinLobby)

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})
	web := &http.Server{Addr: "localhost:3000", Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("chat listening on http://%s", web.Addr)
		if err := web.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	server.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := web.Shutdown(shutdown); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// requireName refuses a client whose handshake carries no usable name.
func requireName(socket *socketio.Socket) error {
	var auth struct {
		Name string `json:"name"`
	}
	if err := socket.Auth(&auth); err != nil {
		return &socketio.ConnectError{Message: "invalid handshake"}
	}
	name := strings.TrimSpace(auth.Name)
	if name == "" || utf8.RuneCountInString(name) > maxNameRunes {
		return &socketio.ConnectError{Message: "a name of 1 to 24 characters is required"}
	}
	socket.Set("name", name)
	return nil
}

func joinLobby(socket *socketio.Socket) {
	value, _ := socket.Get("name")
	name, _ := value.(string)

	socket.Join(room)
	socket.To(room).Emit("joined", name)

	socket.On("message", func(event *socketio.Event) {
		var text string
		if err := event.Bind(&text); err != nil {
			event.Ack("invalid message")
			return
		}
		text = strings.TrimSpace(text)
		if text == "" || utf8.RuneCountInString(text) > maxTextRunes {
			event.Ack("a message of 1 to 500 characters is required")
			return
		}
		socket.Namespace().To(room).Emit("message", message{From: name, Text: text})
		event.Ack(nil)
	})

	socket.OnDisconnect(func(socketio.Reason) {
		socket.To(room).Emit("left", name)
	})
}
