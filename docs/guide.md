# socket.io-golang guide

This guide shows how to build with the library, one task at a time. It is an unofficial Socket.IO server for Go and is not affiliated with or endorsed by the Socket.IO project. The README has the install line, the compatibility statement and the list of supported features.

Conventions used in the snippets:

- `socketio` stands for `socketio "github.com/hilmanzhy/socket.io-golang"`.
- `engineio`, `parser` and `redisadapter` stand for the packages of the same names under that module path.
- Each snippet is a set of top-level declarations. Add the standard-library imports it uses.
- The JavaScript lines assume `socket.io-client` 3.x or 4.x.

Contents:

- [Creating a server and mounting it](#creating-a-server-and-mounting-it)
- [Options](#options)
- [Limits and backpressure](#limits-and-backpressure)
- [Namespaces](#namespaces)
- [Middleware and handshake auth](#middleware-and-handshake-auth)
- [The connect handler](#the-connect-handler)
- [Receiving events](#receiving-events)
- [Acknowledgements](#acknowledgements)
- [Binary data](#binary-data)
- [Rooms](#rooms)
- [Broadcasting](#broadcasting)
- [Per-socket data](#per-socket-data)
- [Disconnecting](#disconnecting)
- [Errors](#errors)
- [Concurrency model](#concurrency-model)
- [Graceful shutdown](#graceful-shutdown)
- [CORS and origins](#cors-and-origins)
- [Scaling with the Redis adapter](#scaling-with-the-redis-adapter)
- [Writing a custom Adapter](#writing-a-custom-adapter)
- [Logging](#logging)
- [Production checklist](#production-checklist)
- [Lower-level packages](#lower-level-packages)
- [Not supported](#not-supported)

## Creating a server and mounting it

`socketio.New` returns a `*socketio.Server`, which is an `http.Handler`. The zero `socketio.Options` value is ready to use.

```go
func serve() error {
	server := socketio.New(socketio.Options{})

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)

	httpServer := &http.Server{
		Addr:              ":3000",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
	}
	return httpServer.ListenAndServe()
}
```

How the route is matched:

- The handler does not look at the URL path. It routes every request on the query parameters `EIO`, `transport` and `sid`. The path is whatever you mounted the handler on.
- The JavaScript client requests `/socket.io/` by default (its `path` option defaults to `/socket.io` and the client appends a trailing slash). Mount the handler on the pattern `"/socket.io/"`, which is a `ServeMux` subtree pattern and matches every path below it.
- A pattern without the trailing slash, `"/socket.io"`, matches only that exact path. The client's request for `/socket.io/` would then not reach the handler.
- To use another path, mount the handler there and tell the client: `mux.Handle("/realtime/", server)` with `io("http://localhost:3000", { path: "/realtime/" })`.

`*socketio.Server` embeds the main namespace `"/"`. `server.OnConnect`, `server.Use`, `server.Emit`, `server.To`, `server.Except`, `server.Local` and `server.Sockets` address the clients that connected without naming a namespace.

Requests the Engine.IO layer refuses are answered with a JSON body `{"code":N,"message":"..."}`:

| HTTP status | Code | Message | When |
|---|---|---|---|
| 400 | 0 | Transport unknown | The `transport` query value is not an enabled transport |
| 400 | 1 | Session ID unknown | `sid` names no session on this server |
| 400 | 2 | Bad handshake method | A handshake that is not a GET |
| 400 | 3 | Bad request | An unsupported polling method, a polling POST that cannot be read or decoded, a second overlapping polling request of the same kind, a polling request for a session that already moved to WebSocket, or a WebSocket upgrade that the session cannot accept |
| 413 | 3 | Bad request | A polling POST body larger than `MaxPayload` |
| 403 | 4 | Forbidden | The origin check or `AllowRequest` refused the request, or panicked (a panic counts as a refusal) |
| 503 | 4 | Forbidden | A handshake would take the server over `MaxSessions` |
| 400 | 5 | Unsupported protocol version | `EIO` is not `4` (Engine.IO v3 clients land here) |

The checks run in this order: origin (403), preflight (204, see [CORS and origins](#cors-and-origins)), `EIO`, `transport`, then the session. A handshake then goes through the method check (400, code 2), `AllowRequest` (403) and `MaxSessions` (503), in that order. One refusal does not use the JSON body: a request for the WebSocket transport that is not a valid WebSocket upgrade (a missing `Upgrade` or `Connection` header, a wrong version or key) is answered by `gorilla/websocket` with HTTP 400 (405 when the method is not GET) and a plain-text body.

## Options

`socketio.Options`. A zero field means its default, and so does a negative number or duration, except for `MaxRooms`, where zero or a negative value means no limit.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `Engine` | `engineio.Options` | zero value (see the next table) | The transport layer |
| `ConnectTimeout` | `time.Duration` | 45 s | How long a connection may stay open without a connected namespace |
| `MaxAttachments` | `int` | 10 | Maximum binary attachments in one received packet |
| `MaxArguments` | `int` | 64 | Maximum elements of one received event or acknowledgement; the event name counts as one, so an event takes 63 arguments by default. It can be raised but not turned off: zero or a negative value means 64. See [Limits and backpressure](#limits-and-backpressure) |
| `MaxRooms` | `int` | 0: no limit | Maximum rooms one socket may be in, not counting the room named after its ID. Zero or negative means no limit |
| `EventBacklog` | `int` | 64 | Received events one socket may have waiting for its handlers before its connection stops being read |
| `Adapter` | `socketio.Adapter` | nil, a single server | Relays broadcasts to the other servers of a cluster |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives the panics of handlers and, unless `Engine.Logger` is set, the panics of `CheckOrigin` and `AllowRequest`; also receives the warning for a Socket.IO packet that ends its connection as malformed, and the warning for a join from another server that hit `MaxRooms` |

`engineio.Options`. A zero field means its default, and so does a negative duration or size.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `PingInterval` | `time.Duration` | 25 s | Pause between two heartbeats |
| `PingTimeout` | `time.Duration` | 20 s | How long a client may take to answer a ping, to accept a write, or to collect its last packets on close |
| `UpgradeTimeout` | `time.Duration` | 10 s | Bound on the switch from polling to WebSocket |
| `MaxPayload` | `int64` | 1,000,000 bytes | Cap on one polling request body and on one WebSocket message; also bounds one polling response body (see [Limits and backpressure](#limits-and-backpressure)) |
| `MaxBuffer` | `int64` | 16 MiB (16,777,216 bytes) | Cap on the bytes queued for a client that is not reading |
| `MaxSessions` | `int` | 0: no limit | Cap on the sessions open at the same time; a handshake over it is answered with HTTP 503. Zero or negative means no limit. The cap is global: one client may open sessions until it is reached, so pair it with a limit per client in `AllowRequest` |
| `Transports` | `[]engineio.Transport` | `engineio.Polling` and `engineio.WebSocket` | Enabled transports |
| `CheckOrigin` | `func(*http.Request) bool` | nil: every cross-origin request is rejected | Decides whether a cross-origin request is served. See [CORS and origins](#cors-and-origins). A panic is recovered, logged to `Logger` and counts as a refusal |
| `AllowRequest` | `func(*http.Request) error` | nil: every handshake is allowed | Can refuse a handshake before a session exists. A non-nil error answers 403. A panic is recovered, logged to `Logger` and refuses the handshake the same way |
| `Logger` | `*slog.Logger` | nil: `slog.Default()` | Receives the panics of `CheckOrigin` and `AllowRequest`. A server built with `socketio.New` fills it with `socketio.Options.Logger` when it is nil |

The handshake tells the client the effective `PingInterval`, `PingTimeout` and `MaxPayload` (in milliseconds and bytes), so the client adapts to what you configure.

Serving only WebSocket:

```go
func websocketOnly() *socketio.Server {
	return socketio.New(socketio.Options{
		Engine: engineio.Options{
			Transports: []engineio.Transport{engineio.WebSocket},
		},
	})
}
```

Configure the client the same way: `io(url, { transports: ["websocket"] })`. The JavaScript client tries polling first by default (the default `transports` list of `engine.io-client` 4.x starts with `"polling"`), and a polling request to a WebSocket-only server is answered with code 0, "Transport unknown". The handshake announces the upgrade to `websocket` only to polling sessions on a server where WebSocket is enabled.

`AllowRequest` runs once per session, on the handshake GET, before the session exists. It does not run for the later requests of the session. Its error is not sent to the client; the client sees the 403 body.

```go
func rateLimited(allow func(*http.Request) bool) socketio.Options {
	return socketio.Options{
		Engine: engineio.Options{
			AllowRequest: func(r *http.Request) error {
				if !allow(r) {
					return errors.New("rate limited")
				}
				return nil
			},
		},
	}
}
```

## Limits and backpressure

| Limit | What happens when it is exceeded |
|---|---|
| `MaxPayload` | A polling POST body over the limit is answered with HTTP 413 and the session ends with `ReasonTransportError`. A WebSocket message over the limit closes the connection and nothing of it is delivered. A polling response is never filled past it: see below |
| `MaxAttachments` | A packet that announces more attachments than the limit is a protocol violation: the connection ends with `ReasonParseError` |
| `MaxArguments` | On by default (64) and not switchable off. An event or acknowledgement with more elements than the limit (the event name counts as one) is a protocol violation, refused before its arguments are parsed: the connection ends with `ReasonParseError`. See below |
| `MaxSessions` | Off by default. A handshake that would take the server over the limit is answered with HTTP 503, code 4, and creates no session. A session holds its slot until it has closed. The cap is global, so one client can fill it: pair it with a limit per client in `AllowRequest` |
| `MaxRooms` | Off by default. A `Join` that would take a socket over the limit joins none of the rooms of that call and returns `ErrRoomLimit`. The room named after the socket's ID never counts; see [Rooms](#rooms) |
| `EventBacklog` | See below |
| `MaxBuffer` | See below |
| `ConnectTimeout` | See below |
| `PingInterval` and `PingTimeout` | The server sends a ping, and the next one follows `PingInterval` after the client's answer. A client that has not answered `PingTimeout` after a ping is closed with `ReasonPingTimeout` |
| `UpgradeTimeout` | If the polling to WebSocket probe does not complete in time, the upgrade is abandoned and the session keeps working over polling |

**MaxArguments.** The cap counts the event name, so with the default 64 an event takes at most 63 arguments, and an acknowledgement, which has no name, at most 64. A packet over the cap ends the connection with `ReasonParseError` (the parser reports "too many arguments", which wraps `parser.ErrMalformed`). `Options.MaxArguments` raises the cap; zero or a negative value means the default, so the cap cannot be turned off. The Node.js server has no such cap (`socket.io-parser` 4.2.7 limits only the attachments), so an application whose clients send more than 63 arguments in one event must raise it.

**EventBacklog.** Each socket runs its handlers one at a time. Events that arrive while a handler is busy wait in a queue of at most `EventBacklog` entries. When the queue is full, the server stops reading the whole connection (every namespace of that client, including its heartbeat answers) until a handler finishes and frees a slot. This slows a client that sends faster than its handlers work, at the price that a handler that never returns freezes its connection until the ping timeout ends it. The connect handler and the disconnect handler are queued without waiting, even when the backlog is full; while they wait they count towards it. The disconnect handler is the last task of the socket: no handler of the socket runs after it. The events queued before it still run first, and an event that was waiting for room in a full backlog at that moment is dropped.

Two consequences follow from reading stopping:

- An acknowledgement that the client sent behind a full backlog is not read until a handler finishes. A handler that calls `EmitWithAck` can therefore end up waiting for an answer that sits behind its own backlog. Always pass a context with a deadline.
- A connection that stays paused for longer than `PingTimeout` after a ping cannot get its heartbeat answer read and ends with `ReasonPingTimeout`. Its sockets are then released: an `EmitWithAck` that was waiting returns `ErrDisconnected`, the event that was waiting for a free slot is dropped, and the disconnect handler runs after the handlers already queued.

The release is covered by a test (`TestFullBacklogDoesNotKeepTheSocketOfAnEndedConnection` in `handler_test.go`). It was not measured against the JavaScript client.

**Memory held for one client.** While its handlers stall, one socket can hold `EventBacklog` + 2 received events: one in the running handler, `EventBacklog` waiting, and one that the reader holds while it waits for a slot. One event can be as large as one text message plus `MaxAttachments` binary messages, each up to `MaxPayload`. The bytes received and held for one socket can therefore reach about (`EventBacklog` + 2) x (1 + `MaxAttachments`) x `MaxPayload`, which is about 726 MB with the defaults (66 x 11 x 1,000,000 bytes). Memory can exceed the bytes received: every argument of an event is kept as a separate value, so an event made of many tiny arguments costs more than its size. `MaxArguments` bounds how many arguments one event or acknowledgement can have, 64 by default with the event name counted as one: a packet over the cap is refused before its arguments are parsed, and the connection ends with `ReasonParseError`. Within the cap, decoding a text message of about 1 MB allocated about 1.0 times its size plus about 10 KB, whether it held 63 strings of 16 KB or one argument that is an array of 500,000 numbers. That is one throwaway run on one machine (Go 1.27.1, Apple arm64), not a benchmark kept in the repository, and it does not cover binary attachments. A client has one socket per namespace it joined. If your handlers can block, size `MaxPayload`, `MaxAttachments`, `MaxArguments` and `EventBacklog` to what your traffic needs.

**MaxBuffer.** Every message sent to a client is queued until the client's transport takes it. The check applies when something is already queued: if the queue holds bytes and the next message would push the total over `MaxBuffer`, the session is closed with `ReasonTransportError`. A single message larger than `MaxBuffer` is accepted into an empty queue. The queue counts the bytes of message data, not the packet type prefixes. A direct `Socket.Emit` to such a session returns `ErrDisconnected`. A broadcast does not report the failure of one recipient: the slow client is closed and the broadcast carries on.

**Polling responses.** A polling response carries as many queued packets as fit in `MaxPayload`, and at least one; the rest wait for the next poll. A binary packet counts at its base64 size, and the separator between packets counts too. A single packet larger than `MaxPayload` is therefore sent alone. `engine.io` 6.6.12, the Node.js server of the pinned test versions, answers a poll with everything that is queued.

**ConnectTimeout.** The timer runs exactly while the connection is open and has no connected socket. It starts when the connection opens and the first successful connection to a namespace stops it. It starts again, for the full `ConnectTimeout`, when the client leaves its last namespace while the connection stays open. If it fires, the connection is closed. A namespace that refuses the client does not stop the timer. This server restarts the timer on purpose; `socket.io` 4.8.3 arms its timer once, when the connection is set up.

## Namespaces

`Server.Of(name)` returns the namespace with that name and creates it on first use. A missing leading `/` is added. Clients can connect only to namespaces that exist: a CONNECT for an unknown namespace is answered with a connect error whose message is `Invalid namespace`. A name that contains a comma is accepted, but no client can reach it, because the packet format ends the namespace at the first comma.

```go
func namespaces(server *socketio.Server) {
	chat := server.Of("/chat")
	chat.OnConnect(func(socket *socketio.Socket) {
		socket.Emit("welcome", chat.Name())
	})
}
```

JavaScript: `io("http://localhost:3000/chat")`.

- One client connection can join several namespaces. Each gets its own `*socketio.Socket` with its own `ID()`, rooms, handlers and data. `Socket.Namespace()` returns the namespace a socket belongs to.
- Rooms are per namespace. A room name used in `/chat` is unrelated to the same name in `/admin`.
- Namespaces are static: there is no regex or function namespace. You cannot remove a namespace.
- A packet for a namespace the client has not joined is ignored. A CONNECT for a namespace the connection already joined ends the connection with `ReasonParseError`, and so does a CONNECT_ERROR, which no client may send.
- The main namespace `"/"` always exists, and a namespace without middleware admits every client. If you authenticate in the middleware of `/chat` only, a client can still connect to `"/"` without credentials, keep its connection open and receive what you broadcast with `server.Emit`. Guard the main namespace with `server.Use` as well, or refuse everyone there when you do not use it.

## Middleware and handshake auth

`Namespace.Use` adds a middleware that runs for every connecting socket, in the order added, before the socket is connected. The socket can be inspected, but it cannot emit or join rooms yet: `Emit` returns `ErrDisconnected` and `Join` has no effect (it returns nil). Put room joins and first emits in the [connect handler](#the-connect-handler).

A non-nil error refuses the connection. The client receives a connect error with the error's message. Return a `*socketio.ConnectError` to attach data. The first error stops the chain.

`Socket.Auth(&target)` decodes the payload the client sent in its handshake (the `auth` option of the JavaScript client). Without a payload the target is left untouched and the error is nil. `Socket.Request()` returns the HTTP request that opened the connection (headers, cookies, query, remote address), with an empty body. Its context is never cancelled.

```go
func requireToken(server *socketio.Server, userFor func(token string) (string, bool)) {
	admin := server.Of("/admin")
	admin.Use(func(socket *socketio.Socket) error {
		var auth struct {
			Token string `json:"token"`
		}
		if err := socket.Auth(&auth); err != nil {
			return &socketio.ConnectError{Message: "invalid handshake"}
		}
		user, ok := userFor(auth.Token)
		if !ok {
			return &socketio.ConnectError{Message: "unauthorized", Data: map[string]int{"code": 401}}
		}
		socket.Set("user", user)
		return nil
	})
}
```

JavaScript:

```js
const socket = io("http://localhost:3000/admin", { auth: { token: "..." } });
socket.on("connect_error", (error) => console.log(error.message, error.data));
```

Things to know:

- The message of the returned error is sent to the client as it is, for any error type. A plain `errors.New("db: connection refused")` would leak its text. Never put secrets or internal details in it.
- `ConnectError.Data` must be encodable by `encoding/json`. If it is not, the client receives `{"message":"Internal server error"}` instead.
- A panic in a middleware is recovered, logged, and refuses the connection with the message `Internal server error`.
- Middleware runs on the goroutine that reads the client's connection. Keep it short and bound its lookups with a timeout: until it returns, nothing else that client sends is read.
- A refused connection stays open at the transport level. The client may try another namespace on it.
- A client's `auth` payload must be a JSON object. A CONNECT payload that is not an object ends the connection with `ReasonParseError`.

## The connect handler

`Namespace.OnConnect` sets the handler that runs once for every connected socket. It replaces the previous handler. Register the socket's event handlers inside it.

```go
func onConnect(server *socketio.Server) {
	server.OnConnect(func(socket *socketio.Socket) {
		socket.Join("lobby")
		socket.Emit("hello", socket.ID())
		socket.OnDisconnect(func(reason socketio.Reason) {
			log.Printf("%s left: %s", socket.ID(), reason)
		})
	})
}
```

When the handler runs, the CONNECT answer has already been sent, the socket is connected, and it is already in the room named after its own ID. The handler runs on the socket's own goroutine, before any event of that socket. A namespace without a connect handler still accepts sockets; their events are dropped for lack of handlers.

`Socket.ID()` is unique per socket, so a client in two namespaces has two IDs. It is a random string produced by `crypto/rand.Text`.

## Receiving events

`Socket.On(name, handler)` sets the handler of one event name, replacing the previous one; a nil handler removes it. An event with no handler is dropped; if the client asked for an acknowledgement, it never gets one.

`Event` carries `Socket`, `Name` and `Args`. `Event.Bind(targets...)` decodes the arguments, in order, into pointers using `encoding/json` rules.

```go
type chatMessage struct {
	Room string `json:"room"`
	Text string `json:"text"`
}

func receive(socket *socketio.Socket) {
	socket.On("message", func(event *socketio.Event) {
		var msg chatMessage
		if err := event.Bind(&msg); err != nil {
			return
		}
		if !slices.Contains(socket.Rooms(), msg.Room) {
			return
		}
		socket.To(msg.Room).Emit("message", msg)
	})

	socket.On("sum", func(event *socketio.Event) {
		var a, b int
		if err := event.Bind(&a, &b); err != nil {
			event.Ack("expected two numbers")
			return
		}
		event.Ack(a + b)
	})
}
```

JavaScript: `socket.emit("message", { room: "lobby", text: "hi" })` and `socket.emit("sum", 1, 2, (result) => console.log(result))`.

Binding rules:

- Targets bind to arguments by position. If there are fewer arguments than targets, the remaining targets are left untouched and no error is returned. Extra arguments are ignored.
- A decode failure returns an error that wraps the `encoding/json` error. The bind stops at the first failure.
- A binary argument binds to `*[]byte` or `*any` only. See [Binary data](#binary-data).

`Event.Args` gives positional access: `Len()`, `IsBinary(i)`, `Bytes(i)` for a binary argument, and `Raw(i)` for the JSON of any other argument.

```go
func inspect(socket *socketio.Socket) {
	socket.On("raw", func(event *socketio.Event) {
		for i := 0; i < event.Args.Len(); i++ {
			if event.Args.IsBinary(i) {
				log.Printf("argument %d: %d bytes", i, len(event.Args.Bytes(i)))
				continue
			}
			log.Printf("argument %d: %s", i, event.Args.Raw(i))
		}
	})
}
```

A received event carries at most `Options.MaxArguments` elements (64 by default), the event name counting as one, so 63 arguments with the default. An acknowledgement the client sends has no name and can carry 64 arguments. A packet over the cap is refused as malformed before its arguments are parsed, and the connection ends with `ReasonParseError`. The Node.js server has no such cap: an application whose clients send more than 63 arguments in one event must raise `Options.MaxArguments` (see [Limits and backpressure](#limits-and-backpressure)).

`Socket.Use` adds a middleware that sees every received event before its handler. A non-nil error drops the event, without an acknowledgement.

```go
func guardEvents(socket *socketio.Socket) {
	socket.Use(func(event *socketio.Event) error {
		if strings.HasPrefix(event.Name, "admin:") {
			return errors.New("not allowed")
		}
		return nil
	})
}
```

Reserved event names are `connect`, `connect_error`, `disconnect`, `disconnecting`, `newListener` and `removeListener`. You cannot emit them (the emit returns `parser.ErrReservedEvent`), and a client that sends an event with one of those names is disconnected with `ReasonParseError`. An event whose first element is not a string is handled the same way.

## Acknowledgements

Client to server: when the client calls `emit` with a callback, `Event.WantsAck()` is true and `Event.Ack(args...)` sends the answer. `Ack` does nothing and returns nil when the client did not ask. Only one answer is sent: the first call that hands it over for sending claims it, and later calls do nothing and return nil. A call that fails, because its arguments cannot be encoded or because the socket is disconnected, returns the error and claims nothing, so a later call may still answer. If your handler never calls `Ack`, the client's callback never runs.

Server to client: `Socket.EmitWithAck(ctx, event, args...)` sends an event and waits for the client's answer. It returns the answer's `Args`, `ErrDisconnected` if the socket disconnects first, or the context's error.

```go
func acknowledgements(socket *socketio.Socket) {
	socket.On("save", func(event *socketio.Event) {
		if !event.WantsAck() {
			return
		}
		event.Ack("saved", 42)
	})
}

func askClient(socket *socketio.Socket) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := socket.EmitWithAck(ctx, "question", "your name?")
	if err != nil {
		return "", err
	}
	var answer string
	err = reply.Bind(&answer)
	return answer, err
}
```

JavaScript:

```js
socket.emit("save", { id: 1 }, (status, id) => console.log(status, id));
socket.on("question", (subject, reply) => reply("alice"));
```

- A JavaScript `socket.emitWithAck()` promise resolves with only the first acknowledgement argument (checked in `socket.io-client` 4.8.3). The callback form receives all of them.
- An acknowledgement may carry binary arguments, in either direction, with the rules of [Binary data](#binary-data).
- `EmitWithAck` never times out by itself. Without a deadline on the context, it waits until the client answers or the socket disconnects.
- Broadcasts cannot request acknowledgements. `BroadcastOperator` has no `EmitWithAck`.

## Binary data

On send, an argument of type `[]byte` at the top level of the argument list travels as a binary attachment. Every other argument is encoded with `encoding/json`, so a `[]byte` nested inside a struct, map or slice becomes base64 text inside the JSON.

On receive, a top-level binary argument binds to `*[]byte` or `*any`. A binary value nested inside a JSON argument is inlined as base64 text, so it binds into a `[]byte` struct field (which is how `encoding/json` decodes base64).

```go
type upload struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

func binary(socket *socketio.Socket) {
	socket.On("upload", func(event *socketio.Event) {
		var name string
		var data []byte
		if err := event.Bind(&name, &data); err != nil {
			return
		}
		event.Ack(len(data))
	})

	socket.On("upload-nested", func(event *socketio.Event) {
		var file upload
		if err := event.Bind(&file); err != nil {
			return
		}
		event.Ack(len(file.Data))
	})

	socket.Emit("blob", []byte{1, 2, 3}, "label")
	socket.Emit("record", upload{Name: "a.bin", Data: []byte{1, 2, 3}})
}
```

JavaScript, in Node.js:

```js
socket.emit("upload", "a.bin", Buffer.from([1, 2, 3]), (length) => console.log(length));
socket.emit("upload-nested", { name: "a.bin", data: Buffer.from([1, 2, 3]) }, (length) => console.log(length));
socket.on("blob", (buffer, label) => console.log(buffer, label));
socket.on("record", (record) => console.log(record.data));
```

Here `blob` arrives at the client as binary, while `record.data` is a base64 string, because the `[]byte` was nested.

Rules and cautions:

- Binding a binary argument to anything but `*[]byte` or `*any` returns an error.
- Do not modify a `[]byte` after passing it to `Emit` or `Ack`. The library does not copy it; it stays referenced until it has been written, and a broadcast shares the same slice among all recipients.
- To forward JSON you already hold as bytes, pass a `json.RawMessage`: it is not a `[]byte` for this purpose, so it is not turned into an attachment, and `encoding/json` writes it as JSON (it must be valid JSON).
- A packet may carry at most `MaxAttachments` attachments (default 10) when received.
- A received packet may reference each attachment once. The JavaScript client numbers every binary value separately, so it never repeats a reference; a packet that does is a protocol violation and ends the connection with `ReasonParseError`.

## Rooms

A room is a name. A socket can be in any number of rooms of its namespace, unless you set `Options.MaxRooms`. Every socket is automatically in a room named after its own `ID()`.

```go
func rooms(socket *socketio.Socket) {
	if err := socket.Join("lobby", "news"); err != nil {
		log.Printf("join: %v", err)
		return
	}
	socket.Leave("news")
	log.Println(socket.Rooms())
}
```

- `Socket.Join(rooms ...string) error` returns nil unless `Options.MaxRooms` is set. The other samples in this guide ignore its result, which is safe while `MaxRooms` is zero.
- `Options.MaxRooms` is off by default. The room named after the socket's ID never counts toward it, so a socket can be in `MaxRooms` rooms besides its own, and joining its own room never fails. A `Join` that would take the socket over the cap joins none of the rooms of that call and returns `socketio.ErrRoomLimit`: it is all or nothing per call. A room the socket is already in is skipped and does not count again, so a call that names only such rooms succeeds at the cap.
- `Socket.Rooms()` returns the rooms the socket is in, sorted, including the room named after its ID.
- `Join` has no effect on a socket that is not connected, for example inside a namespace middleware, and returns nil.
- A socket leaves all its rooms when it disconnects.
- To address one socket by ID, use its room: `server.To(id).Emit(...)`.
- Never pass a room name chosen by the client to `To` or `Join` without checking it. Socket IDs are room names, so an unchecked name lets a client send to a room it never joined or straight to another socket, and an unchecked `Join` lets it listen to any room. The `message` handler in [Receiving events](#receiving-events) checks `socket.Rooms()` first.

## Broadcasting

`Namespace.To`, `Except` and `Local`, and `Socket.To` and `Socket.Broadcast`, return a `BroadcastOperator`. It is an immutable value: every call returns a narrowed copy and leaves the receiver as it was.

| Call | Targets |
|---|---|
| `ns.Emit(...)` | Every socket of the namespace |
| `ns.To("a", "b")` | The sockets in room `a` or room `b` |
| `op.To("c")` | Adds room `c` to the rooms already selected |
| `ns.Except("m")` or `op.Except("m")` | Removes the sockets that are in room `m` from the selection |
| `ns.Local()` or `op.Local()` | Restricts the operation to this server |
| `socket.To("a")` | Room `a`, without the sender |
| `socket.Broadcast()` | The whole namespace, without the sender |

An operator offers `Emit`, `Join`, `Leave`, `Disconnect` and `Sockets`. `Emit`, `Join`, `Leave` and `Disconnect` act on the matching sockets of every server in the cluster unless `Local` was called. `Sockets` returns the matching sockets of this server only.

```go
func broadcasts(server *socketio.Server, socket *socketio.Socket) error {
	server.Emit("tick", time.Now().Unix())
	server.To("lobby").Emit("news", "hello")
	server.To("a", "b").Except("muted").Emit("news", "to a or b, not muted")
	server.Local().Emit("stats", 1)

	socket.To("lobby").Emit("typing", socket.ID())
	socket.Broadcast().Emit("joined", socket.ID())

	if err := server.To("lobby").Join("announcements"); err != nil {
		log.Printf("join: %v", err)
	}
	server.To("lobby").Leave("announcements")
	for _, member := range server.To("lobby").Sockets() {
		log.Println(member.ID())
	}
	return server.To("banned").Disconnect(true)
}
```

How a broadcast runs:

- The packet is encoded once and shared by all recipients.
- The operation is applied to the sockets of this server first, then published to the adapter.
- The returned error concerns only the encoding of the event (a reserved name, a value `encoding/json` cannot encode), the publication to the adapter, and, for `Join`, `Options.MaxRooms`. Delivery to individual sockets is not reported.
- `BroadcastOperator.Join` skips a socket that the cap would be exceeded for and joins the others. On this server the returned error then wraps `ErrRoomLimit` (use `errors.Is`), and it is joined with the adapter's error if the publication failed too. On the other servers of the cluster the socket is skipped the same way and a warning is logged through their `Options.Logger`; the caller is not told.
- With no adapter, `Local` makes no difference.

## Per-socket data

`Socket.Set(key, value)` and `Socket.Get(key)` keep values on a socket, for example the authenticated user. They are safe for concurrent use and live as long as the `*Socket`. A client connected to two namespaces has two sockets with separate data.

```go
func rememberUser(server *socketio.Server) {
	server.Use(func(socket *socketio.Socket) error {
		socket.Set("user", "alice")
		return nil
	})
	server.OnConnect(func(socket *socketio.Socket) {
		value, ok := socket.Get("user")
		user, _ := value.(string)
		log.Printf("%s connected as %q (stored: %t)", socket.ID(), user, ok)
	})
}
```

## Disconnecting

```go
func disconnecting(socket *socketio.Socket) {
	socket.Disconnect(false)
	socket.Disconnect(true)
}
```

- `Disconnect(false)` removes the socket from its namespace and sends the client a DISCONNECT packet for that namespace. The other namespaces of the client stay connected. The JavaScript client reports the reason `io server disconnect`.
- `Disconnect(true)` does that for every namespace of the client and then closes the underlying connection.
- `BroadcastOperator.Disconnect(closeConnection)` does the same for every matching socket.
- `Socket.OnDisconnect(handler)` sets a handler that runs once, with a `socketio.Reason`, after the handlers of the events that were queued before the disconnect. No handler of the socket runs after it: an event that was still waiting for room in a full backlog at that moment is dropped.
- `Socket.Connected()` reports whether the socket is connected to its namespace.

Disconnect reasons, as passed to the `OnDisconnect` handler. `Reason` is a string type:

| Constant | String | When |
|---|---|---|
| `ReasonTransportClose` | `transport close` | The client closed the connection: the WebSocket closed or ended (a close frame, end of stream, or a closed connection while reading), or the client sent an Engine.IO close packet |
| `ReasonTransportError` | `transport error` | The connection failed: a WebSocket read or write error (including a message over `MaxPayload`), a polling POST that could not be read or was over `MaxPayload`, a polling GET the client abandoned, overlapping polling requests, a ping packet sent by the client, or `MaxBuffer` exceeded |
| `ReasonPingTimeout` | `ping timeout` | The client did not answer a heartbeat ping within `PingTimeout` |
| `ReasonParseError` | `parse error` | The client sent data that breaks the protocol: a malformed Engine.IO packet or payload, or a Socket.IO packet that is malformed, has too many attachments or too many arguments, references an attachment twice, names a reserved or non-string event, joins a namespace twice, or has an invalid payload |
| `ReasonForcedClose` | `forced close` | Exported, but not delivered in the normal course of events. See the note below |
| `ReasonServerShutdown` | `server shutting down` | `Server.Close` was called |
| `ReasonClientDisconnect` | `client namespace disconnect` | The client sent a DISCONNECT for this namespace (`socket.disconnect()` in JavaScript). Its other namespaces are unaffected |
| `ReasonServerDisconnect` | `server namespace disconnect` | The server disconnected the socket with `Socket.Disconnect` or `BroadcastOperator.Disconnect` |

Notes:

- The transport-level reasons (everything except the last two) end the whole connection, so every namespace of that client reports the same reason.
- The session ends with `forced close` in two cases: `Disconnect(true)`, and the connect timeout. `Disconnect(true)` first takes every socket out of its namespace, so its disconnect handlers receive `ReasonServerDisconnect`. The connect timeout closes the session only when it has no connected socket, so there is normally no handler left to receive anything. A handler sees `ReasonForcedClose` only when the client joins a namespace at the very moment one of the two closes the session.
- Once a socket has left its namespace, which happens before its disconnect handler runs, `Emit`, `EmitWithAck` and `Ack` on it return `ErrDisconnected`, and an `EmitWithAck` that was waiting returns `ErrDisconnected` too.

## Errors

| Error | Returned by | Meaning |
|---|---|---|
| `socketio.ErrDisconnected` (`socketio: socket is disconnected`) | `Socket.Emit`, `Socket.EmitWithAck`, `Event.Ack` | The socket is not connected to its namespace, or its connection is closing or closed, or `MaxBuffer` was exceeded. `EmitWithAck` also returns it when the socket disconnects while waiting |
| `parser.ErrReservedEvent` | `Socket.Emit`, `Socket.EmitWithAck`, `BroadcastOperator.Emit`, `Namespace.Emit` | The event name is reserved. Wrapped, so use `errors.Is` |
| An `encoding/json` error, wrapped as `parser: argument N: ...` | The same emits, and `Event.Ack` | An argument cannot be encoded |
| The context's error | `Socket.EmitWithAck` | `ctx` was cancelled or its deadline passed |
| `parser: argument N: ...` wrapping an `encoding/json` error, or `parser: argument N is binary and needs a *[]byte target` | `Event.Bind`, `Args.Bind` | An argument does not decode into its target |
| An `encoding/json` error | `Socket.Auth` | The handshake payload does not decode into the target |
| `socketio.ErrRoomLimit` (`socketio: room limit reached`) | `Socket.Join`, `BroadcastOperator.Join` | `Options.MaxRooms` is set and the join would take a socket over it (the room named after the socket's ID is not counted). `Socket.Join` joins none of the rooms of the call. `BroadcastOperator.Join` skips that socket and joins the others; the error wraps `ErrRoomLimit`, and is joined with the adapter's error when there is one (use `errors.Is`) |
| The adapter's error | `BroadcastOperator.Emit`, `Join`, `Leave`, `Disconnect`, `Namespace.Emit` | Publication failed. With the Redis adapter: `redisadapter.ErrBacklogFull`, `redisadapter.ErrClosed`, or an encoding error, for example a room name that is not UTF-8 in a join, a leave or a disconnect, or an emit whose attachments do not match its placeholder offsets |
| The adapter's `Close` error | `Server.Close` | Nil without an adapter, and nil with the Redis adapter |

`engineio.ErrClosed` is what `engineio.Session.Send` returns; the `socketio` layer reports it as `ErrDisconnected`.

```go
func send(socket *socketio.Socket) {
	err := socket.Emit("hello", "world")
	switch {
	case errors.Is(err, socketio.ErrDisconnected):
		log.Println("client is gone")
	case errors.Is(err, parser.ErrReservedEvent):
		log.Println("pick another event name")
	case err != nil:
		log.Printf("emit: %v", err)
	}
}
```

## Concurrency model

Every method of `Server`, `Namespace`, `Socket` and `BroadcastOperator` is safe for concurrent use.

Each socket has a queue and a worker goroutine that exists only while it has tasks to run, so an idle socket costs no goroutine. Your handlers for a socket go through it, one task at a time, in the order the events arrived:

- the connect handler, before any event of the socket;
- the event middleware and the event handlers;
- the disconnect handler, after the handlers of the events queued before the disconnect. No handler of the socket runs after it ([Disconnecting](#disconnecting) says what happens to an event still waiting for room in a full backlog).

Namespace middleware is the exception. It runs before the socket is connected, on the goroutine that reads the client's connection: while it runs, nothing else that client sends is read, in any namespace. Other clients are not affected.

Different sockets run concurrently. State you share between sockets needs your own synchronisation.

What this means for your code:

- A handler that blocks delays the later events of its own socket. Once `EventBacklog` events wait, its whole connection stops being read (see [Limits and backpressure](#limits-and-backpressure)).
- `Emit` and the broadcasts never wait for the network: they queue the message for the connection's writer. Calling them inside a handler is safe.
- `EmitWithAck` blocks until the answer, the disconnect or the context. It is safe inside a handler, and you should give it a deadline.
- A panic in a handler or in event middleware is recovered, logged through `Options.Logger`, and the socket carries on with its next task. A panic in a namespace middleware refuses the connection.
- Goroutines you start yourself may keep using a `*Socket`. Its methods return `ErrDisconnected` or do nothing once the socket has gone, and `Connected()` tells you which case you are in.
- Registering handlers with `On`, `Use`, `OnConnect` and `OnDisconnect` is safe at any time. `On`, `OnConnect` and `OnDisconnect` replace the previous handler. `Use` adds to the chain.

## Graceful shutdown

`Server.Close` disconnects every client with `ReasonServerShutdown`, then closes the adapter. It does not stop your `http.Server`, and it does not wait for the clients: each session gets up to `PingTimeout` to collect what is still queued, while `Close` returns at once. A session opened after `Close` is ended right away. Only the first call has an effect; later calls return the result of the first.

Call `Server.Close` before `http.Server.Shutdown`. `Shutdown` does not close hijacked connections, which are the WebSocket connections, and it waits for in-flight requests, including long-polling requests that `Close` releases.

```go
func run(server *socketio.Server, httpServer *http.Server) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failed := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}()

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}

	if err := server.Close(); err != nil {
		log.Printf("socketio close: %v", err)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdown)
}
```

In `socket.io-client` 4.8.3, a connection closed this way is reported as `transport close`, and with the default options the client's reconnection logic then tries to connect again (checked in `socket.io-client` `manager.js`). Expect clients to come back to whichever server answers next.

## CORS and origins

The origin check runs on every request, WebSocket upgrades included, before anything else.

- A request without an `Origin` header is always served. Browsers send one on cross-origin requests; many non-browser clients do not.
- A request whose `Origin` host equals the request's host (`r.Host`) is same-origin and always served. Only the host is compared, case-insensitively; the scheme is not. Behind a proxy that rewrites the `Host` header, your page's origin may not match and you will need `CheckOrigin`.
- Any other request is cross-origin. With `CheckOrigin` nil it is rejected with HTTP 403 and the body `{"code":4,"message":"Forbidden"}`. This is the default.
- When `CheckOrigin` returns true, the response carries `Access-Control-Allow-Origin` set to the request's origin, `Access-Control-Allow-Credentials: true` and `Vary: Origin`.
- A preflight `OPTIONS` request that passes the check is answered with 204, `Access-Control-Allow-Methods: GET, POST`, `Access-Control-Max-Age: 86400`, and `Access-Control-Allow-Headers` echoing the headers the browser asked for.

`engineio.AllowOrigins` builds a `CheckOrigin` from an exact list, written as browsers send origins (scheme, host and port when not default).

```go
func withOrigins() *socketio.Server {
	return socketio.New(socketio.Options{
		Engine: engineio.Options{
			CheckOrigin: engineio.AllowOrigins("https://app.example.com", "https://admin.example.com"),
		},
	})
}

func withOriginFunc(allowed map[string]bool) *socketio.Server {
	return socketio.New(socketio.Options{
		Engine: engineio.Options{
			CheckOrigin: func(r *http.Request) bool {
				return allowed[r.Header.Get("Origin")]
			},
		},
	})
}
```

The same-origin rule trusts the `Host` header, which the client sends. A page on an attacker's domain whose DNS name is re-pointed at your server (DNS rebinding) is same-origin by this rule, because its `Origin` and `Host` are both the attacker's name. It carries no cookies of your real domain, so this matters when access is granted by network position, for example an internal service without authentication. To close it, refuse unknown hosts: compare `r.Host` with the names you serve in `AllowRequest`, or let the reverse proxy do it.

**Warning.** Allowed origins receive credentialed CORS headers, so any page on an allowed origin can open a connection that carries the visitor's cookies. A `CheckOrigin` that returns `true` for everything lets every website do that. If your deployment authenticates with cookies, that is a cross-site hijacking hole. List your origins explicitly.

## Scaling with the Redis adapter

Engine.IO sessions live in the memory of the node that accepted them, and a broadcast only reaches the sockets connected to the node that runs it. With an `Adapter`, `Emit`, `Join`, `Leave` and `Disconnect` on a `BroadcastOperator` (and so on `Namespace.Emit`) are relayed to the other nodes, which apply them to their own sockets. The `redisadapter` package does this over Redis Pub/Sub.

```go
func clustered(redisAddr string) *socketio.Server {
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	adapter := redisadapter.New(client, redisadapter.Options{
		Key:            "myapp:socketio",
		Backlog:        1024,
		PublishTimeout: 5 * time.Second,
		OnError:        func(err error) { log.Printf("redis adapter: %v", err) },
	})
	return socketio.New(socketio.Options{Adapter: adapter})
}
```

This needs `github.com/redis/go-redis/v9` (package `redis`) and the `redisadapter` package. `redisadapter.New` takes any `redis.UniversalClient`; `*redis.Client` is one.

**Give each application its own `Key`.** The default, `socket.io`, is also the default of the Node.js `@socket.io/redis-adapter`, so every server that keeps it on one Redis server joins the same cluster, Go or Node.js. Pub/Sub ignores the database number: selecting another database does not separate two applications that share a Redis server, only different keys do. Keep `#` out of a key: it separates the parts of a channel name, and a key that holds one can be mistaken for another key followed by a namespace. Do not use `<key>-request` as the key of another application next to `<key>` either: `<key>-request#...` are the request channels of `<key>`, so the two applications would share a channel. Redis is also a trusted boundary: whoever can publish on the channels of the key can emit any event that is not reserved to any client of the cluster (see the end of this section).

`redisadapter.Options`. A zero field means its default.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `Key` | `string` | `socket.io` | The prefix of the Pub/Sub channels of the cluster, with the same meaning as the `key` option of the Node.js `@socket.io/redis-adapter`. Servers with different keys do not see each other; servers with the same key see each other in any database. Keep `#` out of it, and do not pick `<other key>-request` |
| `Backlog` | `int` | 1024 | Operations that may wait for Redis before `Publish` returns `ErrBacklogFull` |
| `PublishTimeout` | `time.Duration` | 5 s | Bound on one PUBLISH command |
| `OnError` | `func(error)` | ignore errors | Receives background errors: a failed PUBLISH, an unreadable message, or a panic raised while a received message is decoded or applied |

How it behaves:

- `Publish` queues the operation in memory and returns at once. It returns `redisadapter.ErrBacklogFull` when `Backlog` operations are already waiting, and `redisadapter.ErrClosed` after `Close`. These surface as the error of the broadcast call.
- A PUBLISH that fails (or times out after `PublishTimeout`) is reported to `OnError` and the operation is dropped. There is no retry.
- Each adapter has a random node ID and ignores the messages it published itself, since the broadcast was already applied locally.
- `Subscribe` is issued while `socketio.New` runs. The adapter sends the `PSUBSCRIBE` command and does not wait for Redis to confirm it, so the subscription may not be active at once: operations published by other nodes in that first moment can be missed. Redis Pub/Sub keeps no messages for subscribers that are not listening, which also means a node cut off from Redis misses what is published meanwhile.
- go-redis hands the messages it receives to the adapter through a channel of 100 messages. If the adapter does not take a message for one minute, go-redis drops it and logs `message is dropped` through its own logger (go-redis v9.7.3, the version in `go.mod`).
- `Server.Close` closes the adapter and waits for its goroutines. The adapter never closes the Redis client you gave it. Close the client yourself after `server.Close()`.
- The channels are `<key>#<namespace>#` and `<key>#<namespace>#<room>#` for emits, and `<key>-request#<namespace>#` for joins, leaves and disconnects. The adapter subscribes to the two patterns `<key>#*` and `<key>-request#*`, so a Redis ACL user needs those two patterns, and the right to publish to the channels. A pattern also matches the channels of a longer key that starts with `<key>#`, so a received emit counts only when it arrives on the channel its sender publishes it on: the one of its namespace, or the one of its room when it selects exactly that one room. An emit on any other channel is ignored without an error.
- A message that cannot be decoded goes to `OnError` and is skipped.
- A panic raised while a received message is decoded or applied goes to `OnError` too. That message is skipped and the adapter keeps running. A panic of `OnError` itself is logged with `slog.Error`.
- A join that arrives from another server and would take a socket over `Options.MaxRooms` skips that socket and is logged as a warning through `socketio.Options.Logger`.

**Sharing Redis with Node.js servers.** The messages are those of the Node.js `@socket.io/redis-adapter` 8 with its default parser (MessagePack). A Go server and a Node.js server that use the same Redis server and the same key form one cluster. On the Node.js side the key is the `key` option of the adapter:

```js
io.adapter(createAdapter(pubClient, subClient, { key: "myapp:socketio" }));
```

In both directions, an emit (with JSON or binary arguments, and with room and except selections), a join, a leave and a disconnect started on one server reach the sockets of the others. `BroadcastOperator.Join`, `Leave` and `Disconnect` correspond to `socketsJoin`, `socketsLeave` and `disconnectSockets` of Node.js. The limits of such a cluster:

- `fetchSockets`, `allSockets` and `serverSideEmit` started on a Node.js server do not see the Go servers: Go subscribes by pattern, so the Redis `PUBSUB NUMSUB` count does not include it and the Node.js server does not wait for it. Those calls keep working among the Node.js servers.
- An emit with acknowledgement started on a Node.js server does not reach Go clients.
- Go has no equivalent of those calls. A Node.js request that Go does not implement is ignored without error.
- The `volatile` and `compress` flags are not carried.
- A message the Go server cannot read is refused whole and reported to `OnError`, never half applied. Examples: a room or except name that is not a string (a `null` in the room list of a join, leave or disconnect request too), a packet whose type is not 2, data that is not an array starting with an event name that is not reserved, a map key that is not a string, an extension other than a timestamp, and data nested more than 510 containers deep. The rules are in [protocol.md](protocol.md#7-redis-adapter-wire-format).
- The Go server does not cap the binary attachments of a message from Redis. The limit of 10 per packet (`parser.DefaultMaxAttachments`) and `Options.MaxArguments` apply to the packets of clients only.
- The codec reads and writes `data` without building a tree of it. In one measurement (Apple arm64, Go 1.27.1, inputs of 0.4 to 0.8 MB), publishing an event whose `data` is one long string allocated about as many bytes as `data` holds, and reading the message about twice its size; inputs built to cost most took 8 to 34 times the message to read, and publishing 200,000 empty arrays about 8 times. These are measurements, not guarantees; the table is in [protocol.md](protocol.md#79-what-a-message-costs).
- The sharded adapter (Redis 7 sharded Pub/Sub) is a different protocol and is not supported.
- Delivery is at most once, as with any Redis Pub/Sub.

Tested with: `socket.io` 4.8.3, `@socket.io/redis-adapter` 8.3.0 and `ioredis` 5.10.1 (pinned in `test/interop/package.json`), Node.js 26, and a Redis 8 server on the loopback interface. `make interop` runs `test/interop` (build tag `interop`; it needs Node.js and a Redis server at `REDIS_ADDR`, `127.0.0.1:6379` by default, and is skipped without one). It runs this server next to a real Node.js server on one Redis. Not tested: other versions of these, `@socket.io/redis-emitter`, the `redis` (node-redis) client, Redis Cluster and the sharded adapter. The codec has its own tests in `redisadapter/msgpack_test.go` and `redisadapter/wire_test.go`. The message layout is in [protocol.md](protocol.md#7-redis-adapter-wire-format).

Receiving nodes apply a remote operation only to namespaces that already exist on that node, and accept only event packets for emits. Redis is nevertheless a trusted boundary: whoever can publish on the channels of the key can emit to every client, make their sockets join or leave rooms, and disconnect them, on every server of the cluster, Go and Node.js alike. Keep Redis on a private network with authentication, and give each application and environment its own `Key`. When Redis is reached over a network you do not control, turn on TLS (`redis.Options.TLSConfig`), and connect as a Redis ACL user that is restricted to the channels of the key.

Things the adapter does not do:

- `Namespace.Sockets()` and `BroadcastOperator.Sockets()` list the sockets of this node only. There is no cluster-wide listing.
- Broadcasts cannot request acknowledgements, with or without an adapter.

**Sticky routing.** Because sessions live in one node's memory, every request of a polling session must reach the node that opened it. A request for a session another node holds is answered with HTTP 400 and the code 1, `Session ID unknown`. Either configure the load balancer to route by session (for example on the `sid` query parameter, or by client IP), or enable only the WebSocket transport on the servers and in the clients, where one connection stays on one node.

## Writing a custom Adapter

Implement `socketio.Adapter` to carry operations over another transport.

```go
type Adapter interface {
	Publish(socketio.Operation) error
	Subscribe(apply func(socketio.Operation))
	Close() error
}
```

The interface as declared in the `socketio` package uses the unqualified name `Operation`; the snippet above qualifies it so that it compiles in your own package.

- `Publish(op)` sends an operation to the other servers. It must not block on the network.
- `Subscribe(apply)` gives you the function that applies an operation from another server to this server's sockets. `socketio.New` calls it once, before it returns.
- `Close()` releases your resources. `Server.Close` calls it and returns its error.
- The server that starts an operation has already applied it locally. Your adapter must not hand a server the operations that server published.
- `Operation` is the unit you carry. It has no JSON tags, so `encoding/json` uses its Go field names (`Kind`, `Namespace`, `Rooms`, `Except`, `Packet`, `Targets`, `Close`), and a value survives a round trip through `encoding/json`. Its `Kind` is one of `socketio.OpEmit`, `socketio.OpJoin`, `socketio.OpLeave` or `socketio.OpDisconnect`.
- The `Packet` of an `OpEmit` is a `*parser.Packet`. The server writes it to its own clients from `Data` and `Attachments`. `Placeholders` holds the offset in `Data` of the placeholder of each attachment, in the order of `Attachments`; `parser.NewEvent` and `parser.NewAck` fill it, and an adapter that carries the whole `Operation`, as `encoding/json` does, keeps it. An adapter that writes the packet in another format needs it to tell a placeholder from an argument that only looks like one. If your adapter builds or changes the `Packet` of an emit, keep `Placeholders` in step with `Attachments`. The Redis adapter refuses to publish a packet whose offsets do not match its attachments (`redisadapter: attachment without its placeholder`), and it sets them on the packets it receives.
- `apply` rewrites the packet's namespace of an emit, so give every receiving server its own copy of an `Operation`, never the same value or the same `*parser.Packet`.
- The same trust rule as for Redis holds: whoever can publish to your transport can emit to every client.

A minimal in-process adapter for tests, which connects several servers of one process:

```go
type memoryBus struct {
	mu    sync.Mutex
	nodes []*memoryAdapter
}

type memoryAdapter struct {
	bus *memoryBus

	mu    sync.Mutex
	apply func(socketio.Operation)
}

var _ socketio.Adapter = (*memoryAdapter)(nil)

func (b *memoryBus) join() *memoryAdapter {
	node := &memoryAdapter{bus: b}
	b.mu.Lock()
	b.nodes = append(b.nodes, node)
	b.mu.Unlock()
	return node
}

func (a *memoryAdapter) Publish(op socketio.Operation) error {
	encoded, err := json.Marshal(op)
	if err != nil {
		return err
	}
	a.bus.mu.Lock()
	peers := slices.Clone(a.bus.nodes)
	a.bus.mu.Unlock()
	for _, peer := range peers {
		if peer == a {
			continue
		}
		var copied socketio.Operation
		if err := json.Unmarshal(encoded, &copied); err != nil {
			return err
		}
		peer.deliver(copied)
	}
	return nil
}

func (a *memoryAdapter) deliver(op socketio.Operation) {
	a.mu.Lock()
	apply := a.apply
	a.mu.Unlock()
	if apply != nil {
		apply(op)
	}
}

func (a *memoryAdapter) Subscribe(apply func(socketio.Operation)) {
	a.mu.Lock()
	a.apply = apply
	a.mu.Unlock()
}

func (a *memoryAdapter) Close() error {
	a.bus.mu.Lock()
	a.bus.nodes = slices.DeleteFunc(a.bus.nodes, func(node *memoryAdapter) bool { return node == a })
	a.bus.mu.Unlock()
	return nil
}

func twoNodes() (*socketio.Server, *socketio.Server) {
	bus := &memoryBus{}
	first := socketio.New(socketio.Options{Adapter: bus.join()})
	second := socketio.New(socketio.Options{Adapter: bus.join()})
	return first, second
}
```

This adapter delivers synchronously. A real one should queue, as `redisadapter` does, so that `Publish` returns without waiting on the network.

## Logging

`Options.Logger` is a `*slog.Logger`. The default is `slog.Default()`. The library logs four things:

- A panic recovered in a handler or middleware, at level `Error` with the message `socketio: handler panic` and the attributes `panic` and `stack`.
- A panic recovered in `Engine.CheckOrigin` or `Engine.AllowRequest`, at level `Error` with the message `engineio: callback panic` and the attributes `callback` (`CheckOrigin` or `AllowRequest`), `panic` and `stack`. It goes to `Engine.Logger`, which `socketio.New` fills with `Options.Logger` when it is nil. The request is refused with HTTP 403.
- A Socket.IO packet that the server refuses and that therefore ends its connection, at level `Warn` with the message `socketio: malformed packet` and the attribute `error`. The client is not told the reason. An Engine.IO frame that does not decode is not logged.
- An operation from another server that could not be applied in full, at level `Warn` with the message `socketio: operation of another server not fully applied` and the attributes `namespace`, `kind` and `error`. Today this is a join that skipped a socket because of `MaxRooms`.

The Redis adapter reports its background errors through `redisadapter.Options.OnError` instead. The one thing it logs is a panic of `OnError` itself, through `slog.Error`.

```go
func withLogger() *socketio.Server {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return socketio.New(socketio.Options{Logger: logger})
}
```

If you want connection logs, add them in a namespace middleware and in `OnDisconnect`, where you know the socket ID, the request and the disconnect reason.

## Production checklist

- **Origins.** Set `Engine.CheckOrigin` to an explicit list, for example with `engineio.AllowOrigins`. Never return `true` for everything. See [CORS and origins](#cors-and-origins).
- **Authentication.** Check credentials in a namespace middleware, from `Socket.Auth` or from `Socket.Request()`. A socket that has not passed the middleware never reaches your handlers. Guard every namespace, the main namespace `"/"` included: it always exists and admits everyone until `server.Use` says otherwise.
- **Room names.** Check a room name that comes from the client before you pass it to `To` or `Join`. See [Rooms](#rooms).
- **No secrets in connect errors.** The text of any error your middleware returns is sent to the client.
- **HTTP server timeouts.** Set `ReadHeaderTimeout` and `ReadTimeout` on your `http.Server`. `ReadTimeout` bounds how long a client may take to send one request, body included, so it cuts a polling POST that trickles in. It does not cut a long-poll that is being held, because the server lifts the read deadline once the request has been read, and it does not apply to a WebSocket after the upgrade. (This follows from the `net/http` source of Go 1.27.1: it sets the read deadline for the whole request, clears it once the body has been read or when the request has none, and clears both deadlines when a handler hijacks the connection, as the WebSocket upgrade does. It was not measured here.) Do not set a `WriteTimeout` shorter than `Engine.PingInterval`: an idle long-polling request is answered only when the next packet is queued (`nextBatch` in `engineio/polling.go` waits for one), which on an idle session is the next heartbeat ping. `net/http` starts the write deadline once the request has been read, so a `WriteTimeout` that expires first would cut the response. (This follows from the code of the two packages; it was not exercised against a live server.)
- **Proxy and load balancer timeouts.** Keep idle timeouts longer than `Engine.PingInterval`, because an idle connection carries traffic only at each heartbeat. Pass WebSocket upgrades through.
- **Sticky routing or WebSocket only.** Needed whenever more than one node serves clients. See [Scaling with the Redis adapter](#scaling-with-the-redis-adapter).
- **Redis.** Private network, authentication, TLS when the network is not yours, a `Key` of its own for each application and environment (the default `socket.io` is shared with every other application that keeps it, in every database), and an `OnError` that you alert on. Close the Redis client after `server.Close()`.
- **Limits.** Review `MaxPayload` (1,000,000 bytes), `MaxBuffer` (16 MiB), `MaxAttachments` (10), `MaxArguments` (64) and `EventBacklog` (64) against your traffic and memory budget. If your clients send more than 63 arguments in one event, raise `MaxArguments`: the Node.js server has no such cap, and over it the connection ends with a parse error. A `MaxBuffer` per client multiplies by your connection count, and so does the memory a stalled socket can hold, which can be more than the bytes it received. See [Limits and backpressure](#limits-and-backpressure).
- **Connection count.** The caps on sessions (`Engine.MaxSessions`) and on the rooms of one socket (`MaxRooms`) exist and are off by default; set them if you need a hard limit. `MaxSessions` is global, so one client can fill it: pair it with a limit per client in `Engine.AllowRequest`. Every session keeps its handshake request, headers included, until it ends. Set `MaxHeaderBytes` on your `http.Server`, limit handshakes in `Engine.AllowRequest`, and cap connections per client at the proxy.
- **Handlers.** Do not block in them. Give every `EmitWithAck` a context with a deadline. Remember that handlers of one socket run one at a time.
- **Shutdown.** `server.Close()` first, then `http.Server.Shutdown`. See [Graceful shutdown](#graceful-shutdown).
- **Rate limiting.** Use `Engine.AllowRequest` for handshakes. The library applies no per-event rate limit; use a socket middleware (`Socket.Use`) if you need one.
- **Logging.** Pass a `Logger` that reaches your log pipeline, so recovered panics are visible.

## Lower-level packages

Most applications need only the root package. The two packages below it are public and work on their own:

- `engineio` is an Engine.IO v4 server without the Socket.IO layer. `engineio.NewServer(options, accept)` returns an `http.Handler`. It calls `accept` for every new `*engineio.Session`, and the `engineio.Receiver` that `accept` returns gets the messages of that session (`Receive`) and its end (`Closed`). `Session.Send` writes messages and `Session.Close` ends the session.
- `parser` encodes and decodes Socket.IO v5 packets. `parser.NewEvent` and `parser.NewAck` build packets, a `parser.Decoder` (one per connection) turns text and binary frames back into packets, and `parser.ParseEvent` and `parser.ParseAck` return their arguments as `parser.Args`.

Their reference is in the Go doc comments (`go doc ./engineio`, `go doc ./parser`). [architecture.md](architecture.md) explains how the root package uses them.

## Not supported

- Connection state recovery
- WebTransport
- Engine.IO v3 clients (`allowEIO3`): a request with `EIO` other than `4` is refused with HTTP 400
- JSONP polling
- Per-message compression
- Volatile emits
- Broadcasts with acknowledgements
- Cluster-wide socket listing: `Sockets` is local to the node
- The parts of the Node.js `@socket.io/redis-adapter` that wait for an answer: `fetchSockets`, `allSockets` and `serverSideEmit` started on a Node.js server do not see Go servers, and an emit with acknowledgement started on a Node.js server does not reach Go clients. See [Scaling with the Redis adapter](#scaling-with-the-redis-adapter)
- The sharded adapter of `@socket.io/redis-adapter` (Redis 7 sharded Pub/Sub)
- Dynamic (regex or function) namespaces
- The `disconnecting` event
- Catch-all listeners (`onAny`)
- Several listeners for one event: `On`, `OnConnect` and `OnDisconnect` replace the previous handler
- Server-to-server events (`serverSideEmit`, `serverSideEmitWithAck`)
- `once`, `off` and `removeAllListeners`; `On(event, nil)` removes the handler of an event
- Outgoing catch-all listeners (`onAnyOutgoing`)
- The `new_namespace` event, the adapter room events (`create-room`, `delete-room`, `join-room`, `leave-room`) and a listing of all rooms of a namespace
- Engine events (`connection_error`, `initial_headers`, `headers`), the `upgrade` event and the current transport of a socket
- The `time` and `issued` fields of `socket.handshake`, and the description argument of the disconnect handler
- The server options `cookie`, `serveClient`, `httpCompression` and `allowUpgrades`, custom session IDs (`io.engine.generateId`), and CORS options other than the origin check
- Custom parsers, for example `socket.io-msgpack-parser`
- `io.engine.clientsCount`
- The admin UI
