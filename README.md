# socket.io-golang

[![Go Reference](https://pkg.go.dev/badge/github.com/hilmanzhy/socket.io-golang.svg)](https://pkg.go.dev/github.com/hilmanzhy/socket.io-golang)

A Socket.IO server for Go. It speaks Socket.IO protocol v5 over Engine.IO v4, the protocols of the JavaScript `socket.io-client` 3.x and 4.x, and gives you namespaces, rooms, events, acknowledgements, binary data and broadcasting behind a plain `http.Handler`. It is a server only: there is no Go client.

This is an unofficial implementation. It is not affiliated with or endorsed by the Socket.IO project.

## Compatibility

| | |
|---|---|
| Socket.IO protocol | v5 |
| Engine.IO protocol | v4 |
| Clients | JavaScript `socket.io-client` 3.x and 4.x |
| Transports | HTTP long-polling, WebSocket, and the upgrade from polling to WebSocket |
| Go | 1.24 or newer (`go.mod` declares `go 1.24`) |
| Dependencies | `github.com/gorilla/websocket`, and `github.com/redis/go-redis/v9` (imported only by the `redisadapter` package; it brings two indirect modules, listed in `go.mod`) |

Tested against: in `test/interop` (build tag `interop`, run by `make interop`), `TestJavaScriptClient` runs one scenario with the real JavaScript `socket.io-client` 4.8.3 (pinned in `test/interop/package.json`) over three transport settings: long-polling only, WebSocket only, and polling upgraded to WebSocket. The scenario covers connecting, the upgrade, acknowledgements in both directions, binary arguments in both directions, a large payload, a burst of events, room broadcasts, namespace middleware refusal, an unknown namespace, two namespaces over one connection, and server-side and client-side disconnects. Other 3.x and 4.x client versions were not run.

`TestNodeCluster` in the same directory needs Node.js and a Redis server at `REDIS_ADDR` (`127.0.0.1:6379` by default), and is skipped without one. It runs this server next to a real Node.js server with `socket.io` 4.8.3, `@socket.io/redis-adapter` 8.3.0 and `ioredis` 5.10.1 (pinned in `test/interop/package.json`) on one Redis, and checks emits with JSON and binary arguments, room and except selections, joins, leaves and disconnects in both directions, and that Node.js requests Go does not implement are ignored without error.

Tested with: Go 1.24.0 and 1.27.1 (darwin/arm64), Node.js 26, `socket.io` 4.8.3, `socket.io-client` 4.8.3, `@socket.io/redis-adapter` 8.3.0, `ioredis` 5.10.1, and Redis 8 on loopback (`make redis` runs the `redisadapter` tests against it). Other versions, Redis Cluster, the sharded adapter, `@socket.io/redis-emitter` and the `redis` (node-redis) client were not tested.

Engine.IO v3 clients are refused: a request whose `EIO` query parameter is not `4` is answered with HTTP 400 and the body `{"code":5,"message":"Unsupported protocol version"}`.

## Install

```
go get github.com/hilmanzhy/socket.io-golang
```

`go get` fetches the module from its GitHub repository, `hilmanzhy/socket.io-golang`. To try a local copy instead, run `go mod edit -replace github.com/hilmanzhy/socket.io-golang=../socket.io-golang` in your module, or use a `go.work` file that lists both directories.

## Quick start

`main.go`:

```go
package main

import (
	"log"
	"net/http"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

func main() {
	server := socketio.New(socketio.Options{})

	server.OnConnect(func(socket *socketio.Socket) {
		socket.Join("lobby")
		socket.On("chat", func(event *socketio.Event) {
			var text string
			if err := event.Bind(&text); err != nil {
				return
			}
			server.To("lobby").Emit("chat", socket.ID(), text)
			event.Ack("delivered")
		})
	})

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)

	httpServer := &http.Server{
		Addr:              ":3000",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
	}
	log.Fatal(httpServer.ListenAndServe())
}
```

JavaScript client:

```js
import { io } from "socket.io-client";

const socket = io("http://localhost:3000");

socket.on("connect", () => {
  socket.emit("chat", "hello", (reply) => console.log(reply));
});

socket.on("chat", (from, text) => console.log(from, text));
```

A browser page served from a different origin than the Go server is refused with HTTP 403 until you configure `engineio.Options.CheckOrigin`; see [CORS and origins](docs/guide.md#cors-and-origins). A client that sends no `Origin` header is always served.

A larger runnable program is in [examples/chat](examples/chat): `go run ./examples/chat`, then open `http://localhost:3000` in two browser tabs.

## Supported features

- Socket.IO v5 over Engine.IO v4: long-polling, WebSocket, and the polling to WebSocket upgrade, with server heartbeat and ping timeout
- Static namespaces, several of them multiplexed over one client connection
- Namespace middleware with the client's handshake `auth` payload, and `ConnectError` with attached data
- Events with JSON arguments, and per-socket event middleware
- Binary arguments, sent and received as `[]byte`
- Acknowledgements in both directions: `Event.Ack` for events the client sent, `Socket.EmitWithAck` with a `context.Context` for events the server sends
- Rooms, and broadcasts with `To`, `Except` and `Local`, plus `Join`, `Leave` and `Disconnect` on a whole selection of sockets
- Per-socket key/value data (`Set`, `Get`)
- One-at-a-time, in-order handler execution per socket, with a backlog limit that slows a client that sends faster than its handlers work
- Limits and guards: maximum payload, maximum outgoing buffer, maximum binary attachments, maximum arguments per received event (64 by default, see Differences from the Node.js server), connect timeout, origin check, and an `AllowRequest` hook that can refuse a handshake. Two more caps are off by default: `engineio.Options.MaxSessions` (open sessions) and `Options.MaxRooms` (rooms of one socket besides the room named after its ID)
- Handler panics, and panics in `CheckOrigin` and `AllowRequest`, are recovered and logged through a `*slog.Logger`. The same logger gets a warning, `socketio: malformed packet`, for each Socket.IO packet that ends its connection
- Several servers behind one load balancer through a pluggable `Adapter`, with a Redis Pub/Sub implementation in `redisadapter` whose messages are those of the Node.js `@socket.io/redis-adapter` 8, so Go and Node.js servers can share one cluster (limits under Not supported)

## Not supported

- Connection state recovery
- WebTransport
- Engine.IO v3 clients (`allowEIO3`)
- JSONP polling
- Per-message compression
- Volatile emits
- Broadcasts with acknowledgements
- Cluster-wide socket listing: `Namespace.Sockets` and `BroadcastOperator.Sockets` return only the sockets of the local node
- The parts of the Node.js `@socket.io/redis-adapter` that wait for an answer. In a cluster of Go and Node.js servers on one Redis, `fetchSockets`, `allSockets` and `serverSideEmit` started on a Node.js server do not see the Go servers (they keep working among the Node.js servers), an emit with acknowledgement started on a Node.js server does not reach Go clients, and Go has no equivalent of those calls. The `volatile` and `compress` flags are not carried between servers, and a message whose room name is not a string is refused by the Go server
- The sharded adapter of `@socket.io/redis-adapter` (Redis 7 sharded Pub/Sub): it is a different protocol
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

## Differences from the Node.js server

Node.js features this library lacks are listed under Not supported. That list was written by hand, so assume that a Node.js feature which is not named under Supported features is missing, even if it is not listed. Beyond those, these behaviours are defined by this library's own code:

- An event or acknowledgement a client sends is capped at 64 elements, the event name included, so an event takes at most 63 arguments (`parser.DefaultMaxArguments`). A packet over the cap ends the connection with the reason `parse error`. `socketio.Options.MaxArguments` raises the cap and cannot turn it off. The Node.js server has no such cap (`socket.io-parser` 4.2.7 limits only the number of attachments), so an application whose clients send an event with more than 63 arguments must raise `MaxArguments`.
- Only top-level `[]byte` arguments of an emit travel as binary attachments. A `[]byte` nested inside a struct, map or slice is encoded by `encoding/json` as base64 text. On receive, a binary value nested inside a JSON argument is inlined as base64, so it binds into a `[]byte` struct field.
- A packet other than CONNECT for a namespace the client has not joined is ignored. A CONNECT for a namespace the client already joined and a client-sent CONNECT_ERROR end the connection with the reason `parse error`, and so does an event with a reserved name in a namespace the client has joined.
- An event that has no handler, or that a `Socket.Use` middleware rejects, is dropped without an acknowledgement.
- Namespace middleware runs before the socket is connected. Returning an error refuses the connection, and the error message (and `ConnectError.Data`) is sent to the client, so never put secrets in it.
- `Server.Close()` ends every session. The JavaScript client 4.8.3 reports `transport close` and, with its default options, tries to reconnect.
- This server bounds a long-polling response by `MaxPayload`: it carries as many queued packets as fit, and at least one, and the rest wait for the next poll. `engine.io` 6.6.12 answers a poll with everything that is queued.
- This server also restarts the connect timeout when a client leaves its last namespace while its connection stays open. `socket.io` 4.8.3 arms it once, when the connection is set up.
- Over Redis, a Go server and a Node.js server on the same Redis server and key form one cluster, for example during a gradual migration, within the limits listed under Not supported. Delivery is at most once.
- With the JavaScript client, `socket.emitWithAck()` resolves with only the first acknowledgement argument (checked in `socket.io-client` 4.8.3); the callback form receives all of them.

The full list, with the wire details, is in [docs/protocol.md](docs/protocol.md).

## Scaling across nodes with Redis

Engine.IO sessions live in the memory of the node that accepted them. The Redis adapter relays broadcasts (`Emit`, `Join`, `Leave`, `Disconnect`) between nodes through Redis Pub/Sub:

```go
package main

import (
	"log"
	"net/http"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
	"github.com/hilmanzhy/socket.io-golang/redisadapter"
	"github.com/redis/go-redis/v9"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	adapter := redisadapter.New(client, redisadapter.Options{})
	server := socketio.New(socketio.Options{Adapter: adapter})

	server.OnConnect(func(socket *socketio.Socket) {
		socket.Join("lobby")
		server.To("lobby").Emit("joined", socket.ID())
	})

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)

	httpServer := &http.Server{
		Addr:              ":3000",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
	}
	log.Fatal(httpServer.ListenAndServe())
}
```

`server.To("lobby").Emit` reaches the members of `lobby` on every node.

The messages are those of the Node.js `@socket.io/redis-adapter` 8 with its default (MessagePack) parser. Go servers and Node.js servers that share a Redis server and the same key (`redisadapter.Options.Key`, `socket.io` by default, the `key` option of the Node.js adapter) form one cluster: an emit, including binary arguments and room and except selections, a join, a leave and a disconnect started on one reaches the sockets of all of them, in both directions. The limits of such a cluster are listed under Not supported. `test/interop` checks it against `socket.io` 4.8.3 with `@socket.io/redis-adapter` 8.3.0 (see Compatibility).

Give each application its own key. The default, `socket.io`, is also the default of the Node.js adapter, so every server that keeps it on one Redis server joins the same cluster, in any database: Pub/Sub ignores the database number, and two applications on one Redis server need two keys. A key must not contain `#`, which separates the parts of a channel name, and must not be another application's key followed by `-request`: the request channels of `<key>` are `<key>-request#...`, which are the emit channels of the key `<key>-request`.

The channels are `<key>#<namespace>#` and `<key>#<namespace>#<room>#` for emits, and `<key>-request#<namespace>#` for joins, leaves and disconnects. The adapter subscribes to the two patterns `<key>#*` and `<key>-request#*`, so a Redis ACL user needs those patterns and the right to publish. A received emit counts only when it arrives on the channel of its namespace, or on the channel of its room when it selects exactly that one room; an emit on any other channel is ignored.

Redis is a trusted boundary: whoever can publish on the channels of the key can emit to, move and disconnect every client of every node, so keep Redis private and authenticated, with TLS when the network is not yours. Long-polling requests of one session must reach the same node. The details, the options and the error values are in [the guide](docs/guide.md#scaling-with-the-redis-adapter).

## Production notes

- Set `engineio.Options.CheckOrigin` to your explicit origins. A `CheckOrigin` that returns `true` for everything exposes cookie-authenticated deployments to cross-site connections.
- Set `ReadHeaderTimeout` and `ReadTimeout` on your `http.Server`; `ReadTimeout` does not cut a long-poll that is being held. Do not set `WriteTimeout` shorter than `Engine.PingInterval` (25 s by default): an idle long-polling request is answered at the next ping.
- Handlers of one socket run one at a time. Do not block in them, and give every `EmitWithAck` a context with a deadline.
- Shut down with `server.Close()` first, then `http.Server.Shutdown`.
- Behind several nodes, route long-polling requests of a session to the same node, or enable only the WebSocket transport. That takes both `Engine.Transports: []engineio.Transport{engineio.WebSocket}` on the server and `transports: ["websocket"]` on the client.
- Never put secrets in a namespace middleware error.
- Guard every namespace with a middleware, the main namespace `"/"` included: it always exists and admits every client until `server.Use` refuses them.
- Check a room name that comes from the client before passing it to `To` or `Join`. Socket IDs are room names.
- A socket whose handlers stall keeps up to `EventBacklog` + 2 received events in memory, and an event can cost more memory than its bytes, because every argument is kept as a separate value. A received event or acknowledgement with more than `Options.MaxArguments` elements (64 by default, the event name counts as one) is refused as malformed before its arguments are parsed, and the connection ends with `parse error`; raise `MaxArguments` if your clients send more than 63 arguments in one event. If handlers can block, lower `EventBacklog` and `MaxPayload`. See [Limits and backpressure](docs/guide.md#limits-and-backpressure).
- Sessions and rooms per socket are not capped by default. To cap them, set `engineio.Options.MaxSessions` (a handshake over the cap is answered with HTTP 503) and `Options.MaxRooms`; `Socket.Join` then returns `ErrRoomLimit` for a call that would go over the cap, and joins none of the rooms of that call. The room named after the socket's ID never counts toward `MaxRooms`. The `MaxSessions` cap is global, so one client can fill it: pair it with a limit per client in `engineio.Options.AllowRequest`.

The full checklist is in [docs/guide.md](docs/guide.md#production-checklist).

## Package layout

| Path | Package | Contents |
|---|---|---|
| `.` | `socketio` | `Server`, `Namespace`, `Socket`, `Event`, `BroadcastOperator`, `Adapter` |
| `engineio` | `engineio` | Engine.IO v4 transport: long-polling, WebSocket, upgrade, heartbeat, limits, origin check |
| `parser` | `parser` | Socket.IO v5 packets: encoding, decoding, binary attachments, `Args` |
| `redisadapter` | `redisadapter` | Multi-node broadcast over Redis Pub/Sub |
| `examples/chat` | `main` | Runnable one-room chat |
| `test/interop` | `interop` | Scenario run by the JavaScript `socket.io-client` 4.8.3, and cluster scenario run next to a Node.js server (build tag `interop`) |

## Documentation

- [docs/guide.md](docs/guide.md): task-oriented guide with a snippet for every feature, the option tables and the disconnect reasons
- [docs/architecture.md](docs/architecture.md): how the layers fit together
- [docs/protocol.md](docs/protocol.md): protocol details and differences from the Node.js server
- [examples/chat](examples/chat): a runnable chat

## Development

| Command | What it does | Needs |
|---|---|---|
| `make check` | `make fmt`, `make vet` and `make test` in sequence | Go |
| `make interop` | Runs `npm ci` in `test/interop`, then `go test -tags interop`: the JavaScript client test, and the cluster test next to a Node.js server | Node.js and npm, with network access for `npm ci`; the cluster test also needs a Redis server at `127.0.0.1:6379`, or the address in `REDIS_ADDR`, and is skipped without one |
| `make redis` | Runs the `redisadapter` tests with `-race` | A Redis server at `127.0.0.1:6379`, or the address in `REDIS_ADDR`; without a reachable server the tests that need one are skipped and the offline tests still run |
| `make bench` | Runs the benchmarks of the `parser` package | Go |

`make fmt` fails when `gofmt -l .` lists a file. `make vet` runs `go vet ./...` and `go vet -tags interop ./test/interop`. `make test` runs `go test -race -count=1 ./...`.

## License

MIT. See [LICENSE](LICENSE).
