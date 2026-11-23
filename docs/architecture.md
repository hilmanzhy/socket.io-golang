# Architecture

This document is for a Go engineer who maintains the library or debugs it on the wire. It answers three questions: which file owns a behaviour, what happens to one packet from socket to handler and back, and which lock or goroutine owns each piece of state.

Every statement below was read from the code at the time of writing and names the file and the function that implements it. Line numbers are left out on purpose, because they move. Where this document mentions how the JavaScript server or client behaves, it quotes `README.md` or a source comment and says so; it never describes Node.js behaviour as if it were this code. The wire formats are in [protocol.md](protocol.md).

Module: `github.com/hilmanzhy/socket.io-golang`, root package `socketio`, `go 1.24` (`go.mod`).

## 1. Layers and the one-way dependency rule

```
        +--------------+
        | redisadapter |   imports: root package, go-redis
        +------+-------+
               |
               v
        +--------------+
        |   socketio   |   imports: engineio, parser
        |   (root)     |
        +---+------+---+
            |      |
            v      v
   +----------+  +--------+
   | engineio |  | parser |
   +----------+  +--------+
   stdlib,       stdlib only
   gorilla/websocket
```

The rule: an arrow is the only direction an import may point.

- `parser` imports the standard library only: `bytes`, `encoding/json`, `errors`, `fmt`, `strconv` (import blocks of `parser/packet.go`, `parser/decoder.go`, `parser/payload.go`).
- `engineio` imports the standard library and `github.com/gorilla/websocket`. It imports neither `parser` nor the root package (import blocks of `engineio/server.go`, `session.go`, `polling.go`, `websocket.go`, `packet.go`).
- The root package imports `engineio` and `parser` (`server.go`, `conn.go`, `errors.go`, `socket.go`, `event.go`, `adapter.go`, `broadcast.go`).
- `redisadapter` imports the root package (as `socketio`) and `github.com/redis/go-redis/v9` (`redisadapter/adapter.go`). Its test file also imports `engineio` and `parser`.
- The root package never imports `redisadapter`. The adapter is injected through `Options.Adapter` (`server.go`), whose type is the `Adapter` interface declared in the root package (`adapter.go`).

What the rule buys, as visible in the code:

- `engineio` moves opaque messages. Its unit of exchange is `engineio.Message{Binary bool, Data []byte}` (`engineio/packet.go`). It never looks inside a message.
- `parser` knows nothing about transports. It turns one text frame plus its binary frames into a `parser.Packet` and back.
- The only callback interface between `engineio` and the root package is `engineio.Receiver` (`engineio/server.go`): `Receive(Message) error` and `Closed(CloseReason)`. The root package implements it with `conn` (`conn.go`) and hands it back from the `accept` function passed to `engineio.NewServer` (`Server.accept` in `server.go`). In the other direction the root package calls `engineio.Session` (`Send`, `Close`, `Request`, `Done`) and uses `engineio.Options`, `engineio.Message` and the `CloseReason` constants.
- The root package's `Reason` constants are conversions of the `engineio` ones plus two of its own (`errors.go`).

## 2. File map

Test files are listed by area in section 10.

### Root package `socketio`

| File | Owns |
|---|---|
| `doc.go` | Package documentation: the three layers, the one-at-a-time handler rule. |
| `server.go` | `Options`, `withDefaults`, `Server`, `New`, `Of`, `Close`/`stop`, `accept`, `lookup`, `recovered` (logs a handler panic). |
| `conn.go` | `conn`: one per Engine.IO session. Implements `engineio.Receiver`. Decodes frames, routes packets, runs the namespace connect sequence (`connect`, `refuse`), owns the connect timer and the per-connection socket map, builds frames (`frames`). |
| `socket.go` | `Socket`: handlers, per-socket middleware, ack table, `Emit`, `EmitWithAck`, `Join`/`Leave`, `Disconnect`, `leave`, `receiveEvent`, `dispatch`, `receiveAck`. |
| `event.go` | `Event`: arguments, `Bind`, `Ack` with the single-answer guard. |
| `mailbox.go` | `mailbox`: the per-socket serial task queue and its lazy worker goroutine; `pushWait` blocks the reader while the queue is full; `finish` queues the last task, after which nothing else is queued. |
| `namespace.go` | `Namespace`: `Use`, `OnConnect`, `admit` (runs namespace middleware), `welcome`, `apply` (applies an `Operation` to local sockets). |
| `rooms.go` | `registry`: connected sockets and room membership of one namespace, and the per-socket room cap (`Options.MaxRooms`). |
| `broadcast.go` | `BroadcastOperator`: immutable target selector, local apply then publish. |
| `adapter.go` | `Operation`, `OperationKind`, the `Adapter` interface, `Server.applyRemote`. |
| `errors.go` | `ErrDisconnected`, `ErrRoomLimit`, `ConnectError`, `Reason` and its constants. |
| `Makefile` | `check`, `fmt`, `vet`, `test`, `interop`, `redis`, `bench`. |
| `go.mod`, `go.sum` | Module and dependencies (`gorilla/websocket`, `go-redis/v9`). |
| `README.md` | User-facing overview. |

### Package `engineio`

| File | Owns |
|---|---|
| `doc.go` | Package documentation. |
| `packet.go` | Packet kinds `0` to `6`, `Message`, the polling payload codec (`appendPayload`, `payloadPackets`), `decodePacket` (one polling chunk or one WebSocket text frame), and `wireSize`, the size of a packet in a polling payload. |
| `server.go` | `Options` and defaults, `Server`, `ServeHTTP` routing, origin check and CORS headers (`allowOrigin`), handshake and `open`, the open packet, HTTP failure bodies (`fail`), `Close`, session registry, the session slots (`reserve`, `release`, `forget`) behind `MaxSessions`, and `protect`, which recovers a panic in `CheckOrigin` or `AllowRequest`. |
| `session.go` | `Session`: state machine, `Send`, `Close`, `end`, `notify`, `deliver`, `handle`, the single timer (`arm`), heartbeat, `takeFront` (the packets of one polling payload). |
| `polling.go` | Long-polling transport: `poll` (GET), `post` (POST), the two slots, `nextBatch`/`takeBatch`, `fitting` (how many queued packets fit in `MaxPayload`), `abandon`. |
| `websocket.go` | WebSocket transport: upgrade from polling, `probe`, `serveWebSocket`, `adopt`, `readLoop`, `writeLoop`. |

### Package `parser`

| File | Owns |
|---|---|
| `packet.go` | Package documentation with the grammar, `Type`, `Packet` (with `Placeholders`, the offset in `Data` of each attachment's placeholder) and `Encode`. |
| `decoder.go` | `Decoder` (stateful, one per connection), `decodeText`, `checkPayload`, `checkLimits` (attachment and argument caps), `ErrMalformed`, `DefaultMaxAttachments`, `DefaultMaxArguments`. |
| `payload.go` | `NewEvent`, `NewAck`, `ParseEvent`, `ParseAck`, reserved event names, placeholder handling (`newPayloadPacket` fills `Packet.Placeholders`), `Args` and `Bind`. |

### Package `redisadapter`

| File | Owns |
|---|---|
| `adapter.go` | `Adapter` backed by Redis Pub/Sub: options, `Publish`, `Subscribe`, `publishLoop`, `receiveLoop`, `deliver`, `report`. |
| `wire.go` | The messages of the Node.js `@socket.io/redis-adapter`: channel names, `encode` and `decode` between `socketio.Operation` and a channel plus payload (`message`, `request`, `selection`), and the rules for reading a received emit (`reader.broadcast`, `packet`, `event`, `members`, `names`). |
| `msgpack.go` | What the writer and the reader share: the depth limit (`maxDepth`), the error values, and the shortest MessagePack header for a sized value (`family`, `appendText`). |
| `pack.go` | The writer. `appendJSON` turns the JSON text of the packet's data straight into MessagePack, without building a tree: it measures the arrays and objects first (`measure`), then writes. It also holds the number rules (`appendNumber`) and the placeholder rule: only the object at an offset of `Packet.Placeholders` is a placeholder (`placeholderLength`). |
| `unpack.go` | The reader. `reader` walks the MessagePack bytes of a message and writes the JSON text of `data` as it goes. It passes over a value it does not need without building anything (`skip`, `drop`). It also reads the timestamp extension (`date`). |

### Other directories

| Path | Owns |
|---|---|
| `test/interop/client.mjs` | The scenario run by the JavaScript `socket.io-client`. |
| `test/interop/interop_test.go` | Go side of the client interop test (build tag `interop`). |
| `test/interop/cluster.mjs` | The scenario run by Node.js for the cluster test: a `socket.io` server with `@socket.io/redis-adapter` on the shared Redis, and `socket.io-client` connections to it and to the Go server. |
| `test/interop/cluster_test.go` | `TestNodeCluster` (build tag `interop`): starts this server with the Redis adapter on a random key (the default key when `INTEROP_DEFAULT_KEY` is set) and runs `cluster.mjs` against it. Skipped without Node.js, without `npm ci` done, or without a Redis server at `REDIS_ADDR`. |
| `test/interop/package.json`, `package-lock.json` | Pin `socket.io-client` 4.8.3, `socket.io` 4.8.3, `@socket.io/redis-adapter` 8.3.0 and `ioredis` 5.10.1 (`package.json`, dependency entries). |
| `examples/chat/main.go`, `index.html` | A runnable one-room chat. |

## 3. Life of a connection

Names in the diagrams are the real functions.

### 3.1 Handshake and open, over polling

```
HTTP handler goroutine (GET, no sid)
  Server.ServeHTTP            allowOrigin (CheckOrigin, recovered), OPTIONS,
                              EIO, transport checks
   -> Server.handshake        method must be GET; AllowRequest (recovered);
                              reserve: 503 when MaxSessions sessions exist
    -> Server.open
       newSession             id, queue = [open packet], farewell = close
       receiving.Lock
         Server.accept        socketio side: newConn, connect timer armed
       receiving.Unlock
       startHeartbeat         timer: PingInterval -> ping
       sessions[id] = session (if Close was called already: end(
                               ServerShutdown) instead)
    -> Session.servePolling -> poll
       claim(polling slot)
       nextBatch -> takeBatch takes the queued packets that fit in
                    MaxPayload (at least one) = the open packet
       200, body = appendPayload(batch)
```

With a WebSocket handshake, `handshake` calls `upgrader.Upgrade` after `reserve` (it gives the slot back with `release` when the upgrade fails), then `open(...).serveWebSocket(conn)`. The open packet is the first thing in the queue, so `writeLoop` sends it as the first frame. The request goroutine becomes the reader (`readLoop`).

### 3.2 Upgrade, polling to WebSocket

```
Server.ServeHTTP (transport=websocket, sid known)
 -> Server.upgrade
    Session.beginUpgrade      needs open && polling && !upgrading
                              else 400 code 3
    Upgrader.Upgrade          on failure: cancelUpgrade, gorilla answered
    Session.probe             read and write deadline = UpgradeTimeout
       expect text "2probe", write "3probe"
       flushing = true, signal (the parked GET wakes)
       expect text "5"
    probe error: cancelUpgrade, conn.Close; session stays on polling
    Session.serveWebSocket
       adopt                  transport = WebSocket, upgrading/flushing off
       go writeLoop           the only data writer from now on
       readLoop               runs on this request goroutine
```

The wire view is in [protocol.md](protocol.md), section 2.

### 3.3 Namespace connect

```
reader goroutine (POST handler goroutine, or the WebSocket readLoop)
  Session.handle (kindMessage) -> deliver      holds Session.receiving
   -> conn.Receive                  Decoder.Text or Decoder.Binary
    -> conn.route (Connect)
     -> conn.connect
        1 socket already joined for this namespace -> ErrMalformed
        2 server.lookup(ns) == nil -> refuse "Invalid namespace"
        3 newSocket                random id, CONNECT payload, mailbox, maps
        4 namespace.admit          Use middleware, with recover;
                                   error -> refuse
        5 c.sockets[ns] = socket; c.timer.Stop
        6 conn.send(CONNECT {"sid":...})   failure -> c.forget, stop
        7 rooms.add                socket is now connected; room = its id
        8 mailbox.push(welcome)    OnConnect runs in the mailbox
```

Why the order matters (`conn.connect` comment and code): the CONNECT answer is queued before step 7, so no broadcast can reach the client ahead of its own CONNECT; the welcome task is queued before the reader can queue any event of the socket, so `OnConnect` always runs before the first event handler. Until step 7 `Socket.Connected()` is false, so `Socket.send` returns `ErrDisconnected`: a namespace middleware cannot emit (`Namespace.Use` doc).

`refuse` sends CONNECT_ERROR and returns `nil`: the Engine.IO session stays open. The connect timer is not stopped by a refusal. It is stopped by a successful step 5 and restarted by `conn.forget` when the last socket of an open connection leaves (section 5).

### 3.4 Inbound event, from socket to handler

```
client frame
  -> WebSocket readLoop | polling post        decodePacket | payloadPackets
  -> Session.handle (kindMessage) -> deliver  [Session.receiving held]
  -> conn.Receive -> Decoder.Text             more attachments or arguments
                                              than the caps: ErrMalformed
       (Decoder.Binary: buffers the packet until all attachments arrived)
  -> conn.route -> c.socket(nsp)      nil: packet dropped, no error
  -> Socket.receiveEvent
       parser.ParseEvent              error: Receive fails -> end(ParseError)
       mailbox.pushWait(dispatch)     blocks while EventBacklog tasks wait;
                                      returns, task dropped, when the
                                      session ends (Session.Done closed)
  ----------------------------------------------------------------------
mailbox worker goroutine
  mailbox.run -> mailbox.call (recover) -> Socket.dispatch
       read handler + middleware snapshot under Socket.mu
       Socket.Use middleware in order; a non-nil error drops the event
       handler(event)                 no handler: dropped, no ack
```

`Receive` runs on the connection's reader and is serialized by `Session.receiving`. The handler never runs there: only decoding, routing, `pushWait`, and namespace middleware (connect only) do.

### 3.5 Outbound emit, from caller to wire

```
any goroutine (handler, HTTP handler, timer, ...)
  Socket.Emit -> parser.NewEvent     JSON-encodes; []byte -> placeholder,
                                     its offset goes to Packet.Placeholders
   -> Socket.send                    not Connected: ErrDisconnected
    -> conn.send -> frames(p)        text frame + one binary frame each
     -> Session.Send(msgs...)        lock mu; MaxBuffer check; append to
                                     queue; signal(wake)
  ----------------------------------------------------------------------
polling:    the parked GET wakes in nextBatch -> takeBatch takes the packets
            that fit in MaxPayload -> appendPayload -> 200
WebSocket:  writeLoop wakes -> drain -> write (text frame, or binary message)
```

`Session.Send` queues all the messages of one call together; no other `Send` is interleaved with them (`Session.Send` doc). That is what keeps a binary event's text frame next to its attachments.

### 3.6 Acknowledgements

Client asks, server answers (`Event.Ack`, `event.go`):

```
conn.route -> Socket.receiveEvent: Event{ackID, wantsAck = p.HasID}
handler -> Event.Ack(args...)
  no wantsAck: returns nil, sends nothing
  parser.NewAck(ns, ackID, args)     encode failure: returned
  Event.mu held; acked already set: returns nil, sends nothing
  Socket.send                        send failure: returned, acked stays false
  acked = true                       only after Socket.send succeeded
```

Server asks, client answers (`Socket.EmitWithAck`, `socket.go`):

```
caller goroutine                         reader goroutine
  NewEvent
  expectAck: id = nextAck++ (from 0),
    acks[id] = make(chan Args, 1)
  defer forgetAck(id)
  packet.ID, HasID = id
  Socket.send
  select { reply | ctx.Done }   <------  conn.route -> Socket.receiveAck
                                           ParseAck; ack without id: ignored
                                           lock Socket.mu
                                           delete acks[id]; reply <- args
                                           unlock
```

The ack is resolved directly on the reader. It is not queued in the mailbox. A closed `reply` channel (the socket left, see `Socket.leave`) makes `EmitWithAck` return `ErrDisconnected`.

### 3.7 Broadcast

```
BroadcastOperator.Emit -> parser.NewEvent       encoded once
 -> run(op)
    1 Namespace.apply(op)
        rooms.match(op.Rooms, op.Except)         under registry.mu (read)
        frames(op.Packet) once; for each target: Session.Send(messages...)
    2 Adapter.Publish(op)       unless Local() or Options.Adapter == nil
  ----------------------------------------------------------------------
other node (adapter's goroutine)
  Server.applyRemote(op)
     unknown namespace: ignored (never created)
     OpEmit: packet must be Event or BinaryEvent; Packet.Namespace forced
     Namespace.apply(op)           a join that skips a socket (MaxRooms):
                                   logged as a warning through Options.Logger
```

`Send` errors on individual targets are ignored in `apply`. The error `run` returns is the error of `apply` joined with the one of `Adapter.Publish` (`errors.Join`; `BroadcastOperator.run`). `apply` returns an error only for a join that skipped a socket because of `MaxRooms`, and then it wraps `ErrRoomLimit`; the other sockets of the selection still joined. All targets share the same `Data` slices, which is why `engineio.Message` says the caller must not modify `Data` after `Send`.

### 3.8 Disconnect and close

`Socket.leave(reason)` is the single exit for a socket. Only the first call proceeds (`registry.remove` reports whether the socket was connected). It then does, in this order: `conn.forget`, close and delete every pending ack channel under `Socket.mu`, and `mailbox.finish` of the disconnect task, which reads `onDisconnect` and runs it. `finish` queues that task behind the tasks already queued and seals the mailbox: nothing is queued after it, so no handler of the socket runs after `OnDisconnect` (section 4.3).

| Trigger | Path | Reason given to `OnDisconnect` |
|---|---|---|
| Client sends DISCONNECT for a namespace | `conn.route` default case: `socket.leave` | `client namespace disconnect` |
| `Socket.Disconnect(false)` | `socket.disconnect`: `leave`, and if it was the first call, send DISCONNECT | `server namespace disconnect` |
| `Socket.Disconnect(true)` | `conn.close`: `disconnect` for every socket, then `Session.Close(ForcedClose)` | `server namespace disconnect` |
| Client closes the transport | polling: client sends packet `1`, `handle` -> `end(TransportClose)`. WebSocket: `readFailure` | `transport close` (a close frame, EOF, or a closed connection); otherwise `transport error` |
| Heartbeat unanswered | `ping` armed `end(PingTimeout)` | `ping timeout` |
| Protocol violation | `Receive` error, `decodePacket` error (a WebSocket text frame), `payloadPackets` error (a polling POST body: the whole body is refused with `400`, code `3`) | `parse error` |
| Write or read failure, `MaxBuffer` exceeded, overlapping polling request, aborted pending GET | `end(TransportError)` | `transport error` |
| `Server.Close` | `engine.Close`: `Session.Close(ServerShutdown)` per session | `server shutting down` |
| Connect timeout with no namespace joined, or after the last one was left | `conn.expire`: `Session.Close(ForcedClose)` | none: no socket exists |

When the session ends, `Session.notify` waits for the `Receive` in flight, then calls `conn.Closed`, which stops the connect timer for good and calls `socket.leave(Reason(reason))` for every socket still in `c.sockets`. Sockets removed earlier by `conn.close` left with `server namespace disconnect`, so `forced close` reaches `OnDisconnect` only for a socket that is added concurrently with `conn.close` (a race between a CONNECT and `Disconnect(true)`).

`Server.Close` is idempotent: `shutdown` is `sync.OnceValue(s.stop)`, and `stop` calls `engine.Close()` and then `Adapter.Close()`. `engine.Close` does not wait for clients. Nothing in this path sends a Socket.IO DISCONNECT packet; what a client sees is the Engine.IO farewell described in [protocol.md](protocol.md), section 2.8. A handshake that arrives after `Close` gets a session that `Server.open` ends at once with `server shutting down`.

## 4. Concurrency model

### 4.1 Goroutines

| Goroutine | Started by | Lives | Does |
|---|---|---|---|
| net/http handler goroutine, polling GET | net/http | until the poll is answered or the request context ends | `poll`: parks in `nextBatch` on `wake`, `done` or the request context. A cancelled context calls `abandon`, which ends a polling session with `transport error`. |
| net/http handler goroutine, polling POST | net/http | one request | `post`: reads the body, decodes all of it first (`payloadPackets`), and only if no packet is malformed calls `handle` per packet. This goroutine is the reader: `Receive` and namespace middleware run here. |
| WebSocket request goroutine | net/http | until the session ends | `serveWebSocket` then `readLoop`. It is the reader. |
| `writeLoop` | `serveWebSocket` | until the session is closed | The only writer of data frames on a WebSocket session. Swaps the queue under `mu`, writes outside it. |
| Session timer | `arm` (`time.AfterFunc`) | one at a time | Runs `ping` or `end(reason)`. See section 6. |
| Connect timer | `newConn` (`time.AfterFunc`) | one timer per connection: stopped by `connect`, restarted by `conn.forget`, stopped for good by `Closed` | `conn.expire`. |
| `notify` | `end` (only if the session was open) and `Close` | short | Waits for `receiving`, calls `Receiver.Closed`. Exactly one is started per session, because only the open-to-not-open transition starts one. |
| Mailbox worker | `mailbox.enqueue` when idle | only while tasks wait | Runs the handlers, middleware, `OnConnect` and `OnDisconnect` of one socket, one at a time. |
| Redis `publishLoop`, `receiveLoop` | `redisadapter.New`, `Subscribe` | until `Close` | See section 7. go-redis keeps its own goroutines, not described here beyond its documentation. |

`CheckOrigin` and `AllowRequest` run on HTTP handler goroutines. `Adapter.Publish` runs on the caller's goroutine (`BroadcastOperator.run`). `Receive` is serialized by `Session.receiving`, not by there being exactly one reader goroutine.

### 4.2 Locks and other synchronization

| Lock | File | Guards |
|---|---|---|
| `engineio.Server.mu` (Mutex) | `engineio/server.go` | `sessions`, `closed`. Held for a lookup, an insert, a delete, or a snapshot in `Close`. |
| `Session.receiving` (Mutex) | `engineio/session.go` | The `receiver` field, and the serialization of `Receive` against `Closed`. Held across `Receive` (so across decode, routing, `pushWait`, namespace middleware) and across `accept` in `Server.open`. |
| `Session.mu` (Mutex) | `engineio/session.go` | `state`, `transport`, `queue`, `buffered`, `conn`, `farewell`, `polling`, `posting`, `upgrading`, `flushing`, `awaitingPong`, `beat`, `timer`. Never held across I/O or a callback. |
| `conn.mu` (Mutex) | `conn.go` | `sockets`. |
| `Socket.mu` (Mutex) | `socket.go` | `handlers`, `middleware`, `onDisconnect`, `acks`, `nextAck`, `values`. |
| `registry.mu` (RWMutex), one per namespace | `rooms.go` | `sockets`, `members`, `joined`. |
| `Server.mu` (RWMutex) | `server.go` | The `namespaces` map. |
| `Namespace.mu` (RWMutex) | `namespace.go` | `middleware`, `onConnect`. |
| `mailbox.mu` (Mutex) | `mailbox.go` | `tasks`, `running`, `finished`. Never held while waiting: `pushWait` waits on channels, outside `mu`. |
| `Event.mu` (Mutex) | `event.go` | `acked`. Held across `Socket.send` in `Event.Ack`, so two calls cannot both send an answer. |

Other primitives:

- `Session.wake`: a channel of capacity 1. `signal` does a non-blocking send, so wake-ups coalesce. `Session.done` is closed once, in `end`.
- `mailbox.space`: a channel of capacity 1. `mailbox.run` does a non-blocking send on it after it takes a task off the queue. `pushWait` retries `offer` each time it receives from `space`, so a stale token costs one extra `offer`. `mailbox.done` is the `Session.Done()` channel of the connection, passed by `newSocket`: `pushWait` also returns when it closes. `mailbox.sealed` is closed by `finish`: `pushWait` also returns when it closes, and drops its task.
- `Event.acked` is a plain bool under `Event.mu` (`event.go`). `Event.Ack` sets it only after `Socket.send` succeeded, so a send that failed leaves the answer to the next call.
- `Server.shutdown` is a `sync.OnceValue` (`server.go`).
- `redisadapter.Adapter`: `subscribe` is a `sync.Once`, `workers` is a `sync.WaitGroup`, and `lifetime` is a cancelable context.

Lock order. `Session.receiving` and `Event.mu` are the only locks held while another lock is acquired. `Event.mu` is taken only by `Event.Ack`, on a handler goroutine with no other lock held, and under it `Socket.send` takes `registry.mu` and `Session.mu`, each one released before the next. Under `Session.receiving`: under it the reader takes `Session.mu`, `conn.mu`, `Socket.mu`, `registry.mu`, `Namespace.mu`, `mailbox.mu` and the `Server.mu` of both packages (`socketio.Server` for the namespace lookup, `engineio.Server` when `end` forgets the session), each one released before the next. No code path takes `receiving` while holding any other lock, and `Session.mu` is never held while `receiving` is acquired (`notify` takes `receiving` first and releases it before calling `Closed`). Every other lock is acquired and released on its own: for example `Session.end` unlocks `mu` before calling `Server.forget`, and `Socket.leave` takes `registry.mu`, `conn.mu`, `Socket.mu` and `mailbox.mu` one after another.

Two short exceptions to "lock then plain code": `receiveAck` sends on a channel of capacity 1 while holding `Socket.mu`. It deletes the entry first, so each channel receives at most one value and the send never blocks. `Socket.leave` closes and deletes the channels under the same lock, so a send on a closed channel cannot happen.

No user callback runs under a library lock, with one exception: namespace middleware (`Namespace.Use`) runs on the reader goroutine while `Session.receiving` is held (`conn.connect` -> `Namespace.admit`). `admit` recovers panics and answers with `errInternal`, text `Internal server error`; the `Options.Logger` call that reports such a panic runs under the same lock.

### 4.3 Ordering guarantees

- `Receive` calls of one session never overlap, and `Closed` is called exactly once after the last `Receive` has returned (`engineio.Receiver` doc; `Session.notify`).
- The events of one socket start in arrival order and run one at a time: every one goes through the same mailbox (`mailbox.run` pops from the front).
- `OnConnect` runs before any event handler of that socket, and `OnDisconnect` runs after the handlers of the events queued before it: `OnConnect` is a `mailbox.push` and `OnDisconnect` a `mailbox.finish` on the same FIFO (`conn.connect`, `Socket.leave`; `Socket.OnDisconnect` doc).
- No handler of a socket runs after its `OnDisconnect`. `finish` sets `finished` and queues the disconnect task under `mailbox.mu`, and `push` and `offer` (the queueing step of `pushWait`) check `finished` under the same lock, so a task is either queued before the disconnect task or dropped (`mailbox.finish`, `mailbox.push`, `mailbox.offer`).
- The messages of one `Session.Send` stay together and in order. `Send` calls are queued in the order they take `Session.mu`.
- A task that `pushWait` finds the mailbox full for, and whose connection ends meanwhile, is dropped and never runs. A task queued before that still runs. The same holds when the socket leaves while its connection lives on (`Disconnect(false)` with a full backlog): the task that `pushWait` is holding is dropped, because `finish` sealed the mailbox and the wait ends (`mailbox.pushWait`, `mailbox.finish`, `Socket.receiveEvent`, `Socket.leave`). The tasks queued before `finish` still run, then the disconnect task. `OnConnect` and `OnDisconnect` are queued without waiting and are not dropped for a full backlog; a `push` after `finish` is dropped.
- The CONNECT answer is queued before the socket joins the registry (section 3.3).
- No ordering is defined between sockets, and none between a local emit and a remote one.

### 4.4 What a handler may and may not block on

Handlers, `Socket.Use` middleware, `OnConnect` and `OnDisconnect` run on the mailbox worker. Panics there are recovered by `mailbox.call`, logged through `Server.recovered`, and the worker continues with the next task.

A handler may:

- Call `Emit`, `Ack`, `Join`, `Leave`, `Rooms`, `Set`, `Get`, broadcasts, and `Disconnect` (either form). None of them waits for the network: `Session.Send` only appends to a queue, and `Socket.leave` uses `mailbox.finish`, which never blocks. `Disconnect(true)` calls `Session.Close`, which starts `notify` on its own goroutine, so it does not wait for `receiving` (`Session.notify` comment).
- Call `Server.Close`: `engine.Close` does not wait for the clients.
- Call `EmitWithAck` with a context that has a deadline.

A handler must not:

- Block for long. While it runs, the later events of that socket wait. Once `EventBacklog` events wait, `pushWait` blocks the reader of the whole Engine.IO connection, which also stops the reads for the other namespaces of that client and the reads of its pongs. The reader is released when the session ends (`mailbox.done`), and the task it was queueing is dropped.
- Wait for an acknowledgement without a deadline. `EmitWithAck` waits for a packet that only the reader can deliver. If the mailbox is full, the reader is parked in `pushWait` waiting for this very handler to finish, so the ack is not read. A context deadline ends the wait. So does the end of the session, which a parked reader makes likely: on WebSocket it reads no pong and `ping` ends the session with `ping timeout`; on polling a second POST finds the posting slot busy and ends it with `transport error`. `end` closes `Session.done`, which releases `pushWait`. `Receive` returns, `Session.notify` takes `receiving` and calls `Closed`, `Socket.leave` closes the pending ack channels, and `EmitWithAck` returns `ErrDisconnected`. Until then the handler, the worker of that socket and the connection's reads stay stuck. This sequence runs through `mailbox.pushWait`, `Session.end`, `Session.notify`, `conn.Closed`, `Socket.leave` and `Socket.EmitWithAck`. `TestFullBacklogDoesNotKeepTheSocketOfAnEndedConnection` (`handler_test.go`) exercises it over WebSocket for a client that vanishes, a server that closes and a client that stays connected; the polling case is read from the code only. `README.md` (Production notes) tells users to give every `EmitWithAck` a deadline.

Namespace middleware is different: it runs on the reader goroutine, so blocking in it blocks every read of that connection (`Namespace.Use` doc: "it should not block for long").

A panic in `CheckOrigin` or `AllowRequest` is recovered by `engineio.protect`: the call counts as a refusal (the request gets 403 `Forbidden`) and the panic, with its stack, is logged as `engineio: callback panic` through `engineio.Options.Logger`, which `socketio.New` sets from `socketio.Options.Logger` when it is not set (`engineio/server.go`, `server.go`). `Options.Adapter` methods run without a `recover` in this library, except that the Redis adapter recovers a panic raised while it decodes or applies a received message (`Adapter.deliver`) and reports it to `redisadapter.Options.OnError`; a panic in `OnError` itself is recovered in `Adapter.report` and logged with `slog.Error` through the default logger.

## 5. Backpressure and limits

| Bound | Option and default | Enforced in | When it is hit |
|---|---|---|---|
| Inbound request or message size | `engineio.Options.MaxPayload`, 1,000,000 bytes | Polling: `Session.post`, `http.MaxBytesReader`. WebSocket: `Conn.SetReadLimit` in `probe` and `serveWebSocket`. | Polling: `end(TransportError)`, answer 413 with `{"code":3,"message":"Bad request"}`. WebSocket: gorilla/websocket sends a close message and returns `ErrReadLimit` (its `SetReadLimit` documentation); that is not a `CloseError`, so `readFailure` returns `transport error`. |
| Queued outbound bytes | `engineio.Options.MaxBuffer`, 16 MiB (`16<<20`) | `Session.Send` | If `buffered > 0 && buffered+size > MaxBuffer`: `end(TransportError)`, `Send` returns `ErrClosed`. Only `Message.Data` bytes count. One message larger than `MaxBuffer` is accepted into an empty queue. `drain` (the whole queue) resets the counter to 0; a polling batch that takes only the front of the queue (`takeFront`) subtracts the `Message.Data` bytes it took. |
| Polling GET response size | `engineio.Options.MaxPayload`, 1,000,000 bytes (the same option as for inbound data) | `Session.takeBatch`, `takeFront`, `fitting`, `packet.wireSize` | A poll answers with the first queued packets that fit in `MaxPayload` bytes as `appendPayload` writes them: separators and the base64 growth of binary packets count. At least one packet is always taken, so one packet larger than `MaxPayload` is answered alone. The rest stays queued, in order, for the next poll. |
| Concurrent sessions | `engineio.Options.MaxSessions`, 0 (no limit; zero or negative) | `Server.reserve` in `handshake` (slot returned by `forget`, or by `release` when the upgrade fails) | The handshake is answered with 503 and `{"code":4,"message":"Forbidden"}`. A request that `AllowRequest` refused did not take a slot. The cap is global, not per client: one client can open sessions until it is reached, so a per-client limit belongs in `AllowRequest` (`MaxSessions` doc in `engineio/server.go`). |
| Heartbeat | `PingInterval` 25 s, `PingTimeout` 20 s | `startHeartbeat`, `ping`, `pong` | No pong within `PingTimeout`: `end(PingTimeout)`. |
| WebSocket write | `PingTimeout` (as write deadline) | `Session.write`, `writeLoop` | A write that misses the deadline: `end(TransportError)`. |
| Graceful close | `PingTimeout` | `Session.Close` | The client has `PingTimeout` to collect the queue; then the timer runs `end(reason)`. |
| Upgrade | `UpgradeTimeout` 10 s | `Session.probe` | Read and write deadline for the probe. A miss cancels the upgrade; the session stays on polling. |
| Time without a namespace | `socketio.Options.ConnectTimeout` 45 s (zero or negative means 45 s) | `conn.expire` (timer from `newConn`, restarted by `conn.forget`) | `Session.Close(ForcedClose)` if `c.sockets` is empty when it fires. The timer runs while the session is open and has no socket: from the start of the connection until the first namespace is joined, and again from the moment the last one is left. |
| Binary attachments per packet | `socketio.Options.MaxAttachments`, 10 (zero or negative means 10) | `Decoder.Text` (`checkLimits`) | `too many attachments`: `Receive` fails, `end(ParseError)`. |
| Arguments per packet | `socketio.Options.MaxArguments`, 64 (`parser.DefaultMaxArguments`) | `Decoder.Text` (`checkLimits`, `exceedsElements`) | A received EVENT, BINARY_EVENT, ACK or BINARY_ACK whose payload array has more elements than the cap (the event name is one element) fails with `too many arguments`, which wraps `parser.ErrMalformed`: `Receive` fails, `end(ParseError)`. The check counts the separators of the outer array when the text frame arrives and does not parse the arguments. Zero or a negative value means 64 (`positiveOr`), so the cap can be raised and not turned off. The Node.js server has no such cap, so an application that sends more than 63 arguments in one event must raise `MaxArguments`. |
| Rooms per socket | `socketio.Options.MaxRooms`, 0 (no limit) | `registry.join` | `Socket.Join` returns `ErrRoomLimit` when the call would take the socket past `MaxRooms` rooms; the room named after the socket's own id is never counted, in any call. The call is all or nothing: if one room would pass the limit, the rooms the call had already entered are left again, so none of its rooms is joined (`registry.join`, `registry.count`). A join that arrives from another server logs a warning instead (section 3.7). |
| Events waiting per socket | `socketio.Options.EventBacklog`, 64 (zero or negative means 64) | `mailbox.pushWait` | The reader blocks until a task starts, or until the connection's session ends: then `pushWait` returns and the task is dropped. `pushWait` tries `offer` first, so a mailbox below the limit still queues the task after the session ended. The running task is not counted, so up to `EventBacklog` tasks wait plus one running. Lifecycle tasks (`mailbox.push`, `mailbox.finish`) never block but do count towards the backlog. |
| Redis publish backlog | `redisadapter.Options.Backlog`, 1024 | `Adapter.Publish` | `ErrBacklogFull`, returned to the caller of `Emit`, `Join`, `Leave` or `Disconnect` after the local effect. |
| Redis publish time | `redisadapter.Options.PublishTimeout`, 5 s | `publishLoop` | The PUBLISH fails and goes to `OnError`. No retry. |
| Enabled transports | `engineio.Options.Transports`, polling and WebSocket | `ServeHTTP` | `400`, code 0. |

How slow peers are handled:

```
slow client reads (outbound)
  Session.Send appends to queue; buffered grows
  buffered > 0 && buffered+size > MaxBuffer -> end(TransportError)
  WebSocket write misses PingTimeout deadline -> end(TransportError)
  polling client stops polling: no pong -> end(PingTimeout)

slow handler (inbound)
  mailbox.tasks reaches EventBacklog
  -> pushWait blocks the reader, inside Session.deliver, holding receiving
  -> the end of the session closes done: pushWait returns, task dropped
  -> WebSocket: readLoop stops calling ReadMessage
  -> polling: the POST is not answered; a second POST finds the posting
     slot busy: errOverlap -> end(TransportError)
```

Not bounded by default: the number of sessions and the number of rooms a socket joins. `engineio.Options.MaxSessions` and `socketio.Options.MaxRooms` bound them, and both are 0, meaning no limit, until set. `MaxSessions` counts all clients together; it does not stop one client from using every slot.

Not bounded by any option (read from the code, no limit appears):

- What the `MaxBuffer` count leaves out. `buffered` counts `Message.Data` bytes, not the base64 growth of binary packets in a polling payload, and not pings or the open packet. For WebSocket, a batch that `writeLoop` has already drained no longer counts either. (The size of one polling GET response is bounded separately, by `MaxPayload`, see the table.)
- How long a handler runs.

## 6. Engine.IO session state machine

```
             Close(reason)               end(reason)
   open ---------------------> closing -----------------> closed
    |                                                        ^
    +--------------------------------------------------------+
                             end(reason)
```

| From | Call | To | Effects |
|---|---|---|---|
| open | `Close(reason)` | closing | `arm(PingTimeout, end(reason))`; `go notify(reason)`; `signal`. |
| open | `end(reason)` | closed | `queue = nil`, `buffered = 0`; farewell becomes noop if the reason is `transport close`; timer stopped; `server.forget`; `done` closed; the WebSocket connection closed; `go notify(reason)`. |
| closing | `end(reason)` | closed | Same, without `notify`: the Receiver was told when `Close` ran. |
| closed | any | closed | `end` and `Close` return at once. |

`Session.Send`, `ping` and `pong` act only in `open`; `deliver` drops messages unless the session is `open`. A session that is closing therefore accepts no new messages, sends no new pings, and ignores incoming messages and pongs, but still lets the client collect what is queued. A client close packet `1` or a stray `2` still ends it at once (`Session.handle`).

How a closing session reaches closed:

- Polling: `takeBatch` serves the queue first, then answers `1` alone and calls `end(ForcedClose)`.
- WebSocket: `writeLoop` flushes the queue, writes a close frame with code 1000 (deadline `PingTimeout`), calls `end(ForcedClose)`.
- Neither happens: the timer armed by `Close` runs `end(reason)` after `PingTimeout`.

### The single timer

`Session.timer` is the only timer of a session. `arm` stops the current one, increments `beat`, starts a new `time.AfterFunc`, and the callback runs its task only if `beat` is still the value it started with, so a replaced timer never runs its task even if it was already firing. Only four places call `arm`:

| Caller | Delay | Task |
|---|---|---|
| `startHeartbeat` (from `Server.open`) | `PingInterval` | `ping` |
| `ping` | `PingTimeout` | `end(PingTimeout)` |
| `pong`, only if open and `awaitingPong` | `PingInterval` | `ping` |
| `Close` | `PingTimeout` | `end(reason)` |

`ping` queues a `2` packet and sets `awaitingPong`. A pong that was not asked for is ignored. Data traffic does not touch the timer: the server pings on its own schedule and expects a pong. A `2` sent by a client ends the session with `transport error` (`Session.handle`).

### Flags that belong to the transport

| Field | Set by | Cleared by | Meaning |
|---|---|---|---|
| `polling` | `claim` for a GET | `release` | A GET is in flight. |
| `posting` | `claim` for a POST | `release` | A POST is in flight. |
| `upgrading` | `beginUpgrade` | `cancelUpgrade`, `adopt` | A WebSocket probe is running. |
| `flushing` | `probe`, after `3probe` | `cancelUpgrade`, `adopt` | Makes the parked GET answer with a noop when the queue is empty. |
| `awaitingPong` | `ping` | `pong` | A ping is outstanding. |

`takeBatch` decides what a GET answers, in this order: session closed gives the farewell packet (`1`, or noop `6` if the end reason was `transport close`); transport is no longer polling gives noop; a non-empty queue gives the packets at its front that fit in `MaxPayload` (`takeFront`: at least one, the rest stays queued for the next poll; the whole queue is drained when it all fits); closing gives `1` alone; `flushing` gives noop; otherwise it waits. The source comment for the close case says the JavaScript client acts on a close packet before the messages that share its payload, which is why `1` travels alone.

## 7. Rooms and broadcasting, and the adapter contract

### Registry

`registry` (`rooms.go`) is one per namespace, behind one `RWMutex`:

- `sockets`: id to `*Socket`. A socket is connected exactly while it is here (`Socket.Connected` is `rooms.has`).
- `members`: room to the sockets in it.
- `joined`: socket id to the set of its rooms.

`add` also puts the socket in a room named after its own id. `join` does nothing for a socket that is not connected. The registry is built with the cap `Options.MaxRooms` (`newRegistry(limit)`; 0 means no limit): when entering a room would take the socket past `limit` rooms, `join` leaves again the rooms that call had entered and returns `ErrRoomLimit`, so a call is all or nothing. The room named after the socket's own id never counts (`registry.count`) and is never refused. A room the socket is already in is skipped, so it never counts twice. `leave` of an unjoined room does nothing. `remove` reports whether the socket was connected, which is what makes `Socket.leave` run once. An empty room is deleted.

`match(rooms, except)` returns the sockets in at least one of `rooms` (all sockets when `rooms` is empty) minus the ones in any `except` room. `Namespace.Sockets` and `BroadcastOperator.Sockets` use it, so they return local sockets only.

### BroadcastOperator

It is an immutable value: `To`, `Except` and `Local` copy it (`slices.Clip` before `append`). `Emit` encodes the packet once. `run` sets `Namespace`, `Rooms` and `Except` on the `Operation`, applies it locally with `Namespace.apply`, and publishes it through the adapter unless the operator is local or `Options.Adapter` is nil. The error it returns is the `apply` error (`ErrRoomLimit` for a join that skipped a socket) joined with the `Publish` error, so `BroadcastOperator.Join` can return both. `Socket.Broadcast` is `namespace.Except(s.id)`, and `Socket.To` is `Broadcast().To`.

### The `Adapter` contract (`adapter.go`)

```go
type Adapter interface {
	Publish(Operation) error
	Subscribe(apply func(Operation))
	Close() error
}
```

- `Publish` sends an operation to the other servers and must not block on the network. It is called on the caller's goroutine after the local apply, so a slow `Publish` slows the caller.
- `Subscribe` is called once, from `New`, with `Server.applyRemote`. The adapter chooses the goroutine it calls `apply` on. Everything `apply` does is non-blocking (`Session.Send`, registry operations, `mailbox.finish`, `Session.Close`).
- `Close` is called from `Server.Close` after `engine.Close()`.
- The adapter must not hand a server the operations that server published: the server already applied them locally (`Adapter` doc).
- Whoever can publish can act on every client, so the transport must be reachable by servers only (`Adapter` doc; `README.md` says the same of the Redis channel).

`applyRemote` rules: an unknown namespace is ignored and never created. For `OpEmit` only, the packet must be non-nil and of type EVENT or BINARY_EVENT, and its namespace is overwritten with the local namespace's name. `OpJoin`, `OpLeave` and `OpDisconnect` from a remote are applied without further checks. An error from `Namespace.apply` (a join that skipped a socket because of `MaxRooms`) is logged as a warning through `Options.Logger` ("socketio: operation of another server not fully applied") and is not reported to the other server.

### Redis adapter structure (`redisadapter/adapter.go`, `wire.go`)

```
caller goroutine                    adapter goroutines          Redis
  Publish: wire.encode(op)
   encoding error: returned
   -> closed: ErrClosed
   -> outbox (chan, cap Backlog)
      full: ErrBacklogFull
                                    publishLoop
                                     PUBLISH channel payload,
                                     PublishTimeout ------------->
                                     error -> OnError (if not closed)
                                    receiveLoop  <------ PSUBSCRIBE
                                                         <key>#*
                                                         <key>-request#*
                                     deliver -> wire.decode
                                      error -> OnError, skipped
                                      own uid, or a message this adapter
                                      does not act on: skipped
                                      else apply(op)
                                      panic -> OnError, skipped
```

The messages are those of the Node.js `@socket.io/redis-adapter` 8 with its default MessagePack parser, so servers written in Go and in Node.js that share a Redis server and a `Key` form one cluster. The channels and the bytes are in [protocol.md](protocol.md), section 7, and so are the parts of the Node.js adapter that are not implemented.

`Options.Key` defaults to `socket.io`, which is also the default of the Node.js adapter, so every server that keeps the default on one Redis server joins the same cluster, in any database: Pub/Sub ignores the database number, and two applications on one Redis server need two keys (`Options.Key` doc and package doc, `redisadapter/adapter.go`). A key must not contain `#`, and it must not be the other application's key followed by `-request`: the request channels of `<key>` are `<key>-request#...`, which are the emit channels of the key `<key>-request`, so the adapter of one key receives the messages of the other. The adapter of `<key>-request` reports a request of `<key>` to `OnError` as malformed, and the adapter of `<key>` ignores an emit of `<key>-request`. The Node.js adapter has the same property. Redis is a trusted boundary: whoever can publish on the channels of the key can emit any event to any client of the cluster (package doc). Who may publish is decided by the Redis deployment.

The codec builds no tree of the message. On publish, `wire.encodeBroadcast` writes `[uid, {type, data, nsp}, {rooms, except, flags}]` in that order, and `appendJSON` turns the JSON text of `data` into MessagePack while it reads that text, keeping the order of its members. On receive, `reader.broadcast` reads the same array in place: it writes the JSON text of `data` as it goes and takes `rooms` and `except` from `opts`; every other member is passed over by `skip` without being read into anything. A message whose sender is this adapter's own uid is dropped as soon as the sender is read, and the rest of the message is not read (`wire.go`, `reader.broadcast`). An emit counts only when it arrived on the channel of its own namespace, `<key>#<nsp>#`, or on `<key>#<nsp>#<room>#` when that room is the only room of the message; a channel without the prefix `<key>#/` is not read at all, and any other emit is dropped without an error (`wire.carries`, `wire.decodeBroadcast`). The attachments of an emit that comes from Go travel as `bin` values only at the offsets of `parser.Packet.Placeholders`: `appendJSON` refuses a packet whose offsets do not lead to the placeholders of its attachments, one after the other (`errAttachment`), and `reader.event` fills `Placeholders` for an emit that arrives from Redis. A message that breaks a rule is refused as a whole with an error, `deliver` passes it to `OnError`, and nothing of it is applied.

The nesting limit is 512 for the whole message, in both directions (`maxDepth`, `envelopeDepth`): the message and its packet are two levels, so `data` may nest 510. The caps of `parser.Decoder` (`MaxAttachments`, `MaxArguments`) are not applied to a message from Redis, because it does not pass through the `Decoder`; the codec has no cap on attachments. What a message costs, as measured once, is in [protocol.md](protocol.md), section 7.9.

`Close` cancels the lifetime context and waits for both loops; operations still in the outbox are dropped, and the Redis client is not closed (`New` doc: "The adapter does not close the client").

## 8. Error handling and logging

| Situation | What happens | Where |
|---|---|---|
| A packet violates the Socket.IO grammar, a second CONNECT, a client CONNECT_ERROR, or, in a namespace the client has joined, a reserved or non-string event name or a bad attachment, or more attachments or arguments than `MaxAttachments` or `MaxArguments` | `Receive` returns an error; the whole Engine.IO session ends with `parse error`. The client is not told why. The error is logged at `Warn` as `socketio: malformed packet` (`conn.reject`). | `Session.deliver` -> `end(ParseError)` |
| An Engine.IO frame or polling body does not decode: an empty one, a first character outside `0` to `6` and `b`, or a `b` followed by invalid base64 (also in a WebSocket text frame) | Same reason, and nothing is logged. Polling also answers 400 code 3, and a body with a malformed packet is refused whole: no packet of it is delivered (`payloadPackets`, two passes). | `post`, `readLoop`, `decodePacket` |
| `CheckOrigin` or `AllowRequest` panics | Recovered and counted as a refusal: 403 with code 4. The panic is logged. | `engineio.protect` |
| The accept callback of `engineio.NewServer` panics | Not recovered and not logged through `Options.Logger`. The session is ended first, which returns its slot, and the panic goes on to `net/http`, which logs it and drops the connection. The callback that `socketio.New` passes only builds the connection state and runs no application code. | `engineio.Server.open` |
| A handshake over `MaxSessions` | 503 with code 4. | `Server.handshake` |
| `Socket.Join` over `MaxRooms` | `ErrRoomLimit`, and none of the rooms of that call is joined. | `registry.join` |
| Namespace middleware returns an error | CONNECT_ERROR with `message` and, for a `*ConnectError`, `data`. The session stays open. | `conn.refuse` |
| A namespace middleware (registered with `Namespace.Use`) panics | Recovered, logged, refused with `Internal server error`. | `Namespace.admit` |
| Socket middleware returns an error, or no handler is registered | The event is dropped. No ack, no log. | `Socket.dispatch` |
| A handler, socket middleware, `OnConnect` or `OnDisconnect` panics | Recovered; the next task runs. | `mailbox.call` |
| `Emit`, `Ack` on a disconnected socket | `ErrDisconnected`. Any `conn.send` error becomes `ErrDisconnected` too. | `Socket.send` |
| Emit of a reserved event name | `parser.ErrReservedEvent`. | `parser.NewEvent` |
| An argument that `encoding/json` cannot encode | An error wrapping the argument index. | `newPayloadPacket` |
| `EmitWithAck` | The reply, `ErrDisconnected` when the socket leaves first, or the context's error. | `Socket.EmitWithAck` |
| Adapter `Publish` fails | The error is returned by the broadcast call, after the local effect. For a join it is joined with the `ErrRoomLimit` of the local apply, if any. | `BroadcastOperator.run` |
| A join from another server skips a socket because of `MaxRooms` | Logged as a warning; the other sockets of the selection still join. | `Server.applyRemote` |
| Redis background errors | Passed to `redisadapter.Options.OnError`; default is to ignore them. This includes a message that cannot be read and a panic raised while a received message is decoded or applied. | `publishLoop`, `receiveLoop`, `Adapter.deliver` |

Logging: the library has these log calls.

- `Server.recovered` calls `Options.Logger.Error("socketio: handler panic", "panic", value, "stack", string(stack))` on a `*slog.Logger` (default `slog.Default()`).
- `Server.applyRemote` calls `Options.Logger.Warn("socketio: operation of another server not fully applied", ...)` when a join from another server hit `MaxRooms`.
- `conn.reject` calls `Options.Logger.Warn("socketio: malformed packet", "error", err)` for the error that `Receive` returns, from the `Decoder` or from routing, just before the connection ends. An Engine.IO frame that does not decode is not logged.
- `engineio.protect` calls `Logger.Error("engineio: callback panic", ...)` for a panic in `CheckOrigin` or `AllowRequest`. `engineio.Options.Logger` defaults to `slog.Default()`; `socketio.New` sets it to `socketio.Options.Logger` unless `Options.Engine.Logger` is set.
- `Adapter.report` of `redisadapter` calls `slog.Error` (the default logger, not `Options.Logger`) when `OnError` itself panics. The adapter reports everything else through `OnError`.

HTTP failures from `engineio` are JSON bodies built by `fail`: `{"code":N,"message":"..."}`. The table is in [protocol.md](protocol.md), section 2.9.

## 9. Design decisions

Each entry gives the choice, the reason, and the cost. Where the source does not state a reason, the entry says so and marks anything inferred.

### Handlers replace instead of accumulate

- Choice: `Socket.On`, `Namespace.OnConnect`, `Socket.OnDisconnect` replace the previous handler (docs on each method; `README.md` lists it under "Not supported"). The handler table is `map[string]func(*Event)`.
- Reason: not stated in the source. Inferred from the code: one function per key means there is no list to order, iterate or remove from.
- Cost: two independent pieces of code cannot both listen to one event on a socket; the application composes them.

### One mailbox per socket

- Choice: every handler of a socket runs on that socket's mailbox worker, one at a time, in arrival order, and none of them after `OnDisconnect`: the disconnect task is the last one the mailbox accepts (`mailbox.go`, `finish`).
- Reason, from the source: the `mailbox` comment says an idle socket costs no goroutine, because the worker exists only while tasks wait; the `pushWait` comment says blocking the connection's reader is what slows a client that sends faster than its handlers work (`README.md` repeats this in its feature list). The `pushWait` comment also says a connection that ends meanwhile releases the reader and the task is dropped; the mailbox receives the session's `Done` channel from `newSocket` for that.
- Cost: a slow handler delays that socket's later events. A full backlog stops reads for the whole connection, including its other namespaces and its pongs, until a task starts or the session ends (section 4.4).

### Acknowledgements are resolved on the reader

- Choice: `receiveAck` hands the answer to the waiting `EmitWithAck` directly, instead of queueing it in the mailbox.
- Reason: not stated in a comment. Inferred from the code: a handler that calls `EmitWithAck` occupies the mailbox worker, so an ack queued behind it in the same mailbox could never run.
- Cost: acks bypass the per-socket ordering, and the ack path shares the reader with everything else, so it stops when the reader is parked in `pushWait` (section 4.4).

### Packets for an unjoined namespace are dropped

- Choice: `conn.route` returns `nil` when `c.socket(p.Namespace)` is nil, so the packet is ignored and the connection continues (`README.md`, "Differences from the Node.js server"). A second CONNECT and a client CONNECT_ERROR do end the connection, and so does a reserved event name in a namespace the client has joined.
- Reason: not stated in the source. Inferred: a packet can legitimately arrive after the server removed the socket or after a refused CONNECT, and ignoring it does not cost the client its other namespaces.
- Cost: a client that sends events to a namespace it never joined gets no signal.

### Redis messages of the Node.js adapter

- Choice: the Redis adapter writes and reads the messages of the Node.js `@socket.io/redis-adapter` 8 with its default MessagePack parser, and translates between them and `socketio.Operation` (`redisadapter/wire.go`, `msgpack.go`, `pack.go`, `unpack.go`). It includes its own MessagePack codec instead of a dependency. The codec turns the JSON text of `data` into MessagePack, and back, without building a tree of the value.
- Reason: not stated beyond the package comment, which says that Go and Node.js servers that share a Redis server and a key form one cluster. Observable: `go.mod` lists no MessagePack library, so the codec is in the package. The comments in `pack.go` and `msgpack.go` give the reason for the streaming design: no tree, and a depth limit so that no message can exhaust the stack of the server that decodes it. The cost measured for it is in protocol.md, section 7.9.
- Cost: only the messages that need no answer are implemented (emit, join, leave, disconnect). Every server that keeps the default `Key` on one Redis server joins the same cluster, and Redis is trusted: whoever can publish on the channels of the key can emit to every client (`redisadapter` package doc). `fetchSockets`, `allSockets`, `serverSideEmit` and an emit with an acknowledgement started on a Node.js server do not reach the Go servers, and the sharded adapter is not supported (protocol.md, section 7.6). The messages carry no version field, so they follow `@socket.io/redis-adapter` 8; other versions were not tested.

### Binary only at the top level

- Choice: when building a packet, only an argument whose type is `[]byte` travels as an attachment; everything else goes through `encoding/json` (`parser/payload.go`, `newPayloadPacket`). `README.md` states the consequence: a nested `[]byte` is base64 text. On receive, nested placeholders are inlined as base64 (`attachmentSet.inline`) so they bind to `[]byte` fields; `placeholders` finds them in one pass over the text of the argument, which is never decoded into a tree, so the memory used stays in proportion to the argument. An attachment can be referenced by one placeholder only (`attachmentSet.take`). `Packet.Placeholders` records where the attachment placeholders of a packet built here are, so that the Redis adapter can tell them from an object of the same bytes that the application passed as an argument; only `redisadapter` reads it.
- Reason: not stated in the source. Observable: the send path needs no walk of the JSON value to find binary data.
- Cost: an application cannot send a binary attachment nested inside a struct, map or slice.

## 10. Testing

What exists, by area. No counts are given on purpose. `make test` runs `go test -race -count=1 ./...` (`Makefile`).

| Area | Files | What they cover |
|---|---|---|
| Parser | `parser/decoder_test.go`, `malformed_test.go`, `packet_test.go`, `payload_test.go`, `args_test.go`, `args_bind_test.go`, `placeholder_test.go`, `arguments_test.go`, `allocs_test.go`, `fuzz_test.go`, `bench_test.go`, `helpers_test.go` | Encode and decode vectors, round trips, attachment limits, the argument cap (`MaxArguments`), namespaces, ack ids, malformed input, `Args` and nested binary binding, which object counts as a placeholder, allocation behaviour for huge payloads, a fuzz test for the decoder, benchmarks (`make bench`). |
| Engine.IO | `engineio/handshake_test.go`, `polling_test.go`, `pollbound_test.go`, `postmemory_test.go`, `takefront_test.go`, `sessions_test.go`, `callbacks_test.go`, `websocket_test.go`, `upgrade_test.go`, `heartbeat_test.go`, `lifecycle_test.go`, `send_test.go`, `cors_test.go`, `server_test.go`, `options_test.go`, `packet_test.go`, `clients_test.go`, `helpers_test.go`, `wsutil_test.go` | Open packet, rejections, transports option, `AllowRequest`, polling echo and overlap, payload limits, the bound on a polling GET response (`MaxPayload`, oversized packet alone, leftovers, buffer accounting), the memory a polling POST with many separators takes, negative option values meaning the defaults, `MaxSessions`, panics in `CheckOrigin` and `AllowRequest`, aborted poll, graceful close, upgrade success and failure cases, heartbeat, `Receiver` ordering (`Closed` waits for `Receive`, closed once under concurrent ends, close inside `Receive`), `MaxBuffer` on both transports, origin and CORS, `Server.Close`, a fuzz test for the payload decoder. |
| Root: connect | `connect_test.go`, `conn_test.go`, `connecttimer_test.go`, `protocol_log_test.go`, `client_test.go`, `support_test.go` | Connect, auth, unknown namespace, middleware refusal and chain, protocol violations and the warning logged for each, unjoined namespace dropped, connect timeout, including its restart when the last namespace is left, and the limits that `conn` hands to the decoder. `client_test.go` is a raw WebSocket test client. |
| Root: events and handlers | `event_test.go`, `ack_test.go`, `handler_test.go`, `limits_test.go`, `mailbox_test.go`, `mailbox_finish_test.go`, `socket_test.go` | Events, acks in both directions, `MaxArguments` on received events and acks, one-at-a-time ordering, a slow handler not delaying another socket, panic recovery and logging, backpressure stopping the reads of a connection. `mailbox_test.go` covers order, one task at a time, `push` never blocking, `pushWait` blocking at the limit and giving up when the connection ends. `mailbox_finish_test.go` covers `finish`: queued tasks run first, later tasks are dropped, blocked producers are released, it never blocks, and the last task runs once. |
| Root: rooms and broadcast | `registry_test.go`, `registry_limit_test.go`, `roomlimit_test.go`, `rooms_test.go`, `broadcast_test.go`, `operator_test.go` | Registry and room semantics, the room cap (`MaxRooms`, `ErrRoomLimit`) at registry level and through `Socket.Join` and `BroadcastOperator.Join`, a join from another server over the cap logged as a warning, operators, join, leave, disconnect, binary broadcast, concurrent binary emits staying paired. |
| Root: disconnect | `disconnect_test.go`, `disconnect_backlog_test.go` | Client and server disconnects, transport close, `Server.Close`, ping timeout, the disconnect handler running after earlier events, and no event handler running after `OnDisconnect`, also with a full backlog. |
| Root: cluster | `adapter_test.go`, `cluster_test.go`, `cluster_edge_test.go` | A fake in-process bus whose `Publish` hands peers a JSON round trip and never the sender; published operations, non-event packets ignored, unknown namespaces ignored, adapter subscribed once, adapter closed by `Server.Close`, a publish failure returned after the local effect. |
| Options | `options_test.go`, `options_negative_test.go`, `options_wiring_test.go`, `engine_logger_test.go` | Defaults, negative values meaning the defaults (no limit for `MaxRooms`), `MaxArguments`, `MaxRooms` and the engine logger reaching the places that use them, an engine callback panic going to the configured logger. |
| Redis | `redisadapter/adapter_test.go`, `wire_test.go`, `msgpack_test.go`, `race_on_test.go`, `race_off_test.go` | `adapter_test.go`: delivery to other nodes only, an unreadable message reported and skipped, requests reaching the other nodes, a panic on a received message reported and skipped, a cluster broadcast over Redis, backlog full, publish failure, publish after close; it uses a real Redis or an offline client, `make redis` runs it with `-race`, the tests that need a server skip when none is reachable and the offline ones always run. `wire_test.go` and `msgpack_test.go` cover the codec: the channels, the bytes of emits and requests, the rules for reading a received message, and the MessagePack writer and reader. Read the two files for the cases; this document names none of them. `race_on_test.go` and `race_off_test.go` hold `raceEnabled`, which tells the allocation tests that the race detector is on. |
| Interop | `test/interop/client.mjs`, `interop_test.go`, `cluster.mjs`, `cluster_test.go` | See below. |

Interop. `make interop` runs `npm ci` in `test/interop` and then `go test -tags interop`, which runs both tests below. `TestJavaScriptClient` runs `client.mjs` under Node.js with the real `socket.io-client` 4.8.3 (`package.json`), once each with the transports `polling`, `websocket` and `polling,websocket`, with a 40 s context. The script covers: connect and `socket.id`, the upgrade, an echo ack, binary both ways, nested binary arriving as base64, a 300,000-character payload, an ordered burst of 200 events, a server-requested ack, a room broadcast, the sender being skipped, a binary broadcast, middleware refusal in `/admin` (message `unauthorized`, data `{code: 401}`), an unknown namespace, two namespaces over one connection, a server `Disconnect`, a closed connection, and a client disconnect. The Go side expects to have seen the reasons `client namespace disconnect`, `server namespace disconnect` and `transport close`, and skips the test when `node` or `node_modules/socket.io-client` is missing.

`TestNodeCluster` (build tag `interop`, in `cluster_test.go`) needs Node.js and a Redis server at `REDIS_ADDR`, `127.0.0.1:6379` by default, and is skipped without one. It runs this server next to a real Node.js server with `socket.io` 4.8.3, `@socket.io/redis-adapter` 8.3.0 and `ioredis` 5.10.1 (pinned in `test/interop/package.json`) on one Redis, and checks emits with JSON and binary arguments, room and except selections, joins, leaves and disconnects in both directions, and that Node.js requests Go does not implement are ignored without error. The run used Node.js 26 and a Redis 8 server on the loopback interface.

Not covered:

- The official Engine.IO and Socket.IO conformance suites have not been run.
- Only `socket.io-client` 4.8.3 is exercised (`test/interop/package.json`); `README.md` says other 3.x and 4.x versions were not run.
- `Server.Close` as seen by a real JavaScript client is not part of the interop scenario. `README.md` makes a claim about what client 4.8.3 reports; it is not checked from this repository.
- The Redis tests need a reachable Redis, or they skip; the offline tests do not exercise a real server.
- Not tested: other versions of the packages and servers named above, `@socket.io/redis-emitter`, the `redis` (node-redis) client, Redis Cluster, and the sharded adapter.
