# Architecture

This document is for a Go engineer who maintains the library or debugs it on the wire. It answers three questions: which file owns a behaviour, what happens to one packet from socket to handler and back, and which lock or goroutine owns each piece of state.

Every statement below was read from the code at the time of writing and names the file and the function that implements it. Line numbers are left out on purpose, because they move. Where this document mentions how the JavaScript server or client behaves, it quotes `README.md` or a source comment and says so; it never describes Node.js behaviour as if it were this code. The wire formats are in [protocol.md](protocol.md).

Module: `github.com/nuriontech/socket.io-golang`, root package `socketio`, `go 1.24` (`go.mod`).

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
| `server.go` | `Options`, `withDefaults`, `Server`, `New`, `Of`, `Close`/`stop`, `accept`, `lookup`, `recovered` (the only place that logs). |
| `conn.go` | `conn`: one per Engine.IO session. Implements `engineio.Receiver`. Decodes frames, routes packets, runs the namespace connect sequence (`connect`, `refuse`), owns the connect timer and the per-connection socket map, builds frames (`frames`). |
| `socket.go` | `Socket`: handlers, per-socket middleware, ack table, `Emit`, `EmitWithAck`, `Join`/`Leave`, `Disconnect`, `leave`, `receiveEvent`, `dispatch`, `receiveAck`. |
| `event.go` | `Event`: arguments, `Bind`, `Ack` with the single-answer guard. |
| `mailbox.go` | `mailbox`: the per-socket serial task queue and its lazy worker goroutine; `pushWait` blocks the reader while the queue is full. |
| `namespace.go` | `Namespace`: `Use`, `OnConnect`, `admit` (runs namespace middleware), `welcome`, `apply` (applies an `Operation` to local sockets). |
| `rooms.go` | `registry`: connected sockets and room membership of one namespace. |
| `broadcast.go` | `BroadcastOperator`: immutable target selector, local apply then publish. |
| `adapter.go` | `Operation`, `OperationKind`, the `Adapter` interface, `Server.applyRemote`. |
| `errors.go` | `ErrDisconnected`, `ConnectError`, `Reason` and its constants. |
| `Makefile` | `check`, `fmt`, `vet`, `test`, `interop`, `redis`, `bench`. |
| `go.mod`, `go.sum` | Module and dependencies (`gorilla/websocket`, `go-redis/v9`). |
| `README.md` | User-facing overview. |

### Package `engineio`

| File | Owns |
|---|---|
| `doc.go` | Package documentation. |
| `packet.go` | Packet kinds `0` to `6`, `Message`, the polling payload codec (`appendPayload`, `decodePayload`) and `decodePacket`. |
| `server.go` | `Options` and defaults, `Server`, `ServeHTTP` routing, origin check and CORS headers (`allowOrigin`), handshake and `open`, the open packet, HTTP failure bodies (`fail`), `Close`, session registry. |
| `session.go` | `Session`: state machine, `Send`, `Close`, `end`, `notify`, `deliver`, `handle`, the single timer (`arm`), heartbeat. |
| `polling.go` | Long-polling transport: `poll` (GET), `post` (POST), the two slots, `nextBatch`/`takeBatch`, `abandon`. |
| `websocket.go` | WebSocket transport: upgrade from polling, `probe`, `serveWebSocket`, `adopt`, `readLoop`, `writeLoop`. |

### Package `parser`

| File | Owns |
|---|---|
| `packet.go` | Package documentation with the grammar, `Type`, `Packet` and `Encode`. |
| `decoder.go` | `Decoder` (stateful, one per connection), `decodeText`, `checkPayload`, `ErrMalformed`, `DefaultMaxAttachments`. |
| `payload.go` | `NewEvent`, `NewAck`, `ParseEvent`, `ParseAck`, reserved event names, placeholder handling, `Args` and `Bind`. |

### Package `redisadapter`

| File | Owns |
|---|---|
| `adapter.go` | `Adapter` backed by Redis Pub/Sub: options, the JSON `envelope`, `publishLoop`, `receiveLoop`. |

### Other directories

| Path | Owns |
|---|---|
| `test/interop/client.mjs` | The scenario run by the JavaScript `socket.io-client`. |
| `test/interop/interop_test.go` | Go side of the interop test (build tag `interop`). |
| `test/interop/package.json`, `package-lock.json` | Pins `socket.io-client` 4.8.3 (`package.json`, dependency entry). |
| `examples/chat/main.go`, `index.html` | A runnable one-room chat. |

## 3. Life of a connection

Names in the diagrams are the real functions.

### 3.1 Handshake and open, over polling

```
HTTP handler goroutine (GET, no sid)
  Server.ServeHTTP            allowOrigin, OPTIONS, EIO, transport checks
   -> Server.handshake        method must be GET; AllowRequest
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
       nextBatch -> takeBatch drains the queue = the open packet
       200, body = appendPayload(batch)
```

With a WebSocket handshake, `handshake` calls `upgrader.Upgrade` first, then `open(...).serveWebSocket(conn)`. The open packet is the first thing in the queue, so `writeLoop` sends it as the first frame. The request goroutine becomes the reader (`readLoop`).

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

`refuse` sends CONNECT_ERROR and returns `nil`: the Engine.IO session stays open. The connect timer is not stopped by a refusal and is never re-armed (section 5).

### 3.4 Inbound event, from socket to handler

```
client frame
  -> WebSocket readLoop | polling post        decodePacket / decodePayload
  -> Session.handle (kindMessage) -> deliver  [Session.receiving held]
  -> conn.Receive -> Decoder.Text
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
  Socket.Emit -> parser.NewEvent     JSON-encodes; []byte -> placeholder
   -> Socket.send                    not Connected: ErrDisconnected
    -> conn.send -> frames(p)        text frame + one binary frame each
     -> Session.Send(msgs...)        lock mu; MaxBuffer check; append to
                                     queue; signal(wake)
  ----------------------------------------------------------------------
polling:    the parked GET wakes in nextBatch -> takeBatch drains the queue
            -> appendPayload -> 200
WebSocket:  writeLoop wakes -> drain -> write (text frame, or binary message)
```

`Session.Send` queues all the messages of one call together; no other `Send` is interleaved with them (`Session.Send` doc). That is what keeps a binary event's text frame next to its attachments.

### 3.6 Acknowledgements

Client asks, server answers (`Event.Ack`, `event.go`):

```
conn.route -> Socket.receiveEvent: Event{ackID, wantsAck = p.HasID}
handler -> Event.Ack(args...)
  no wantsAck: returns nil, sends nothing
  parser.NewAck(ns, ackID, args)     encode failure: returned, nothing claimed
  acked.CompareAndSwap(false, true)  first call that encodes claims the answer
  Socket.send                        send failure: the claim is already spent
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
     Namespace.apply(op)
```

`Send` errors on individual targets are ignored in `apply`. The error `run` returns concerns publication only (`BroadcastOperator.run` comment). All targets share the same `Data` slices, which is why `engineio.Message` says the caller must not modify `Data` after `Send`.

### 3.8 Disconnect and close

`Socket.leave(reason)` is the single exit for a socket. Only the first call proceeds (`registry.remove` reports whether the socket was connected). It then does, in this order: `conn.forget`, close and delete every pending ack channel under `Socket.mu`, and `mailbox.push` of the disconnect task, which reads `onDisconnect` and runs it.

| Trigger | Path | Reason given to `OnDisconnect` |
|---|---|---|
| Client sends DISCONNECT for a namespace | `conn.route` default case: `socket.leave` | `client namespace disconnect` |
| `Socket.Disconnect(false)` | `socket.disconnect`: `leave`, and if it was the first call, send DISCONNECT | `server namespace disconnect` |
| `Socket.Disconnect(true)` | `conn.close`: `disconnect` for every socket, then `Session.Close(ForcedClose)` | `server namespace disconnect` |
| Client closes the transport | polling: client sends packet `1`, `handle` -> `end(TransportClose)`. WebSocket: `readFailure` | `transport close` (a close frame, EOF, or a closed connection); otherwise `transport error` |
| Heartbeat unanswered | `ping` armed `end(PingTimeout)` | `ping timeout` |
| Protocol violation | `Receive` error, `decodePayload`/`decodePacket` error | `parse error` |
| Write or read failure, `MaxBuffer` exceeded, overlapping polling request, aborted pending GET | `end(TransportError)` | `transport error` |
| `Server.Close` | `engine.Close`: `Session.Close(ServerShutdown)` per session | `server shutting down` |
| Connect timeout with no namespace joined | `conn.expire`: `Session.Close(ForcedClose)` | none: no socket exists |

When the session ends, `Session.notify` waits for the `Receive` in flight, then calls `conn.Closed`, which stops the connect timer and calls `socket.leave(Reason(reason))` for every socket still in `c.sockets`. Sockets removed earlier by `conn.close` left with `server namespace disconnect`, so `forced close` reaches `OnDisconnect` only for a socket that is added concurrently with `conn.close` (a race between a CONNECT and `Disconnect(true)`).

`Server.Close` is idempotent: `shutdown` is `sync.OnceValue(s.stop)`, and `stop` calls `engine.Close()` and then `Adapter.Close()`. `engine.Close` does not wait for clients. Nothing in this path sends a Socket.IO DISCONNECT packet; what a client sees is the Engine.IO farewell described in [protocol.md](protocol.md), section 2.8. A handshake that arrives after `Close` gets a session that `Server.open` ends at once with `server shutting down`.

## 4. Concurrency model

### 4.1 Goroutines

| Goroutine | Started by | Lives | Does |
|---|---|---|---|
| net/http handler goroutine, polling GET | net/http | until the poll is answered or the request context ends | `poll`: parks in `nextBatch` on `wake`, `done` or the request context. A cancelled context calls `abandon`, which ends a polling session with `transport error`. |
| net/http handler goroutine, polling POST | net/http | one request | `post`: reads the body, decodes, calls `handle` per packet. This goroutine is the reader: `Receive` and namespace middleware run here. |
| WebSocket request goroutine | net/http | until the session ends | `serveWebSocket` then `readLoop`. It is the reader. |
| `writeLoop` | `serveWebSocket` | until the session is closed | The only writer of data frames on a WebSocket session. Swaps the queue under `mu`, writes outside it. |
| Session timer | `arm` (`time.AfterFunc`) | one at a time | Runs `ping` or `end(reason)`. See section 6. |
| Connect timer | `newConn` (`time.AfterFunc`) | until stopped or fired | `conn.expire`. |
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
| `mailbox.mu` (Mutex) | `mailbox.go` | `tasks`, `running`. Never held while waiting: `pushWait` waits on channels, outside `mu`. |

Other primitives:

- `Session.wake`: a channel of capacity 1. `signal` does a non-blocking send, so wake-ups coalesce. `Session.done` is closed once, in `end`.
- `mailbox.space`: a channel of capacity 1. `mailbox.run` does a non-blocking send on it after it takes a task off the queue. `pushWait` retries `offer` each time it receives from `space`, so a stale token costs one extra `offer`. `mailbox.done` is the `Session.Done()` channel of the connection, passed by `newSocket`: `pushWait` also returns when it closes.
- `Event.acked` is an `atomic.Bool` (`event.go`).
- `Server.shutdown` is a `sync.OnceValue` (`server.go`).
- `redisadapter.Adapter`: `subscribe` is a `sync.Once`, `workers` is a `sync.WaitGroup`, and `lifetime` is a cancelable context.

Lock order. `Session.receiving` is the only lock held while another lock is acquired: under it the reader takes `Session.mu`, `conn.mu`, `Socket.mu`, `registry.mu`, `Namespace.mu`, `mailbox.mu` and the `Server.mu` of both packages (`socketio.Server` for the namespace lookup, `engineio.Server` when `end` forgets the session), each one released before the next. No code path takes `receiving` while holding any other lock, and `Session.mu` is never held while `receiving` is acquired (`notify` takes `receiving` first and releases it before calling `Closed`). Every other lock is acquired and released on its own: for example `Session.end` unlocks `mu` before calling `Server.forget`, and `Socket.leave` takes `registry.mu`, `conn.mu`, `Socket.mu` and `mailbox.mu` one after another.

Two short exceptions to "lock then plain code": `receiveAck` sends on a channel of capacity 1 while holding `Socket.mu`. It deletes the entry first, so each channel receives at most one value and the send never blocks. `Socket.leave` closes and deletes the channels under the same lock, so a send on a closed channel cannot happen.

No user callback runs under a library lock, with one exception: namespace middleware (`Namespace.Use`) runs on the reader goroutine while `Session.receiving` is held (`conn.connect` -> `Namespace.admit`). `admit` recovers panics and answers with `errInternal`, text `Internal server error`; the `Options.Logger` call that reports such a panic runs under the same lock.

### 4.3 Ordering guarantees

- `Receive` calls of one session never overlap, and `Closed` is called exactly once after the last `Receive` has returned (`engineio.Receiver` doc; `Session.notify`).
- The events of one socket start in arrival order and run one at a time: every one goes through the same mailbox (`mailbox.run` pops from the front).
- `OnConnect` runs before any event handler of that socket, and `OnDisconnect` runs after the handlers of the events queued before it: both are `mailbox.push` calls on the same FIFO (`conn.connect`, `Socket.leave`; `Socket.OnDisconnect` doc).
- The messages of one `Session.Send` stay together and in order. `Send` calls are queued in the order they take `Session.mu`.
- A task that `pushWait` finds the mailbox full for, and whose connection ends meanwhile, is dropped and never runs. A task queued before that still runs, and `mailbox.push` tasks (`OnConnect`, `OnDisconnect`) are never dropped (`mailbox.pushWait`, `mailbox.push`). A task that `pushWait` is still holding when the socket leaves while its connection lives on (`Disconnect(false)` with a full backlog) is not dropped either: it is queued once a slot frees, behind the disconnect task, so that one event handler runs after `OnDisconnect` (`Socket.receiveEvent`, `Socket.leave`). This is a known limitation, not a design goal.
- The CONNECT answer is queued before the socket joins the registry (section 3.3).
- No ordering is defined between sockets, and none between a local emit and a remote one.

### 4.4 What a handler may and may not block on

Handlers, `Socket.Use` middleware, `OnConnect` and `OnDisconnect` run on the mailbox worker. Panics there are recovered by `mailbox.call`, logged through `Server.recovered`, and the worker continues with the next task.

A handler may:

- Call `Emit`, `Ack`, `Join`, `Leave`, `Rooms`, `Set`, `Get`, broadcasts, and `Disconnect` (either form). None of them waits for the network: `Session.Send` only appends to a queue, and `Socket.leave` uses `mailbox.push`, which never blocks. `Disconnect(true)` calls `Session.Close`, which starts `notify` on its own goroutine, so it does not wait for `receiving` (`Session.notify` comment).
- Call `Server.Close`: `engine.Close` does not wait for the clients.
- Call `EmitWithAck` with a context that has a deadline.

A handler must not:

- Block for long. While it runs, the later events of that socket wait. Once `EventBacklog` events wait, `pushWait` blocks the reader of the whole Engine.IO connection, which also stops the reads for the other namespaces of that client and the reads of its pongs. The reader is released when the session ends (`mailbox.done`), and the task it was queueing is dropped.
- Wait for an acknowledgement without a deadline. `EmitWithAck` waits for a packet that only the reader can deliver. If the mailbox is full, the reader is parked in `pushWait` waiting for this very handler to finish, so the ack is not read. A context deadline ends the wait. So does the end of the session, which a parked reader makes likely: on WebSocket it reads no pong and `ping` ends the session with `ping timeout`; on polling a second POST finds the posting slot busy and ends it with `transport error`. `end` closes `Session.done`, which releases `pushWait`. `Receive` returns, `Session.notify` takes `receiving` and calls `Closed`, `Socket.leave` closes the pending ack channels, and `EmitWithAck` returns `ErrDisconnected`. Until then the handler, the worker of that socket and the connection's reads stay stuck. This sequence runs through `mailbox.pushWait`, `Session.end`, `Session.notify`, `conn.Closed`, `Socket.leave` and `Socket.EmitWithAck`. `TestFullBacklogDoesNotKeepTheSocketOfAnEndedConnection` (`handler_test.go`) exercises it over WebSocket for a client that vanishes, a server that closes and a client that stays connected; the polling case is read from the code only. `README.md` (Production notes) tells users to give every `EmitWithAck` a deadline.

Namespace middleware is different: it runs on the reader goroutine, so blocking in it blocks every read of that connection (`Namespace.Use` doc: "it should not block for long").

`CheckOrigin`, `AllowRequest`, `Options.Adapter` methods and `redisadapter.Options.OnError` run without a `recover` in this library.

## 5. Backpressure and limits

| Bound | Option and default | Enforced in | When it is hit |
|---|---|---|---|
| Inbound request or message size | `engineio.Options.MaxPayload`, 1,000,000 bytes | Polling: `Session.post`, `http.MaxBytesReader`. WebSocket: `Conn.SetReadLimit` in `probe` and `serveWebSocket`. | Polling: `end(TransportError)`, answer 413 with `{"code":3,"message":"Bad request"}`. WebSocket: gorilla/websocket sends a close message and returns `ErrReadLimit` (its `SetReadLimit` documentation); that is not a `CloseError`, so `readFailure` returns `transport error`. |
| Queued outbound bytes | `engineio.Options.MaxBuffer`, 16 MiB (`16<<20`) | `Session.Send` | If `buffered > 0 && buffered+size > MaxBuffer`: `end(TransportError)`, `Send` returns `ErrClosed`. Only `Message.Data` bytes count. One message larger than `MaxBuffer` is accepted into an empty queue. `drain` resets the counter to 0. |
| Heartbeat | `PingInterval` 25 s, `PingTimeout` 20 s | `startHeartbeat`, `ping`, `pong` | No pong within `PingTimeout`: `end(PingTimeout)`. |
| WebSocket write | `PingTimeout` (as write deadline) | `Session.write`, `writeLoop` | A write that misses the deadline: `end(TransportError)`. |
| Graceful close | `PingTimeout` | `Session.Close` | The client has `PingTimeout` to collect the queue; then the timer runs `end(reason)`. |
| Upgrade | `UpgradeTimeout` 10 s | `Session.probe` | Read and write deadline for the probe. A miss cancels the upgrade; the session stays on polling. |
| Time without a namespace | `socketio.Options.ConnectTimeout` 45 s | `conn.expire` (timer from `newConn`) | `Session.Close(ForcedClose)` if `c.sockets` is empty when it fires. |
| Binary attachments per packet | `socketio.Options.MaxAttachments`, 10 | `Decoder.Text` | `too many attachments`: `Receive` fails, `end(ParseError)`. |
| Events waiting per socket | `socketio.Options.EventBacklog`, 64 | `mailbox.pushWait` | The reader blocks until a task starts, or until the connection's session ends: then `pushWait` returns and the task is dropped. `pushWait` tries `offer` first, so a mailbox below the limit still queues the task after the session ended. The running task is not counted, so up to `EventBacklog` tasks wait plus one running. Lifecycle tasks (`mailbox.push`) never block but do count towards the backlog. |
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

Not bounded by any option (read from the code, no limit appears):

- The size of one polling GET response. `takeBatch` drains the whole queue. `MaxPayload` applies to inbound data only. `buffered` counts `Message.Data` bytes, not the base64 growth of binary packets, and not pings or the open packet. For WebSocket, a batch that `writeLoop` has already drained no longer counts either.
- The number of sessions, and the number of rooms a socket joins.
- How long a handler runs.
- The connect timer is armed once, in `newConn`. `connect` stops it and nothing re-arms it, so a connection that connected and then left every namespace is not closed by it; it lives as long as it answers pings.

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

`takeBatch` decides what a GET answers, in this order: session closed gives the farewell packet (`1`, or noop `6` if the end reason was `transport close`); transport is no longer polling gives noop; a non-empty queue is drained; closing gives `1` alone; `flushing` gives noop; otherwise it waits. The source comment for the close case says the JavaScript client acts on a close packet before the messages that share its payload, which is why `1` travels alone.

## 7. Rooms and broadcasting, and the adapter contract

### Registry

`registry` (`rooms.go`) is one per namespace, behind one `RWMutex`:

- `sockets`: id to `*Socket`. A socket is connected exactly while it is here (`Socket.Connected` is `rooms.has`).
- `members`: room to the sockets in it.
- `joined`: socket id to the set of its rooms.

`add` also puts the socket in a room named after its own id. `join` does nothing for a socket that is not connected. `leave` of an unjoined room does nothing. `remove` reports whether the socket was connected, which is what makes `Socket.leave` run once. An empty room is deleted.

`match(rooms, except)` returns the sockets in at least one of `rooms` (all sockets when `rooms` is empty) minus the ones in any `except` room. `Namespace.Sockets` and `BroadcastOperator.Sockets` use it, so they return local sockets only.

### BroadcastOperator

It is an immutable value: `To`, `Except` and `Local` copy it (`slices.Clip` before `append`). `Emit` encodes the packet once. `run` sets `Namespace`, `Rooms` and `Except` on the `Operation`, applies it locally with `Namespace.apply`, and publishes it through the adapter unless the operator is local or `Options.Adapter` is nil. `Socket.Broadcast` is `namespace.Except(s.id)`, and `Socket.To` is `Broadcast().To`.

### The `Adapter` contract (`adapter.go`)

```go
type Adapter interface {
	Publish(Operation) error
	Subscribe(apply func(Operation))
	Close() error
}
```

- `Publish` sends an operation to the other servers and must not block on the network. It is called on the caller's goroutine after the local apply, so a slow `Publish` slows the caller.
- `Subscribe` is called once, from `New`, with `Server.applyRemote`. The adapter chooses the goroutine it calls `apply` on. Everything `apply` does is non-blocking (`Session.Send`, registry operations, `mailbox.push`, `Session.Close`).
- `Close` is called from `Server.Close` after `engine.Close()`.
- The adapter must not hand a server the operations that server published: the server already applied them locally (`Adapter` doc).
- Whoever can publish can act on every client, so the transport must be reachable by servers only (`Adapter` doc; `README.md` says the same of the Redis channel).

`applyRemote` rules: an unknown namespace is ignored and never created. For `OpEmit` only, the packet must be non-nil and of type EVENT or BINARY_EVENT, and its namespace is overwritten with the local namespace's name. `OpJoin`, `OpLeave` and `OpDisconnect` from a remote are applied without further checks.

### Redis adapter structure (`redisadapter/adapter.go`)

```
caller goroutine                    adapter goroutines          Redis
  Publish: json.Marshal(envelope)
   -> outbox (chan, cap Backlog)
      full: ErrBacklogFull
      closed: ErrClosed
                                    publishLoop
                                     PUBLISH, PublishTimeout ------>
                                     error -> OnError (if not closed)
                                    receiveLoop  <------ Channel
                                     bad JSON -> OnError, skipped
                                     envelope.Node == own node: skipped
                                     else apply(op)
```

`Close` cancels the lifetime context and waits for both loops; operations still in the outbox are dropped, and the Redis client is not closed (`New` doc: "The adapter does not close the client"). The wire format is in [protocol.md](protocol.md), section 7.

## 8. Error handling and logging

| Situation | What happens | Where |
|---|---|---|
| A packet violates the Socket.IO grammar, a second CONNECT, a client CONNECT_ERROR, or, in a namespace the client has joined, a reserved or non-string event name or a bad attachment | `Receive` returns an error; the whole Engine.IO session ends with `parse error`. The error text is not kept or logged. | `Session.deliver` -> `end(ParseError)` |
| An Engine.IO frame or polling body does not decode | Same reason. Polling also answers 400 code 3. | `post`, `readLoop` |
| Namespace middleware returns an error | CONNECT_ERROR with `message` and, for a `*ConnectError`, `data`. The session stays open. | `conn.refuse` |
| A namespace middleware (registered with `Namespace.Use`) panics | Recovered, logged, refused with `Internal server error`. | `Namespace.admit` |
| Socket middleware returns an error, or no handler is registered | The event is dropped. No ack, no log. | `Socket.dispatch` |
| A handler, socket middleware, `OnConnect` or `OnDisconnect` panics | Recovered; the next task runs. | `mailbox.call` |
| `Emit`, `Ack` on a disconnected socket | `ErrDisconnected`. Any `conn.send` error becomes `ErrDisconnected` too. | `Socket.send` |
| Emit of a reserved event name | `parser.ErrReservedEvent`. | `parser.NewEvent` |
| An argument that `encoding/json` cannot encode | An error wrapping the argument index. | `newPayloadPacket` |
| `EmitWithAck` | The reply, `ErrDisconnected` when the socket leaves first, or the context's error. | `Socket.EmitWithAck` |
| Adapter `Publish` fails | The error is returned by the broadcast call, after the local effect. | `BroadcastOperator.run` |
| Redis background errors | Passed to `redisadapter.Options.OnError`; default is to ignore them. | `publishLoop`, `receiveLoop` |

Logging: the only log call in the library is `Server.recovered`, which calls `Options.Logger.Error("socketio: handler panic", "panic", value, "stack", string(stack))` on a `*slog.Logger` (default `slog.Default()`). `engineio` and `redisadapter` have no logger; the Redis adapter reports through `OnError`.

HTTP failures from `engineio` are JSON bodies built by `fail`: `{"code":N,"message":"..."}`. The table is in [protocol.md](protocol.md), section 2.9.

## 9. Design decisions

Each entry gives the choice, the reason, and the cost. Where the source does not state a reason, the entry says so and marks anything inferred.

### Handlers replace instead of accumulate

- Choice: `Socket.On`, `Namespace.OnConnect`, `Socket.OnDisconnect` replace the previous handler (docs on each method; `README.md` lists it under "Not supported"). The handler table is `map[string]func(*Event)`.
- Reason: not stated in the source. Inferred from the code: one function per key means there is no list to order, iterate or remove from.
- Cost: two independent pieces of code cannot both listen to one event on a socket; the application composes them.

### One mailbox per socket

- Choice: every handler of a socket runs on that socket's mailbox worker, one at a time, in arrival order (`mailbox.go`).
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

### Go-only Redis wire format

- Choice: the Redis message is a JSON `envelope{node, op}` whose `op` is `socketio.Operation` as it is (`redisadapter/adapter.go`). The package comment states it is not compatible with the Node.js `@socket.io/redis-adapter`.
- Reason: not stated beyond that comment. Observable in the code: the adapter publishes the library's own `Operation` type with no translation layer.
- Cost: a Go node and a Node.js node cannot share a channel. The envelope has no version field.

### Binary only at the top level

- Choice: when building a packet, only an argument whose type is `[]byte` travels as an attachment; everything else goes through `encoding/json` (`parser/payload.go`, `newPayloadPacket`). `README.md` states the consequence: a nested `[]byte` is base64 text. On receive, nested placeholders are inlined as base64 (`attachmentSet.inline`) so they bind to `[]byte` fields. An attachment can be referenced by one placeholder only (`attachmentSet.take`).
- Reason: not stated in the source. Observable: the send path needs no walk of the JSON value to find binary data.
- Cost: an application cannot send a binary attachment nested inside a struct, map or slice.

## 10. Testing

What exists, by area. No counts are given on purpose. `make test` runs `go test -race -count=1 ./...` (`Makefile`).

| Area | Files | What they cover |
|---|---|---|
| Parser | `parser/decoder_test.go`, `malformed_test.go`, `packet_test.go`, `payload_test.go`, `args_test.go`, `args_bind_test.go`, `fuzz_test.go`, `bench_test.go`, `helpers_test.go` | Encode and decode vectors, round trips, attachment limits, namespaces, ack ids, malformed input, `Args` and nested binary binding, a fuzz test for the decoder, benchmarks (`make bench`). |
| Engine.IO | `engineio/handshake_test.go`, `polling_test.go`, `websocket_test.go`, `upgrade_test.go`, `heartbeat_test.go`, `lifecycle_test.go`, `send_test.go`, `cors_test.go`, `server_test.go`, `packet_test.go`, `clients_test.go`, `helpers_test.go`, `wsutil_test.go` | Open packet, rejections, transports option, `AllowRequest`, polling echo and overlap, payload limits, aborted poll, graceful close, upgrade success and failure cases, heartbeat, `Receiver` ordering (`Closed` waits for `Receive`, closed once under concurrent ends, close inside `Receive`), `MaxBuffer` on both transports, origin and CORS, `Server.Close`, a fuzz test for the payload decoder. |
| Root: connect | `connect_test.go`, `client_test.go`, `support_test.go` | Connect, auth, unknown namespace, middleware refusal and chain, protocol violations, unjoined namespace dropped, connect timeout. `client_test.go` is a raw WebSocket test client. |
| Root: events and handlers | `event_test.go`, `ack_test.go`, `handler_test.go`, `mailbox_test.go`, `socket_test.go` | Events, acks in both directions, one-at-a-time ordering, a slow handler not delaying another socket, panic recovery and logging, backpressure stopping the reads of a connection. `mailbox_test.go` covers order, one task at a time, `push` never blocking, `pushWait` blocking at the limit and giving up when the connection ends. |
| Root: rooms and broadcast | `registry_test.go`, `rooms_test.go`, `broadcast_test.go`, `operator_test.go` | Registry and room semantics, operators, join, leave, disconnect, binary broadcast, concurrent binary emits staying paired. |
| Root: disconnect | `disconnect_test.go` | Client and server disconnects, transport close, `Server.Close`, ping timeout, the disconnect handler running after earlier events. |
| Root: cluster | `adapter_test.go`, `cluster_test.go`, `cluster_edge_test.go` | A fake in-process bus whose `Publish` hands peers a JSON round trip and never the sender; published operations, non-event packets ignored, unknown namespaces ignored, adapter subscribed once, adapter closed by `Server.Close`, a publish failure returned after the local effect. |
| Options | `options_test.go` | Defaults. |
| Redis | `redisadapter/adapter_test.go` | Delivery to other nodes only, an unreadable message reported and skipped, a cluster broadcast over Redis, backlog full, publish failure, publish after close. Uses a real Redis or an offline client; `make redis` runs it with `-race`; the three tests that need a server skip when none is reachable, and the three offline tests always run. |
| Interop | `test/interop/client.mjs`, `interop_test.go` | See below. |

Interop. `make interop` runs `npm ci` in `test/interop` and then `go test -tags interop`. `TestJavaScriptClient` runs `client.mjs` under Node.js with the real `socket.io-client` 4.8.3 (`package.json`), once each with the transports `polling`, `websocket` and `polling,websocket`, with a 40 s context. The script covers: connect and `socket.id`, the upgrade, an echo ack, binary both ways, nested binary arriving as base64, a 300,000-character payload, an ordered burst of 200 events, a server-requested ack, a room broadcast, the sender being skipped, a binary broadcast, middleware refusal in `/admin` (message `unauthorized`, data `{code: 401}`), an unknown namespace, two namespaces over one connection, a server `Disconnect`, a closed connection, and a client disconnect. The Go side expects to have seen the reasons `client namespace disconnect`, `server namespace disconnect` and `transport close`, and skips the test when `node` or `node_modules/socket.io-client` is missing.

Not covered:

- The official Engine.IO and Socket.IO conformance suites have not been run.
- Only `socket.io-client` 4.8.3 is exercised (`test/interop/package.json`); `README.md` says other 3.x and 4.x versions were not run.
- `Server.Close` as seen by a real JavaScript client is not part of the interop scenario. `README.md` makes a claim about what client 4.8.3 reports; it is not checked from this repository.
- The Redis tests need a reachable Redis, or they skip; the offline tests do not exercise a real server.
