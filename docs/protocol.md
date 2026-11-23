# Protocol

What this library puts on the wire and what it accepts, packet by packet. For who owns which behaviour inside the code, see [architecture.md](architecture.md).

Every example is what the code emits or accepts, derived from the function named next to it. Identifiers in examples are fake: `FAKE-ENGINE-SID` is an Engine.IO session id, `FAKE-SOCKET-ID` a Socket.IO socket id, `FAKE-UID` a Redis adapter uid. Real ids are random strings produced with `crypto/rand.Text`; this code does not fix their length, so do not depend on one.

Statements about the JavaScript server or client are quoted from `README.md` and labelled as such. This repository does not contain their source, and nothing here asserts their behaviour on its own authority.

## 1. Versions and reference specs

| | |
|---|---|
| Socket.IO protocol | v5 (`README.md`, opening paragraph; `parser` package doc) |
| Engine.IO protocol | v4 (`engineio/server.go`, `protocolVersion = "4"`) |
| Engine.IO reference | https://socket.io/docs/v4/engine-io-protocol/ |
| Socket.IO reference | https://socket.io/docs/v4/socket-io-protocol/ (revision 5) |

What has been verified, and what has not:

- `test/interop` runs one scenario with the real JavaScript `socket.io-client` 4.8.3 (pinned in `test/interop/package.json`) over three transport settings: polling only, WebSocket only, and polling upgraded to WebSocket (`README.md`, "Compatibility"; `test/interop/interop_test.go`).
- `test/interop` also has `TestNodeCluster` (build tag `interop`, run by `make interop`; it needs Node.js and a Redis server at `REDIS_ADDR`, `127.0.0.1:6379` by default, and is skipped without one). It runs this server next to a real Node.js server with `socket.io` 4.8.3, `@socket.io/redis-adapter` 8.3.0 and `ioredis` 5.10.1 (pinned in `test/interop/package.json`) on one Redis, and checks emits with JSON and binary arguments, room and except selections, joins, leaves and disconnects in both directions, and that Node.js requests Go does not implement are ignored without error. The codec has its own tests in `redisadapter/msgpack_test.go` and `redisadapter/wire_test.go`. Section 7 describes the messages. Not tested: other versions of these, `@socket.io/redis-emitter`, the `redis` (node-redis) client, Redis Cluster and the sharded adapter.
- The official Engine.IO and Socket.IO conformance suites have NOT been run against this code.
- No other client version has been run. `README.md` ("Compatibility") says other 3.x and 4.x versions "speak the same protocols but were not run".

Where this document says the reference behaviour is "not checked in this repository", it means exactly that: there is no test or source here that compares it.

## 2. Engine.IO on the wire

### 2.1 URL and query

The server is an `http.Handler` (`engineio.Server.ServeHTTP`). It does not look at the path; the application chooses where to mount it (`README.md` mounts it at `/socket.io/`). It reads three query parameters:

| Parameter | Meaning |
|---|---|
| `EIO` | Must be exactly `4`, otherwise 400 with code 5. |
| `transport` | `polling` or `websocket`, and the transport must be enabled in `Options.Transports` (default: both), otherwise 400 with code 0. |
| `sid` | Absent: this is a handshake. Present: must name a live session, otherwise 400 with code 1. |

Order of checks in `ServeHTTP`:

```
1 origin           allowOrigin       refused: 403 code 4
2 OPTIONS          any query         answered: 204, no body
3 EIO != "4"                         400 code 5
4 transport not enabled              400 code 0
5 no sid                             handshake
6 sid unknown                        400 code 1
7 transport=websocket                upgrade
8 otherwise                          servePolling
```

Because the origin check comes first, a refused origin gets 403 even for a request that would also fail later checks. Because the OPTIONS answer comes before the `EIO` check, a preflight needs no query at all.

### 2.2 Handshake

A handshake is a GET without `sid` (`Server.handshake`). A non-GET gets 400 with code 2. If `Options.AllowRequest` returns an error, the answer is 403 with code 4. A panic in `AllowRequest` is recovered, logged to `engineio.Options.Logger` (`slog.Default()` when nil; `socketio.New` sets a nil engine logger to its own `Options.Logger`) and counts as a refusal, so the answer is the same 403. Then the server claims a session slot: when `Options.MaxSessions` is greater than zero and that many sessions exist, the answer is 503 with code 4 and the body `{"code":4,"message":"Forbidden"}`. The default, zero, means no limit. The checks run in that order: method, `AllowRequest`, session slot.

Over polling the response is 200, `Content-Type: text/plain; charset=UTF-8`, and the body is the open packet, which is the first thing in the new session's queue:

```text
GET /socket.io/?EIO=4&transport=polling

200 OK
0{"sid":"FAKE-ENGINE-SID","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}
```

The body is built by `Server.openPacket`. The leading `0` is the packet kind (open); the rest is JSON:

| Field | Value |
|---|---|
| `sid` | The session id. |
| `upgrades` | `["websocket"]` when the handshake transport is polling and WebSocket is enabled; `[]` otherwise. |
| `pingInterval` | `Options.PingInterval` in milliseconds (default 25 s). |
| `pingTimeout` | `Options.PingTimeout` in milliseconds (default 20 s). |
| `maxPayload` | `Options.MaxPayload` in bytes (default 1,000,000). |

A handshake that asks for `transport=websocket` is upgraded right away by `Server.handshake`. The open packet is then the first text frame the server writes, with `"upgrades":[]`:

```text
GET /socket.io/?EIO=4&transport=websocket        (WebSocket upgrade request)

[text frame] 0{"sid":"FAKE-ENGINE-SID","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}
```

### 2.3 Packet types

Engine.IO packets are identified by one character (`engineio/packet.go`):

| Kind | Name | Sent by the server | Handled when received (`Session.handle`) |
|---|---|---|---|
| `0` | open | in the handshake | ignored |
| `1` | close | at the end of a graceful close (polling) | the session ends with reason `transport close` |
| `2` | ping | every `PingInterval` | the session ends with reason `transport error` |
| `3` | pong | never | accepted only while a ping is outstanding; otherwise ignored |
| `4` | message | carries one Socket.IO frame | passed to the Socket.IO layer |
| `5` | upgrade | never | ignored, except as the last probe step (section 2.7) |
| `6` | noop | to release a parked poll | ignored |

A Socket.IO text frame travels as an Engine.IO message: the character `4` followed by the Socket.IO frame. A Socket.IO binary attachment travels as an Engine.IO binary message, with no kind character (section 2.4 and 2.5). Throughout this document, a frame written as `42["greet","hi"]` is the Engine.IO message `4` plus the Socket.IO packet `2["greet","hi"]`.

Note the difference between the Engine.IO close packet `1` and the Engine.IO message `41`: `41` is a message (`4`) carrying a Socket.IO DISCONNECT (`1`) and leaves the session alive.

### 2.4 Polling

Two request kinds exist per session: GET (the long poll) and POST (client to server). At most one of each may be in flight; a second request for a busy slot ends the session with reason `transport error` and is answered 400 with code 3 (`Session.claim`). A request for a session that has already moved to WebSocket is answered 400 with code 3 and does not end the session.

Payload encoding (`appendPayload`, `payloadPackets`):

- Packets are joined with the byte `0x1e` (record separator). There is no trailing separator.
- A text packet is its kind character plus its data.
- A binary packet is the character `b` followed by the standard base64 encoding (`encoding/base64.StdEncoding`, padded) of the bytes. There is no kind character.
- An empty chunk is malformed. A chunk starting with `b` is base64 binary. Any other chunk must start with a character from `0` to `6`.

GET, answered by `Session.poll`: the request parks in `nextBatch` until `takeBatch` has something to send. The response is 200, `text/plain; charset=UTF-8`, with the batch as the body. `takeBatch` decides in this order:

1. Session closed: the farewell packet, which is `1` normally and `6` when the session ended with reason `transport close`.
2. Transport is no longer polling: `6`.
3. The queue is not empty: the first queued packets that fit in `MaxPayload` (`takeFront`, `fitting`). The size counts each packet as `wireSize` (the kind character, or `b`, plus the data, base64-encoded for a binary packet) and one byte for each separator. A response always holds at least one packet, so a single packet larger than `MaxPayload` is sent alone. The packets that do not fit stay queued, in order, for the next poll.
4. Session closing: `1` alone, then the session ends with reason `forced close`. The source comment (`engineio/polling.go`, `takeBatch`) says the JavaScript client acts on a close packet before the messages sharing its payload, which is why it is sent alone.
5. An upgrade is flushing: `6`.
6. Otherwise: wait.

If the request context ends while the GET is parked (client went away), `abandon` ends a polling session with reason `transport error`.

POST, handled by `Session.post`: the body is read through `http.MaxBytesReader` with `MaxPayload` bytes as the limit, decoded, and every packet is passed to `handle`. The body is decoded in two passes: the first checks every packet, and only when all of them decode does the second hand them to `handle` one by one. A body with one malformed packet is refused whole, so none of its other packets is delivered. The success response is 200, `Content-Type: text/html`, body `ok`. Failures:

| Cause | Session | Response |
|---|---|---|
| Body longer than `MaxPayload` | ends, `transport error` | 413, code 3 |
| Other read error | ends, `transport error` | 400, code 3 |
| Body does not decode: any packet inside is malformed, and none of the body is delivered | ends, `parse error` | 400, code 3 |
| A packet inside is rejected by the Socket.IO layer | ends, `parse error` | still 200 `ok`: the verdict is not part of the response |

Example of a client sending one Socket.IO packet and one binary attachment in one POST:

```text
POST /socket.io/?EIO=4&transport=polling&sid=FAKE-ENGINE-SID

451-["file",{"_placeholder":true,"num":0}]<0x1e>bAQID
```

`bAQID` is the base64 of the three bytes `01 02 03`. The response is `200 text/html` with the body `ok`. The server's own GET response for a binary event has the same shape.

### 2.5 WebSocket

Each Engine.IO packet is one WebSocket message (`engineio/websocket.go`):

- A text message carries a kind character plus data: `2`, `3`, `4...`, `5`, `6`, `1`.
- A binary message carries the raw bytes of one attachment, with no kind character. The server delivers it to the Socket.IO layer as a binary message. It writes binary messages the same way.
- The read limit is `MaxPayload` (`Conn.SetReadLimit`). A larger message makes gorilla/websocket send a close message and return `ErrReadLimit`; that is not a clean close, so the session ends with reason `transport error` (`readFailure`).
- A text message is decoded like a polling chunk (`decodePacket`, section 2.4). One that fails to decode ends the session with reason `parse error`: an empty message, a first character outside `0` to `6` and `b`, or a `b` followed by text that is not valid padded base64.
- A text message that starts with `b` is base64 binary, as in polling: the server delivers the decoded bytes to the Socket.IO layer as a binary message, the same as a WebSocket binary message. A JavaScript client created with `forceBase64: true` sends its binary packets this way.
- The server writes a binary packet as the text message `b<base64>`, the polling form, instead of a binary message when the request that opened the WebSocket carries a non-empty `b64` query parameter, for example `b64=1` (`base64Requested` in `engineio/websocket.go`). That is the request of the handshake, or the upgrade request when the session upgrades from polling. When the parameter is repeated, the last value counts. Without it, or with an empty value, the server writes binary messages.
- A read that ends with a close frame, EOF, or a closed connection ends the session with reason `transport close`. Any other read error ends it with `transport error`.
- A write has a deadline of `PingTimeout` per batch; a missed deadline ends the session with reason `transport error`.

### 2.6 Heartbeat

Only the server initiates (`Session.startHeartbeat`, `ping`, `pong`):

1. When the session is created, a timer is armed for `PingInterval`.
2. When it fires, the server queues a `2` packet, marks a pong as expected, and arms a timer for `PingTimeout`.
3. A `3` from the client, if one is expected, arms the `PingInterval` timer again.
4. If the `PingTimeout` timer fires first, the session ends with reason `ping timeout`.

A `3` that was not asked for is ignored. A `2` sent by the client ends the session with reason `transport error`. Data traffic does not move the timer. On polling, the ping is delivered in the next GET response, so an idle long poll is answered at the next ping (`README.md`, "Production notes").

### 2.7 Upgrade from polling to WebSocket

`Server.upgrade` handles a WebSocket request that names a live session. `Session.beginUpgrade` requires the session to be open, on polling, and not already upgrading; otherwise the answer is 400 with code 3 and the session is untouched. Then gorilla/websocket completes the handshake, and `Session.probe` runs:

```text
C -> S   GET ...&transport=polling&sid=FAKE-ENGINE-SID     (already parked)
C -> S   GET ...&transport=websocket&sid=FAKE-ENGINE-SID   (Upgrade request)
S -> C   WebSocket handshake completes (written by gorilla/websocket)
C -> S   [text] 2probe       server expects exactly this text message
S -> C   [text] 3probe
S -> C   200 on the parked GET, body 6
                             the server has set the flushing flag; the poll
                             answers with the queued messages first if any
C -> S   [text] 5            server expects exactly this text message
                             adopt: transport is now WebSocket, writeLoop starts
```

Details:

- `probe` sets a read limit of `MaxPayload` and a read and write deadline of `UpgradeTimeout` (default 10 s).
- If the first message is not the text `2probe`, or the last is not the text `5`, or a deadline passes, the upgrade is cancelled. The probe connection is closed and the session stays on polling. No packet is sent to say why.
- The session tracks the probe connection (`Session.track`). If the session ends during the probe, the connection is closed at once instead of staying open until `UpgradeTimeout`. If the session has already ended when the probe starts, `probe` fails and the connection is closed.
- The parked GET is answered with `6`, or with the queued messages if the queue is not empty (`takeBatch`, rules 3 and 5).
- `adopt` fails if the session ended during the probe; the server then closes the connection.
- After `adopt`, only `writeLoop` writes data frames, and the request goroutine reads.

A WebSocket request that is not a valid upgrade (missing `Connection` or `Upgrade` header token, non-GET method, bad `Sec-WebSocket-Version` or `Sec-WebSocket-Key`) is refused by gorilla/websocket v1.5.3 (`server.go`, `Upgrader.Upgrade`) with a plain-text error body, not the JSON body of section 2.9: 400 for a missing header token or a bad version or key, and 405 for a method other than GET. The server's upgrader has no custom error function. `engineio` test `TestUpgradeWithoutWebSocketHeaders` expects 400. The 405 is taken from gorilla's source and is not asserted by a test I read.

### 2.8 Closing

Client closes:

- Polling: POST of the packet `1`. The session ends with reason `transport close` and the POST is answered `ok`. A GET that was parked is released with `6` (the farewell for this reason), after which the session is forgotten and any further request gets 400 with code 1.
- WebSocket: a close frame, or the connection going away, gives reason `transport close`. A text message `1` does the same.

Server closes (`Session.Close`, reached by `Socket.Disconnect(true)` or the connect timeout with reason `forced close`, and by `Server.Close` with reason `server shutting down`):

- The session turns `closing`. From then on `Session.Send` returns `ErrClosed`, no ping is queued, and incoming messages are dropped, but what is already queued is still delivered.
- Polling: the next GET returns the queued packets. The GET after that returns `1` alone. Then the session ends with reason `forced close`. If nothing is queued, the first GET returns `1` at once.
- WebSocket: `writeLoop` writes the queued packets, then a WebSocket close frame with code 1000 (write deadline `PingTimeout`), then the session ends with reason `forced close`.
- If the client does neither in time, the timer armed by `Close` ends the session after `PingTimeout`, with the reason given to `Close`.

`Server.Close` closes every session this way and sends no Socket.IO DISCONNECT. A handshake that arrives after `Server.Close` gets a session that the server ends at once: a polling handshake is answered 200 `text/plain; charset=UTF-8` with the body `1` (the open packet is dropped), and a WebSocket handshake completes the upgrade and is then closed by the server. `README.md` ("Differences from the Node.js server") says what the JavaScript client 4.8.3 does after `Server.Close()`; that is not checked in this repository, and the interop scenario does not call `Server.Close`.

### 2.9 HTTP errors

Every failure of this layer is built by `fail` (`engineio/server.go`): the status below, `Content-Type: application/json`, and the body `{"code":N,"message":"..."}`:

| HTTP | Code | Message | When |
|---|---|---|---|
| 403 | 4 | Forbidden | Origin refused (`allowOrigin`), including a panic in `CheckOrigin`; or `AllowRequest` returned an error or panicked at the handshake. |
| 503 | 4 | Forbidden | A handshake while `Options.MaxSessions` sessions are open (`Server.reserve`). Off by default. |
| 204 | none | none | `OPTIONS` that passed the origin check. No JSON body. |
| 400 | 5 | Unsupported protocol version | `EIO` is not `4`. `README.md` ("Compatibility") quotes this body. |
| 400 | 0 | Transport unknown | `transport` is not an enabled transport. |
| 400 | 2 | Bad handshake method | A handshake that is not a GET. |
| 400 | 1 | Session ID unknown | `sid` names no live session. |
| 400 | 3 | Bad request | Polling method other than GET and POST; a second GET or POST while one is in flight; a request on polling for a session that has moved to WebSocket; an unreadable or undecodable POST; an upgrade the session cannot accept. |
| 413 | 3 | Bad request | POST body over `MaxPayload`. |

Origin handling (`Server.allowOrigin`):

- A request without an `Origin` header passes.
- A request whose `Origin` host equals the request `Host` passes. Only the host is compared; the scheme is ignored. No CORS headers are added for it.
- Otherwise `Options.CheckOrigin` decides. If it is nil, the request is refused. If it returns false, the request is refused. If it panics, the panic is recovered, logged to `engineio.Options.Logger` (`slog.Default()` when nil) and the request is refused (`protect`).
- When `CheckOrigin` allows a cross-origin request, the response gets `Access-Control-Allow-Origin` set to the request origin, `Access-Control-Allow-Credentials: true`, and `Vary: Origin`. An `OPTIONS` request also gets `Access-Control-Allow-Methods: GET, POST`, `Access-Control-Max-Age: 86400`, and, if the request carried `Access-Control-Request-Headers`, `Access-Control-Allow-Headers` with the same value.

```text
OPTIONS /socket.io/
Origin: https://app.example
Access-Control-Request-Headers: content-type

204 No Content    (only if CheckOrigin returned true)
Access-Control-Allow-Origin: https://app.example
Access-Control-Allow-Credentials: true
Vary: Origin
Access-Control-Allow-Methods: GET, POST
Access-Control-Max-Age: 86400
Access-Control-Allow-Headers: content-type
```

## 3. Socket.IO on the wire

### 3.1 Grammar

From the `parser` package documentation and `parser.Packet.Encode`:

```
<type>[<attachments>-][<namespace>,][<ack id>][<JSON payload>]
```

- `<type>` is one digit.
- `<attachments>-` appears only on types 5 and 6: the number of binary frames that follow, in decimal, then `-`.
- `<namespace>,` is written when the namespace is not `/`. The default namespace is omitted.
- `<ack id>` is a decimal number, written when the packet has one.

What `Decoder.Text` (`decodeText`) accepts:

- The first byte must be `0` to `6`.
- For types 5 and 6, a decimal count up to the first `-` is required. It is parsed with `strconv.ParseUint(..., 10, 31)`.
- A namespace is present only if the next byte is `/`. It runs up to the first `,`, or to the end of the frame if there is none. So `40/admin` and `40/admin,` are both read as a CONNECT to `/admin`.
- Leading digits after that are an acknowledgement id, parsed as a `uint64`. This is done for every type, not only for EVENT and ACK.
- The remaining bytes are the payload.

Namespace names that contain a comma cannot round-trip: the decoder cuts the name at the first comma, while `Encode` writes `name,`.

### 3.2 Packet types

| Type | Name | Direction | Payload |
|---|---|---|---|
| `0` | CONNECT | client to server; server to client | Client: absent or a JSON object (the auth data). Server: `{"sid":"FAKE-SOCKET-ID"}`. |
| `1` | DISCONNECT | both | None. A payload is malformed. |
| `2` | EVENT | both | A JSON array: event name, then arguments. |
| `3` | ACK | both | A JSON array: the arguments. |
| `4` | CONNECT_ERROR | server to client | `{"message":"..."}` with an optional `"data"`. A client that sends one ends the connection with `parse error` (`conn.route`). |
| `5` | BINARY_EVENT | both | Like EVENT, with `N-` before the namespace; placeholders refer to attachments. |
| `6` | BINARY_ACK | both | Like ACK, with `N-` before the namespace. |

`checkPayload` checks the shape per type: CONNECT must have no payload or a valid JSON object; DISCONNECT must have none; CONNECT_ERROR must be valid JSON; every other type must start with `[`. Whether the array is valid JSON is checked later, by `ParseEvent` or `ParseAck`, when the packet reaches a socket that has joined the namespace.

### 3.3 Namespaces

One Engine.IO session can carry several namespaces. The client sends a CONNECT per namespace; the server answers each one. A namespace is known only if the application created it (`Server.Of`, or the default `/` that `New` creates); it is never created from the wire. A name that contains a comma is accepted by `Server.Of`, but no client can reach it, because the packet format ends the namespace at the first comma.

```text
C -> S   40                              join /
S -> C   40{"sid":"FAKE-SOCKET-ID"}
C -> S   40/admin,{"token":"abc"}        join /admin with auth data
S -> C   40/admin,{"sid":"FAKE-SOCKET-ID"}
```

The socket id in the answer is a fresh random id per namespace join. It is not the Engine.IO session id.

Refusals send CONNECT_ERROR and leave the Engine.IO session open (`conn.refuse`):

```text
C -> S   40/nope,
S -> C   44/nope,{"message":"Invalid namespace"}

C -> S   40/admin,
S -> C   44/admin,{"message":"unauthorized","data":{"code":401}}

C -> S   40                        default namespace, middleware refuses
S -> C   44{"message":"unauthorized"}
```

`message` is the text of the error returned by the namespace middleware. `data` is present only when that error is, or wraps, a `*socketio.ConnectError` with data (`errors.As`). If the answer cannot be marshalled, the server sends `{"message":"Internal server error"}`. A panic in the middleware is recovered and produces the same message.

Other namespace rules:

- A second CONNECT for a namespace the client already joined is a protocol violation: the Engine.IO session ends with reason `parse error`.
- A packet other than CONNECT for a namespace the client has not joined is dropped without an answer (`conn.route`), with one exception: a CONNECT_ERROR sent by a client ends the session with `parse error` whatever its namespace. The event name and attachment checks of `ParseEvent` run only for a namespace the client has joined.
- DISCONNECT leaves one namespace and keeps the Engine.IO session. When the client sends it, the server removes the socket with reason `client namespace disconnect` and sends nothing back (`conn.route`). When the application calls `Socket.Disconnect(false)`, the server removes the socket with reason `server namespace disconnect` and then sends it (`Socket.disconnect`):

```text
C -> S   41/chat,           client leaves /chat; the server answers nothing
S -> C   41/chat,           the application removed the socket from /chat
S -> C   41                 the same, for the default namespace
```

- If the connection has no namespace for `ConnectTimeout` (default 45 s), the server closes the session (`conn.expire`, `Session.Close` with `forced close`). The timer starts when the session is created and is stopped by a successful join. When the client leaves its last namespace while the Engine.IO session stays open, `conn.forget` starts it again, so a connection cannot idle without a namespace for longer than `ConnectTimeout`. A second namespace joined before the last one is left keeps the timer stopped.

### 3.4 Events and acknowledgement ids

```text
S -> C   42["greet","hi"]                 server emit, default namespace
S -> C   42/chat,["greet","hi"]           server emit, /chat
```

Client asks, server answers (`Event.Ack`): the client's event carries an id, and the answer repeats it:

```text
C -> S   423["echo","hi"]                 event with ack id 3
S -> C   433["hi"]                        ACK for id 3

C -> S   42/chat,3["echo","hi"]
S -> C   43/chat,3["hi"]
```

Server asks, client answers (`Socket.EmitWithAck`): the server assigns ids per socket, starting at 0 and incrementing; the client's ACK carries the same id:

```text
S -> C   420["ask","q"]                   server event, ack id 0
C -> S   430["answer"]                    client ACK for id 0
```

Behaviour of the server on ACK packets:

- An ACK without an id is ignored.
- An ACK whose id matches no waiting `EmitWithAck` is ignored, without error.
- An event that has no handler, or that `Socket.Use` middleware rejects, is dropped and no ACK is sent.
- `Event.Ack` sends at most one answer per event: the first call that hands the answer over for sending claims it. A call that fails, because its arguments cannot be encoded or because the socket is disconnected, claims nothing and leaves the answer to a later call. Once an answer has been handed over, later calls do nothing.
- The `EmitWithAck` of a socket that disconnects first returns `ErrDisconnected`.

An event name must be a JSON string. These are rejected as malformed (`ParseEvent`, "event name must be a string") and, in a namespace the client has joined, end the Engine.IO session with `parse error`:

```text
C -> S   2[null]
C -> S   2[1,"x"]
C -> S   2[]
```

How the reference implementation treats such names is not checked in this repository. The names `connect`, `connect_error`, `disconnect`, `disconnecting`, `newListener` and `removeListener` are reserved: the server refuses to emit them (`parser.ErrReservedEvent`), and a client event with one of them, in a namespace the client has joined, ends the session with `parse error` (`README.md`, "Differences from the Node.js server").

### 3.5 Binary attachments

Only an argument whose Go type is exactly `[]byte` becomes an attachment (`parser.newPayloadPacket`). The text frame carries a placeholder in its place, and each attachment follows as its own Engine.IO message, in order. One `Session.Send` queues the text frame and all of its attachments together, so no other packet falls between them.

On WebSocket:

```text
S -> C   [text]   451-["file",{"_placeholder":true,"num":0}]
S -> C   [binary] 01 02 03
```

On polling the same two messages are in one payload when the poll drains both:

```text
451-["file",{"_placeholder":true,"num":0}]<0x1e>bAQID
```

The type becomes BINARY_EVENT (`5`) or BINARY_ACK (`6`) when there is at least one attachment. With a namespace and an ack id, the order inside the frame is: type, count, `-`, namespace, comma, id, payload. Both lines below are Engine.IO messages, so each starts with the character `4`:

```text
S -> C   451-/chat,12["file",{"_placeholder":true,"num":0}]
         BINARY_EVENT, 1 attachment, namespace /chat, ack id 12
S -> C   461-3[{"_placeholder":true,"num":0}]
         BINARY_ACK, 1 attachment, default namespace, ack id 3
```

On receive (`Decoder`, `parseArgs`):

- `Decoder.Text` keeps the packet until all the announced attachments have arrived. The Decoder is per connection, so attachments do not have to arrive in one polling request.
- A placeholder is a JSON object whose member `_placeholder` is `true`; its member `num` is the index of the attachment. The members may stand in any order, next to other members, and when a name is repeated the last one counts (`placeholders`). The two names are compared as the client wrote them: a name spelled with a JSON escape or in another case does not make a placeholder. What a placeholder object holds is not looked into.
- A top-level placeholder becomes a binary argument. `Args.Bind` binds it to a `*[]byte` or a `*any`.
- A placeholder nested inside another JSON value is replaced by the base64 text of the attachment (`attachmentSet.inline`), so it binds into a `[]byte` struct field. The rest of the argument stays as the client wrote it: the argument is read once and is not decoded into a tree, so `Args.Raw` keeps its member order, its escapes and its spaces.
- A placeholder whose index is outside the attachments, or whose `num` is written with a fraction or an exponent, is malformed ("attachment out of range"). A placeholder whose `num` is missing or is not a JSON number is malformed ("invalid attachment reference").
- An attachment may be referenced once. A second placeholder that points at the same index is malformed ("attachment referenced twice", `attachmentSet.take`).
- Placeholders are ignored when a packet has no attachments. Attachments that no placeholder refers to are not rejected: `parseArgs` marks the ones it takes but never checks that all were taken.
- A binary event name (a placeholder at index 0) is not a string and is rejected.

When the server sends, a `[]byte` nested in a struct, map or slice is not an attachment: `encoding/json` writes it as base64 text (`README.md`, "Differences from the Node.js server").

### 3.6 Errors ending the connection

A packet that violates the rules ends the whole Engine.IO session with reason `parse error`. The server does not send a CONNECT_ERROR or a DISCONNECT first, and the error text is not sent to the client. When the Socket.IO layer refuses the packet, the library logs the error at Warn level through `Options.Logger`, with the message `socketio: malformed packet` and the error as the `error` attribute (`conn.reject`). An Engine.IO frame that does not decode (section 2.4 and 2.5) is not logged. `Session.deliver` itself discards the error text.

What the client sees: `Session.end` runs at once, not as a graceful close. The session is forgotten, so later polling requests get 400 with code 1. A GET that is parked is released with the farewell packet, which is `1` for this reason. On WebSocket the server closes the connection. The POST that carried the bad packet is still answered 200 `ok`.

Examples of frames that are rejected, with the message from the code:

| Frame | Rejected by | Message |
|---|---|---|
| `9` | `decodeText` | unknown packet type |
| `51["a"]` | `decodeText` | invalid attachment count |
| `2{}` | `checkPayload` | EVENT payload must be a JSON array |
| `40"x"` | `checkPayload` | CONNECT payload must be a JSON object |
| `41{}` | `checkPayload` | DISCONNECT carries no payload |
| `2["connect"]` | `ParseEvent` | reserved event name |
| `2[null]` | `ParseEvent` | event name must be a string |
| a second CONNECT for one namespace | `conn.connect` | namespace joined twice |
| a client `44{...}` | `conn.route` | malformed (CONNECT_ERROR from a client) |
| a text frame while attachments are expected | `Decoder.Text` | text frame while attachments are expected |
| a binary frame with no pending packet | `Decoder.Binary` | binary frame without a packet to attach it to |

## 4. Limits enforced while parsing

| Limit | Value | Enforced in | Effect |
|---|---|---|---|
| Polling POST body | `MaxPayload`, default 1,000,000 bytes | `Session.post` (`http.MaxBytesReader`) | 413, session ends with `transport error`. |
| WebSocket message | `MaxPayload` | `Conn.SetReadLimit` | gorilla/websocket returns `ErrReadLimit`; session ends with `transport error`. |
| Engine.IO kind | `0` to `6`, or `b` (base64 binary) in a polling chunk or a WebSocket text message; a `b` must be followed by valid padded base64 | `decodePacket` | Malformed: `parse error`. |
| Socket.IO type | `0` to `6` | `decodeText` | Malformed: `parse error`. |
| Attachment count | `Options.MaxAttachments`, default 10 | `Decoder.Text`, checked when the text frame arrives | "too many attachments": `parse error`. The count itself must fit 31 bits when parsed. |
| Acknowledgement id | Must fit a `uint64` | `decodeText` | "invalid acknowledgement id". |
| Arguments of an event or acknowledgement | `Options.MaxArguments`, default `parser.DefaultMaxArguments` (64); the event name counts as one element | `Decoder.checkLimits`, called by `Decoder.Text` when the text frame arrives, before the arguments are parsed and whether or not the namespace is joined | "too many arguments", wrapping `ErrMalformed`: `parse error`. A packet with exactly 64 elements passes and one with 65 is refused. `MaxArguments` raises the cap; zero or a negative value means 64, so it cannot be turned off. The Node.js server has no such cap. |
| Payload shape | By type (section 3.2) | `checkPayload` | `parse error`. |
| Event name | A string, not reserved | `ParseEvent` | `parse error`. The check runs only for a joined namespace. |
| Attachment index | Within the attachments of the packet, each referenced at most once | `parseArgs`, `attachmentSet.take` | "attachment out of range" or "attachment referenced twice": `parse error`. |

No other size limit is applied while parsing: for example, nothing in `parser` caps the size of one attachment beyond what `MaxPayload` allows at the transport. The argument count is counted from the JSON text without validating it (`exceedsElements`); a malformed array is rejected later by `ParseEvent` or `ParseAck`.

## 5. Deviations

### 5.1 Listed in the README

Each row follows one item of the README section "Differences from the Node.js server", in the order of that list, and restates it against the code. Reasons are quoted from the README or the source where they exist; where neither states one, this document says so.

| README item | Behaviour | Code | Reason |
|---|---|---|---|
| 1 | An event or acknowledgement a client sends is capped at 64 elements, the event name included. A packet over the cap ends the connection with `parse error`. `Options.MaxArguments` raises the cap and cannot turn it off. | `parser/decoder.go`: `DefaultMaxArguments`, `Decoder.checkLimits`, `exceedsElements`; `server.go`: `Options.MaxArguments` | The Node.js server has no such cap: `socket.io-parser` 4.2.7 throws only for too many attachments (`build/cjs/index.js:201` in `test/interop/node_modules`). An application whose clients send an event with more than 63 arguments must raise `MaxArguments`. Section 4. |
| 2 | Only a top-level `[]byte` argument becomes an attachment. A nested one is base64 text. A nested placeholder received from a client is inlined as base64. | `parser/payload.go`: `newPayloadPacket`, `attachmentSet.inline` | Not stated in the source. |
| 3 | A packet other than CONNECT and CONNECT_ERROR for an unjoined namespace is ignored. A second CONNECT and a client CONNECT_ERROR end the connection with `parse error`, and so does a reserved event name in a joined namespace. | `conn.route`, `conn.connect`, `ParseEvent` | Not stated in the source. |
| 4 | An event with no handler, or one that `Socket.Use` middleware rejects, is dropped without an ACK. | `Socket.dispatch` | Not stated in the source. |
| 5 | Namespace middleware runs before the socket is connected; its error message and `ConnectError.Data` go to the client. | `Namespace.admit`, `conn.refuse` | Stated in `Namespace.Use` doc: the socket cannot emit or join yet. |
| 6 | `Server.Close()` ends every session. README states what the JavaScript client 4.8.3 then reports. | `Server.stop`, `engineio.Server.Close` | The client claim is the README's, not verified here. The code sends no Socket.IO DISCONNECT on `Server.Close`. |
| 7 | This server bounds a polling GET response by `MaxPayload`: it carries as many queued packets as fit, and at least one. | `engineio/polling.go`: `takeBatch`, `fitting`; `engineio/session.go`: `takeFront` | `engine.io` 6.6.12 flushes its whole write buffer in one response (`build/socket.js:422-423` in `test/interop/node_modules`); this server does not. Section 2.4. |
| 8 | This server restarts the connect timeout when a client leaves its last namespace while the connection stays open. | `conn.forget`, `conn.expire` | `socket.io` 4.8.3 arms its timer once, in `Client.setup` (called by the constructor), and only clears it afterwards (`dist/client.js:52`, `103-105`, `268-270`). The source comment on `conn` states the invariant of this server: the timer runs exactly while the session is open and has no socket. Section 3.3. |
| 9 | A Go server and a Node.js server on one Redis server and key form one cluster, within limits. Delivery is at most once. | `redisadapter/wire.go`, `redisadapter/adapter.go` | The messages are those of `@socket.io/redis-adapter` 8 with the default MessagePack parser. Section 7. |
| 10 | With the JavaScript client, `socket.emitWithAck()` resolves with only the first acknowledgement argument. | None: client behaviour | README says "checked in `socket.io-client` 4.8.3". This server sends and returns every argument. |

The "Not supported" list of `README.md` is in section 6.

### 5.2 Behaviours specific to this code

For each, the reference behaviour is not checked in this repository.

- An event name must be a JSON string. `2[null]` and `2[1,"x"]` are rejected as malformed and, in a joined namespace, end the session. (`parser/payload.go`, `ParseEvent`.)
- The member names of a placeholder are compared as written, so an object whose name is spelled with a JSON escape, `"\u005fplaceholder"` for example, is an ordinary object. (`parser/payload.go`, `container.set`.)
- An acknowledgement id is accepted on every packet type. (`decodeText`.)
- A client ACK without an id, or with an id that no `EmitWithAck` is waiting for, is ignored. (`Socket.receiveAck`.)
- The `parse error` detail is not sent to the client. The Socket.IO layer logs the error of a packet it refuses, at Warn level, and an Engine.IO frame that does not decode is not logged (section 3.6). (`conn.reject`, `Session.deliver`.)
- Server-side ack ids are per socket and start at 0. (`Socket.expectAck`.)
- The socket id sent in the CONNECT answer is a fresh random id, not the Engine.IO session id. (`newSocket`.)
- A refused namespace join sends CONNECT_ERROR and keeps the session. (`conn.refuse`.)
- The server answers a preflight after the origin check and before the `EIO` check. Same-origin means equal hosts, ignoring the scheme. (`Server.ServeHTTP`, `sameOrigin`.)
- A WebSocket request that is not a valid upgrade gets gorilla's plain-text error, not the JSON error body. (Section 2.7.)
- A second concurrent GET or POST on one session ends the session. (`Session.claim`.)
- A client ping (`2`) ends the session with `transport error`. (`Session.handle`.)
- The polling GET that is pending during an upgrade receives the queued messages first, and `6` only when the queue is empty. (`takeBatch`.)
- `MaxBuffer` is checked as `buffered > 0 && buffered+size > MaxBuffer`, counting only `Message.Data` bytes. One message larger than `MaxBuffer` is accepted into an empty queue. Exceeding it ends the session with `transport error`. (`Session.Send`.)
- A text message starting with `b` is base64 binary on both transports, and the server delivers it as a binary message. With text that is not valid base64 after the `b`, the session ends with `parse error`. (`engineio/websocket.go` `readLoop`, `decodePacket`.)
- A panic in `CheckOrigin` or `AllowRequest` is recovered, logged to `engineio.Options.Logger` and treated as a refusal (403). (`protect`.)
- A handshake over `Options.MaxSessions` is answered 503 with code 4. The limit is off by default. (`Server.reserve`, `Server.handshake`.)
- An event or acknowledgement with more than `Options.MaxArguments` elements (default 64, the event name counts as one) is refused as malformed before its arguments are parsed. (`Decoder.checkLimits`.)
- After `Server.Close`, a polling handshake is answered with the body `1` and a WebSocket handshake is upgraded and then closed. (Section 2.8.)

## 6. Unsupported features

From the "Not supported" list of `README.md`, and checked against the code where noted:

- Connection state recovery
- WebTransport (the `transport` values accepted are `polling` and `websocket`; anything else is 400 with code 0)
- Engine.IO v3 clients: a request whose `EIO` is not `4` is answered 400 with code 5 (`README.md`, "Compatibility"; `Server.ServeHTTP`)
- JSONP polling
- Per-message compression
- Volatile emits
- Broadcasts with acknowledgements
- Cluster-wide socket listing: `Namespace.Sockets` and `BroadcastOperator.Sockets` return local sockets only (`rooms.match`)
- The parts of the Node.js `@socket.io/redis-adapter` that wait for an answer: `fetchSockets`, `allSockets` and `serverSideEmit` started on a Node.js server do not see Go servers, an emit with acknowledgement started on a Node.js server does not reach Go clients, and Go has no equivalent of those calls (details in section 7)
- The sharded adapter of `@socket.io/redis-adapter` (Redis 7 sharded Pub/Sub), which is a different protocol
- Dynamic (regex or function) namespaces: a namespace exists only if the application created it with `Server.Of` (`Server.lookup`)
- The `disconnecting` event: the name is in the reserved list (`parser.IsReserved`), so it cannot be emitted
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

`README.md` (opening paragraph) also says this is a server only: there is no Go client.

## 7. Redis adapter wire format

The adapter (`redisadapter/adapter.go`, `redisadapter/wire.go`, `redisadapter/pack.go`, `redisadapter/unpack.go`, `redisadapter/msgpack.go`) relays every `socketio.Operation` another server must apply, over Redis Pub/Sub. The messages are those of the Node.js `@socket.io/redis-adapter` 8 with its default parser (`notepack.io`, a MessagePack encoder), so Go and Node.js servers that share a Redis server and a key form one cluster (package doc of `redisadapter`). The limits of that cluster are in section 7.6, what has been tested in section 7.8, and what a message costs in section 7.9.

The codec is a streaming one. It never builds the message as a tree of Go values: the writer measures the JSON text of the event, checks that it is valid and then writes it as MessagePack, and the reader turns MessagePack into JSON text while it walks the bytes (`appendJSON` in `pack.go`, `reader` in `unpack.go`).

### 7.1 Key and channels

`redisadapter.Options.Key` is the prefix of every channel. It is the `key` option of the Node.js adapter, and its default is `socket.io`, which is also the default of the Node.js adapter. Servers with different keys do not see each other; servers with the same key see each other whatever database their Redis client selected, because Pub/Sub ignores the database number (`redisadapter/adapter.go`, the doc of `Options.Key` and the package doc). Two applications on one Redis server therefore need two keys. A key should not contain `#`, the character that separates the parts of a channel name: the subscription of a key also matches the channels of every longer key that starts with it and a `#`, and section 7.4 tells how an emit of such a neighbour is recognised and ignored. For the same reason, never use `<key>-request` as the key of another application next to `<key>`: the request channels of `<key>` are `<key>-request#...`, which are the emit channels of the key `<key>-request`. Each side reports the traffic of the other to `OnError` or ignores it, and no operation is applied, but the two applications share a channel. The Node.js adapter has the same property. In the table, `<nsp>` is the namespace name as written on the wire, for example `/` or `/admin`.

| Channel | Carries | Payload |
|---|---|---|
| `<key>#<nsp>#` | An emit that selects no room, or several, or none at all. | MessagePack |
| `<key>#<nsp>#<room>#` | An emit that selects exactly one room (`wire.encodeBroadcast`). | MessagePack |
| `<key>-request#<nsp>#` | A join, a leave or a disconnect. | JSON |

With the default key, an emit to the room `lobby` in the default namespace goes to `socket.io#/#lobby#` and a join request in the default namespace goes to `socket.io-request#/#`.

`Adapter.Subscribe` does one `PSUBSCRIBE` with two patterns, `<key>#*` and `<key>-request#*` (`wire.patterns`). The adapter does not subscribe to the response channels of the Node.js adapter (`<key>-response#...`) and never publishes on them.

Every message carries the `uid` of its sender: a random string made with `crypto/rand.Text` when the adapter is created. A receiver skips a message whose `uid` equals its own, because the sending server has already applied the operation locally. There is no version field.

### 7.2 Emit

The payload is a MessagePack array of three elements, `[uid, packet, opts]`:

| Element | Written by Go | Meaning |
|---|---|---|
| `uid` | string | The sender's id. |
| `packet` | map with `type`, `data`, `nsp`, in that order | `type` is always 2 (EVENT), also when the event carries binary arguments. `data` is the event name followed by its arguments. `nsp` is the namespace name. |
| `opts` | map with `rooms`, `except`, `flags`, in that order | `rooms` and `except` are arrays of strings, empty when there is no selection. `flags` is an empty map. |

Go writes the message member by member, in the order of the Node.js adapter (`wire.encodeBroadcast`), and keeps the order of the object members inside `data` as they stand in the JSON text of the event (`appendJSON`). The `data` is not decoded into a tree and encoded again. The writer walks its text once to count the elements of every array and object (`measure`), checks it with `json.Valid`, and walks it again to write. How the values are written:

- Every string, array, map and `bin` header is the shortest one that holds its length (`family.header` in `msgpack.go`), as `notepack.io` writes them.
- A JSON number is written by its value (`appendNumber`, `appendInt`). One without a fraction is an integer however it is spelled, so `1`, `1.0` and `1e2` are the integers 1, 1 and 100, each in the shortest integer form that holds it: a positive fixint, `cc`, `cd`, `ce` or `cf`, or a negative fixint, `d0`, `d1`, `d2` or `d3`. One with a fraction, or an integer that does not fit in 64 bits, is a float64 (`cb`). Go never writes a float32. An integer above 2^53 that fits in 64 bits keeps its exact value, where a JavaScript number would already have been rounded.
- A string is written as the UTF-8 text `encoding/json` decodes from its JSON form (`appendUnquoted`): a lone surrogate or a byte that is not UTF-8 becomes U+FFFD.
- A Socket.IO placeholder in `data` is replaced by the attachment it stands for, written as a MessagePack `bin` value. That is how the Node.js servers hold binary arguments, so there is no separate binary frame. Go finds the placeholders by their offsets, not by their text (`appendJSON` in `pack.go`):
  - `parser.Packet.Placeholders` holds the offset in `Data` of the placeholder of each attachment, in the order of `Attachments`. `parser.NewEvent` and `parser.NewAck` fill it (`newPayloadPacket` in `parser/payload.go`). `parser.Decoder` leaves it nil, because the placeholders of a client are found by reading `Data`. The adapter takes the offsets from the packet of the `Operation`.
  - Only the object that starts at one of those offsets is a placeholder. An object with the same bytes anywhere else, one that the application passed as an argument or that a client sent, stays an ordinary map. The attachment is written exactly where its placeholder stands.
  - Each offset must lead to the exact bytes `{"_placeholder":true,"num":N}`, where N is the number of that attachment counted from 0, written in digits, followed by `}`.
  - Offsets and attachments must match one to one, in order. `Publish` fails with `redisadapter: attachment without its placeholder` when their counts differ, when an offset does not lead to the placeholder of its attachment, or when an offset is never reached. A packet without attachments has nothing to check. The packets this library builds always satisfy the rule. Code that builds a `parser.Packet` by hand and sets `Attachments` must set `Placeholders` too.
- The text of `data` must be valid JSON (`json.Valid`), and the whole message may nest at most 512 arrays and maps. The message and its packet are two of them (`envelopeDepth`), so `data`, which is one, may hold at most 510 nested containers, itself included. A deeper `data` is an encoding error (`value nested too deeply`).

The event `chat` with the arguments `"hi"` and `{"z":1,"a":300}`, to the room `lobby` of the default namespace, from a server whose `uid` is `FAKE-UID`, goes to the channel `socket.io#/#lobby#`. The value that was encoded, as a JavaScript value:

```text
["FAKE-UID",
 {type: 2, data: ["chat", "hi", {z: 1, a: 300}], nsp: "/"},
 {rooms: ["lobby"], except: [], flags: {}}]
```

`notepack.io` 3.0.1 (`encode`, from `test/interop/node_modules`) turns that value into the bytes below, and `wire.encode` turns the same event into the same bytes, checked on a copy of the package outside the repository. Byte by byte:

```text
93                                    array of 3
   a8 46 41 4b 45 2d 55 49 44         "FAKE-UID"
   83                                 map of 3
      a4 74 79 70 65                  "type"
      02                              2
      a4 64 61 74 61                  "data"
      93                              array of 3
         a4 63 68 61 74               "chat"
         a2 68 69                     "hi"
         82                           map of 2
            a1 7a                     "z"
            01                        1
            a1 61                     "a"
            cd 01 2c                  300
      a3 6e 73 70                     "nsp"
      a1 2f                           "/"
   83                                 map of 3
      a5 72 6f 6f 6d 73               "rooms"
      91                              array of 1
         a5 6c 6f 62 62 79            "lobby"
      a6 65 78 63 65 70 74            "except"
      90                              []
      a5 66 6c 61 67 73               "flags"
      80                              {}
```

The members `z` and `a` stay in the order of the JSON text, and `300` takes the three bytes `cd 01 2c`, the shortest integer form for it.

A binary event, `["file", <bytes 01 02 03>]`, differs only in `data`, which is `92 a4 66 69 6c 65 c4 03 01 02 03`: the string `file` followed by a `bin` value of three bytes. The `type` stays 2.

A broadcast that skips a room has no room in the channel name and one entry in `except`. `Socket.Broadcast` is `Namespace.Except` with the socket's own id, because every socket is in a room named after its id:

```text
channel  socket.io#/#
value    ["FAKE-UID", {type: 2, data: ["chat","hi"], nsp: "/"}, {rooms: [], except: ["muted"], flags: {}}]
```

### 7.3 Join, leave and disconnect

The payload is a JSON object (`encoding/json`) on `<key>-request#<nsp>#`:

| Field | Type | Meaning |
|---|---|---|
| `uid` | string | The sender's id. |
| `type` | number | 2 join, 3 leave, 4 disconnect. |
| `opts` | object | The selection: `rooms` and `except`, both arrays of strings, `[]` when empty. |
| `rooms` | array of strings | For join and leave: the rooms to join or leave. Not written for a disconnect. |
| `close` | bool | For a disconnect only: also close the connection. Always written for a disconnect, `false` included. Not written for a join or a leave. |

```text
channel  socket.io-request#/#
{"uid":"FAKE-UID","type":2,"opts":{"rooms":["lobby"],"except":[]},"rooms":["vip"]}
{"uid":"FAKE-UID","type":3,"opts":{"rooms":[],"except":[]},"rooms":["vip"]}
{"uid":"FAKE-UID","type":4,"opts":{"rooms":[],"except":[]},"close":true}
```

The first line makes the sockets of `lobby` join `vip` on every server. The last disconnects every socket, closing its connection. An `opts` with an empty `rooms` selects all the sockets of the namespace.

The text is written the way `JSON.stringify` writes it: `<`, `>` and `&` stay as they are, with no HTML escaping (`wire.encodeRequest`). JSON text cannot hold a room name that is not valid UTF-8, and `encoding/json` would publish another name in its place, so `Publish` refuses a request that carries one, in `opts` or in `rooms`, with `redisadapter: room name is not UTF-8`.

### 7.4 Receiving

The adapter looks at the channel (`wire.decode`): a channel that starts with `<key>-request#` is a request, one that starts with `<key>#` is an emit, and any other is ignored. Every message that is malformed is passed to `Options.OnError` and skipped; the adapter keeps running.

Emit (`wire.decodeBroadcast`, `reader.broadcast`). The reader reads the message in place. For `data` it first passes over the value to count its `bin` values and size its buffers, then reads it again and writes the JSON text (`reader.event`). A message that breaks one of these rules is refused as a whole and passed to `OnError`; nothing of it is applied.

- The payload must be one MessagePack value. A truncated value, bytes after the value, nesting deeper than the limit below and the one type byte MessagePack leaves unused (`c1`) are errors wherever they occur. Inside `data`, an extension other than the timestamp and a map key that is not a string are errors too; a skipped member is only passed over, so it may hold them.
- The value must be an array of at least three elements. Elements after the third are skipped.
- The `uid` is read first. If it is a string equal to the receiver's own, the message is ignored at once and the rest of it is not read, so even a malformed rest raises no error. Any other `uid`, of any type, is passed over.
- `packet` must be a map. Its `type`, `data` and `nsp` members are read, and any other member is skipped. When a name occurs twice, the last occurrence wins, as it does in JavaScript.
  - `type` must be the integer 2, written as any MessagePack integer. A float 2.0 is refused. `type` and `data` are both required.
  - `data` must be a non-empty array whose first element is a string that is not a reserved event name (`parser.IsReserved`).
  - `nsp` may be absent, and then means `/`. A `nsp` that is `null` or any other non-string is refused. The namespace of an emit is the `nsp` of the packet. The emit counts only when the channel it arrived on is one a sender publishes it on (`wire.carries`): `<key>#<nsp>#`, or `<key>#<nsp>#<room>#` when `opts.rooms` holds that one room and no other. A channel that does not start with `<key>#/` is ignored before the message is read, because the name of a namespace starts with a slash. An emit on any other channel, another room's channel included, is ignored without an error: it is the traffic of a neighbour, a cluster whose key starts with this one and a `#`, or a message published on the channel of another namespace. Only a message that reads as an emit is ignored that way. A request or a response of such a neighbour arrives on a matching channel too, does not read as an emit, and is reported to `OnError` as malformed; so is a request of the key `<key>` on a server whose own key is `<key>-request`. The Node.js adapter applies the same rule, since each of its instances serves one namespace, takes only the channels of that namespace and drops a packet whose `nsp` is another one (`onmessage` in `@socket.io/redis-adapter` 8.3.0, `dist/index.js`). A key that itself holds a `#` can still be shaped to look like another key followed by a namespace, which is why a key should not contain one (section 7.1). The Node.js adapter narrows that case further: it also skips a message whose channel names a room that none of its sockets is in. Go only checks that the room of the channel is the one room of the message, and does not look at its own sockets.
- `opts` must be a map. Its `rooms` and `except` members are read and any other member, `flags` included, is skipped. Each of the two may be `null`, meaning none, or an array of strings; anything else, an array with a member that is not a string included, refuses the whole message, because dropping a room could widen the selection to every socket. The selection is `opts.rooms`. The room in the channel name is not read as a selection: it is only compared with `opts.rooms`, by the rule of the `nsp` member above.
- A `bin` value inside `data` becomes a Socket.IO placeholder, and its bytes an attachment. The offset of each placeholder in the text is kept in `Packet.Placeholders`, in the order of the attachments (`reader.event`), the same field the writer reads. A packet with at least one attachment becomes a BINARY_EVENT. Outside `data`, a `bin` value is refused where a type, a namespace or a room is expected and is passed over inside a skipped member.
- Inside `data`, the MessagePack timestamp extension (type -1, in its 4, 8 and 12 byte forms) becomes the string that `JSON.stringify` makes of the JavaScript `Date`, for example `"2025-01-02T03:04:05.000Z"`, with a signed six-digit year outside the years 0 to 9999 and `null` beyond the range of a `Date`. `NaN` and the infinities become `null`, as in `JSON.stringify`. A float32 is widened to a float64. A map in `data` becomes a JSON object whose members stay in the order of the bytes, and a string is written with the escapes `JSON.stringify` writes, without HTML escaping.
- The limit of 512 nested arrays and maps counts the message and its packet as two, as on the writing side, so `data` may nest 510 of them, itself included (`reader.enter`). Skipped members are not walked into and are not counted.
- The Go server puts no cap on the binary attachments of a message from Redis. The cap of 10 (`parser.DefaultMaxAttachments`) belongs to `parser.Decoder` and so to the packets of clients, and `Options.MaxArguments` is not applied to a message from Redis either.

Request (`wire.decodeRequest`):

- A payload that does not start with `{` is ignored without an error. Other JSON that does not unmarshal is malformed.
- A request without `opts`, or with the receiver's own `uid`, is ignored. The first case is the older per-socket form of the Node.js request.
- The types 2, 3 and 4 are applied. Any other type is ignored without an error. The Node.js adapter uses the other types for the requests that wait for an answer (`fetchSockets`, `allSockets`, `serverSideEmit`) and for an emit with an acknowledgement.
- The namespace of a request is the one in the channel name.
- `opts.rooms`, `opts.except` and `rooms` are each absent, `null` or an array of strings. An element that is not a string, `null` included, makes the whole request malformed and nothing of it is applied (`roomList.UnmarshalJSON`): `encoding/json` would read a `null` as the room with the empty name, which is another room.
- For a disconnect, `close` is true only when the JSON value is `true`. Absent, `false` and `null` mean false. A value of another type is malformed.
- The member names are matched the way `encoding/json` matches them, without regard to case, so `"UID"` reads as `uid`. JavaScript reads the exact names only; the Node.js adapter never writes another spelling, and whoever can publish on the channel is trusted anyway.

### 7.5 What a receiving server does

`Server.applyRemote` receives the operation the adapter decoded:

- A namespace that does not exist on this server is ignored, and never created.
- For an emit, the packet must be present and of type EVENT or BINARY_EVENT; anything else is ignored. The packet's namespace is set to the local namespace name.
- `Namespace.apply` then runs the operation against the matching local sockets: the sockets in any of `rooms` (all when there is none), minus the sockets in any of `except`.
- A join that skips a selected socket because it would exceed `Options.MaxRooms` (`ErrRoomLimit`) is logged as a warning through `Options.Logger`, with the message "socketio: operation of another server not fully applied". The other selected sockets still join. `MaxRooms` is off by default.

### 7.6 Mixed clusters and limits

What does not carry over between Go and Node.js servers on one key:

- `fetchSockets`, `allSockets` and `serverSideEmit` started on a Node.js server do not see the Go servers. The Node.js adapter counts the servers that answer with `PUBSUB NUMSUB` on its request channel (`serverCount` in `@socket.io/redis-adapter` 8.3.0, `dist/index.js`). Redis does not count pattern subscriptions in `NUMSUB`, and the Go adapter subscribes by pattern, so the Node.js server neither counts it nor waits for it. Among Node.js servers those calls keep working.
- An emit with an acknowledgement started on a Node.js server does not reach Go clients: the Node.js adapter sends it as a request of type 7 written in MessagePack, not JSON, and Go ignores a request payload that does not start with `{` (section 7.4).
- Go has no equivalent of those calls: it publishes no request that expects an answer and publishes no response.
- The `volatile` and `compress` flags are not carried. Go writes `flags` as an empty map and does not read it.
- A message that breaks a rule of section 7.4, for example a `rooms` or `except` that holds a value that is not a string, a `nsp` that is `null`, or a `type` that is not the integer 2, is refused by the Go server as a whole.
- The sharded adapter of `@socket.io/redis-adapter` (`createShardedAdapter`, Redis 7 sharded Pub/Sub) is a different protocol and is not supported.
- Delivery is at most once (section 7.7).

### 7.7 Delivery and failure behaviour

- `Publish` encodes the operation first and returns the encoding error: an emit without a packet, `data` that is not valid JSON, `data` nested more than 510 containers deep (section 7.2), a value too large for MessagePack (a length above 2^32-1), an attachment whose placeholder offset does not match (section 7.2), or, for a join, a leave or a disconnect, a room name that is not UTF-8 (section 7.3). It then returns `ErrClosed` if the adapter is closed, and otherwise puts the message on a buffered queue of `Options.Backlog` entries (default 1024), returning `ErrBacklogFull` instead of blocking when it is full.
- One goroutine (`publishLoop`) sends each queued message with `PUBLISH`, with a context of `Options.PublishTimeout` (default 5 s). A failure is passed to `Options.OnError` unless the adapter is closed. There is no retry.
- `Options.OnError` also receives a message that cannot be read, and a panic raised while a received message is decoded or applied (`Adapter.deliver`): that message is skipped and the adapter keeps running. A panic in `OnError` itself is logged with `slog.Error` through the default logger.
- `Close` stops the loops and waits for them; messages still queued are dropped. It does not close the Redis client.
- Delivery is at most once. The adapter sends `PSUBSCRIBE` and does not wait for Redis to confirm it, so a message published in the first moment may be missed. go-redis hands the received messages over through a channel of 100, and drops a message, with a line in its own log, when the adapter has not taken one for a minute (go-redis v9.7.3, the version in `go.mod`: `pubsub.go`, `chanSize`, `chanSendTimeout`).
- Redis is a trusted boundary. Whoever can publish on the channels of a key (`<key>#...` and `<key>-request#...`) can emit to, move between rooms and disconnect every client of every server that uses that key. Who may publish is decided by the Redis deployment, not by this library (`README.md`, "Scaling across nodes with Redis", and the `Adapter` documentation).

### 7.8 What has been tested

`test/interop` has `TestNodeCluster` (build tag `interop`, run by `make interop`; it needs Node.js and a Redis server at `REDIS_ADDR`, `127.0.0.1:6379` by default, and is skipped without one). It runs this server next to a real Node.js server with `socket.io` 4.8.3, `@socket.io/redis-adapter` 8.3.0 and `ioredis` 5.10.1 (pinned in `test/interop/package.json`) on one Redis, and checks emits with JSON and binary arguments, room and except selections, joins, leaves and disconnects in both directions, and that Node.js requests Go does not implement are ignored without error. The run used Node.js 26 and a Redis 8 server on the loopback interface.

The codec has its own tests in `redisadapter/msgpack_test.go` and `redisadapter/wire_test.go`.

Not tested: other versions of the packages above, `@socket.io/redis-emitter`, the `redis` (node-redis) client, Redis Cluster and the sharded adapter.

### 7.9 What a message costs

The codec turns only the `data` of a packet into JSON text, and it sizes its buffers before it writes into them (`appendJSON` measures the containers first; `reader.event` counts the attachments and estimates the text in a first pass). The figures below are measurements, not guarantees. They come from one run of a throwaway benchmark that is not part of the repository and that no test asserts: Apple arm64, Go 1.27.1, inputs of 0.4 to 0.8 MB, bytes allocated per operation divided by the size of the input. A different machine, Go version or input shape gives different figures.

| Operation | Allocated, relative to the input | Allocations |
|---|---|---|
| Publish an emit whose `data` is one 800 KB string and a small object | about 1.0x the size of `data` | 6, or 7 with one room and one except |
| Decode the message that publish made | about 2x the size of the message | 3, or 6 with one room and one except |

Publishing other shapes of `data` costs more. The shapes tried:

| `data` | Allocated, relative to the size of `data` | Allocations |
|---|---|---|
| 400,000 small numbers (800 KB) | about 1.0x | 6 |
| a string of 130,000 `\u0001` escapes (780 KB) | about 1.7x | 30 |
| 100,000 objects `{"a":1}` (800 KB) | about 1.5x | 6 |
| 200,000 empty arrays (600 KB) | about 2.4x | 6 |
| a string of 800,000 `[` characters (800 KB) | about 5x | 7 |

The last row is the cost of the table of container sizes: `measure` allocates it once, with four bytes for every `[` and `{` of the text, those inside strings included.

The inputs that made decoding cost most, each built by hand:

| Input | Allocated, relative to the message | Allocations |
|---|---|---|
| `data` with 400,000 empty `bin` values | about 34x | 4 |
| `data` holding a string of 800,000 control characters | about 23x | 8 |
| `rooms` holding 400,000 empty strings | about 16x | 4 |
| `data` holding 400,000 empty arrays, or 400,000 empty strings | about 8x | 5 |

These are the shapes that were tried, not a search for the worst case.

A member the codec does not read is passed over without building anything: a message of 800 KB whose `flags` held 800 KB of values allocated 240 bytes in 3 allocations. A second `data` member starts over in the buffers of the first, so a message with two of 400,000 empty arrays allocated as much as one with a single one.

The codec puts no limit of its own on the size of a message or on its attachments (section 7.4). Beyond what MessagePack itself allows (a length up to 2^32-1), the only limit it enforces on the shape of a message is the nesting depth of 512.

## 8. A complete annotated session

This transcript is constructed from the code paths named in it. It was not captured from a running server. It assumes default options, a handler for the default namespace that registers `On("echo", ...)` and answers with `event.Ack("hi")`, a client that asks for polling and then upgrades, and an application that later calls `Socket.Disconnect(true)` and has previously emitted a binary event. Server-side effects are on the lines starting with `#`.

```text
1   C -> S   GET /socket.io/?EIO=4&transport=polling
    # Server.ServeHTTP -> handshake -> open -> newSession, accept (newConn),
    # startHeartbeat (PingInterval timer), servePolling -> poll.
    S -> C   200 text/plain; charset=UTF-8
             0{"sid":"FAKE-ENGINE-SID","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}

2   C -> S   POST /socket.io/?EIO=4&transport=polling&sid=FAKE-ENGINE-SID
             40{"token":"abc"}
    # Session.post -> handle -> deliver -> conn.Receive -> route -> connect:
    # admit (no middleware), send CONNECT answer, rooms.add, push welcome.
    S -> C   200 text/html
             ok

3   C -> S   GET /socket.io/?EIO=4&transport=polling&sid=FAKE-ENGINE-SID
    # Parked in nextBatch; takeBatch drains the queued CONNECT answer.
    S -> C   200 text/plain; charset=UTF-8
             40{"sid":"FAKE-SOCKET-ID"}

4   C -> S   GET /socket.io/?EIO=4&transport=polling&sid=FAKE-ENGINE-SID
    # A new poll, parked.
    C -> S   GET /socket.io/?EIO=4&transport=websocket&sid=FAKE-ENGINE-SID
             (WebSocket upgrade request)
    # Server.upgrade -> beginUpgrade -> Upgrade -> probe.
    C -> S   [text] 2probe
    S -> C   [text] 3probe
    # probe sets flushing and signals: the parked GET is released.
    S -> C   200 on the parked GET: 6
    C -> S   [text] 5
    # adopt: transport = WebSocket. serveWebSocket starts writeLoop and runs
    # readLoop on this request goroutine.

5   C -> S   [text] 423["echo","hi"]
    # readLoop -> handle -> deliver -> conn.Receive -> Decoder.Text ->
    # route -> Socket.receiveEvent -> ParseEvent -> mailbox.pushWait.
    # On the mailbox worker: dispatch -> handler -> Event.Ack("hi") ->
    # NewAck -> Socket.send -> Session.Send; writeLoop writes it.
    S -> C   [text] 433["hi"]

6   # Application code calls socket.Emit("file", []byte{1, 2, 3}).
    S -> C   [text]   451-["file",{"_placeholder":true,"num":0}]
    S -> C   [binary] 01 02 03

7   # Heartbeat. The first ping is PingInterval (25 s) after the session was
    # created in step 1, and later ones PingInterval after each pong. This
    # step may therefore come before step 5 or 6 in real time.
    S -> C   [text] 2
    # ping: awaitingPong set, PingTimeout timer armed.
    C -> S   [text] 3
    # pong: PingInterval timer armed again.

8   # Application code calls socket.Disconnect(true) -> conn.close.
    # socket.disconnect: leave(server namespace disconnect) queues OnDisconnect
    # with that reason; sends DISCONNECT. Then Session.Close(forced close).
    S -> C   [text] 41
    # writeLoop sees stateClosing with an empty queue.
    S -> C   [close frame, code 1000]
    # Session.Close started notify, which calls Closed(forced close): it
    # finds no sockets left in the conn, so OnDisconnect is not run a second
    # time. The later end(forced close) only forgets the session.
```

If the same session had stayed on polling at step 8, the wire would instead be: the next GET returns `41`, the GET after that returns `1`, and a further request gets 400 `{"code":1,"message":"Session ID unknown"}`.
