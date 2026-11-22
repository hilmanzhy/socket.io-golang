# Protocol

What this library puts on the wire and what it accepts, packet by packet. For who owns which behaviour inside the code, see [architecture.md](architecture.md).

Every example is what the code emits or accepts, derived from the function named next to it. Identifiers in examples are fake: `FAKE-ENGINE-SID` is an Engine.IO session id, `FAKE-SOCKET-ID` a Socket.IO socket id, `FAKE-NODE-ID` a Redis adapter node id. Real ids are random strings produced with `crypto/rand.Text`; this code does not fix their length, so do not depend on one.

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

A handshake is a GET without `sid` (`Server.handshake`). A non-GET gets 400 with code 2. If `Options.AllowRequest` returns an error, the answer is 403 with code 4.

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

Payload encoding (`appendPayload`, `decodePayload`):

- Packets are joined with the byte `0x1e` (record separator). There is no trailing separator.
- A text packet is its kind character plus its data.
- A binary packet is the character `b` followed by the standard base64 encoding (`encoding/base64.StdEncoding`, padded) of the bytes. There is no kind character.
- An empty chunk is malformed. A chunk starting with `b` is base64 binary. Any other chunk must start with a character from `0` to `6`.

GET, answered by `Session.poll`: the request parks in `nextBatch` until `takeBatch` has something to send. The response is 200, `text/plain; charset=UTF-8`, with the batch as the body. `takeBatch` decides in this order:

1. Session closed: the farewell packet, which is `1` normally and `6` when the session ended with reason `transport close`.
2. Transport is no longer polling: `6`.
3. The queue is not empty: the whole queue.
4. Session closing: `1` alone, then the session ends with reason `forced close`. The source comment (`engineio/polling.go`, `takeBatch`) says the JavaScript client acts on a close packet before the messages sharing its payload, which is why it is sent alone.
5. An upgrade is flushing: `6`.
6. Otherwise: wait.

If the request context ends while the GET is parked (client went away), `abandon` ends a polling session with reason `transport error`.

POST, handled by `Session.post`: the body is read through `http.MaxBytesReader` with `MaxPayload` bytes as the limit, decoded, and every packet is passed to `handle`. The success response is 200, `Content-Type: text/html`, body `ok`. Failures:

| Cause | Session | Response |
|---|---|---|
| Body longer than `MaxPayload` | ends, `transport error` | 413, code 3 |
| Other read error | ends, `transport error` | 400, code 3 |
| Body does not decode | ends, `parse error` | 400, code 3 |
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
- A text message that fails to decode ends the session with reason `parse error`. The same decoder is used as for polling chunks, so a text message that starts with `b` is read as base64 binary.
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
| 403 | 4 | Forbidden | Origin refused (`allowOrigin`), or `AllowRequest` returned an error at the handshake. |
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
- Otherwise `Options.CheckOrigin` decides. If it is nil, the request is refused. If it returns false, the request is refused.
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

One Engine.IO session can carry several namespaces. The client sends a CONNECT per namespace; the server answers each one. A namespace is known only if the application created it (`Server.Of`, or the default `/` that `New` creates); it is never created from the wire.

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

- If the client has not joined any namespace within `ConnectTimeout` (default 45 s) of the session being created, the server closes the session (`conn.expire`, `Session.Close` with `forced close`). The timer is stopped by the first successful join and is not armed again.

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
- `Event.Ack` sends at most one answer per event: the first call whose arguments can be encoded claims it. A later call does nothing, even if the first call's send failed.
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
- A top-level placeholder `{"_placeholder":true,"num":N}` becomes a binary argument. `Args.Bind` binds it to a `*[]byte` or a `*any`.
- A placeholder nested inside another JSON value is replaced by the base64 text of the attachment (`attachmentSet.inline`), so it binds into a `[]byte` struct field.
- A placeholder whose index is outside the attachments is malformed ("attachment out of range"). A placeholder whose `num` is not a JSON number is malformed ("invalid attachment reference").
- An attachment may be referenced once. A second placeholder that points at the same index is malformed ("attachment referenced twice", `attachmentSet.take`).
- Placeholders are ignored when a packet has no attachments. Attachments that no placeholder refers to are not rejected: `parseArgs` marks the ones it takes but never checks that all were taken.
- A binary event name (a placeholder at index 0) is not a string and is rejected.

When the server sends, a `[]byte` nested in a struct, map or slice is not an attachment: `encoding/json` writes it as base64 text (`README.md`, "Differences from the Node.js server").

### 3.6 Errors ending the connection

A packet that violates the rules ends the whole Engine.IO session with reason `parse error`. The server does not send a CONNECT_ERROR or a DISCONNECT first, and the error text is not sent to the client or logged by the library (`Session.deliver` discards it).

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
| Engine.IO kind | `0` to `6`, or `b` | `decodePacket` | Malformed: `parse error`. |
| Socket.IO type | `0` to `6` | `decodeText` | Malformed: `parse error`. |
| Attachment count | `Options.MaxAttachments`, default 10 | `Decoder.Text`, checked when the text frame arrives | "too many attachments": `parse error`. The count itself must fit 31 bits when parsed. |
| Acknowledgement id | Must fit a `uint64` | `decodeText` | "invalid acknowledgement id". |
| Payload shape | By type (section 3.2) | `checkPayload` | `parse error`. |
| Event name | A string, not reserved | `ParseEvent` | `parse error`. The check runs only for a joined namespace. |
| Attachment index | Within the attachments of the packet, each referenced at most once | `parseArgs`, `attachmentSet.take` | "attachment out of range" or "attachment referenced twice": `parse error`. |

No other size limit is applied while parsing: for example, nothing in `parser` caps the number of arguments or the size of one attachment beyond what `MaxPayload` allows at the transport.

## 5. Deviations

### 5.1 Listed in the README

Each row follows one item of the README section "Differences from the Node.js server", in the order of that list, and restates it against the code. Reasons are quoted from the README or the source where they exist; where neither states one, this document says so.

| README item | Behaviour | Code | Reason |
|---|---|---|---|
| 1 | Only a top-level `[]byte` argument becomes an attachment. A nested one is base64 text. A nested placeholder received from a client is inlined as base64. | `parser/payload.go`: `newPayloadPacket`, `attachmentSet.inline` | Not stated in the source. |
| 2 | A packet other than CONNECT and CONNECT_ERROR for an unjoined namespace is ignored. A second CONNECT and a client CONNECT_ERROR end the connection with `parse error`, and so does a reserved event name in a joined namespace. | `conn.route`, `conn.connect`, `ParseEvent` | Not stated in the source. |
| 3 | An event with no handler, or one that `Socket.Use` middleware rejects, is dropped without an ACK. | `Socket.dispatch` | Not stated in the source. |
| 4 | Namespace middleware runs before the socket is connected; its error message and `ConnectError.Data` go to the client. | `Namespace.admit`, `conn.refuse` | Stated in `Namespace.Use` doc: the socket cannot emit or join yet. |
| 5 | `Server.Close()` ends every session. README states what the JavaScript client 4.8.3 then reports. | `Server.stop`, `engineio.Server.Close` | The client claim is the README's, not verified here. The code sends no Socket.IO DISCONNECT on `Server.Close`. |
| 6 | The Redis wire format is this library's own. | `redisadapter/adapter.go` | Stated in the package doc: not compatible with the Node.js `@socket.io/redis-adapter`. |
| 7 | With the JavaScript client, `socket.emitWithAck()` resolves with only the first acknowledgement argument. | None: client behaviour | README says "checked in `socket.io-client` 4.8.3". This server sends and returns every argument. |

The "Not supported" list of `README.md` is in section 6.

### 5.2 Behaviours specific to this code

For each, the reference behaviour is not checked in this repository.

- An event name must be a JSON string. `2[null]` and `2[1,"x"]` are rejected as malformed and, in a joined namespace, end the session. (`parser/payload.go`, `ParseEvent`.)
- An acknowledgement id is accepted on every packet type. (`decodeText`.)
- A client ACK without an id, or with an id that no `EmitWithAck` is waiting for, is ignored. (`Socket.receiveAck`.)
- The `parse error` detail is not sent to the client and not logged. (`Session.deliver`.)
- Server-side ack ids are per socket and start at 0. (`Socket.expectAck`.)
- The socket id sent in the CONNECT answer is a fresh random id, not the Engine.IO session id. (`newSocket`.)
- A refused namespace join sends CONNECT_ERROR and keeps the session. (`conn.refuse`.)
- The server answers a preflight after the origin check and before the `EIO` check. Same-origin means equal hosts, ignoring the scheme. (`Server.ServeHTTP`, `sameOrigin`.)
- A WebSocket request that is not a valid upgrade gets gorilla's plain-text error, not the JSON error body. (Section 2.7.)
- A second concurrent GET or POST on one session ends the session. (`Session.claim`.)
- A client ping (`2`) ends the session with `transport error`. (`Session.handle`.)
- The polling GET that is pending during an upgrade receives the queued messages first, and `6` only when the queue is empty. (`takeBatch`.)
- `MaxBuffer` is checked as `buffered > 0 && buffered+size > MaxBuffer`, counting only `Message.Data` bytes. One message larger than `MaxBuffer` is accepted into an empty queue. Exceeding it ends the session with `transport error`. (`Session.Send`.)
- A WebSocket text message starting with `b` is decoded as base64 binary, because it shares the decoder used for polling chunks. (`decodePacket`.)
- A polling GET response is not bounded by `MaxPayload`: that option applies to inbound data only. (`takeBatch`.)
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
- Wire compatibility with the Node.js `@socket.io/redis-adapter`
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

The adapter (`redisadapter/adapter.go`) publishes every `socketio.Operation` another node must apply, over Redis Pub/Sub on one channel (`Options.Channel`, default `socket.io-golang`). Each message is the JSON encoding (`encoding/json`) of an envelope:

| Field | Type | Meaning |
|---|---|---|
| `node` | string | The id of the publishing adapter: a random string made with `crypto/rand.Text` when the adapter is created. |
| `op` | object | The `Operation`. |

There is no version field. A receiving adapter skips any message whose `node` equals its own id (`receiveLoop`), because the publishing server has already applied the operation locally.

`Operation` (`adapter.go`), in the order the fields are written; every field except `kind` and `nsp` is omitted when empty:

| Field | Type | Meaning |
|---|---|---|
| `kind` | string | `emit`, `join`, `leave` or `disconnect`. |
| `nsp` | string | Target namespace. |
| `rooms` | array of strings | Select sockets in any of these rooms; absent selects all. |
| `except` | array of strings | Remove sockets in any of these rooms. |
| `packet` | object | For `emit`: a `parser.Packet`. |
| `targets` | array of strings | For `join` and `leave`: the rooms to join or leave. |
| `close` | bool | For `disconnect`: also close the connection. |

`packet` (`parser.Packet`): `type` (the number of the packet type), `nsp`, `id` and `hasId` (omitted when zero or false), `data` (the payload as raw JSON, with placeholders), `attachments` (an array of base64 strings, omitted when none).

Examples. The first is a broadcast to the room `lobby`:

```text
{"node":"FAKE-NODE-ID","op":{"kind":"emit","nsp":"/","rooms":["lobby"],"packet":{"type":2,"nsp":"/","data":["chat","u1","hi"]}}}
```

A binary event, which carries its attachment inline as base64:

```text
{"node":"FAKE-NODE-ID","op":{"kind":"emit","nsp":"/","rooms":["lobby"],"packet":{"type":5,"nsp":"/","data":["file",{"_placeholder":true,"num":0}],"attachments":["AQID"]}}}
```

A broadcast that skips a room. `Socket.Broadcast` is `Namespace.Except` with the socket's own id, because every socket is in a room named after its id:

```text
{"node":"FAKE-NODE-ID","op":{"kind":"emit","nsp":"/","except":["muted"],"packet":{"type":2,"nsp":"/","data":["chat","u1","hi"]}}}
{"node":"FAKE-NODE-ID","op":{"kind":"emit","nsp":"/","except":["FAKE-SOCKET-ID"],"packet":{"type":2,"nsp":"/","data":["chat","u1","hi"]}}}
```

Make the sockets of `lobby` join `vip` on every node, and disconnect all sockets with their connections closed:

```text
{"node":"FAKE-NODE-ID","op":{"kind":"join","nsp":"/","rooms":["lobby"],"targets":["vip"]}}
{"node":"FAKE-NODE-ID","op":{"kind":"disconnect","nsp":"/","close":true}}
```

What a receiving node does (`Server.applyRemote`):

- A namespace that does not exist on this node is ignored, and never created.
- For `emit`, the packet must be present and of type EVENT or BINARY_EVENT; anything else is ignored. The packet's namespace is overwritten with the local namespace name.
- `join`, `leave` and `disconnect` are applied without further checks.
- Then `Namespace.apply` runs the operation against the matching local sockets.

Delivery and failure behaviour, as the code stands:

- `Publish` marshals the envelope, returns `ErrClosed` if the adapter is closed, and otherwise puts the bytes on a buffered queue of `Options.Backlog` entries (default 1024), returning `ErrBacklogFull` instead of blocking when it is full.
- One goroutine (`publishLoop`) sends each queued message with `PUBLISH`, with a context of `Options.PublishTimeout` (default 5 s). A failure is passed to `Options.OnError` unless the adapter is closed. There is no retry.
- `Close` stops the loops and waits for them; messages still queued are dropped. It does not close the Redis client.
- A message that does not unmarshal is passed to `OnError` and skipped.
- Delivery is at most once. The go-redis documentation for `PubSub.Channel` says a message is dropped when the channel stays full for one minute, and for `Client.Subscribe` that it does not wait for Redis to answer.
- The channel is trusted: whoever can publish to it can act on every client of every node (`README.md`, "Scaling across nodes with Redis", and the `Adapter` documentation).

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
