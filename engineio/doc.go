// Package engineio implements the server side of Engine.IO protocol v4, the
// transport layer under Socket.IO.
//
// A Session is one client connection. It starts on HTTP long-polling or on
// WebSocket, and a polling session may upgrade to WebSocket without losing a
// message. The server pings every session and closes the ones that stop
// answering.
//
// The package knows nothing about Socket.IO packets: it moves opaque text and
// binary messages between a Session and the Receiver that owns it.
package engineio
