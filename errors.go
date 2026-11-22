package socketio

import (
	"errors"

	"github.com/nuriontech/socket.io-golang/engineio"
)

// ErrDisconnected is returned when a packet is sent through a socket that is
// not connected to its namespace.
var ErrDisconnected = errors.New("socketio: socket is disconnected")

// ConnectError lets a namespace middleware refuse a connection with details.
// Message and Data are sent to the client, which receives them in its
// connect_error event.
type ConnectError struct {
	Message string
	Data    any
}

// Error returns the message sent to the client.
func (e *ConnectError) Error() string {
	return e.Message
}

// Reason tells a disconnect handler why its socket left the namespace.
type Reason string

// Reasons a socket disconnects.
const (
	ReasonTransportClose   = Reason(engineio.ReasonTransportClose)
	ReasonTransportError   = Reason(engineio.ReasonTransportError)
	ReasonPingTimeout      = Reason(engineio.ReasonPingTimeout)
	ReasonParseError       = Reason(engineio.ReasonParseError)
	ReasonForcedClose      = Reason(engineio.ReasonForcedClose)
	ReasonServerShutdown   = Reason(engineio.ReasonServerShutdown)
	ReasonClientDisconnect = Reason("client namespace disconnect")
	ReasonServerDisconnect = Reason("server namespace disconnect")
)
