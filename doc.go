// Package socketio is a Socket.IO server for Go. It speaks Socket.IO protocol
// v5 over Engine.IO v4, the protocols of the JavaScript socket.io-client 3.x
// and 4.x.
//
//	server := socketio.New(socketio.Options{})
//
//	server.OnConnect(func(socket *socketio.Socket) {
//		socket.Join("lobby")
//		socket.On("chat", func(event *socketio.Event) {
//			var text string
//			if err := event.Bind(&text); err != nil {
//				return
//			}
//			socket.To("lobby").Emit("chat", socket.ID(), text)
//			event.Ack("delivered")
//		})
//	})
//
//	http.Handle("/socket.io/", server)
//
// The library has three layers, each in its own package:
//
//   - engineio carries opaque messages over long-polling and WebSocket and
//     keeps connections alive.
//   - parser encodes and decodes Socket.IO packets.
//   - socketio, this package, adds namespaces, rooms, events,
//     acknowledgements and broadcasting.
//
// Handlers of one socket run one at a time, in the order the events arrived,
// on a goroutine of their own. Every method of Server, Namespace, Socket and
// BroadcastOperator is safe for concurrent use.
package socketio
