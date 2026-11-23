// Package redisadapter connects the servers of a cluster through Redis
// Pub/Sub, so that a broadcast started on one server reaches the clients of
// all of them.
//
//	adapter := redisadapter.New(redis.NewClient(&redis.Options{Addr: "localhost:6379"}), redisadapter.Options{})
//	server := socketio.New(socketio.Options{Adapter: adapter})
//
// The messages are those of the Node.js @socket.io/redis-adapter 8 with its
// default parser. Go and Node.js servers that share a Redis server and a key
// therefore form one cluster: an emit, a join, a leave or a disconnect
// started on any of them reaches the sockets of all of them.
//
// The parts of the Node.js adapter that wait for an answer are not
// implemented. Started on a Node.js server, fetchSockets, allSockets and
// serverSideEmit do not see the Go servers, and an emit with acknowledgement
// does not reach their clients. The sharded adapter of Redis 7 is another
// protocol and is not supported.
//
// Redis is trusted: whoever can publish on the channels of the key can emit
// any event to any client of the cluster. Pub/Sub channels are shared by all
// the databases of a Redis server, so two applications on one server need two
// keys, whatever database each of them selects.
package redisadapter

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

// Errors returned by Publish.
var (
	ErrBacklogFull = errors.New("redisadapter: publish backlog is full")
	ErrClosed      = errors.New("redisadapter: adapter is closed")
)

// Options configures an Adapter. The zero value is ready to use.
type Options struct {
	// Key is the prefix of the Pub/Sub channels of the cluster, the key
	// option of the Node.js adapter. Servers with different keys do not see
	// each other. Default "socket.io", which is also the default of the
	// Node.js adapter: every server that keeps the default on one Redis
	// server joins the same cluster, in any database, because Pub/Sub ignores
	// the database number. Give each application its own key, without the
	// character '#', which separates the parts of a channel name. Two
	// applications on one Redis server must not use keys where one is the
	// other followed by "-request": the request channels of the first are
	// the broadcast channels of the second.
	Key string
	// Backlog is how many operations may wait for Redis before Publish
	// returns ErrBacklogFull. Default 1024.
	Backlog int
	// PublishTimeout bounds one PUBLISH command. Default 5 s.
	PublishTimeout time.Duration
	// OnError receives the errors that happen in the background: a failed
	// PUBLISH or an unreadable message. Default: ignore them.
	OnError func(error)
}

// Adapter is a socketio.Adapter backed by Redis Pub/Sub.
type Adapter struct {
	client redis.UniversalClient
	opts   Options
	wire   wire
	outbox chan message

	lifetime  context.Context
	stop      context.CancelFunc
	workers   sync.WaitGroup
	subscribe sync.Once
}

// New returns an adapter that publishes through client. The adapter does not
// close the client.
func New(client redis.UniversalClient, opts Options) *Adapter {
	opts.Key = cmp.Or(opts.Key, "socket.io")
	opts.Backlog = cmp.Or(opts.Backlog, 1024)
	opts.PublishTimeout = cmp.Or(opts.PublishTimeout, 5*time.Second)
	if opts.OnError == nil {
		opts.OnError = func(error) {}
	}
	lifetime, stop := context.WithCancel(context.Background())
	a := &Adapter{
		client:   client,
		opts:     opts,
		wire:     newWire(opts.Key, rand.Text()),
		outbox:   make(chan message, opts.Backlog),
		lifetime: lifetime,
		stop:     stop,
	}
	a.workers.Add(1)
	go a.publishLoop()
	return a
}

// Publish queues an operation for the other servers and returns at once.
func (a *Adapter) Publish(op socketio.Operation) error {
	out, err := a.wire.encode(op)
	if err != nil {
		return err
	}
	if a.lifetime.Err() != nil {
		return ErrClosed
	}
	select {
	case a.outbox <- out:
		return nil
	default:
		return ErrBacklogFull
	}
}

// Subscribe starts delivering the operations of the other servers to apply.
// Only the first call has an effect.
func (a *Adapter) Subscribe(apply func(socketio.Operation)) {
	a.subscribe.Do(func() {
		pubsub := a.client.PSubscribe(a.lifetime, a.wire.patterns()...)
		a.workers.Add(1)
		go a.receiveLoop(pubsub, apply)
	})
}

// Close stops the adapter and waits for its goroutines.
func (a *Adapter) Close() error {
	a.stop()
	a.workers.Wait()
	return nil
}

func (a *Adapter) publishLoop() {
	defer a.workers.Done()
	for {
		select {
		case <-a.lifetime.Done():
			return
		case out := <-a.outbox:
			ctx, cancel := context.WithTimeout(a.lifetime, a.opts.PublishTimeout)
			err := a.client.Publish(ctx, out.channel, out.payload).Err()
			cancel()
			if err != nil && a.lifetime.Err() == nil {
				a.report(err)
			}
		}
	}
}

func (a *Adapter) receiveLoop(pubsub *redis.PubSub, apply func(socketio.Operation)) {
	defer a.workers.Done()
	defer pubsub.Close()
	messages := pubsub.Channel()
	for {
		select {
		case <-a.lifetime.Done():
			return
		case received, open := <-messages:
			if !open {
				return
			}
			a.deliver(received, apply)
		}
	}
}

// deliver applies one received message. A panic while reading or applying it
// costs that message only.
func (a *Adapter) deliver(received *redis.Message, apply func(socketio.Operation)) {
	defer func() {
		if value := recover(); value != nil {
			a.report(fmt.Errorf("redisadapter: panic on a received message: %v\n%s", value, debug.Stack()))
		}
	}()
	op, relevant, err := a.wire.decode(received.Channel, []byte(received.Payload))
	if err != nil {
		a.report(err)
		return
	}
	if relevant {
		apply(op)
	}
}

// report hands an error to OnError. A panic of OnError is logged instead of
// taking the process down from a goroutine of the adapter.
func (a *Adapter) report(err error) {
	defer func() {
		if value := recover(); value != nil {
			slog.Error("redisadapter: OnError panicked", "panic", value, "error", err)
		}
	}()
	a.opts.OnError(err)
}
