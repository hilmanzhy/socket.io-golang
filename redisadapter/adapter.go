// Package redisadapter connects the servers of a cluster through Redis
// Pub/Sub, so that a broadcast started on one server reaches the clients of
// all of them.
//
//	adapter := redisadapter.New(redis.NewClient(&redis.Options{Addr: "localhost:6379"}), redisadapter.Options{})
//	server := socketio.New(socketio.Options{Adapter: adapter})
//
// The wire format belongs to this library. It is not compatible with the
// Node.js @socket.io/redis-adapter, so Go and Node.js servers cannot share a
// channel.
package redisadapter

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	socketio "github.com/nuriontech/socket.io-golang"
)

// Errors returned by Publish.
var (
	ErrBacklogFull = errors.New("redisadapter: publish backlog is full")
	ErrClosed      = errors.New("redisadapter: adapter is closed")
)

// Options configures an Adapter. The zero value is ready to use.
type Options struct {
	// Channel is the Pub/Sub channel shared by the cluster. Servers on
	// different channels do not see each other. Default "socket.io-golang".
	Channel string
	// Backlog is how many operations may wait for Redis before Publish
	// returns ErrBacklogFull. Default 1024.
	Backlog int
	// PublishTimeout bounds one PUBLISH command. Default 5 s.
	PublishTimeout time.Duration
	// OnError receives the errors that happen in the background: a failed
	// PUBLISH or an unreadable message. Default: ignore them.
	OnError func(error)
}

type envelope struct {
	Node string             `json:"node"`
	Op   socketio.Operation `json:"op"`
}

// Adapter is a socketio.Adapter backed by Redis Pub/Sub.
type Adapter struct {
	client redis.UniversalClient
	opts   Options
	node   string
	outbox chan []byte

	lifetime  context.Context
	stop      context.CancelFunc
	workers   sync.WaitGroup
	subscribe sync.Once
}

// New returns an adapter that publishes through client. The adapter does not
// close the client.
func New(client redis.UniversalClient, opts Options) *Adapter {
	opts.Channel = cmp.Or(opts.Channel, "socket.io-golang")
	opts.Backlog = cmp.Or(opts.Backlog, 1024)
	opts.PublishTimeout = cmp.Or(opts.PublishTimeout, 5*time.Second)
	if opts.OnError == nil {
		opts.OnError = func(error) {}
	}
	lifetime, stop := context.WithCancel(context.Background())
	a := &Adapter{
		client:   client,
		opts:     opts,
		node:     rand.Text(),
		outbox:   make(chan []byte, opts.Backlog),
		lifetime: lifetime,
		stop:     stop,
	}
	a.workers.Add(1)
	go a.publishLoop()
	return a
}

// Publish queues an operation for the other servers and returns at once.
func (a *Adapter) Publish(op socketio.Operation) error {
	payload, err := json.Marshal(envelope{Node: a.node, Op: op})
	if err != nil {
		return err
	}
	if a.lifetime.Err() != nil {
		return ErrClosed
	}
	select {
	case a.outbox <- payload:
		return nil
	default:
		return ErrBacklogFull
	}
}

// Subscribe starts delivering the operations of the other servers to apply.
// Only the first call has an effect.
func (a *Adapter) Subscribe(apply func(socketio.Operation)) {
	a.subscribe.Do(func() {
		pubsub := a.client.Subscribe(a.lifetime, a.opts.Channel)
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
		case payload := <-a.outbox:
			ctx, cancel := context.WithTimeout(a.lifetime, a.opts.PublishTimeout)
			err := a.client.Publish(ctx, a.opts.Channel, payload).Err()
			cancel()
			if err != nil && a.lifetime.Err() == nil {
				a.opts.OnError(err)
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
		case message, open := <-messages:
			if !open {
				return
			}
			var received envelope
			if err := json.Unmarshal([]byte(message.Payload), &received); err != nil {
				a.opts.OnError(err)
				continue
			}
			if received.Node != a.node {
				apply(received.Op)
			}
		}
	}
}
