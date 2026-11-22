package socketio

import (
	"runtime/debug"
	"sync"
)

// mailbox runs the tasks of one socket one at a time, in order. Its worker
// goroutine exists only while there are tasks to run, so an idle socket costs
// no goroutine.
type mailbox struct {
	limit   int
	done    <-chan struct{}
	space   chan struct{}
	onPanic func(value any, stack []byte)

	mu      sync.Mutex
	tasks   []func()
	running bool
}

// newMailbox returns a mailbox that holds up to limit waiting tasks. done is
// closed when the connection of the socket has ended.
func newMailbox(limit int, done <-chan struct{}, onPanic func(any, []byte)) *mailbox {
	return &mailbox{limit: limit, done: done, space: make(chan struct{}, 1), onPanic: onPanic}
}

// push queues a task without ever blocking. It is for the lifecycle tasks,
// which may be queued from inside a handler.
func (m *mailbox) push(task func()) {
	m.mu.Lock()
	m.enqueue(task)
	m.mu.Unlock()
}

// pushWait queues a task once fewer than limit tasks are waiting. Blocking
// the reader of a connection here is what slows down a client that sends
// faster than its handlers work. A connection that ends meanwhile releases
// the reader and the task is dropped.
func (m *mailbox) pushWait(task func()) {
	for !m.offer(task) {
		select {
		case <-m.space:
		case <-m.done:
			return
		}
	}
}

func (m *mailbox) offer(task func()) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.tasks) >= m.limit {
		return false
	}
	m.enqueue(task)
	return true
}

func (m *mailbox) enqueue(task func()) {
	m.tasks = append(m.tasks, task)
	if !m.running {
		m.running = true
		go m.run()
	}
}

func (m *mailbox) run() {
	for {
		m.mu.Lock()
		if len(m.tasks) == 0 {
			m.tasks, m.running = nil, false
			m.mu.Unlock()
			return
		}
		task := m.tasks[0]
		m.tasks[0] = nil
		m.tasks = m.tasks[1:]
		m.mu.Unlock()
		select {
		case m.space <- struct{}{}:
		default:
		}
		m.call(task)
	}
}

func (m *mailbox) call(task func()) {
	defer func() {
		if value := recover(); value != nil {
			m.onPanic(value, debug.Stack())
		}
	}()
	task()
}
