package socketio

import (
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	mailboxPatience = 3 * time.Second
	mailboxQuiet    = 80 * time.Millisecond
)

type panicRecord struct {
	value any
	stack []byte
}

func collectPanics() (func(any, []byte), <-chan panicRecord) {
	records := make(chan panicRecord, 16)
	return func(value any, stack []byte) { records <- panicRecord{value, stack} }, records
}

func awaitSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(mailboxPatience):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

func stays[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s happened too early", what)
	case <-time.After(mailboxQuiet):
	}
}

func queued(m *mailbox) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tasks)
}

func worker(m *mailbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

func awaitIdle(t *testing.T, m *mailbox) {
	t.Helper()
	deadline := time.Now().Add(mailboxPatience)
	for worker(m) {
		if time.Now().After(deadline) {
			t.Fatal("the mailbox still has a worker after its tasks finished")
		}
		runtime.Gosched()
	}
}

func TestMailboxRunsTasksInOrder(t *testing.T) {
	t.Parallel()
	const total = 300
	for _, tc := range []struct {
		name string
		push func(m *mailbox, task func())
	}{
		{"push", (*mailbox).push},
		{"pushWait", (*mailbox).pushWait},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newMailbox(total, nil, func(any, []byte) { t.Error("unexpected panic") })
			var mu sync.Mutex
			var order []int
			done := make(chan struct{})
			for i := range total {
				tc.push(m, func() {
					mu.Lock()
					order = append(order, i)
					mu.Unlock()
					if i == total-1 {
						close(done)
					}
				})
			}
			awaitSignal(t, done, "the last task")
			mu.Lock()
			defer mu.Unlock()
			if len(order) != total || !slices.IsSorted(order) {
				t.Errorf("tasks ran in order %v", order)
			}
		})
	}
}

func TestMailboxRunsOneTaskAtATime(t *testing.T) {
	t.Parallel()
	const producers, perProducer = 8, 40
	m := newMailbox(4, nil, func(any, []byte) { t.Error("unexpected panic") })
	var running, peak, ran atomic.Int32
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perProducer {
				m.pushWait(func() {
					now := running.Add(1)
					for seen := peak.Load(); now > seen && !peak.CompareAndSwap(seen, now); seen = peak.Load() {
					}
					runtime.Gosched()
					running.Add(-1)
					ran.Add(1)
				})
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(mailboxPatience)
	for ran.Load() < producers*perProducer && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if ran.Load() != producers*perProducer || peak.Load() != 1 {
		t.Errorf("%d tasks ran with up to %d at once, want %d one at a time", ran.Load(), peak.Load(), producers*perProducer)
	}
	awaitIdle(t, m)
}

func TestMailboxPushWaitBlocksAtTheLimit(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{1, 2, 5} {
		t.Run(string(rune('0'+limit))+" waiting tasks", func(t *testing.T) {
			t.Parallel()
			m := newMailbox(limit, nil, func(any, []byte) { t.Error("unexpected panic") })
			gate, started := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			t.Cleanup(release)
			var mu sync.Mutex
			var order []int
			record := func(n int) func() {
				return func() {
					mu.Lock()
					order = append(order, n)
					mu.Unlock()
				}
			}
			m.pushWait(func() {
				close(started)
				<-gate
				record(0)()
			})
			awaitSignal(t, started, "the first task to start")

			for n := 1; n <= limit; n++ {
				m.pushWait(record(n))
			}
			if got := queued(m); got != limit {
				t.Fatalf("%d tasks waiting, want %d", got, limit)
			}
			returned := make(chan struct{})
			finished := make(chan struct{})
			go func() {
				m.pushWait(record(limit + 1))
				close(returned)
				m.pushWait(func() { close(finished) })
			}()
			stays(t, returned, "pushWait over the limit")
			if got := queued(m); got != limit {
				t.Errorf("%d tasks waiting while a producer is blocked, want %d", got, limit)
			}

			release()
			awaitSignal(t, returned, "pushWait to resume")
			awaitSignal(t, finished, "the last task")
			mu.Lock()
			defer mu.Unlock()
			want := make([]int, limit+2)
			for i := range want {
				want[i] = i
			}
			if !slices.Equal(order, want) {
				t.Errorf("tasks ran in order %v, want %v", order, want)
			}
		})
	}
}

func TestMailboxPushWaitDoesNotBlockBelowTheLimit(t *testing.T) {
	t.Parallel()
	m := newMailbox(3, nil, func(any, []byte) {})
	gate, started := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(gate) })
	m.pushWait(func() {
		close(started)
		<-gate
	})
	awaitSignal(t, started, "the first task to start")
	returned := make(chan struct{})
	go func() {
		m.pushWait(func() {})
		m.pushWait(func() {})
		close(returned)
	}()
	awaitSignal(t, returned, "pushWait below the limit")
}

func TestMailboxPushNeverBlocks(t *testing.T) {
	t.Parallel()
	m := newMailbox(1, nil, func(any, []byte) {})
	gate, started := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(gate) })
	m.pushWait(func() {
		close(started)
		<-gate
	})
	awaitSignal(t, started, "the first task to start")
	done := make(chan struct{})
	go func() {
		for range 100 {
			m.push(func() {})
		}
		close(done)
	}()
	awaitSignal(t, done, "100 pushes beyond the limit")
	if got := queued(m); got != 100 {
		t.Errorf("%d tasks waiting, want 100", got)
	}
}

func TestMailboxPushFromInsideATask(t *testing.T) {
	t.Parallel()
	m := newMailbox(1, nil, func(any, []byte) { t.Error("unexpected panic") })
	var mu sync.Mutex
	var order []string
	note := func(entry string) {
		mu.Lock()
		order = append(order, entry)
		mu.Unlock()
	}
	done := make(chan struct{})
	m.pushWait(func() {
		for _, name := range []string{"a", "b", "c"} {
			m.push(func() { note(name) })
		}
		m.push(func() { close(done) })
		note("outer")
	})
	awaitSignal(t, done, "the tasks pushed from inside a task")
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"outer", "a", "b", "c"}; !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestMailboxPanicGoesToOnPanic(t *testing.T) {
	t.Parallel()
	failure := errors.New("failed")
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"string", "boom"},
		{"error", failure},
		{"integer", 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			onPanic, records := collectPanics()
			m := newMailbox(8, nil, onPanic)
			next := make(chan struct{})
			m.push(func() { panic(tc.value) })
			m.push(func() { close(next) })
			record := awaitSignal(t, records, "the panic to be reported")
			if record.value != tc.value {
				t.Errorf("recovered %v, want %v", record.value, tc.value)
			}
			if len(record.stack) == 0 {
				t.Error("the stack is empty")
			}
			awaitSignal(t, next, "the task after the panic")
			m.pushWait(func() { panic(tc.value) })
			awaitSignal(t, records, "the second panic")
			awaitIdle(t, m)
		})
	}
}

func TestMailboxIsIdleWithoutTasks(t *testing.T) {
	t.Parallel()
	m := newMailbox(4, nil, func(any, []byte) {})
	if worker(m) || queued(m) != 0 {
		t.Fatal("a new mailbox has a worker or tasks")
	}
	for round := range 3 {
		done := make(chan struct{})
		m.push(func() { close(done) })
		awaitSignal(t, done, "the task")
		awaitIdle(t, m)
		if got := queued(m); got != 0 {
			t.Fatalf("round %d: %d tasks left after the worker stopped", round, got)
		}
	}
}

func TestMailboxWakesEveryBlockedProducer(t *testing.T) {
	t.Parallel()
	const producers, perProducer = 20, 10
	m := newMailbox(2, nil, func(any, []byte) { t.Error("unexpected panic") })
	gate, started := make(chan struct{}), make(chan struct{})
	m.pushWait(func() {
		close(started)
		<-gate
	})
	awaitSignal(t, started, "the first task to start")
	var ran atomic.Int32
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perProducer {
				m.pushWait(func() { ran.Add(1) })
			}
		}()
	}
	for queued(m) < 2 {
		runtime.Gosched()
	}
	close(gate)
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	awaitSignal(t, finished, "every producer to finish")
	deadline := time.Now().Add(mailboxPatience)
	for ran.Load() < producers*perProducer && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := ran.Load(); got != producers*perProducer {
		t.Errorf("%d tasks ran, want %d", got, producers*perProducer)
	}
	awaitIdle(t, m)
}

func TestMailboxPushWaitGivesUpWhenTheConnectionEnds(t *testing.T) {
	t.Parallel()
	ended := make(chan struct{})
	m := newMailbox(1, ended, func(any, []byte) { t.Error("unexpected panic") })
	gate, started := make(chan struct{}), make(chan struct{})
	m.pushWait(func() {
		close(started)
		<-gate
	})
	awaitSignal(t, started, "the first task to start")
	m.pushWait(func() {})
	var ran atomic.Bool
	returned := make(chan struct{})
	go func() {
		m.pushWait(func() { ran.Store(true) })
		close(returned)
	}()
	stays(t, returned, "pushWait on a full mailbox")
	close(ended)
	awaitSignal(t, returned, "pushWait after the connection ended")
	if got := queued(m); got != 1 {
		t.Errorf("%d tasks waiting, want 1", got)
	}
	close(gate)
	awaitIdle(t, m)
	if ran.Load() {
		t.Error("the task that was given up ran anyway")
	}
}

func TestMailboxPushWaitQueuesBelowTheLimitAfterTheConnectionEnded(t *testing.T) {
	t.Parallel()
	ended := make(chan struct{})
	close(ended)
	m := newMailbox(2, ended, func(any, []byte) { t.Error("unexpected panic") })
	ran := make(chan struct{})
	m.pushWait(func() { close(ran) })
	awaitSignal(t, ran, "the task")
	awaitIdle(t, m)
}
