package socketio

import (
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

type taskLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *taskLog) note(entry string) func() {
	return func() {
		l.mu.Lock()
		l.entries = append(l.entries, entry)
		l.mu.Unlock()
	}
}

func (l *taskLog) got() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.entries)
}

func blockWorker(t *testing.T, m *mailbox) (release func()) {
	t.Helper()
	gate, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	m.pushWait(func() {
		close(started)
		<-gate
	})
	awaitSignal(t, started, "the first task to start")
	return release
}

func failOnPanic(t *testing.T) func(any, []byte) {
	return func(value any, _ []byte) { t.Errorf("unexpected panic: %v", value) }
}

func TestMailboxFinishRunsQueuedTasksThenTheLastOne(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		queued int
		push   func(m *mailbox, task func())
	}{
		{"nothing queued", 0, (*mailbox).push},
		{"tasks queued with push", 5, (*mailbox).push},
		{"tasks queued with pushWait", 5, (*mailbox).pushWait},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newMailbox(8, nil, failOnPanic(t))
			var log taskLog
			release := blockWorker(t, m)
			var want []string
			for i := range tc.queued {
				tc.push(m, log.note(strconv.Itoa(i)))
				want = append(want, strconv.Itoa(i))
			}
			m.finish(log.note("last"))
			want = append(want, "last")
			release()
			awaitIdle(t, m)
			if got := log.got(); !slices.Equal(got, want) {
				t.Errorf("tasks ran in order %v, want %v", got, want)
			}
		})
	}
}

func TestMailboxFinishOnAnIdleMailboxRunsTheLastTask(t *testing.T) {
	t.Parallel()
	m := newMailbox(2, nil, failOnPanic(t))
	var log taskLog
	if worker(m) {
		t.Fatal("a new mailbox has a worker")
	}
	m.finish(log.note("last"))
	awaitIdle(t, m)
	if got, want := log.got(), []string{"last"}; !slices.Equal(got, want) {
		t.Errorf("tasks ran in order %v, want %v", got, want)
	}
	if got := queued(m); got != 0 {
		t.Errorf("%d tasks left after the worker stopped", got)
	}
}

func TestMailboxFinishDropsLaterTasks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		push func(m *mailbox, task func())
	}{
		{"push", (*mailbox).push},
		{"pushWait", (*mailbox).pushWait},
	} {
		t.Run(tc.name+" while the last task waits", func(t *testing.T) {
			t.Parallel()
			m := newMailbox(1, nil, failOnPanic(t))
			var log taskLog
			release := blockWorker(t, m)
			m.finish(log.note("last"))
			before := queued(m)
			returned := make(chan struct{})
			go func() {
				tc.push(m, log.note("late"))
				close(returned)
			}()
			awaitSignal(t, returned, "the push after finish to return")
			if got := queued(m); got != before {
				t.Errorf("%d tasks waiting after a late push, want %d", got, before)
			}
			release()
			awaitIdle(t, m)
			if got, want := log.got(), []string{"last"}; !slices.Equal(got, want) {
				t.Errorf("tasks ran in order %v, want %v", got, want)
			}
		})
		t.Run(tc.name+" after the last task ran", func(t *testing.T) {
			t.Parallel()
			m := newMailbox(1, nil, failOnPanic(t))
			var log taskLog
			m.finish(log.note("last"))
			awaitIdle(t, m)
			tc.push(m, log.note("late"))
			if worker(m) || queued(m) != 0 {
				t.Errorf("a late push started a worker or queued a task: worker %v, queued %d", worker(m), queued(m))
			}
			awaitIdle(t, m)
			if got, want := log.got(), []string{"last"}; !slices.Equal(got, want) {
				t.Errorf("tasks ran in order %v, want %v", got, want)
			}
		})
	}
}

func TestMailboxFinishReleasesBlockedProducers(t *testing.T) {
	t.Parallel()
	for _, producers := range []int{1, 4} {
		t.Run(strconv.Itoa(producers)+" blocked producers", func(t *testing.T) {
			t.Parallel()
			m := newMailbox(1, nil, failOnPanic(t))
			var log taskLog
			release := blockWorker(t, m)
			m.pushWait(log.note("waiting"))
			returned := make(chan int, producers)
			for i := range producers {
				go func() {
					m.pushWait(log.note("blocked " + strconv.Itoa(i)))
					returned <- i
				}()
			}
			stays(t, returned, "pushWait on a full backlog")
			if got := queued(m); got != 1 {
				t.Fatalf("%d tasks waiting while the producers are blocked, want 1", got)
			}
			m.finish(log.note("last"))
			for range producers {
				awaitSignal(t, returned, "a blocked producer to be released")
			}
			release()
			awaitIdle(t, m)
			if got, want := log.got(), []string{"waiting", "last"}; !slices.Equal(got, want) {
				t.Errorf("tasks ran in order %v, want %v", got, want)
			}
		})
	}
}

func TestMailboxFinishNeverBlocks(t *testing.T) {
	t.Parallel()
	m := newMailbox(1, nil, failOnPanic(t))
	var log taskLog
	release := blockWorker(t, m)
	m.push(log.note("first"))
	m.push(log.note("second"))
	done := make(chan struct{})
	go func() {
		m.finish(log.note("last"))
		close(done)
	}()
	awaitSignal(t, done, "finish on a full backlog")
	if got := queued(m); got != 3 {
		t.Errorf("%d tasks waiting, want 3", got)
	}
	release()
	awaitIdle(t, m)
	if got, want := log.got(), []string{"first", "second", "last"}; !slices.Equal(got, want) {
		t.Errorf("tasks ran in order %v, want %v", got, want)
	}
}

func TestMailboxFinishRunsTheLastTaskAfterTheConnectionEnded(t *testing.T) {
	t.Parallel()
	ended := make(chan struct{})
	close(ended)
	m := newMailbox(1, ended, failOnPanic(t))
	var log taskLog
	release := blockWorker(t, m)
	m.push(log.note("first"))
	m.finish(log.note("last"))
	release()
	awaitIdle(t, m)
	if got, want := log.got(), []string{"first", "last"}; !slices.Equal(got, want) {
		t.Errorf("tasks ran in order %v, want %v", got, want)
	}
}

func TestMailboxFinishFromInsideATask(t *testing.T) {
	t.Parallel()
	m := newMailbox(1, nil, failOnPanic(t))
	var log taskLog
	m.pushWait(func() {
		m.finish(log.note("last"))
		m.push(log.note("late"))
		m.pushWait(log.note("later"))
		log.note("outer")()
	})
	awaitIdle(t, m)
	if got, want := log.got(), []string{"outer", "last"}; !slices.Equal(got, want) {
		t.Errorf("tasks ran in order %v, want %v", got, want)
	}
}

func TestMailboxFinishTwiceRunsTheFinalTaskOnce(t *testing.T) {
	t.Parallel()
	t.Run("while the first waits", func(t *testing.T) {
		t.Parallel()
		m := newMailbox(2, nil, failOnPanic(t))
		var log taskLog
		release := blockWorker(t, m)
		m.finish(log.note("first"))
		m.finish(log.note("second"))
		if got := queued(m); got != 1 {
			t.Errorf("%d tasks waiting after two calls, want 1", got)
		}
		release()
		awaitIdle(t, m)
		if got, want := log.got(), []string{"first"}; !slices.Equal(got, want) {
			t.Errorf("tasks ran in order %v, want %v", got, want)
		}
	})
	t.Run("after the first ran", func(t *testing.T) {
		t.Parallel()
		m := newMailbox(2, nil, failOnPanic(t))
		var log taskLog
		m.finish(log.note("first"))
		awaitIdle(t, m)
		m.finish(log.note("second"))
		if worker(m) || queued(m) != 0 {
			t.Errorf("the second call started a worker or queued a task: worker %v, queued %d", worker(m), queued(m))
		}
		awaitIdle(t, m)
		if got, want := log.got(), []string{"first"}; !slices.Equal(got, want) {
			t.Errorf("tasks ran in order %v, want %v", got, want)
		}
	})
	t.Run("from many goroutines", func(t *testing.T) {
		t.Parallel()
		const callers = 32
		m := newMailbox(2, nil, failOnPanic(t))
		var ran atomic.Int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				m.finish(func() { ran.Add(1) })
			}()
		}
		close(start)
		wg.Wait()
		awaitIdle(t, m)
		if got := ran.Load(); got != 1 {
			t.Errorf("%d final tasks ran, want 1", got)
		}
	})
}
