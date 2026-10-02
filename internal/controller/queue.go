// Package controller contains the runtime-neutral reconciliation engine, its
// work queue and status model. It must not import any Kubernetes or AWS packages.
package controller

import (
	"sync"
	"time"
)

// QueueOptions configures a Queue.
type QueueOptions struct {
	// BaseDelay is the first retry delay used by AddRateLimited. Default 5ms.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff. Default 1000s.
	MaxDelay time.Duration
}

// Queue is an in-memory work queue of string keys with the semantics of the
// client-go workqueue: a key waiting to be processed is queued once; a key
// added while it is being processed is marked dirty and requeued on Done; no
// key is processed by two workers at once.
type Queue struct {
	opts QueueOptions

	mu           sync.Mutex
	cond         *sync.Cond
	queue        []string
	dirty        map[string]struct{}
	processing   map[string]struct{}
	failures     map[string]int
	timers       map[*time.Timer]struct{}
	shuttingDown bool
}

// NewQueue creates a Queue.
func NewQueue(opts QueueOptions) *Queue {
	if opts.BaseDelay <= 0 {
		opts.BaseDelay = 5 * time.Millisecond
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = 1000 * time.Second
	}
	q := &Queue{
		opts:       opts,
		dirty:      map[string]struct{}{},
		processing: map[string]struct{}{},
		failures:   map[string]int{},
		timers:     map[*time.Timer]struct{}{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Add queues key unless it is already waiting. Ignored after ShutDown.
func (q *Queue) Add(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shuttingDown {
		return
	}
	if _, ok := q.dirty[key]; ok {
		return
	}
	q.dirty[key] = struct{}{}
	if _, ok := q.processing[key]; ok {
		return // requeued by Done
	}
	q.queue = append(q.queue, key)
	q.cond.Signal()
}

// AddAfter queues key once d has elapsed.
func (q *Queue) AddAfter(key string, d time.Duration) {
	if d <= 0 {
		q.Add(key)
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shuttingDown {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(d, func() {
		q.mu.Lock()
		delete(q.timers, t)
		q.mu.Unlock()
		q.Add(key)
	})
	q.timers[t] = struct{}{}
}

// AddRateLimited queues key after an exponential backoff of
// BaseDelay * 2^(failures so far), capped at MaxDelay, and counts a retry.
func (q *Queue) AddRateLimited(key string) {
	q.mu.Lock()
	n := q.failures[key]
	q.failures[key] = n + 1
	q.mu.Unlock()

	d := q.opts.BaseDelay
	for i := 0; i < n && d < q.opts.MaxDelay; i++ {
		d *= 2
	}
	if d > q.opts.MaxDelay {
		d = q.opts.MaxDelay
	}
	q.AddAfter(key, d)
}

// NumRequeues returns how many times key was rate-limited since the last Forget.
func (q *Queue) NumRequeues(key string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.failures[key]
}

// Forget resets the retry count for key.
func (q *Queue) Forget(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.failures, key)
}

// Len returns the number of keys waiting to be processed.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

// Get blocks until a key is available or the queue is shut down and empty, in
// which case shutdown is true. The caller must call Done for every key returned.
func (q *Queue) Get() (key string, shutdown bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.queue) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		return "", true
	}
	key = q.queue[0]
	q.queue = q.queue[1:]
	q.processing[key] = struct{}{}
	delete(q.dirty, key)
	return key, false
}

// Done marks key as processed. If it was re-added meanwhile it is requeued.
func (q *Queue) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, key)
	if _, ok := q.dirty[key]; ok {
		q.queue = append(q.queue, key)
		q.cond.Signal()
	} else if len(q.processing) == 0 {
		q.cond.Broadcast() // wake ShutDownWithDrain
	}
}

// ShutDown stops accepting keys and unblocks Get once the queue is empty.
// Pending delayed adds are cancelled.
func (q *Queue) ShutDown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutDownLocked()
}

// ShutDownWithDrain is ShutDown that also blocks until every key handed out by
// Get has been marked Done.
func (q *Queue) ShutDownWithDrain() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutDownLocked()
	for len(q.processing) > 0 {
		q.cond.Wait()
	}
}

func (q *Queue) shutDownLocked() {
	q.shuttingDown = true
	for t := range q.timers {
		t.Stop()
		delete(q.timers, t)
	}
	q.cond.Broadcast()
}

// ShuttingDown reports whether ShutDown was called.
func (q *Queue) ShuttingDown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shuttingDown
}
