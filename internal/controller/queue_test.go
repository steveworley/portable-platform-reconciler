package controller

import (
	"sync"
	"testing"
	"time"
)

func TestQueueDedup(t *testing.T) {
	q := NewQueue(QueueOptions{})
	q.Add("a")
	q.Add("a")
	q.Add("b")
	if got := q.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	if k, _ := q.Get(); k != "a" {
		t.Errorf("Get() = %q, want a", k)
	}
	if k, _ := q.Get(); k != "b" {
		t.Errorf("Get() = %q, want b", k)
	}
}

func TestQueueDirtyRequeue(t *testing.T) {
	q := NewQueue(QueueOptions{})
	q.Add("a")
	k, _ := q.Get()
	q.Add(k) // while processing
	q.Add(k)
	if got := q.Len(); got != 0 {
		t.Errorf("Len() while processing = %d, want 0", got)
	}
	q.Done(k)
	if got := q.Len(); got != 1 {
		t.Errorf("Len() after Done = %d, want 1", got)
	}
	k2, _ := q.Get()
	q.Done(k2)
	if got := q.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
}

func TestQueueBackoffAndRetries(t *testing.T) {
	base := 20 * time.Millisecond
	q := NewQueue(QueueOptions{BaseDelay: base, MaxDelay: 50 * time.Millisecond})

	wantDelays := []time.Duration{base, 2 * base, 50 * time.Millisecond, 50 * time.Millisecond}
	for i, want := range wantDelays {
		start := time.Now()
		q.AddRateLimited("k")
		if got := q.NumRequeues("k"); got != i+1 {
			t.Fatalf("NumRequeues() = %d, want %d", got, i+1)
		}
		if q.Len() != 0 {
			t.Fatalf("attempt %d: key available immediately", i)
		}
		k, shutdown := q.Get()
		elapsed := time.Since(start)
		if shutdown || k != "k" {
			t.Fatalf("Get() = %q, %v", k, shutdown)
		}
		if elapsed < want {
			t.Errorf("attempt %d: delay %v, want >= %v", i, elapsed, want)
		}
		if elapsed > want+40*time.Millisecond {
			t.Errorf("attempt %d: delay %v, want about %v", i, elapsed, want)
		}
		q.Done(k)
	}
	q.Forget("k")
	if got := q.NumRequeues("k"); got != 0 {
		t.Errorf("NumRequeues() after Forget = %d, want 0", got)
	}
}

func TestQueueShutdownUnblocksGet(t *testing.T) {
	q := NewQueue(QueueOptions{})
	done := make(chan bool)
	go func() {
		_, shutdown := q.Get()
		done <- shutdown
	}()
	time.Sleep(10 * time.Millisecond)
	q.ShutDown()
	select {
	case shutdown := <-done:
		if !shutdown {
			t.Error("Get() shutdown = false, want true")
		}
	case <-time.After(time.Second):
		t.Fatal("Get() still blocked after ShutDown")
	}
	q.Add("x")
	if q.Len() != 0 {
		t.Error("Add after ShutDown queued a key")
	}
}

func TestQueueShutdownDrainsQueuedKeys(t *testing.T) {
	q := NewQueue(QueueOptions{})
	q.Add("a")
	q.ShutDown()
	if k, shutdown := q.Get(); shutdown || k != "a" {
		t.Errorf("Get() = %q, %v, want a, false", k, shutdown)
	}
	q.Done("a")
	if _, shutdown := q.Get(); !shutdown {
		t.Error("Get() shutdown = false on empty shut-down queue")
	}
}

func TestQueueShutDownWithDrainWaitsForDone(t *testing.T) {
	q := NewQueue(QueueOptions{})
	q.Add("a")
	k, _ := q.Get()

	var wg sync.WaitGroup
	wg.Add(1)
	returned := make(chan struct{})
	go func() {
		defer wg.Done()
		q.ShutDownWithDrain()
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("ShutDownWithDrain returned before Done")
	case <-time.After(30 * time.Millisecond):
	}
	q.Done(k)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("ShutDownWithDrain did not return after Done")
	}
	wg.Wait()
}
