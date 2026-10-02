// Package events defines the event-source abstraction. Event sources only
// enqueue keys; they never reconcile and never carry authoritative state.
package events

import "context"

// Enqueuer accepts work-queue keys. controller.Engine implements it.
type Enqueuer interface {
	Enqueue(key string)
}

// Source wakes the engine. Start blocks until ctx is done (returning nil) or
// a fatal error occurs. Keys must match RuntimeAdapter.QueueKey or
// policy.Key so the engine can map them to policies.
type Source interface {
	Name() string
	Start(ctx context.Context, q Enqueuer) error
}
