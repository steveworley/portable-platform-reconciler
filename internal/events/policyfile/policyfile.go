// Package policyfile provides an event source that reloads a policy directory
// on an interval and applies changes to a policy store.
package policyfile

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"github.com/example/portable-platform-reconciler/internal/events"
	"github.com/example/portable-platform-reconciler/internal/policy"
)

// Store is the policy store the source writes to. controller.MemoryPolicyStore
// satisfies it; its Upsert/Delete notifications enqueue the affected policy.
type Store interface {
	List() []policy.ObservabilityPolicy
	Get(name string) (policy.ObservabilityPolicy, bool)
	Upsert(p policy.ObservabilityPolicy)
	Delete(name string)
}

// Source loads dir immediately on Start and then every interval.
type Source struct {
	dir      string
	interval time.Duration
	store    Store
	opts     []policy.Option
	logger   *slog.Logger
}

var _ events.Source = (*Source)(nil)

// New creates a source. opts are passed to policy.LoadDir (e.g. a target validator).
func New(dir string, interval time.Duration, store Store, logger *slog.Logger, opts ...policy.Option) (*Source, error) {
	if interval <= 0 {
		return nil, errors.New("policy reload interval must be positive")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Source{dir: dir, interval: interval, store: store, opts: opts, logger: logger}, nil
}

// Name implements events.Source.
func (s *Source) Name() string { return "policyfile" }

// Start implements events.Source. The store, not q, receives changes; it
// notifies the engine itself. q is unused.
func (s *Source) Start(ctx context.Context, _ events.Enqueuer) error {
	s.Sync()
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.Sync()
		}
	}
}

// Sync reloads the directory once: new and changed policies are upserted
// (generation incremented when the spec or target changed and the file did not
// raise it), and policies whose files disappeared are deleted. A load error
// leaves the store untouched.
func (s *Source) Sync() {
	loaded, err := policy.LoadDir(s.dir, s.opts...)
	if err != nil {
		s.logger.Error("policy reload failed, keeping current policies", "dir", s.dir, "error", err)
		return
	}
	seen := map[string]struct{}{}
	for _, p := range loaded {
		name := p.Metadata.Name
		seen[name] = struct{}{}
		old, exists := s.store.Get(name)
		switch {
		case !exists:
			s.logger.Info("policy added", "policy", name)
			s.store.Upsert(p)
		case !reflect.DeepEqual(old.Spec, p.Spec) || !reflect.DeepEqual(old.Target, p.Target):
			if p.Metadata.Generation <= old.Metadata.Generation {
				p.Metadata.Generation = old.Metadata.Generation + 1
			}
			s.logger.Info("policy changed", "policy", name, "generation", p.Metadata.Generation)
			s.store.Upsert(p)
		}
	}
	for _, old := range s.store.List() {
		if _, ok := seen[old.Metadata.Name]; !ok {
			s.logger.Info("policy removed", "policy", old.Metadata.Name)
			s.store.Delete(old.Metadata.Name)
		}
	}
}
