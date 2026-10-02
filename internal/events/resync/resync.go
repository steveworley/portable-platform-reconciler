// Package resync provides the periodic resync event source, which guarantees
// that missed events are eventually corrected.
package resync

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/example/portable-platform-reconciler/internal/events"
	"github.com/example/portable-platform-reconciler/internal/policy"
)

// Lister lists known policies. controller.PolicyStore satisfies it.
type Lister interface {
	List() []policy.ObservabilityPolicy
}

// Source enqueues "ObservabilityPolicy:<name>" for every known policy on each tick.
type Source struct {
	lister   Lister
	interval time.Duration
	logger   *slog.Logger
}

var _ events.Source = (*Source)(nil)

// New creates a resync source. interval must be positive.
func New(lister Lister, interval time.Duration, logger *slog.Logger) (*Source, error) {
	if interval <= 0 {
		return nil, errors.New("resync interval must be positive")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Source{lister: lister, interval: interval, logger: logger}, nil
}

// Name implements events.Source.
func (s *Source) Name() string { return "resync" }

// Start implements events.Source.
func (s *Source) Start(ctx context.Context, q events.Enqueuer) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			ps := s.lister.List()
			s.logger.Info("periodic resync", "policies", len(ps))
			for _, p := range ps {
				q.Enqueue(policy.Key(p.Metadata.Name))
			}
		}
	}
}
