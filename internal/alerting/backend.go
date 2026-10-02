package alerting

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"

	"github.com/example/portable-platform-reconciler/internal/config"
)

// Backend is an alert engine (New Relic, Prometheus, ...). List is the
// authoritative read; the controller never assumes a prior write succeeded.
type Backend interface {
	// Name identifies the backend instance.
	Name() string
	// List returns only rules tagged as managed by this controller for owner.
	List(ctx context.Context, owner Owner) ([]ManagedRule, error)
	// Create adds a rule.
	Create(ctx context.Context, r ManagedRule) error
	// Update replaces the rule with the same ID and owner.
	Update(ctx context.Context, r ManagedRule) error
	// Delete removes the rule with the same ID and owner.
	Delete(ctx context.Context, r ManagedRule) error
}

// ErrUnknownBackend is returned for a backend that is not registered.
var ErrUnknownBackend = errors.New("unknown alert backend")

// Factory builds a backend from the controller configuration. logger may be nil.
type Factory func(cfg config.Config, logger *slog.Logger) (Backend, error)

// BackendRegistry maps backend names to factories. Adding an engine is one
// package that registers itself; nothing else changes.
type BackendRegistry struct {
	mu      sync.RWMutex
	entries map[string]Factory
}

// NewBackendRegistry returns an empty registry.
func NewBackendRegistry() *BackendRegistry { return &BackendRegistry{entries: map[string]Factory{}} }

// DefaultBackends is the process-wide registry backends register into from init().
var DefaultBackends = NewBackendRegistry()

// Register adds a backend. Registering a name twice panics (programmer error).
func (r *BackendRegistry) Register(name string, f Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name == "" || f == nil {
		panic("alerting: Register requires a name and a factory")
	}
	if _, dup := r.entries[name]; dup {
		panic(fmt.Sprintf("alerting: backend %q registered twice", name))
	}
	r.entries[name] = f
}

// New builds the named backend. A nil logger discards logs.
func (r *BackendRegistry) New(name string, cfg config.Config, logger *slog.Logger) (Backend, error) {
	r.mu.RLock()
	f, ok := r.entries[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownBackend, name, r.Names())
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return f(cfg, logger)
}

// Names returns the registered backend names, sorted.
func (r *BackendRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.entries))
	for n := range r.entries {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Register adds a backend to DefaultBackends.
func Register(name string, f Factory) { DefaultBackends.Register(name, f) }

// New builds a backend from DefaultBackends.
func New(name string, cfg config.Config, logger *slog.Logger) (Backend, error) {
	return DefaultBackends.New(name, cfg, logger)
}

// Names lists the backends in DefaultBackends.
func Names() []string { return DefaultBackends.Names() }
