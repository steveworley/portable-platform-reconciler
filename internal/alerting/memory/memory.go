// Package memory is a thread-safe in-memory alerting.Backend for tests and
// demos. It registers itself as backend "memory".
package memory

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/config"
)

// BackendName is the registered name of the default memory backend.
const BackendName = "memory"

func init() {
	alerting.Register(BackendName, func(config.Config, *slog.Logger) (alerting.Backend, error) {
		return New(), nil
	})
}

// Writes counts the mutating calls a Backend has received.
type Writes struct {
	Creates, Updates, Deletes int
}

// Total is the number of writes of any kind.
func (w Writes) Total() int { return w.Creates + w.Updates + w.Deletes }

type key struct {
	owner alerting.Owner
	id    string
}

// Backend stores rules in a map.
type Backend struct {
	name string

	mu     sync.Mutex
	rules  map[key]alerting.ManagedRule
	writes Writes
}

var (
	_ alerting.Backend = (*Backend)(nil)
	_ alerting.Lister  = (*Backend)(nil)
)

// Option customises a Backend.
type Option func(*Backend)

// WithName sets the instance name returned by Name (default "memory"). Use it
// to run two independent instances, e.g. for a migration demo.
func WithName(name string) Option { return func(b *Backend) { b.name = name } }

// New creates an empty backend.
func New(opts ...Option) *Backend {
	b := &Backend{name: BackendName, rules: map[key]alerting.ManagedRule{}}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Name implements alerting.Backend.
func (b *Backend) Name() string { return b.name }

// List implements alerting.Backend.
func (b *Backend) List(_ context.Context, owner alerting.Owner) ([]alerting.ManagedRule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []alerting.ManagedRule
	for k, r := range b.rules {
		if k.owner == owner {
			out = append(out, r)
		}
	}
	sortRules(out)
	return out, nil
}

// ListAll implements alerting.Lister. Every stored rule is managed.
func (b *Backend) ListAll(context.Context) ([]alerting.ManagedRule, error) {
	return b.Rules(), nil
}

// Create implements alerting.Backend. Creating an existing rule is an error.
func (b *Backend) Create(_ context.Context, r alerting.ManagedRule) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{r.Owner, r.ID}
	if _, ok := b.rules[k]; ok {
		return fmt.Errorf("rule %s already exists", r.ID)
	}
	b.rules[k] = r
	b.writes.Creates++
	return nil
}

// Update implements alerting.Backend. Updating a missing rule is an error.
func (b *Backend) Update(_ context.Context, r alerting.ManagedRule) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{r.Owner, r.ID}
	if _, ok := b.rules[k]; !ok {
		return fmt.Errorf("rule %s not found", r.ID)
	}
	b.rules[k] = r
	b.writes.Updates++
	return nil
}

// Delete implements alerting.Backend. Deleting a missing rule is an error.
func (b *Backend) Delete(_ context.Context, r alerting.ManagedRule) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{r.Owner, r.ID}
	if _, ok := b.rules[k]; !ok {
		return fmt.Errorf("rule %s not found", r.ID)
	}
	delete(b.rules, k)
	b.writes.Deletes++
	return nil
}

// Writes returns the write counters.
func (b *Backend) Writes() Writes {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writes
}

// ResetWrites zeroes the write counters.
func (b *Backend) ResetWrites() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.writes = Writes{}
}

// Rules returns a snapshot of every stored rule sorted by workload, then rule ID.
func (b *Backend) Rules() []alerting.ManagedRule {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]alerting.ManagedRule, 0, len(b.rules))
	for _, r := range b.rules {
		out = append(out, r)
	}
	sortRules(out)
	return out
}

// Tamper simulates an out-of-band edit: it sets the threshold of every stored
// rule with ruleID (not counted as a write). It reports how many rules changed.
func (b *Backend) Tamper(ruleID string, threshold float64) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for k, r := range b.rules {
		if k.id == ruleID {
			r.Threshold = threshold
			b.rules[k] = r
			n++
		}
	}
	return n
}

// Remove simulates an out-of-band deletion of every stored rule with ruleID
// (not counted as a write). It reports how many rules were removed.
func (b *Backend) Remove(ruleID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for k := range b.rules {
		if k.id == ruleID {
			delete(b.rules, k)
			n++
		}
	}
	return n
}

func sortRules(rs []alerting.ManagedRule) {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Owner.WorkloadKey != rs[j].Owner.WorkloadKey {
			return rs[i].Owner.WorkloadKey < rs[j].Owner.WorkloadKey
		}
		return rs[i].ID < rs[j].ID
	})
}
