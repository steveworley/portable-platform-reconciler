package controller

import (
	"sort"
	"sync"

	"github.com/example/portable-platform-reconciler/internal/policy"
)

// PolicyStore is the engine's read view of known policies.
type PolicyStore interface {
	List() []policy.ObservabilityPolicy
	Get(name string) (policy.ObservabilityPolicy, bool)
}

// subscribable is implemented by stores that notify on change; the engine
// subscribes automatically so policy changes enqueue the policy.
type subscribable interface {
	Subscribe(fn func(name string))
}

// MemoryPolicyStore is an in-memory PolicyStore with change notifications.
type MemoryPolicyStore struct {
	mu   sync.RWMutex
	m    map[string]policy.ObservabilityPolicy
	subs []func(name string)
}

var _ PolicyStore = (*MemoryPolicyStore)(nil)

// NewMemoryPolicyStore creates a store, optionally seeded with policies.
func NewMemoryPolicyStore(ps ...policy.ObservabilityPolicy) *MemoryPolicyStore {
	s := &MemoryPolicyStore{m: map[string]policy.ObservabilityPolicy{}}
	for _, p := range ps {
		s.m[p.Metadata.Name] = p
	}
	return s
}

// Subscribe registers fn to be called with the policy name after every Upsert or Delete.
func (s *MemoryPolicyStore) Subscribe(fn func(name string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs = append(s.subs, fn)
}

// Upsert adds or replaces a policy and notifies subscribers.
func (s *MemoryPolicyStore) Upsert(p policy.ObservabilityPolicy) {
	s.mu.Lock()
	s.m[p.Metadata.Name] = p
	subs := append([]func(string){}, s.subs...)
	s.mu.Unlock()
	for _, fn := range subs {
		fn(p.Metadata.Name)
	}
}

// Delete removes a policy and notifies subscribers.
func (s *MemoryPolicyStore) Delete(name string) {
	s.mu.Lock()
	_, existed := s.m[name]
	delete(s.m, name)
	subs := append([]func(string){}, s.subs...)
	s.mu.Unlock()
	if !existed {
		return
	}
	for _, fn := range subs {
		fn(name)
	}
}

// Get returns the named policy.
func (s *MemoryPolicyStore) Get(name string) (policy.ObservabilityPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[name]
	return p, ok
}

// List returns all policies sorted by name.
func (s *MemoryPolicyStore) List() []policy.ObservabilityPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]policy.ObservabilityPolicy, 0, len(s.m))
	for _, p := range s.m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Metadata.Name < out[j].Metadata.Name })
	return out
}
