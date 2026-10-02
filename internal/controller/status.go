package controller

import (
	"sort"
	"sync"
	"time"

	"github.com/example/portable-platform-reconciler/internal/state"
)

// Phase summarises the reconciliation state of a policy.
type Phase string

// Phases.
const (
	PhasePending  Phase = "Pending"  // target not resolved (yet)
	PhaseReady    Phase = "Ready"    // compliant
	PhaseDegraded Phase = "Degraded" // reconciled but not fully compliant or supported
	PhaseError    Phase = "Error"    // last attempt failed
)

// ConditionType names a condition.
type ConditionType string

// Condition types.
const (
	ConditionTargetResolved         ConditionType = "TargetResolved"
	ConditionCapabilitiesSatisfied  ConditionType = "CapabilitiesSatisfied"
	ConditionConfigurationCompliant ConditionType = "ConfigurationCompliant"
	ConditionReconciled             ConditionType = "Reconciled"
)

// ConditionStatus is "True", "False" or "Unknown".
type ConditionStatus string

// Condition statuses.
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// Condition is one observation about a policy.
type Condition struct {
	Type    ConditionType   `json:"type"`
	Status  ConditionStatus `json:"status"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
	// LastTransitionTime changes only when Status changes.
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

// PolicyStatus is the runtime-neutral reconciliation status of one policy.
type PolicyStatus struct {
	PolicyName         string                  `json:"policyName"`
	Phase              Phase                   `json:"phase"`
	ObservedGeneration int64                   `json:"observedGeneration"`
	Conditions         []Condition             `json:"conditions,omitempty"`
	LastReconciledAt   *time.Time              `json:"lastReconciledAt,omitempty"`
	Workload           *state.WorkloadIdentity `json:"workload,omitempty"`
	LastActions        []state.Action          `json:"lastActions,omitempty"`
	// Details holds extension data contributed by reconcile steps, e.g. a count
	// of managed alert rules.
	Details map[string]string `json:"details,omitempty"`
}

// Condition returns the condition of type t, if present.
func (s PolicyStatus) Condition(t ConditionType) (Condition, bool) {
	for _, c := range s.Conditions {
		if c.Type == t {
			return c, true
		}
	}
	return Condition{}, false
}

// StatusStore is a thread-safe in-memory store of policy statuses.
type StatusStore struct {
	mu  sync.RWMutex
	now func() time.Time
	m   map[string]PolicyStatus
}

// NewStatusStore creates an empty store.
func NewStatusStore() *StatusStore {
	return &StatusStore{now: time.Now, m: map[string]PolicyStatus{}}
}

// Get returns the status for policy name.
func (s *StatusStore) Get(name string) (PolicyStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.m[name]
	return clone(st), ok
}

// List returns all statuses sorted by policy name.
func (s *StatusStore) List() []PolicyStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PolicyStatus, 0, len(s.m))
	for _, st := range s.m {
		out = append(out, clone(st))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })
	return out
}

// Set stores st under st.PolicyName. A condition's LastTransitionTime is
// carried over from the previous status when its Status is unchanged, and
// stamped with the current time otherwise.
func (s *StatusStore) Set(st PolicyStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.m[st.PolicyName]
	now := s.now()
	st = clone(st)
	for i, c := range st.Conditions {
		if old, ok := prev.Condition(c.Type); ok && old.Status == c.Status {
			st.Conditions[i].LastTransitionTime = old.LastTransitionTime
		} else {
			st.Conditions[i].LastTransitionTime = now
		}
	}
	s.m[st.PolicyName] = st
}

// Delete removes the status for name.
func (s *StatusStore) Delete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, name)
}

func clone(st PolicyStatus) PolicyStatus {
	st.Conditions = append([]Condition(nil), st.Conditions...)
	st.LastActions = append([]state.Action(nil), st.LastActions...)
	if st.LastReconciledAt != nil {
		t := *st.LastReconciledAt
		st.LastReconciledAt = &t
	}
	if st.Details != nil {
		d := make(map[string]string, len(st.Details))
		for k, v := range st.Details {
			d[k] = v
		}
		st.Details = d
	}
	if st.Workload != nil {
		w := *st.Workload
		st.Workload = &w
	}
	return st
}
