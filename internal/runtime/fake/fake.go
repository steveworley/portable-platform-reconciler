// Package fake provides an in-memory runtime.RuntimeAdapter for tests and demos.
package fake

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// Operation names accepted by FailNext.
const (
	OpResolve   = "resolve"
	OpObserve   = "observe"
	OpReconcile = "reconcile"
)

// KeyPrefix prefixes queue keys produced by the fake adapter.
const KeyPrefix = "FakeWorkload:"

// Key returns the queue key for a workload named name.
func Key(name string) string { return KeyPrefix + name }

// Call records one Reconcile invocation.
type Call struct {
	Desired state.DesiredState
	Actions []state.Action
}

type workload struct {
	revision int
	logs     bool
	metrics  bool
	traces   bool
}

// Adapter is a RuntimeAdapter backed by memory. Targets are identified by
// TargetRef.Name. Safe for concurrent use.
type Adapter struct {
	rt   runtime.RuntimeType
	caps runtime.Capabilities

	mu        sync.Mutex
	workloads map[string]*workload
	calls     []Call
	failures  map[string]*failure
}

type failure struct {
	n   int
	err error
}

var _ runtime.RuntimeAdapter = (*Adapter)(nil)

// New creates a fake adapter for runtime type rt with all capabilities enabled
// unless caps is given.
func New(rt runtime.RuntimeType, caps ...runtime.Capabilities) *Adapter {
	c := runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, AlertReconciliation: true}
	if len(caps) > 0 {
		c = caps[0]
	}
	return &Adapter{rt: rt, caps: c, workloads: map[string]*workload{}, failures: map[string]*failure{}}
}

// AddWorkload registers a workload named name with the given observed telemetry.
func (a *Adapter) AddWorkload(name string, logs, metrics, traces bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.workloads[name] = &workload{revision: 1, logs: logs, metrics: metrics, traces: traces}
}

// RemoveWorkload deletes a workload so ResolveTarget returns ErrNotFound.
func (a *Adapter) RemoveWorkload(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.workloads, name)
}

// SetTelemetry mutates observed state out-of-band, simulating drift without an event.
func (a *Adapter) SetTelemetry(name string, logs, metrics, traces bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if w, ok := a.workloads[name]; ok {
		w.logs, w.metrics, w.traces = logs, metrics, traces
	}
}

// FailNext makes the next n calls of op ("resolve", "observe", "reconcile") return err.
func (a *Adapter) FailNext(op string, n int, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failures[op] = &failure{n: n, err: err}
}

// ReconcileCalls returns a copy of the recorded Reconcile calls.
func (a *Adapter) ReconcileCalls() []Call {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Call(nil), a.calls...)
}

// Runtime implements runtime.RuntimeAdapter.
func (a *Adapter) Runtime() runtime.RuntimeType { return a.rt }

// Capabilities implements runtime.RuntimeAdapter.
func (a *Adapter) Capabilities() runtime.Capabilities { return a.caps }

// QueueKey implements runtime.RuntimeAdapter. The key is Key(target.Name).
func (a *Adapter) QueueKey(t policy.TargetRef) (string, error) {
	if t.Name == "" {
		return "", fmt.Errorf("fake target requires name")
	}
	return Key(t.Name), nil
}

// ResolveTarget implements runtime.RuntimeAdapter.
func (a *Adapter) ResolveTarget(_ context.Context, t policy.TargetRef) (runtime.Workload, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.injected(OpResolve); err != nil {
		return runtime.Workload{}, err
	}
	w, ok := a.workloads[t.Name]
	if !ok {
		return runtime.Workload{}, fmt.Errorf("fake workload %q: %w", t.Name, runtime.ErrNotFound)
	}
	return runtime.Workload{Identity: a.identity(t), Ref: t, Revision: strconv.Itoa(w.revision)}, nil
}

// Observe implements runtime.RuntimeAdapter.
func (a *Adapter) Observe(_ context.Context, wl runtime.Workload) (state.ObservedState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.injected(OpObserve); err != nil {
		return state.ObservedState{}, err
	}
	w, ok := a.workloads[wl.Ref.Name]
	if !ok {
		return state.ObservedState{}, fmt.Errorf("fake workload %q: %w", wl.Ref.Name, runtime.ErrNotFound)
	}
	return state.ObservedState{
		Workload: wl.Identity,
		Revision: strconv.Itoa(w.revision),
		Logs:     w.logs, Metrics: w.metrics, Traces: w.traces,
		Details: map[string]string{"logDriver": "fake"},
	}, nil
}

// Reconcile implements runtime.RuntimeAdapter. It applies actions to the
// in-memory state, so a subsequent Observe reflects them.
func (a *Adapter) Reconcile(_ context.Context, desired state.DesiredState, observed state.ObservedState, actions []state.Action) (state.ReconcileResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, Call{Desired: desired, Actions: append([]state.Action(nil), actions...)})
	if err := a.injected(OpReconcile); err != nil {
		return state.ReconcileResult{}, err
	}
	w, ok := a.workloads[observed.Workload.Workload]
	if !ok {
		return state.ReconcileResult{}, fmt.Errorf("fake workload %q: %w", observed.Workload.Workload, runtime.ErrNotFound)
	}
	for _, act := range actions {
		on := act.Name == state.ActionEnableLogs || act.Name == state.ActionEnableMetrics || act.Name == state.ActionEnableTraces
		switch act.Signal {
		case state.SignalLogs:
			w.logs = on
		case state.SignalMetrics:
			w.metrics = on
		case state.SignalTraces:
			w.traces = on
		}
	}
	if len(actions) > 0 {
		w.revision++
	}
	return state.ReconcileResult{Applied: actions, Changed: len(actions) > 0, Message: "applied to fake runtime"}, nil
}

func (a *Adapter) identity(t policy.TargetRef) runtime.WorkloadIdentity {
	return runtime.WorkloadIdentity{
		Environment: t.Environment, Service: t.Name, Deployment: t.Name, Workload: t.Name,
		Runtime: string(a.rt), RuntimeID: t.Name,
	}
}

// injected must be called with a.mu held.
func (a *Adapter) injected(op string) error {
	if f, ok := a.failures[op]; ok && f.n > 0 {
		f.n--
		return f.err
	}
	return nil
}
