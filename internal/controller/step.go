package controller

import (
	"context"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// StepInput is everything a ReconcileStep receives. Steps must treat it as read-only.
type StepInput struct {
	Policy policy.ObservabilityPolicy
	// Desired is the runtime-neutral desired state derived from Policy.
	Desired state.DesiredState
	// Workload is the resolved target; Workload.Identity is the canonical identity.
	Workload runtime.Workload
	// Observed is the runtime state after the runtime reconcile (re-read if actions ran).
	Observed state.ObservedState
	// Capabilities are those of the runtime adapter.
	Capabilities runtime.Capabilities
}

// StepResult is what a step contributes to the policy status and outcome.
type StepResult struct {
	// Conditions are merged into the policy status by type.
	Conditions []Condition
	// Actions the step performed; appended to Outcome.Actions and PolicyStatus.LastActions.
	Actions []state.Action
	// Unsupported lists desired features the step could not satisfy; reported, not failed.
	Unsupported []string
	// Degraded marks the policy phase Degraded without failing the reconcile.
	Degraded bool
	// Details are merged into PolicyStatus.Details (e.g. "alerts.managedRules": "4").
	Details map[string]string
}

// ReconcileStep is an additional per-policy reconcile stage that runs after the
// runtime observe/diff/reconcile (e.g. reconciling alert rules into a backend).
// It must be idempotent and read authoritative state itself. A returned error
// fails the reconcile (and retries it); the engine then records a False
// condition named after Name() unless the step already supplied one.
type ReconcileStep interface {
	Name() string
	Reconcile(ctx context.Context, in StepInput) (StepResult, error)
}

// WithSteps appends reconcile steps, run in order after the runtime stage.
func WithSteps(steps ...ReconcileStep) Option {
	return func(e *Engine) { e.steps = append(e.steps, steps...) }
}
