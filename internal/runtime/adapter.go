// Package runtime defines the runtime adapter abstraction that keeps the
// reconciliation engine independent of Kubernetes and ECS APIs.
package runtime

import (
	"context"
	"errors"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// RuntimeType identifies a runtime. It is an open string: adding a platform
// means registering a new value (see Register), not editing a closed set.
type RuntimeType string

// ErrNotFound is returned (possibly wrapped) by ResolveTarget when the target
// workload does not exist.
var ErrNotFound = errors.New("target not found")

// Workload is a resolved target.
type Workload struct {
	Identity WorkloadIdentity
	Ref      policy.TargetRef
	// Revision is the task-definition revision or deployment generation.
	Revision string
	// Capabilities, when non-nil, overrides the adapter-wide capabilities for
	// this workload (e.g. ECS Fargate vs EC2 launch type).
	Capabilities *Capabilities
	// Raw is an adapter-private handle. The engine must not inspect it.
	Raw any
}

// EffectiveCapabilities returns the workload's own capabilities if it carries
// them, otherwise def (normally the adapter-wide RuntimeAdapter.Capabilities).
func (w Workload) EffectiveCapabilities(def Capabilities) Capabilities {
	if w.Capabilities != nil {
		return *w.Capabilities
	}
	return def
}

// RuntimeAdapter hides a runtime's APIs from the engine. Adapters read
// authoritative state; the engine decides what to do and passes the actions in.
type RuntimeAdapter interface {
	// Runtime returns the runtime this adapter serves.
	Runtime() RuntimeType
	// Capabilities reports the runtime-wide default capabilities. A resolved
	// Workload may override them; see Workload.EffectiveCapabilities.
	Capabilities() Capabilities
	// QueueKey returns the work-queue key for a target, e.g.
	// "ECSService:<arn>" or "KubernetesDeployment:ns/name". Event sources must
	// produce identical keys so the engine can map events to policies.
	QueueKey(target policy.TargetRef) (string, error)
	// ResolveTarget locates the workload. It returns ErrNotFound if absent.
	ResolveTarget(ctx context.Context, target policy.TargetRef) (Workload, error)
	// Observe reads the authoritative current state from the runtime API.
	Observe(ctx context.Context, workload Workload) (state.ObservedState, error)
	// Reconcile executes the engine-computed actions against the runtime.
	Reconcile(ctx context.Context, desired state.DesiredState, observed state.ObservedState, actions []state.Action) (state.ReconcileResult, error)
}
