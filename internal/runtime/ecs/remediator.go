package ecs

import (
	"context"
	"log/slog"

	"github.com/example/portable-platform-reconciler/internal/state"
)

// Remediator applies engine-computed actions for an ECS service. The adapter
// never edits real ECS task definitions itself; what "applying" means is
// delegated to a Remediator.
type Remediator interface {
	Remediate(ctx context.Context, id state.WorkloadIdentity, actions []state.Action) (state.ReconcileResult, error)
}

// LogRemediator only logs the actions it would take. It is used in "aws" mode
// because the POC never modifies production task definitions (spec section 7).
//
// It reports Changed=false, so the engine's re-observe still sees drift and
// marks the policy Degraded. That is expected for simulated remediation
// against a real backend.
type LogRemediator struct {
	logger *slog.Logger
}

var _ Remediator = (*LogRemediator)(nil)

// NewLogRemediator returns a LogRemediator; a nil logger discards output.
func NewLogRemediator(logger *slog.Logger) *LogRemediator {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &LogRemediator{logger: logger}
}

// Remediate logs one line per action and changes nothing.
func (r *LogRemediator) Remediate(_ context.Context, id state.WorkloadIdentity, actions []state.Action) (state.ReconcileResult, error) {
	for _, a := range actions {
		r.logger.Info("simulated remediation",
			"resource", id.Workload,
			"runtime", id.Runtime,
			"action", string(a.Name),
			"reason", state.ReasonDrift,
		)
	}
	return state.ReconcileResult{
		Applied: actions,
		Changed: false,
		Message: "simulated: ECS task definitions are never modified; actions were only logged",
	}, nil
}
