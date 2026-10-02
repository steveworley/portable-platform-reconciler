package state

import "github.com/example/portable-platform-reconciler/internal/policy"

// DesiredState is the runtime-neutral desired observability configuration.
// Reconcile steps that need more (e.g. alert rules) read the full policy from
// StepInput.Policy; DesiredState only carries what Diff and adapters consume.
type DesiredState struct {
	PolicyName   string `json:"policyName"`
	Profile      string `json:"profile,omitempty"`
	Generation   int64  `json:"generation"`
	Logs         bool   `json:"logs"`
	Metrics      bool   `json:"metrics"`
	Traces       bool   `json:"traces"`
	AlertProfile string `json:"alertProfile,omitempty"`
}

// FromPolicy derives the desired state from a policy.
func FromPolicy(p policy.ObservabilityPolicy) DesiredState {
	return DesiredState{
		PolicyName:   p.Metadata.Name,
		Profile:      p.Spec.Profile,
		Generation:   p.Metadata.Generation,
		Logs:         p.Spec.Telemetry.Logs.Enabled,
		Metrics:      p.Spec.Telemetry.Metrics.Enabled,
		Traces:       p.Spec.Telemetry.Traces.Enabled,
		AlertProfile: p.Spec.Alerts.Profile,
	}
}
