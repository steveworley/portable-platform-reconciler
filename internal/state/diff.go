package state

import "fmt"

// Signal names what an action applies to. It is an open string; the built-in
// values cover telemetry and other reconcile steps may define their own.
type Signal string

// Telemetry signals.
const (
	SignalLogs    Signal = "logs"
	SignalMetrics Signal = "metrics"
	SignalTraces  Signal = "traces"
)

// ActionName names a remediation step. Open string, like Signal.
type ActionName string

// Action names produced by Diff.
const (
	ActionEnableLogs     ActionName = "enable_logs"
	ActionDisableLogs    ActionName = "disable_logs"
	ActionEnableMetrics  ActionName = "enable_metrics"
	ActionDisableMetrics ActionName = "disable_metrics"
	ActionEnableTraces   ActionName = "enable_traces"
	ActionDisableTraces  ActionName = "disable_traces"
)

// ReasonDrift is the reason attached to every action produced by Diff.
const ReasonDrift = "desired state differs from observed state"

// Action is one step required to move observed state to desired state.
type Action struct {
	Name   ActionName `json:"name"`
	Signal Signal     `json:"signal"`
	Reason string     `json:"reason"`
}

// Capability is a runtime feature that desired state may depend on.
type Capability string

// Capabilities consulted by Diff. They mirror runtime.Capabilities fields.
const (
	CapTelemetryConfig Capability = "TelemetryConfig"
)

// CapabilityChecker reports whether a runtime supports a capability.
// runtime.Capabilities implements it.
type CapabilityChecker interface {
	Supports(c Capability) bool
}

// DiffResult is the outcome of comparing desired and observed state.
type DiffResult struct {
	// Actions are the steps needed to converge; empty means compliant.
	Actions []Action
	// Unsupported lists drifted features the runtime cannot satisfy. No action
	// is produced for them.
	Unsupported []string
}

// InSync reports whether no action is required.
func (r DiffResult) InSync() bool { return len(r.Actions) == 0 }

// Diff compares desired with observed state. It is pure. Every signal that
// differs requires CapTelemetryConfig; if caps lacks it the signal is reported
// in Unsupported instead of Actions. A nil caps means all capabilities.
func Diff(desired DesiredState, observed ObservedState, caps CapabilityChecker) DiffResult {
	var res DiffResult
	signals := []struct {
		sig         Signal
		want, have  bool
		enable, off ActionName
	}{
		{SignalLogs, desired.Logs, observed.Logs, ActionEnableLogs, ActionDisableLogs},
		{SignalMetrics, desired.Metrics, observed.Metrics, ActionEnableMetrics, ActionDisableMetrics},
		{SignalTraces, desired.Traces, observed.Traces, ActionEnableTraces, ActionDisableTraces},
	}
	for _, s := range signals {
		if s.want == s.have {
			continue
		}
		if caps != nil && !caps.Supports(CapTelemetryConfig) {
			res.Unsupported = append(res.Unsupported, fmt.Sprintf("%s: requires %s capability", s.sig, CapTelemetryConfig))
			continue
		}
		name := s.enable
		if !s.want {
			name = s.off
		}
		res.Actions = append(res.Actions, Action{Name: name, Signal: s.sig, Reason: ReasonDrift})
	}
	return res
}

// ReconcileResult reports what an adapter did.
type ReconcileResult struct {
	Applied []Action `json:"applied,omitempty"`
	Changed bool     `json:"changed"`
	Message string   `json:"message,omitempty"`
}
