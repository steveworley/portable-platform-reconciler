package runtime

import "github.com/example/portable-platform-reconciler/internal/state"

// Capabilities declares which features a runtime supports. Runtimes need not
// expose identical capabilities (e.g. ECS Fargate has no host-level telemetry).
type Capabilities struct {
	WorkloadDiscovery   bool
	TelemetryConfig     bool
	HostTelemetry       bool
	NetworkTelemetry    bool
	AlertReconciliation bool
}

var _ state.CapabilityChecker = Capabilities{}

// Supports reports whether the capability is available.
func (c Capabilities) Supports(cp state.Capability) bool {
	switch cp {
	case state.CapTelemetryConfig:
		return c.TelemetryConfig
	}
	return false
}
