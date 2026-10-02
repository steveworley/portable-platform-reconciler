package alerting

import (
	"fmt"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// DesiredRules expands the policy's alerts profile from DefaultProfiles.
// See Profiles.Desired.
func DesiredRules(p policy.ObservabilityPolicy, id state.WorkloadIdentity, caps runtime.Capabilities) (rules []ManagedRule, unsupported []string, err error) {
	return DefaultProfiles.Desired(p, id, caps)
}

// Desired is a pure function that expands spec.alerts.profile against the
// canonical identity. Templates whose required capability or telemetry is
// missing are returned in unsupported (not as errors), in profile order. An
// empty profile yields no rules; an unknown one wraps ErrUnknownProfile.
func (p *Profiles) Desired(pol policy.ObservabilityPolicy, id state.WorkloadIdentity, caps runtime.Capabilities) (rules []ManagedRule, unsupported []string, err error) {
	name := pol.Spec.Alerts.Profile
	if name == "" {
		return nil, nil, nil
	}
	prof, err := p.Get(name)
	if err != nil {
		return nil, nil, err
	}
	owner := OwnerFor(pol.Metadata.Name, id)
	for _, t := range prof.Rules {
		if t.RequiresCapability != "" && !caps.Supports(t.RequiresCapability) {
			unsupported = append(unsupported, fmt.Sprintf("alert %s: runtime lacks capability %s", t.ID, t.RequiresCapability))
			continue
		}
		if len(t.RequiresTelemetry) > 0 && !anyTelemetry(pol.Spec.Telemetry, t.RequiresTelemetry) {
			unsupported = append(unsupported, fmt.Sprintf("alert %s: requires one of %v telemetry enabled in the policy", t.ID, t.RequiresTelemetry))
			continue
		}
		rules = append(rules, ManagedRule{Rule: t, Owner: owner, Identity: id})
	}
	return rules, unsupported, nil
}

func anyTelemetry(t policy.Telemetry, names []string) bool {
	for _, n := range names {
		switch n {
		case TelemetryMetrics:
			if t.Metrics.Enabled {
				return true
			}
		case TelemetryTraces:
			if t.Traces.Enabled {
				return true
			}
		}
	}
	return false
}
