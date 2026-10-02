package alerting

import (
	"errors"
	"reflect"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

var (
	testID = state.WorkloadIdentity{Service: "payments-api", Workload: "payments-api", Runtime: "kubernetes", RuntimeID: "payments/payments-api"}

	fullCaps    = runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, HostTelemetry: true, AlertReconciliation: true}
	fargateCaps = runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, AlertReconciliation: true}
)

func testPolicy(profile string, metrics, traces bool) policy.ObservabilityPolicy {
	return policy.ObservabilityPolicy{
		Metadata: policy.Metadata{Name: "payments-api"},
		Spec: policy.Spec{
			Telemetry: policy.Telemetry{Metrics: policy.SignalSpec{Enabled: metrics}, Traces: policy.SignalSpec{Enabled: traces}},
			Alerts:    policy.Alerts{Profile: profile},
		},
	}
}

func ids(rs []ManagedRule) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

func TestDesiredRules(t *testing.T) {
	tests := []struct {
		name            string
		pol             policy.ObservabilityPolicy
		caps            runtime.Capabilities
		wantIDs         []string
		wantUnsupported int
		wantErr         error
	}{
		{name: "standard with host telemetry", pol: testPolicy("standard", true, false), caps: fullCaps,
			wantIDs: []string{RuleLatencyP95, RuleErrorRate, RuleTraffic, RuleContainerCPU, RuleContainerMemory, RuleHostCPU}},
		{name: "standard on fargate gates host cpu", pol: testPolicy("standard", true, false), caps: fargateCaps,
			wantIDs: []string{RuleLatencyP95, RuleErrorRate, RuleTraffic, RuleContainerCPU, RuleContainerMemory}, wantUnsupported: 1},
		{name: "minimal", pol: testPolicy("minimal", false, true), caps: fargateCaps,
			wantIDs: []string{RuleErrorRate, RuleLatencyP95}},
		{name: "no telemetry gates request signals", pol: testPolicy("standard", false, false), caps: fullCaps,
			wantIDs: []string{RuleContainerCPU, RuleContainerMemory, RuleHostCPU}, wantUnsupported: 3},
		{name: "empty profile", pol: testPolicy("", true, true), caps: fullCaps},
		{name: "unknown profile", pol: testPolicy("gold", true, true), caps: fullCaps, wantErr: ErrUnknownProfile},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules, unsupported, err := DesiredRules(tt.pol, testID, tt.caps)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("DesiredRules() error = %v, want %v", err, tt.wantErr)
			}
			if got := ids(rules); len(got)+len(tt.wantIDs) > 0 && !reflect.DeepEqual(got, tt.wantIDs) {
				t.Errorf("rule IDs = %v, want %v", got, tt.wantIDs)
			}
			if len(unsupported) != tt.wantUnsupported {
				t.Errorf("unsupported = %v, want %d entries", unsupported, tt.wantUnsupported)
			}
			for _, r := range rules {
				if r.Owner != (Owner{PolicyName: "payments-api", WorkloadKey: "kubernetes:payments/payments-api", Runtime: "kubernetes"}) {
					t.Errorf("rule %s owner = %+v", r.ID, r.Owner)
				}
			}
		})
	}
}

func TestDesiredRulesPure(t *testing.T) {
	a, _, _ := DesiredRules(testPolicy("standard", true, true), testID, fullCaps)
	b, _, _ := DesiredRules(testPolicy("standard", true, true), testID, fullCaps)
	if !reflect.DeepEqual(a, b) {
		t.Error("DesiredRules is not deterministic")
	}
}

func TestFingerprint(t *testing.T) {
	base := ManagedRule{Rule: ruleLatency, Identity: testID}
	if base.Fingerprint() != (ManagedRule{Rule: ruleLatency, Identity: testID}).Fingerprint() {
		t.Error("fingerprint not deterministic")
	}
	// Requires* are not content.
	other := base
	other.RequiresCapability = "HostTelemetry"
	if base.Fingerprint() != other.Fingerprint() {
		t.Error("fingerprint must ignore Requires fields")
	}
	mut := map[string]func(*ManagedRule){
		"threshold": func(r *ManagedRule) { r.Threshold = 501 },
		"window":    func(r *ManagedRule) { r.Window = 0 },
		"severity":  func(r *ManagedRule) { r.Severity = SeverityWarning },
		"operator":  func(r *ManagedRule) { r.Operator = OpBelow },
		"service":   func(r *ManagedRule) { r.Identity.Service = "other" },
		"divergent": func(r *ManagedRule) { r.Divergent = true },
	}
	for name, f := range mut {
		t.Run(name, func(t *testing.T) {
			r := base
			f(&r)
			if r.Fingerprint() == base.Fingerprint() {
				t.Errorf("fingerprint unchanged after changing %s", name)
			}
		})
	}
}

func TestTagsRoundTrip(t *testing.T) {
	r := ManagedRule{Rule: ruleErrors, Owner: OwnerFor("p", testID), Identity: testID}
	tags := r.Tags()
	if tags[TagManagedBy] != ManagedByValue || tags[TagWorkload] != "payments/payments-api" || tags[TagRuleID] != RuleErrorRate || tags[TagFingerprint] != r.Fingerprint() {
		t.Errorf("Tags() = %v", tags)
	}
	owner, id, ok := OwnerFromTags(tags)
	if !ok || owner != r.Owner || id != r.ID {
		t.Errorf("OwnerFromTags() = %+v, %q, %v", owner, id, ok)
	}
	if _, _, ok := OwnerFromTags(map[string]string{TagRuleID: "x"}); ok {
		t.Error("untagged rule must not be recognised as managed")
	}
}

func TestProfiles(t *testing.T) {
	if got := DefaultProfiles.Names(); !reflect.DeepEqual(got, []string{"minimal", "standard"}) {
		t.Errorf("Names() = %v", got)
	}
	if _, err := DefaultProfiles.Get("nope"); !errors.Is(err, ErrUnknownProfile) {
		t.Errorf("Get(nope) error = %v", err)
	}
}
