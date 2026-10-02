package state

import (
	"reflect"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/policy"
)

type caps struct{ telemetry bool }

func (c caps) Supports(cp Capability) bool { return cp == CapTelemetryConfig && c.telemetry }

func TestDiff(t *testing.T) {
	all := DesiredState{Logs: true, Metrics: true, Traces: true}
	tests := []struct {
		name        string
		desired     DesiredState
		observed    ObservedState
		caps        CapabilityChecker
		wantActions []ActionName
		wantUnsup   int
	}{
		{name: "equal", desired: all, observed: ObservedState{Logs: true, Metrics: true, Traces: true}, caps: caps{true}},
		{name: "all disabled none desired", desired: DesiredState{}, observed: ObservedState{}, caps: caps{true}},
		{name: "logs drift", desired: all, observed: ObservedState{Metrics: true, Traces: true}, caps: caps{true}, wantActions: []ActionName{ActionEnableLogs}},
		{name: "all drift", desired: all, observed: ObservedState{}, caps: caps{true}, wantActions: []ActionName{ActionEnableLogs, ActionEnableMetrics, ActionEnableTraces}},
		{name: "disable", desired: DesiredState{Logs: true}, observed: ObservedState{Logs: true, Traces: true}, caps: caps{true}, wantActions: []ActionName{ActionDisableTraces}},
		{name: "unsupported", desired: all, observed: ObservedState{Logs: true, Metrics: true}, caps: caps{false}, wantUnsup: 1},
		{name: "nil caps", desired: all, observed: ObservedState{}, wantActions: []ActionName{ActionEnableLogs, ActionEnableMetrics, ActionEnableTraces}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Diff(tt.desired, tt.observed, tt.caps)
			var names []ActionName
			for _, a := range got.Actions {
				names = append(names, a.Name)
				if a.Reason != ReasonDrift {
					t.Errorf("Reason = %q, want %q", a.Reason, ReasonDrift)
				}
			}
			if !reflect.DeepEqual(names, tt.wantActions) {
				t.Errorf("actions = %v, want %v", names, tt.wantActions)
			}
			if len(got.Unsupported) != tt.wantUnsup {
				t.Errorf("Unsupported = %v, want %d entries", got.Unsupported, tt.wantUnsup)
			}
			if got.InSync() != (len(tt.wantActions) == 0) {
				t.Errorf("InSync() = %v", got.InSync())
			}
		})
	}
}

func TestFromPolicy(t *testing.T) {
	p := policy.ObservabilityPolicy{
		Metadata: policy.Metadata{Name: "x", Generation: 3},
		Spec: policy.Spec{
			Telemetry: policy.Telemetry{Logs: policy.SignalSpec{Enabled: true}, Traces: policy.SignalSpec{Enabled: true}},
			Alerts:    policy.Alerts{Profile: "standard"},
		},
	}
	got := FromPolicy(p)
	want := DesiredState{PolicyName: "x", Generation: 3, Logs: true, Traces: true, AlertProfile: "standard"}
	if got != want {
		t.Errorf("FromPolicy() = %+v, want %+v", got, want)
	}
}

func TestIdentityString(t *testing.T) {
	id := WorkloadIdentity{Runtime: "kubernetes", RuntimeID: "payments/payments-api"}
	if got, want := id.String(), "kubernetes:payments/payments-api"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
