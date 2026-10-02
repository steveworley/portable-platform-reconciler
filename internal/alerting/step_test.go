package alerting_test

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/alerting/memory"
	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/runtime/fake"
)

const rt runtime.RuntimeType = "fake"

func pol(name, profile string) policy.ObservabilityPolicy {
	return policy.ObservabilityPolicy{
		APIVersion: policy.APIVersion, Kind: policy.Kind,
		Metadata: policy.Metadata{Name: name, Generation: 1},
		Target:   policy.TargetRef{Runtime: string(rt), Name: name},
		Spec: policy.Spec{
			Telemetry: policy.Telemetry{
				Logs: policy.SignalSpec{Enabled: true}, Metrics: policy.SignalSpec{Enabled: true}, Traces: policy.SignalSpec{Enabled: true},
			},
			Alerts: policy.Alerts{Profile: profile},
		},
	}
}

type env struct {
	adapter  *fake.Adapter
	store    *controller.MemoryPolicyStore
	statuses *controller.StatusStore
	engine   *controller.Engine
}

func newEnv(t *testing.T, store *controller.MemoryPolicyStore, backend alerting.Backend, caps runtime.Capabilities) *env {
	t.Helper()
	a := fake.New(rt, caps)
	a.AddWorkload("payments-api", true, true, true)
	e := &env{adapter: a, store: store, statuses: controller.NewStatusStore()}
	e.engine = controller.NewEngine(a, store, e.statuses, nil, controller.WithSteps(alerting.NewStep(backend)))
	return e
}

func (e *env) reconcile(t *testing.T) controller.Outcome {
	t.Helper()
	outs, err := e.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil || len(outs) != 1 {
		t.Fatalf("ReconcileOnce() = %+v, %v", outs, err)
	}
	return outs[0]
}

var allCaps = runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, HostTelemetry: true, AlertReconciliation: true}

func TestStepCreateIdempotentDriftAndRemove(t *testing.T) {
	be := memory.New()
	store := controller.NewMemoryPolicyStore(pol("payments-api", "standard"))
	e := newEnv(t, store, be, allCaps)

	out := e.reconcile(t)
	if got := be.Writes(); got != (memory.Writes{Creates: 6}) {
		t.Errorf("first reconcile writes = %+v, want 6 creates", got)
	}
	if len(out.Actions) != 6 || out.Actions[0].Name != alerting.ActionCreateRule || out.Actions[0].Reason != alerting.ReasonRuleDrift {
		t.Errorf("actions = %+v", out.Actions)
	}
	if out.Phase != controller.PhaseReady {
		t.Errorf("Phase = %q, want Ready", out.Phase)
	}
	st, _ := e.statuses.Get("payments-api")
	c, ok := st.Condition(alerting.ConditionAlertsReconciled)
	if !ok || c.Status != controller.ConditionTrue {
		t.Errorf("AlertsReconciled = %+v", c)
	}
	if st.Details["alerts.backend"] != "memory" || st.Details["alerts.managedRules"] != "6" || st.Details["alerts.profile"] != "standard" {
		t.Errorf("Details = %v", st.Details)
	}

	be.ResetWrites()
	out = e.reconcile(t)
	if be.Writes().Total() != 0 || !out.NoOp {
		t.Errorf("second reconcile: writes = %+v, NoOp = %v, want none", be.Writes(), out.NoOp)
	}

	if n := be.Tamper(alerting.RuleLatencyP95, 9999); n != 1 {
		t.Fatalf("Tamper() = %d", n)
	}
	out = e.reconcile(t)
	if got := be.Writes(); got != (memory.Writes{Updates: 1}) {
		t.Errorf("after tamper writes = %+v, want exactly 1 update", got)
	}
	if len(out.Actions) != 1 || out.Actions[0].Name != alerting.ActionUpdateRule || out.Actions[0].Signal != alerting.RuleLatencyP95 {
		t.Errorf("actions = %+v", out.Actions)
	}

	be.ResetWrites()
	be.Remove(alerting.RuleErrorRate)
	e.reconcile(t)
	if got := be.Writes(); got != (memory.Writes{Creates: 1}) {
		t.Errorf("after remove writes = %+v, want exactly 1 create", got)
	}
	be.ResetWrites()
	e.reconcile(t)
	if be.Writes().Total() != 0 {
		t.Errorf("final reconcile wrote %+v", be.Writes())
	}
}

func TestStepProfileChangeDeletesStaleRules(t *testing.T) {
	be := memory.New()
	store := controller.NewMemoryPolicyStore(pol("payments-api", "standard"))
	e := newEnv(t, store, be, allCaps)
	e.reconcile(t)
	be.ResetWrites()

	store.Upsert(pol("payments-api", "minimal"))
	e.reconcile(t)
	if got := be.Writes(); got != (memory.Writes{Deletes: 4}) {
		t.Errorf("writes = %+v, want 4 deletes", got)
	}
	if n := len(be.Rules()); n != 2 {
		t.Errorf("rules left = %d, want 2", n)
	}
}

func TestStepFargateHostCPUUnsupported(t *testing.T) {
	be := memory.New()
	caps := allCaps
	caps.HostTelemetry = false
	e := newEnv(t, controller.NewMemoryPolicyStore(pol("payments-api", "standard")), be, caps)
	out := e.reconcile(t)
	if out.Phase != controller.PhaseDegraded || len(out.Unsupported) != 1 {
		t.Errorf("Phase = %q, Unsupported = %v, want Degraded with one entry", out.Phase, out.Unsupported)
	}
	if n := len(be.Rules()); n != 5 {
		t.Errorf("rules = %d, want 5", n)
	}
	st, _ := e.statuses.Get("payments-api")
	if c, _ := st.Condition(alerting.ConditionAlertsReconciled); c.Status != controller.ConditionTrue {
		t.Errorf("AlertsReconciled = %+v, want True", c)
	}
}

func TestStepDisabledAndUnsupportedRuntimeAndBadProfile(t *testing.T) {
	tests := []struct {
		name       string
		profile    string
		caps       runtime.Capabilities
		wantStatus controller.ConditionStatus
		wantReason string
		wantPhase  controller.Phase
	}{
		{"no profile", "", allCaps, controller.ConditionTrue, alerting.ReasonDisabled, controller.PhaseReady},
		{"no alert capability", "standard", runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true}, controller.ConditionFalse, alerting.ReasonUnsupported, controller.PhaseDegraded},
		{"unknown profile", "gold", allCaps, controller.ConditionFalse, alerting.ReasonInvalidProfile, controller.PhaseDegraded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			be := memory.New()
			e := newEnv(t, controller.NewMemoryPolicyStore(pol("payments-api", tt.profile)), be, tt.caps)
			out := e.reconcile(t)
			st, _ := e.statuses.Get("payments-api")
			c, _ := st.Condition(alerting.ConditionAlertsReconciled)
			if c.Status != tt.wantStatus || c.Reason != tt.wantReason {
				t.Errorf("condition = %+v, want %s/%s", c, tt.wantStatus, tt.wantReason)
			}
			if out.Phase != tt.wantPhase {
				t.Errorf("Phase = %q, want %q", out.Phase, tt.wantPhase)
			}
			if be.Writes().Total() != 0 {
				t.Errorf("writes = %+v, want none", be.Writes())
			}
		})
	}
}

type failingBackend struct {
	*memory.Backend
	err error
}

func (f failingBackend) Create(context.Context, alerting.ManagedRule) error { return f.err }

func TestStepBackendFailureSurfacesCondition(t *testing.T) {
	boom := errors.New("boom")
	e := newEnv(t, controller.NewMemoryPolicyStore(pol("payments-api", "standard")), failingBackend{memory.New(), boom}, allCaps)
	outs, err := e.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if outs[0].Phase != controller.PhaseError {
		t.Errorf("Phase = %q, want Error", outs[0].Phase)
	}
	st, _ := e.statuses.Get("payments-api")
	if c, _ := st.Condition(alerting.ConditionAlertsReconciled); c.Status != controller.ConditionFalse || c.Reason != alerting.ReasonBackendFailed {
		t.Errorf("condition = %+v", c)
	}
}

// Scenario E: the same policies reconciled into backend A, then the backend
// is switched to B with no policy change.
func TestMigrationBetweenBackends(t *testing.T) {
	store := controller.NewMemoryPolicyStore(pol("payments-api", "standard"))
	a := memory.New(memory.WithName("a"))
	b := memory.New(memory.WithName("b"))

	newEnv(t, store, a, allCaps).reconcile(t)

	eb := newEnv(t, store, b, allCaps)
	eb.reconcile(t)

	ra, rb := a.Rules(), b.Rules()
	if len(rb) != 6 || len(ra) != len(rb) {
		t.Fatalf("rules: a=%d b=%d, want 6 each", len(ra), len(rb))
	}
	for i := range ra {
		if ra[i].ID != rb[i].ID || ra[i].Fingerprint() != rb[i].Fingerprint() || ra[i].Owner != rb[i].Owner {
			t.Errorf("rule %d differs: a=%s/%s b=%s/%s", i, ra[i].ID, ra[i].Fingerprint(), rb[i].ID, rb[i].Fingerprint())
		}
	}
	// Old backend is left untouched (cleanup is opt-in) and B is now idempotent.
	if a.Writes().Total() != 6 {
		t.Errorf("a writes = %+v, want unchanged", a.Writes())
	}
	b.ResetWrites()
	eb.reconcile(t)
	if b.Writes().Total() != 0 {
		t.Errorf("b second reconcile wrote %+v", b.Writes())
	}
	st, _ := eb.statuses.Get("payments-api")
	if st.Details["alerts.backend"] != "b" {
		t.Errorf("alerts.backend = %q, want b", st.Details["alerts.backend"])
	}
}

func TestBackendRegistry(t *testing.T) {
	r := alerting.NewBackendRegistry()
	r.Register("x", func(config.Config, *slog.Logger) (alerting.Backend, error) {
		return memory.New(memory.WithName("x")), nil
	})
	be, err := r.New("x", config.Default(), nil)
	if err != nil || be.Name() != "x" {
		t.Fatalf("New(x) = %v, %v", be, err)
	}
	if _, err := r.New("y", config.Default(), nil); !errors.Is(err, alerting.ErrUnknownBackend) {
		t.Errorf("New(y) error = %v", err)
	}
	if got := r.Names(); !reflect.DeepEqual(got, []string{"x"}) {
		t.Errorf("Names() = %v", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("duplicate Register must panic")
		}
	}()
	r.Register("x", func(config.Config, *slog.Logger) (alerting.Backend, error) { return nil, nil })
}

func TestDefaultRegistryHasMemory(t *testing.T) {
	be, err := alerting.New("memory", config.Default(), nil)
	if err != nil || be.Name() != "memory" {
		t.Errorf("New(memory) = %v, %v", be, err)
	}
}
