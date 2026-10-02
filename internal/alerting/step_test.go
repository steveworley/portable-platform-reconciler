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
	"github.com/example/portable-platform-reconciler/internal/state"
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

func TestFinalizeDeletesOnlyDeletedPolicysRules(t *testing.T) {
	ctx := context.Background()
	be := memory.New()
	store := controller.NewMemoryPolicyStore(pol("payments-api", "standard"), pol("orders", "minimal"))
	e := newEnv(t, store, be, allCaps)
	e.adapter.AddWorkload("orders", true, true, true)
	for _, name := range []string{"payments-api", "orders"} {
		if _, err := e.engine.ReconcileOnce(ctx, policy.Key(name)); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(be.Rules()); n != 8 {
		t.Fatalf("rules = %d, want 6 + 2", n)
	}

	store.Delete("payments-api")
	be.ResetWrites()
	if _, err := e.engine.ReconcileOnce(ctx, policy.Key("payments-api")); err != nil {
		t.Fatalf("ReconcileOnce() after delete = %v", err)
	}
	if got := be.Writes(); got != (memory.Writes{Deletes: 6}) {
		t.Errorf("writes = %+v, want 6 deletes", got)
	}
	rules := be.Rules()
	if len(rules) != 2 {
		t.Fatalf("rules left = %d, want 2", len(rules))
	}
	for _, r := range rules {
		if r.Owner.PolicyName != "orders" {
			t.Errorf("rule %s of %q survived, want only orders", r.ID, r.Owner.PolicyName)
		}
	}
	if _, ok := e.statuses.Get("payments-api"); ok {
		t.Error("status kept after finalize")
	}
}

type deleteFailingBackend struct {
	*memory.Backend
	fail bool
}

func (d *deleteFailingBackend) Delete(ctx context.Context, r alerting.ManagedRule) error {
	if d.fail {
		return errors.New("backend down")
	}
	return d.Backend.Delete(ctx, r)
}

func TestFinalizeFailureKeepsStatusAndRetries(t *testing.T) {
	ctx := context.Background()
	be := &deleteFailingBackend{Backend: memory.New()}
	store := controller.NewMemoryPolicyStore(pol("payments-api", "minimal"))
	e := newEnv(t, store, be, allCaps)
	e.reconcile(t)

	store.Delete("payments-api")
	be.fail = true
	if _, err := e.engine.ReconcileOnce(ctx, policy.Key("payments-api")); err == nil {
		t.Fatal("ReconcileOnce() = nil error, want finalize failure")
	}
	st, ok := e.statuses.Get("payments-api")
	if !ok {
		t.Fatal("status dropped while rules remain")
	}
	if c, _ := st.Condition(controller.ConditionFinalized); c.Status != controller.ConditionFalse {
		t.Errorf("Finalized = %+v, want False", c)
	}
	if len(be.Rules()) != 2 {
		t.Errorf("rules = %d, want 2 still present", len(be.Rules()))
	}

	be.fail = false
	if _, err := e.engine.ReconcileOnce(ctx, policy.Key("payments-api")); err != nil {
		t.Fatalf("retry error = %v", err)
	}
	if len(be.Rules()) != 0 {
		t.Errorf("rules = %d, want 0", len(be.Rules()))
	}
	if _, ok := e.statuses.Get("payments-api"); ok {
		t.Error("status kept after successful finalize")
	}
}

func seedRules(t *testing.T, be *memory.Backend, policies ...string) {
	t.Helper()
	for _, name := range policies {
		id := state.WorkloadIdentity{Service: name, Workload: name, Runtime: "fake", RuntimeID: name}
		rules, _, err := alerting.DesiredRules(pol(name, "minimal"), id, allCaps)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rules {
			if err := be.Create(context.Background(), r); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestPruneOrphans(t *testing.T) {
	ctx := context.Background()
	be := memory.New()
	seedRules(t, be, "live", "orphan")
	keep := func(o alerting.Owner) bool { return o.PolicyName == "live" }

	rep, err := alerting.Prune(ctx, be, keep, true)
	if err != nil || !rep.DryRun || len(rep.Pruned) != 2 || rep.Kept != 2 {
		t.Fatalf("dry run = %+v, %v, want 2 reported and 2 kept", rep, err)
	}
	if len(be.Rules()) != 4 || be.Writes().Deletes != 0 {
		t.Errorf("dry run changed the backend: %d rules, %+v", len(be.Rules()), be.Writes())
	}

	rep, err = alerting.Prune(ctx, be, keep, false)
	if err != nil || rep.DryRun || len(rep.Pruned) != 2 {
		t.Fatalf("prune = %+v, %v", rep, err)
	}
	for _, r := range be.Rules() {
		if r.Owner.PolicyName != "live" {
			t.Errorf("rule of %q survived", r.Owner.PolicyName)
		}
	}
	if len(be.Rules()) != 2 {
		t.Errorf("rules = %d, want 2", len(be.Rules()))
	}
}

func TestPruneKeepNothingCleansOldBackend(t *testing.T) {
	be := memory.New(memory.WithName("old"))
	seedRules(t, be, "a", "b")
	rep, err := alerting.Prune(context.Background(), be, nil, false)
	if err != nil || len(rep.Pruned) != 4 || rep.Kept != 0 || len(be.Rules()) != 0 {
		t.Errorf("Prune(nil keep) = %+v, %v, %d rules left", rep, err, len(be.Rules()))
	}
}

type listOnlyBackend struct{ alerting.Backend }

func TestPruneErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := alerting.Prune(ctx, listOnlyBackend{memory.New()}, nil, false); !errors.Is(err, alerting.ErrPruneUnsupported) {
		t.Errorf("non-Lister error = %v, want ErrPruneUnsupported", err)
	}

	be := &deleteFailingBackend{Backend: memory.New(), fail: true}
	seedRules(t, be.Backend, "a")
	rep, err := alerting.Prune(ctx, be, nil, false)
	if err == nil || len(rep.Pruned) != 0 {
		t.Errorf("Prune() = %+v, %v, want failures reported and none counted as pruned", rep, err)
	}
}
