package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/runtime/fake"
	"github.com/example/portable-platform-reconciler/internal/state"
)

const testRuntime runtime.RuntimeType = "fake"

func testPolicy(name, rt string) policy.ObservabilityPolicy {
	return policy.ObservabilityPolicy{
		APIVersion: policy.APIVersion, Kind: policy.Kind,
		Metadata: policy.Metadata{Name: name, Generation: 1},
		Target:   policy.TargetRef{Runtime: rt, Name: name},
		Spec: policy.Spec{
			Telemetry: policy.Telemetry{
				Logs: policy.SignalSpec{Enabled: true}, Metrics: policy.SignalSpec{Enabled: true}, Traces: policy.SignalSpec{Enabled: true},
			},
			Alerts: policy.Alerts{Profile: "standard"},
		},
	}
}

type harness struct {
	adapter  *fake.Adapter
	store    *MemoryPolicyStore
	statuses *StatusStore
	engine   *Engine

	mu       sync.Mutex
	outcomes []Outcome
}

func newHarness(t *testing.T, caps *runtime.Capabilities, opts ...Option) *harness {
	t.Helper()
	h := &harness{store: NewMemoryPolicyStore(), statuses: NewStatusStore()}
	if caps != nil {
		h.adapter = fake.New(testRuntime, *caps)
	} else {
		h.adapter = fake.New(testRuntime)
	}
	h.adapter.AddWorkload("payments-api", false, false, false)
	h.store.Upsert(testPolicy("payments-api", string(testRuntime)))
	opts = append([]Option{
		WithBackoff(time.Millisecond, 5*time.Millisecond),
		WithObserver(func(o Outcome) { h.mu.Lock(); h.outcomes = append(h.outcomes, o); h.mu.Unlock() }),
	}, opts...)
	h.engine = NewEngine(h.adapter, h.store, h.statuses, nil, opts...)
	return h
}

func (h *harness) seen() []Outcome {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Outcome(nil), h.outcomes...)
}

func (h *harness) run(t *testing.T, workers int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = h.engine.Run(ctx, workers); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestReconcileDriftThenIdempotent(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	key := policy.Key("payments-api")

	outs, err := h.engine.ReconcileOnce(ctx, key)
	if err != nil || len(outs) != 1 {
		t.Fatalf("ReconcileOnce() = %v, %v", outs, err)
	}
	if outs[0].NoOp || len(outs[0].Actions) != 3 || outs[0].Actions[0].Name != state.ActionEnableLogs {
		t.Errorf("first outcome = %+v, want 3 actions starting with enable_logs", outs[0])
	}
	if outs[0].Phase != PhaseReady {
		t.Errorf("Phase = %q, want Ready", outs[0].Phase)
	}

	outs, err = h.engine.ReconcileOnce(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !outs[0].NoOp || len(outs[0].Actions) != 0 {
		t.Errorf("second outcome = %+v, want no-op", outs[0])
	}
	if got := len(h.adapter.ReconcileCalls()); got != 1 {
		t.Errorf("adapter.Reconcile calls = %d, want 1", got)
	}

	st, ok := h.statuses.Get("payments-api")
	if !ok || st.Phase != PhaseReady || st.ObservedGeneration != 1 || st.LastReconciledAt == nil {
		t.Fatalf("status = %+v", st)
	}
	for _, ct := range []ConditionType{ConditionTargetResolved, ConditionConfigurationCompliant, ConditionReconciled, ConditionCapabilitiesSatisfied} {
		if c, ok := st.Condition(ct); !ok || c.Status != ConditionTrue {
			t.Errorf("condition %s = %+v, want True", ct, c)
		}
	}
	if len(st.LastActions) != 3 {
		t.Errorf("LastActions retained across no-op = %v, want 3", st.LastActions)
	}
	if st.Workload == nil || st.Workload.RuntimeID != "payments-api" {
		t.Errorf("Workload = %+v", st.Workload)
	}
}

func TestEventKeyMapsToPolicy(t *testing.T) {
	h := newHarness(t, nil)
	h.store.Upsert(testPolicy("other", string(testRuntime)))
	h.adapter.AddWorkload("other", true, true, true)

	outs, err := h.engine.ReconcileOnce(context.Background(), fake.Key("payments-api"))
	if err != nil || len(outs) != 1 || outs[0].Policy != "payments-api" || len(outs[0].Actions) != 3 {
		t.Errorf("ReconcileOnce(workload key) = %+v, %v", outs, err)
	}

	outs, err = h.engine.ReconcileOnce(context.Background(), "FakeWorkload:unknown")
	if err != nil || len(outs) != 0 {
		t.Errorf("ReconcileOnce(unknown key) = %+v, %v; want no outcomes, nil", outs, err)
	}
}

func TestEnqueueRunsReconcile(t *testing.T) {
	h := newHarness(t, nil)
	h.run(t, 2)
	h.engine.Enqueue(fake.Key("payments-api"))
	h.engine.Enqueue(fake.Key("payments-api"))
	waitFor(t, "reconcile", func() bool { return len(h.seen()) >= 1 })
	if st, ok := h.statuses.Get("payments-api"); !ok || st.Phase != PhaseReady {
		t.Errorf("status = %+v", st)
	}
}

func TestPolicyUpsertEnqueues(t *testing.T) {
	h := newHarness(t, nil)
	h.run(t, 1)
	// The seeded policy was upserted before the engine subscribed; a new upsert must trigger reconcile.
	h.store.Upsert(testPolicy("payments-api", string(testRuntime)))
	waitFor(t, "reconcile after upsert", func() bool { return len(h.seen()) >= 1 })
	if got := h.seen()[0].Policy; got != "payments-api" {
		t.Errorf("Policy = %q", got)
	}
}

func TestDriftIntroducedOutOfBand(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	key := policy.Key("payments-api")
	if _, err := h.engine.ReconcileOnce(ctx, key); err != nil {
		t.Fatal(err)
	}
	h.adapter.SetTelemetry("payments-api", false, true, true)
	outs, err := h.engine.ReconcileOnce(ctx, key)
	if err != nil || len(outs[0].Actions) != 1 || outs[0].Actions[0].Name != state.ActionEnableLogs {
		t.Errorf("outcome = %+v, %v; want single enable_logs", outs, err)
	}
}

func TestRetryOnAdapterErrorThenSuccess(t *testing.T) {
	h := newHarness(t, nil)
	h.adapter.FailNext(fake.OpObserve, 2, errors.New("boom"))
	h.run(t, 1)
	h.engine.Enqueue(policy.Key("payments-api"))
	waitFor(t, "success after retries", func() bool {
		o := h.seen()
		return len(o) >= 3 && o[len(o)-1].Err == nil
	})
	o := h.seen()
	if len(o) != 3 || o[0].Err == nil || o[1].Err == nil || o[2].Phase != PhaseReady {
		t.Errorf("outcomes = %+v, want 2 errors then Ready", o)
	}
	if got := h.engine.queue.NumRequeues(policy.Key("payments-api")); got != 0 {
		t.Errorf("NumRequeues after success = %d, want 0", got)
	}
}

func TestRetryGivesUpAfterMaxRetries(t *testing.T) {
	h := newHarness(t, nil, WithMaxRetries(2))
	h.adapter.FailNext(fake.OpResolve, 100, errors.New("boom"))
	h.run(t, 1)
	key := policy.Key("payments-api")
	h.engine.Enqueue(key)
	waitFor(t, "give up", func() bool { return len(h.seen()) >= 3 && h.engine.queue.NumRequeues(key) == 0 })
	time.Sleep(30 * time.Millisecond)
	if got := len(h.seen()); got != 3 {
		t.Errorf("attempts = %d, want 3 (1 + 2 retries)", got)
	}
	if st, _ := h.statuses.Get("payments-api"); st.Phase != PhaseError {
		t.Errorf("Phase = %q, want Error", st.Phase)
	}
}

func TestUnsupportedCapabilityDegraded(t *testing.T) {
	h := newHarness(t, &runtime.Capabilities{WorkloadDiscovery: true})
	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil {
		t.Fatal(err)
	}
	if outs[0].Phase != PhaseDegraded || len(outs[0].Unsupported) != 3 || len(outs[0].Actions) != 0 {
		t.Errorf("outcome = %+v", outs[0])
	}
	if got := len(h.adapter.ReconcileCalls()); got != 0 {
		t.Errorf("Reconcile calls = %d, want 0", got)
	}
	st, _ := h.statuses.Get("payments-api")
	if c, _ := st.Condition(ConditionCapabilitiesSatisfied); c.Status != ConditionFalse {
		t.Errorf("CapabilitiesSatisfied = %+v", c)
	}
}

func TestTargetNotFoundIsPendingNotError(t *testing.T) {
	h := newHarness(t, nil)
	h.adapter.RemoveWorkload("payments-api")
	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil {
		t.Fatalf("ReconcileOnce() error = %v, want nil", err)
	}
	if outs[0].Phase != PhasePending {
		t.Errorf("Phase = %q, want Pending", outs[0].Phase)
	}
	st, _ := h.statuses.Get("payments-api")
	if c, _ := st.Condition(ConditionTargetResolved); c.Status != ConditionFalse {
		t.Errorf("TargetResolved = %+v", c)
	}
}

func TestPolicyForOtherRuntimeSkipped(t *testing.T) {
	h := newHarness(t, nil)
	h.store.Upsert(testPolicy("elsewhere", "other-runtime"))
	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("elsewhere"))
	if err != nil || len(outs) != 1 || !outs[0].NoOp {
		t.Errorf("outcome = %+v, %v; want skipped no-op", outs, err)
	}
	if _, ok := h.statuses.Get("elsewhere"); ok {
		t.Error("status recorded for policy of another runtime")
	}
	if got := len(h.adapter.ReconcileCalls()); got != 0 {
		t.Errorf("Reconcile calls = %d, want 0", got)
	}
}

func TestDeletedPolicyDropsStatus(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	if _, err := h.engine.ReconcileOnce(ctx, policy.Key("payments-api")); err != nil {
		t.Fatal(err)
	}
	h.store.Delete("payments-api")
	if _, err := h.engine.ReconcileOnce(ctx, policy.Key("payments-api")); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.statuses.Get("payments-api"); ok {
		t.Error("status still present after policy deleted")
	}
}

type stubStep struct {
	res   StepResult
	err   error
	calls int
	in    StepInput
}

func (s *stubStep) Name() string { return "AlertsReconciled" }
func (s *stubStep) Reconcile(_ context.Context, in StepInput) (StepResult, error) {
	s.calls++
	s.in = in
	return s.res, s.err
}

func TestReconcileStepContributesStatus(t *testing.T) {
	step := &stubStep{res: StepResult{
		Conditions: []Condition{{Type: "AlertsReconciled", Status: ConditionTrue, Reason: "InSync"}},
		Actions:    []state.Action{{Name: "create_alert_rule", Signal: "latency", Reason: "missing"}},
		Details:    map[string]string{"alerts.managedRules": "4"},
	}}
	h := newHarness(t, nil, WithSteps(step))
	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil {
		t.Fatal(err)
	}
	if step.in.Workload.Identity.Workload != "payments-api" || step.in.Policy.Metadata.Name != "payments-api" || !step.in.Observed.Logs {
		t.Errorf("step input = %+v", step.in)
	}
	if len(outs[0].Actions) != 4 {
		t.Errorf("Actions = %d, want 3 runtime + 1 step", len(outs[0].Actions))
	}
	st, _ := h.statuses.Get("payments-api")
	if c, ok := st.Condition("AlertsReconciled"); !ok || c.Status != ConditionTrue {
		t.Errorf("AlertsReconciled = %+v", c)
	}
	if st.Details["alerts.managedRules"] != "4" {
		t.Errorf("Details = %v", st.Details)
	}

	// Second pass: runtime in sync; step returns no actions -> overall no-op.
	step.res.Actions = nil
	outs, _ = h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if !outs[0].NoOp || step.calls != 2 {
		t.Errorf("outcome = %+v calls = %d, want no-op and 2 step calls", outs[0], step.calls)
	}
}

func TestReconcileStepErrorAndUnsupported(t *testing.T) {
	step := &stubStep{err: errors.New("backend down")}
	h := newHarness(t, nil, WithSteps(step))
	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err == nil || outs[0].Phase != PhaseError {
		t.Fatalf("outcome = %+v, err = %v; want Error phase and error", outs[0], err)
	}
	st, _ := h.statuses.Get("payments-api")
	if c, _ := st.Condition("AlertsReconciled"); c.Status != ConditionFalse {
		t.Errorf("AlertsReconciled = %+v, want False", c)
	}

	step.err = nil
	step.res = StepResult{Unsupported: []string{"saturation: needs host telemetry"}}
	outs, err = h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil || outs[0].Phase != PhaseDegraded {
		t.Errorf("outcome = %+v, err = %v; want Degraded, nil", outs[0], err)
	}
}

func TestDryRunDegradedWithoutRetry(t *testing.T) {
	h := newHarness(t, nil)
	h.adapter.SetDryRun(true)

	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil {
		t.Fatalf("ReconcileOnce() error = %v, want nil (no retry)", err)
	}
	if len(outs) != 1 || !outs[0].DryRun || outs[0].Phase != PhaseDegraded || len(outs[0].Actions) != 3 {
		t.Fatalf("outcome = %+v, want DryRun Degraded with 3 actions", outs)
	}
	st, _ := h.statuses.Get("payments-api")
	c, _ := st.Condition(ConditionConfigurationCompliant)
	if st.Phase != PhaseDegraded || c.Status != ConditionFalse || c.Reason != "DryRun" || !strings.Contains(c.Message, string(state.ActionEnableLogs)) {
		t.Errorf("phase = %q, condition = %+v", st.Phase, c)
	}
	if r, _ := st.Condition(ConditionReconciled); r.Status != ConditionTrue {
		t.Errorf("Reconciled = %+v, want True", r)
	}

	// Through the queue: one reconcile, no backoff retries.
	h.run(t, 1)
	h.engine.Enqueue(policy.Key("payments-api"))
	waitFor(t, "first reconcile", func() bool { return len(h.seen()) >= 2 })
	time.Sleep(30 * time.Millisecond)
	if n := len(h.seen()); n != 2 {
		t.Errorf("observed %d reconciles, want 2 (one direct, one queued, no retries)", n)
	}
}

func TestPerWorkloadCapabilities(t *testing.T) {
	h := newHarness(t, nil)
	h.adapter.SetWorkloadCapabilities("payments-api", runtime.Capabilities{WorkloadDiscovery: true})

	outs, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api"))
	if err != nil || len(outs) != 1 {
		t.Fatalf("ReconcileOnce() = %+v, %v", outs, err)
	}
	if len(outs[0].Actions) != 0 || len(outs[0].Unsupported) != 3 || outs[0].Phase != PhaseDegraded {
		t.Errorf("outcome = %+v, want 3 unsupported from workload caps (adapter default supports telemetry)", outs[0])
	}
}

type recordingStep struct {
	caps    runtime.Capabilities
	fin     []FinalizeInput
	finErrs int
}

func (s *recordingStep) Name() string { return "rec" }
func (s *recordingStep) Reconcile(_ context.Context, in StepInput) (StepResult, error) {
	s.caps = in.Capabilities
	return StepResult{}, nil
}
func (s *recordingStep) Finalize(_ context.Context, in FinalizeInput) error {
	if s.finErrs > 0 {
		s.finErrs--
		return errors.New("backend down")
	}
	s.fin = append(s.fin, in)
	return nil
}

func TestStepInputUsesWorkloadCapabilities(t *testing.T) {
	step := &recordingStep{}
	h := newHarness(t, nil, WithSteps(step))
	want := runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, HostTelemetry: true}
	h.adapter.SetWorkloadCapabilities("payments-api", want)
	if _, err := h.engine.ReconcileOnce(context.Background(), policy.Key("payments-api")); err != nil {
		t.Fatal(err)
	}
	if step.caps != want {
		t.Errorf("StepInput.Capabilities = %+v, want %+v", step.caps, want)
	}
}

func TestFinalizeOnPolicyDelete(t *testing.T) {
	step := &recordingStep{finErrs: 1}
	h := newHarness(t, nil, WithSteps(step))
	ctx := context.Background()
	key := policy.Key("payments-api")
	if _, err := h.engine.ReconcileOnce(ctx, key); err != nil {
		t.Fatal(err)
	}

	h.store.Delete("payments-api")
	if _, err := h.engine.ReconcileOnce(ctx, key); err == nil {
		t.Fatal("ReconcileOnce() = nil error, want finalize failure")
	}
	st, ok := h.statuses.Get("payments-api")
	if !ok {
		t.Fatal("status deleted despite finalizer failure")
	}
	if c, _ := st.Condition(ConditionFinalized); c.Status != ConditionFalse || c.Reason != "FinalizeFailed" {
		t.Errorf("Finalized condition = %+v", c)
	}

	if _, err := h.engine.ReconcileOnce(ctx, key); err != nil {
		t.Fatalf("retry error = %v", err)
	}
	if _, ok := h.statuses.Get("payments-api"); ok {
		t.Error("status kept after successful finalize")
	}
	if len(step.fin) != 1 || step.fin[0].PolicyName != "payments-api" || step.fin[0].Workload.Workload != "payments-api" {
		t.Errorf("finalize inputs = %+v", step.fin)
	}
}
