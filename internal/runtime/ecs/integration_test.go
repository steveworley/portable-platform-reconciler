package ecs

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
)

func configAuto() config.RuntimeConfig { return config.RuntimeConfig{Type: config.RuntimeAuto} }

func loadPolicy(t *testing.T) policy.ObservabilityPolicy {
	t.Helper()
	data, err := os.ReadFile("../../../examples/ecs/ecs-policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	p, err := policy.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestScenarios exercises scenarios A (drift), B (duplicate event) and C
// (missed event, caught by resync) through the real engine.
func TestScenarios(t *testing.T) {
	ctx := context.Background()
	sim := NewSeededSimulation()
	adapter := New(sim, WithRemediator(sim.Remediator()), WithLogger(slog.New(slog.DiscardHandler)))
	pol := loadPolicy(t)
	store := controller.NewMemoryPolicyStore(pol)
	statuses := controller.NewStatusStore()
	engine := controller.NewEngine(adapter, store, statuses, nil)

	eventKey, err := adapter.QueueKey(pol.Target)
	if err != nil {
		t.Fatal(err)
	}
	arn := ServiceARN(SeedRegion, SeedAccount, SeedCluster, SeedService)

	// A: event key -> drift detected -> enable_logs.
	outs, err := engine.ReconcileOnce(ctx, eventKey)
	if err != nil || len(outs) != 1 {
		t.Fatalf("first ReconcileOnce() = %+v, %v", outs, err)
	}
	if outs[0].NoOp || len(outs[0].Actions) != 1 || outs[0].Actions[0].Name != "enable_logs" || outs[0].Phase != controller.PhaseReady {
		t.Errorf("first outcome = %+v, want one enable_logs action and Ready", outs[0])
	}

	// B: identical event again -> no-op.
	registered := sim.CallCount(CallRegisterTaskDefinition)
	outs, err = engine.ReconcileOnce(ctx, eventKey)
	if err != nil || len(outs) != 1 || !outs[0].NoOp || len(outs[0].Actions) != 0 {
		t.Fatalf("second ReconcileOnce() = %+v, %v, want NoOp", outs, err)
	}
	if sim.CallCount(CallRegisterTaskDefinition) != registered {
		t.Error("second reconcile registered a task definition")
	}

	// C: out-of-band drift with no event; a policy-key reconcile (resync) repairs it.
	if err := sim.SetLogging(arn, false); err != nil {
		t.Fatal(err)
	}
	outs, err = engine.ReconcileOnce(ctx, policy.Key(pol.Metadata.Name))
	if err != nil || len(outs) != 1 || len(outs[0].Actions) != 1 || outs[0].Actions[0].Name != "enable_logs" {
		t.Fatalf("resync ReconcileOnce() = %+v, %v, want enable_logs", outs, err)
	}
	st, _ := statuses.Get(pol.Metadata.Name)
	if st.Phase != controller.PhaseReady {
		t.Errorf("phase = %s, want Ready", st.Phase)
	}
	if rev, _ := sim.Revision(arn); rev != 4 {
		t.Errorf("revision = %d, want 4 (initial, remediation, drift, remediation)", rev)
	}
}

// TestLogRemediatorEngineDegraded documents that "aws" mode with the
// LogRemediator leaves drift in place, so the engine reports it as not converged.
func TestLogRemediatorEngineDegraded(t *testing.T) {
	sim := NewSeededSimulation()
	adapter := New(sim) // LogRemediator
	pol := loadPolicy(t)
	statuses := controller.NewStatusStore()
	engine := controller.NewEngine(adapter, controller.NewMemoryPolicyStore(pol), statuses, nil)
	if _, err := engine.ReconcileOnce(context.Background(), policy.Key(pol.Metadata.Name)); err == nil {
		t.Error("ReconcileOnce() = nil error, want drift-remains error")
	}
	if st, _ := statuses.Get(pol.Metadata.Name); st.Phase != controller.PhaseDegraded {
		t.Errorf("phase = %s, want Degraded", st.Phase)
	}
}

func TestFactorySimulated(t *testing.T) {
	cfg := config.Default()
	a, err := runtime.New(RuntimeType, cfg)
	if err != nil {
		t.Fatal(err)
	}
	wl, err := a.ResolveTarget(context.Background(), loadPolicy(t).Target)
	if err != nil {
		t.Fatalf("ResolveTarget() on seeded simulation: %v", err)
	}
	obs, err := a.Observe(context.Background(), wl)
	if err != nil || obs.Logs || !obs.Metrics || !obs.Traces {
		t.Errorf("seeded observe = %+v, %v, want logs off, metrics+traces on", obs, err)
	}
	cfg.ECS.Mode = "bogus"
	if _, err := runtime.New(RuntimeType, cfg); err == nil {
		t.Error("New(bogus mode) = nil error")
	}
}
