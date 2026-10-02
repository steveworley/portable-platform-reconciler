package resync

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime/fake"
	"github.com/example/portable-platform-reconciler/internal/state"
)

type recorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *recorder) Enqueue(k string) { r.mu.Lock(); r.keys = append(r.keys, k); r.mu.Unlock() }
func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keys...)
}

func testPolicy(name string) policy.ObservabilityPolicy {
	return policy.ObservabilityPolicy{
		Metadata: policy.Metadata{Name: name, Generation: 1},
		Target:   policy.TargetRef{Runtime: "fake", Name: name},
		Spec:     policy.Spec{Telemetry: policy.Telemetry{Logs: policy.SignalSpec{Enabled: true}}},
	}
}

func TestResyncEnqueuesAllPolicies(t *testing.T) {
	store := controller.NewMemoryPolicyStore(testPolicy("a"), testPolicy("b"))
	src, err := New(store, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- src.Start(ctx, rec) }()
	time.Sleep(60 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	keys := rec.snapshot()
	if len(keys) < 4 {
		t.Fatalf("keys = %v, want at least two ticks of two policies", keys)
	}
	set := map[string]bool{}
	for _, k := range keys {
		set[k] = true
	}
	var uniq []string
	for k := range set {
		uniq = append(uniq, k)
	}
	sort.Strings(uniq)
	if len(uniq) != 2 || uniq[0] != "ObservabilityPolicy:a" || uniq[1] != "ObservabilityPolicy:b" {
		t.Errorf("unique keys = %v", uniq)
	}
}

func TestNewRejectsBadInterval(t *testing.T) {
	if _, err := New(controller.NewMemoryPolicyStore(), 0, nil); err == nil {
		t.Error("New(0) error = nil")
	}
}

// TestResyncCorrectsMissedEvent covers scenario C: drift is introduced with no
// event; the periodic resync alone triggers reconciliation.
func TestResyncCorrectsMissedEvent(t *testing.T) {
	adapter := fake.New("fake")
	adapter.AddWorkload("svc", true, true, true)
	store := controller.NewMemoryPolicyStore()
	p := testPolicy("svc")
	p.Spec.Telemetry.Metrics.Enabled = true
	p.Spec.Telemetry.Traces.Enabled = true
	store.Upsert(p)

	var mu sync.Mutex
	var outs []controller.Outcome
	engine := controller.NewEngine(adapter, store, controller.NewStatusStore(), nil,
		controller.WithObserver(func(o controller.Outcome) { mu.Lock(); outs = append(outs, o); mu.Unlock() }))
	src, err := New(store, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = engine.Run(ctx, 1) }()
	go func() { _ = src.Start(ctx, engine) }()

	// Introduce drift directly in the runtime, with no event.
	adapter.SetTelemetry("svc", false, true, true)

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		fixed := false
		for _, o := range outs {
			if len(o.Actions) == 1 && o.Actions[0].Name == state.ActionEnableLogs {
				fixed = true
			}
		}
		mu.Unlock()
		if fixed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resync did not reconcile the drift")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(adapter.ReconcileCalls()); got != 1 {
		t.Errorf("Reconcile calls = %d, want 1", got)
	}
}
