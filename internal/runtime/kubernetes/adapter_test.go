package kubernetes

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

func deployment(ns, name string, labels, annotations map[string]string) *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels, Annotations: annotations, Generation: 3},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"pod": name}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 3, ReadyReplicas: 1},
	}
}

func pod(ns, name, deploy string, ready bool) *corev1.Pod {
	st := corev1.ConditionFalse
	if ready {
		st = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{"pod": deploy}},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: st}}},
	}
}

func target(ns, name string) policy.TargetRef {
	return policy.TargetRef{Runtime: "kubernetes", Namespace: ns, Name: name}
}

func writes(c *fake.Clientset) int {
	n := 0
	for _, a := range c.Actions() {
		switch a.GetVerb() {
		case "patch", "update", "create", "delete":
			n++
		}
	}
	return n
}

func TestQueueKey(t *testing.T) {
	a := New(fake.NewSimpleClientset())
	got, err := a.QueueKey(target("payments", "api"))
	if err != nil {
		t.Fatalf("QueueKey() error = %v", err)
	}
	if want := "KubernetesDeployment:payments/api"; got != want {
		t.Errorf("QueueKey() = %q, want %q", got, want)
	}
	if got != DeploymentKey("payments", "api") {
		t.Errorf("QueueKey() = %q differs from DeploymentKey", got)
	}
}

func TestValidateTarget(t *testing.T) {
	tests := []struct {
		name    string
		target  policy.TargetRef
		wantErr string
	}{
		{"ok default kind", policy.TargetRef{Namespace: "ns", Name: "n"}, ""},
		{"ok deployment", policy.TargetRef{Namespace: "ns", Name: "n", Kind: "Deployment"}, ""},
		{"missing namespace", policy.TargetRef{Name: "n"}, "namespace"},
		{"missing name", policy.TargetRef{Namespace: "ns"}, "name"},
		{"statefulset", policy.TargetRef{Namespace: "ns", Name: "n", Kind: "StatefulSet"}, "StatefulSet"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTarget(tt.target)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("ValidateTarget() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("ValidateTarget() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRegistered(t *testing.T) {
	err := runtime.ValidateTarget(policy.TargetRef{Runtime: "kubernetes", Name: "x"})
	if err == nil {
		t.Error("registry ValidateTarget() = nil, want error for missing namespace")
	}
	if got := runtime.Default.Types(); !contains(got, RuntimeType) {
		t.Errorf("Default.Types() = %v, want to contain %q", got, RuntimeType)
	}
}

func contains(ts []runtime.RuntimeType, t runtime.RuntimeType) bool {
	for _, x := range ts {
		if x == t {
			return true
		}
	}
	return false
}

func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name      string
		objs      []*appsv1.Deployment
		target    policy.TargetRef
		opts      []Option
		wantErrIs error
		wantID    state.WorkloadIdentity
		wantRev   string
	}{
		{
			name:    "labels win",
			objs:    []*appsv1.Deployment{deployment("payments", "api-deploy", map[string]string{LabelAppName: "api", LabelApp: "legacy", LabelEnvironment: "prod"}, map[string]string{AnnotationRevision: "7"})},
			target:  policy.TargetRef{Runtime: "kubernetes", Namespace: "payments", Name: "api-deploy", Environment: "staging"},
			wantID:  state.WorkloadIdentity{Environment: "prod", Service: "api", Deployment: "api-deploy", Workload: "api-deploy", Runtime: "kubernetes", RuntimeID: "payments/api-deploy"},
			wantRev: "generation=3,observedGeneration=3,revision=7",
		},
		{
			name:    "app label and target environment",
			objs:    []*appsv1.Deployment{deployment("payments", "api", map[string]string{LabelApp: "legacy"}, nil)},
			target:  policy.TargetRef{Runtime: "kubernetes", Namespace: "payments", Name: "api", Environment: "staging"},
			wantID:  state.WorkloadIdentity{Environment: "staging", Service: "legacy", Deployment: "api", Workload: "api", Runtime: "kubernetes", RuntimeID: "payments/api"},
			wantRev: "generation=3,observedGeneration=3",
		},
		{
			name:    "adapter default environment",
			objs:    []*appsv1.Deployment{deployment("payments", "api", nil, nil)},
			target:  target("payments", "api"),
			opts:    []Option{WithEnvironment("dev")},
			wantID:  state.WorkloadIdentity{Environment: "dev", Service: "api", Deployment: "api", Workload: "api", Runtime: "kubernetes", RuntimeID: "payments/api"},
			wantRev: "generation=3,observedGeneration=3",
		},
		{
			name:    "namespace fallback",
			objs:    []*appsv1.Deployment{deployment("payments", "api", nil, nil)},
			target:  target("payments", "api"),
			wantID:  state.WorkloadIdentity{Environment: "payments", Service: "api", Deployment: "api", Workload: "api", Runtime: "kubernetes", RuntimeID: "payments/api"},
			wantRev: "generation=3,observedGeneration=3",
		},
		{
			name:      "not found",
			target:    target("payments", "missing"),
			wantErrIs: runtime.ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewSimpleClientset()
			for _, o := range tt.objs {
				if _, err := c.AppsV1().Deployments(o.Namespace).Create(context.Background(), o, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			wl, err := New(c, tt.opts...).ResolveTarget(context.Background(), tt.target)
			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("ResolveTarget() error = %v, want %v", err, tt.wantErrIs)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveTarget() error = %v", err)
			}
			if wl.Identity != tt.wantID {
				t.Errorf("Identity = %+v, want %+v", wl.Identity, tt.wantID)
			}
			if wl.Revision != tt.wantRev {
				t.Errorf("Revision = %q, want %q", wl.Revision, tt.wantRev)
			}
		})
	}
}

func TestObserve(t *testing.T) {
	tests := []struct {
		name                string
		annotations         map[string]string
		wantL, wantM, wantT bool
	}{
		{"none", nil, false, false, false},
		{"logs and traces", map[string]string{AnnotationLogs: "enabled", AnnotationTraces: "enabled"}, true, false, true},
		{"non-enabled value ignored", map[string]string{AnnotationMetrics: "disabled"}, false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewSimpleClientset(
				deployment("ns", "api", nil, tt.annotations),
				pod("ns", "p1", "api", true), pod("ns", "p2", "api", false), pod("ns", "other", "other", true),
			)
			a := New(c)
			wl, err := a.ResolveTarget(context.Background(), target("ns", "api"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := a.Observe(context.Background(), wl)
			if err != nil {
				t.Fatalf("Observe() error = %v", err)
			}
			if got.Logs != tt.wantL || got.Metrics != tt.wantM || got.Traces != tt.wantT {
				t.Errorf("Observe() signals = %v/%v/%v, want %v/%v/%v", got.Logs, got.Metrics, got.Traces, tt.wantL, tt.wantM, tt.wantT)
			}
			for k, want := range map[string]string{"pods.total": "2", "pods.ready": "1", "replicas.desired": "2", "replicas.ready": "1"} {
				if got.Details[k] != want {
					t.Errorf("Details[%q] = %q, want %q", k, got.Details[k], want)
				}
			}
		})
	}
}

func TestObserveIgnoresStaleRaw(t *testing.T) {
	c := fake.NewSimpleClientset(deployment("ns", "api", nil, nil))
	a := New(c)
	wl, err := a.ResolveTarget(context.Background(), target("ns", "api"))
	if err != nil {
		t.Fatal(err)
	}
	d := deployment("ns", "api", nil, map[string]string{AnnotationLogs: "enabled"})
	if _, err := c.AppsV1().Deployments("ns").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := a.Observe(context.Background(), wl)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Logs {
		t.Error("Observe().Logs = false, want true (must re-read live Deployment)")
	}
}

func enableAll() []state.Action {
	return []state.Action{
		{Name: state.ActionEnableLogs, Signal: state.SignalLogs, Reason: state.ReasonDrift},
		{Name: state.ActionEnableMetrics, Signal: state.SignalMetrics, Reason: state.ReasonDrift},
		{Name: state.ActionEnableTraces, Signal: state.SignalTraces, Reason: state.ReasonDrift},
	}
}

func TestReconcileDriftThenCompliant(t *testing.T) {
	c := fake.NewSimpleClientset(deployment("ns", "api", nil, map[string]string{AnnotationMetrics: "enabled", "keep": "me"}))
	a := New(c, WithApplyAnnotations(true))
	ctx := context.Background()
	wl, err := a.ResolveTarget(ctx, target("ns", "api"))
	if err != nil {
		t.Fatal(err)
	}
	obs, err := a.Observe(ctx, wl)
	if err != nil {
		t.Fatal(err)
	}
	desired := state.DesiredState{Logs: true, Metrics: false, Traces: true}
	diff := state.Diff(desired, obs, a.Capabilities())
	if len(diff.Actions) != 3 {
		t.Fatalf("Diff actions = %v, want 3", diff.Actions)
	}
	res, err := a.Reconcile(ctx, desired, obs, diff.Actions)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !res.Changed || len(res.Applied) != 3 {
		t.Errorf("Reconcile() = %+v, want Changed with 3 applied", res)
	}
	d, err := c.AppsV1().Deployments("ns").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Annotations[AnnotationLogs] != "enabled" || d.Annotations[AnnotationTraces] != "enabled" {
		t.Errorf("annotations = %v, want logs and traces enabled", d.Annotations)
	}
	if _, ok := d.Annotations[AnnotationMetrics]; ok {
		t.Errorf("metrics annotation still present: %v", d.Annotations)
	}
	if d.Annotations[AnnotationReconciled] != "true" || d.Annotations["keep"] != "me" {
		t.Errorf("annotations = %v, want reconciled=true and keep preserved", d.Annotations)
	}
	if len(d.Spec.Template.Annotations) != 0 {
		t.Errorf("pod template annotations modified: %v", d.Spec.Template.Annotations)
	}
	obs, err = a.Observe(ctx, wl)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Diff(desired, obs, a.Capabilities()).InSync() {
		t.Errorf("not in sync after reconcile: %+v", obs)
	}
}

func TestReconcileCompliantNoWrites(t *testing.T) {
	c := fake.NewSimpleClientset(deployment("ns", "api", nil, map[string]string{AnnotationLogs: "enabled", AnnotationMetrics: "enabled", AnnotationTraces: "enabled"}))
	a := New(c, WithApplyAnnotations(true))
	ctx := context.Background()
	wl, err := a.ResolveTarget(ctx, target("ns", "api"))
	if err != nil {
		t.Fatal(err)
	}
	obs, err := a.Observe(ctx, wl)
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Reconcile(ctx, state.DesiredState{}, obs, enableAll())
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed {
		t.Errorf("Changed = true, want false")
	}
	if n := writes(c); n != 0 {
		t.Errorf("write actions = %d, want 0 (%v)", n, c.Actions())
	}
}

func TestReconcileDryRun(t *testing.T) {
	c := fake.NewSimpleClientset(deployment("ns", "api", nil, nil))
	a := New(c) // applyAnnotations defaults to false
	ctx := context.Background()
	wl, err := a.ResolveTarget(ctx, target("ns", "api"))
	if err != nil {
		t.Fatal(err)
	}
	obs, err := a.Observe(ctx, wl)
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Reconcile(ctx, state.DesiredState{}, obs, enableAll())
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed || !res.DryRun {
		t.Errorf("result = %+v, want DryRun and not Changed", res)
	}
	if n := writes(c); n != 0 {
		t.Errorf("write actions = %d, want 0", n)
	}
}

func TestEngineDryRunDoesNotRetry(t *testing.T) {
	c := fake.NewSimpleClientset(deployment("ns", "api", nil, nil))
	pol := policy.ObservabilityPolicy{
		Metadata: policy.Metadata{Name: "api", Generation: 1},
		Target:   target("ns", "api"),
		Spec:     policy.Spec{Telemetry: policy.Telemetry{Logs: policy.SignalSpec{Enabled: true}}},
	}
	statuses := controller.NewStatusStore()
	engine := controller.NewEngine(New(c), controller.NewMemoryPolicyStore(pol), statuses, nil)
	outs, err := engine.ReconcileOnce(context.Background(), policy.Key("api"))
	if err != nil || len(outs) != 1 || !outs[0].DryRun {
		t.Fatalf("ReconcileOnce() = %+v, %v, want one DryRun outcome and no error", outs, err)
	}
	st, _ := statuses.Get("api")
	if cond, _ := st.Condition(controller.ConditionConfigurationCompliant); st.Phase != controller.PhaseDegraded || cond.Reason != "DryRun" {
		t.Errorf("phase = %q, condition = %+v", st.Phase, cond)
	}
}

func TestReconcileNotFound(t *testing.T) {
	c := fake.NewSimpleClientset()
	a := New(c, WithApplyAnnotations(true))
	obs := state.ObservedState{Workload: state.WorkloadIdentity{Workload: "api", Runtime: "kubernetes", RuntimeID: "ns/api"}}
	_, err := a.Reconcile(context.Background(), state.DesiredState{}, obs, enableAll())
	if !errors.Is(err, runtime.ErrNotFound) {
		t.Errorf("Reconcile() error = %v, want ErrNotFound", err)
	}
}

func TestEngineIntegration(t *testing.T) {
	c := fake.NewSimpleClientset(deployment("payments", "api", map[string]string{LabelAppName: "payments-api"}, nil))
	a := New(c, WithApplyAnnotations(true))
	pol := policy.ObservabilityPolicy{
		APIVersion: policy.APIVersion, Kind: policy.Kind,
		Metadata: policy.Metadata{Name: "payments-api", Generation: 1},
		Target:   policy.TargetRef{Runtime: "kubernetes", Namespace: "payments", Name: "api", Kind: "Deployment"},
		Spec: policy.Spec{Telemetry: policy.Telemetry{
			Logs: policy.SignalSpec{Enabled: true}, Metrics: policy.SignalSpec{Enabled: true}, Traces: policy.SignalSpec{Enabled: true},
		}},
	}
	engine := controller.NewEngine(a, controller.NewMemoryPolicyStore(pol), controller.NewStatusStore(), nil)
	key := DeploymentKey("payments", "api")
	ctx := context.Background()

	outs, err := engine.ReconcileOnce(ctx, key)
	if err != nil {
		t.Fatalf("first ReconcileOnce() error = %v", err)
	}
	if len(outs) != 1 || outs[0].NoOp || len(outs[0].Actions) != 3 {
		t.Fatalf("first outcome = %+v, want 3 enable actions", outs)
	}
	for _, act := range outs[0].Actions {
		if act.Name != state.ActionEnableLogs && act.Name != state.ActionEnableMetrics && act.Name != state.ActionEnableTraces {
			t.Errorf("unexpected action %q", act.Name)
		}
	}
	if outs[0].Phase != controller.PhaseReady {
		t.Errorf("phase = %q, want %q", outs[0].Phase, controller.PhaseReady)
	}

	before := writes(c)
	outs, err = engine.ReconcileOnce(ctx, key)
	if err != nil {
		t.Fatalf("second ReconcileOnce() error = %v", err)
	}
	if len(outs) != 1 || !outs[0].NoOp {
		t.Errorf("second outcome = %+v, want NoOp", outs)
	}
	if after := writes(c); after != before {
		t.Errorf("second reconcile issued %d writes, want 0", after-before)
	}
}
