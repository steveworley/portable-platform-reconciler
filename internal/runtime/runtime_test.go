package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/state"
)

func envOf(m map[string]string) Env { return func(k string) string { return m[k] } }

type stubAdapter struct{ t RuntimeType }

func (s stubAdapter) Runtime() RuntimeType       { return s.t }
func (s stubAdapter) Capabilities() Capabilities { return Capabilities{} }
func (s stubAdapter) QueueKey(policy.TargetRef) (string, error) {
	return "", nil
}
func (s stubAdapter) ResolveTarget(context.Context, policy.TargetRef) (Workload, error) {
	return Workload{}, ErrNotFound
}
func (s stubAdapter) Observe(context.Context, Workload) (state.ObservedState, error) {
	return state.ObservedState{}, nil
}
func (s stubAdapter) Reconcile(context.Context, state.DesiredState, state.ObservedState, []state.Action) (state.ReconcileResult, error) {
	return state.ReconcileResult{}, nil
}

func stubFactory(t RuntimeType) Factory {
	return func(config.Config) (RuntimeAdapter, error) { return stubAdapter{t}, nil }
}

// newTestRegistry registers kubernetes-like, ecs-like and a dummy third runtime,
// proving the registry has no built-in platform knowledge.
func newTestRegistry() *Registry {
	r := NewRegistry()
	r.Register("kubernetes", stubFactory("kubernetes"), EnvSet("KUBERNETES_SERVICE_HOST"))
	r.Register("ecs", stubFactory("ecs"), AnyOf(EnvSet("ECS_CONTAINER_METADATA_URI_V4"), EnvHasPrefix("AWS_EXECUTION_ENV", "AWS_ECS")))
	r.Register("dummy", stubFactory("dummy"), EnvSet("DUMMY_RUNTIME"),
		WithTargetValidator(func(t policy.TargetRef) error {
			if t.Attributes["job"] == "" {
				return errors.New("attributes.job is required")
			}
			return nil
		}))
	return r
}

func TestSelect(t *testing.T) {
	tests := []struct {
		name    string
		typ     string
		env     map[string]string
		want    RuntimeType
		wantErr error
	}{
		{name: "explicit ecs beats k8s env", typ: "ecs", env: map[string]string{"KUBERNETES_SERVICE_HOST": "x"}, want: "ecs"},
		{name: "explicit k8s beats ecs env", typ: "kubernetes", env: map[string]string{"ECS_CONTAINER_METADATA_URI_V4": "x"}, want: "kubernetes"},
		{name: "explicit is case-insensitive", typ: "ECS", want: "ecs"},
		{name: "auto k8s", typ: "auto", env: map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1"}, want: "kubernetes"},
		{name: "empty type is auto", typ: "", env: map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1"}, want: "kubernetes"},
		{name: "auto ecs metadata", typ: "auto", env: map[string]string{"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4"}, want: "ecs"},
		{name: "auto ecs exec env", typ: "auto", env: map[string]string{"AWS_EXECUTION_ENV": "AWS_ECS_FARGATE"}, want: "ecs"},
		{name: "auto lambda is not ecs", typ: "auto", env: map[string]string{"AWS_EXECUTION_ENV": "AWS_Lambda_go"}, wantErr: ErrNoRuntime},
		{name: "auto none", typ: "auto", wantErr: ErrNoRuntime},
		{name: "auto ambiguous", typ: "auto", env: map[string]string{"KUBERNETES_SERVICE_HOST": "x", "DUMMY_RUNTIME": "1"}, wantErr: ErrAmbiguousRuntime},
		{name: "auto third runtime", typ: "auto", env: map[string]string{"DUMMY_RUNTIME": "1"}, want: "dummy"},
		{name: "explicit third runtime", typ: "dummy", want: "dummy"},
		{name: "explicit unregistered", typ: "nomad", wantErr: ErrUnknownRuntime},
	}
	r := newTestRegistry()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Select(config.RuntimeConfig{Type: tt.typ}, envOf(tt.env))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Select() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Select() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRegisterThirdRuntime(t *testing.T) {
	r := newTestRegistry()
	typ, err := r.Select(config.RuntimeConfig{Type: "auto"}, envOf(map[string]string{"DUMMY_RUNTIME": "1"}))
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.New(typ, config.Default())
	if err != nil {
		t.Fatal(err)
	}
	if a.Runtime() != "dummy" {
		t.Errorf("adapter Runtime() = %q, want dummy", a.Runtime())
	}
	if err := r.ValidateTarget(policy.TargetRef{Runtime: "dummy"}); err == nil {
		t.Error("ValidateTarget(no job) error = nil")
	}
	if err := r.ValidateTarget(policy.TargetRef{Runtime: "dummy", Attributes: map[string]string{"job": "j"}}); err != nil {
		t.Errorf("ValidateTarget(job) error = %v", err)
	}
	if err := r.ValidateTarget(policy.TargetRef{Runtime: "ecs"}); err != nil {
		t.Errorf("ValidateTarget(no validator) error = %v", err)
	}
	if err := r.ValidateTarget(policy.TargetRef{Runtime: "nomad"}); !errors.Is(err, ErrUnknownRuntime) {
		t.Errorf("ValidateTarget(unregistered) error = %v, want ErrUnknownRuntime", err)
	}
	if _, err := r.New("nomad", config.Default()); !errors.Is(err, ErrUnknownRuntime) {
		t.Errorf("New(unregistered) error = %v", err)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	r := newTestRegistry()
	defer func() {
		if recover() == nil {
			t.Error("duplicate Register did not panic")
		}
	}()
	r.Register("ecs", stubFactory("ecs"), nil)
}

func TestIdentityString(t *testing.T) {
	id := WorkloadIdentity{Runtime: "dummy", RuntimeID: "a/b"}
	if got, want := id.String(), "dummy:a/b"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestCapabilitiesSupports(t *testing.T) {
	if !(Capabilities{TelemetryConfig: true}).Supports(state.CapTelemetryConfig) {
		t.Error("Supports(TelemetryConfig) = false, want true")
	}
	if (Capabilities{}).Supports(state.CapTelemetryConfig) {
		t.Error("Supports(TelemetryConfig) = true, want false")
	}
}
