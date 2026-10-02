package ecs

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

func target() policy.TargetRef {
	return policy.TargetRef{
		Runtime: "ecs", Account: SeedAccount, Region: SeedRegion,
		Cluster: SeedCluster, Service: SeedService,
	}
}

func awslogs() *types.LogConfiguration {
	return &types.LogConfiguration{LogDriver: types.LogDriverAwslogs}
}

func app(name string, lc *types.LogConfiguration, env ...types.KeyValuePair) types.ContainerDefinition {
	return types.ContainerDefinition{Name: aws.String(name), Essential: aws.Bool(true), LogConfiguration: lc, Environment: env}
}

func newSim(t *testing.T, spec ServiceSpec) (*SimulatedECS, string) {
	t.Helper()
	sim := NewSimulatedECS()
	spec.Account, spec.Region, spec.Cluster, spec.Name = SeedAccount, SeedRegion, SeedCluster, SeedService
	return sim, sim.AddService(spec)
}

func TestServiceARNAndKey(t *testing.T) {
	arn := ServiceARN("ap-southeast-2", "123456789012", "production", "payments-api")
	if want := "arn:aws:ecs:ap-southeast-2:123456789012:service/production/payments-api"; arn != want {
		t.Errorf("ServiceARN() = %q, want %q", arn, want)
	}
	if got, want := ServiceKey(arn), "ECSService:"+arn; got != want {
		t.Errorf("ServiceKey() = %q, want %q", got, want)
	}
	r, a, c, s, ok := ParseServiceARN(arn)
	if !ok || r != "ap-southeast-2" || a != "123456789012" || c != "production" || s != "payments-api" {
		t.Errorf("ParseServiceARN() = %q %q %q %q %v", r, a, c, s, ok)
	}
	for _, bad := range []string{"", "arn:aws:ecs:r:a:service/legacy", "arn:aws:ecs:r:a:cluster/x", "nonsense"} {
		if _, _, _, _, ok := ParseServiceARN(bad); ok {
			t.Errorf("ParseServiceARN(%q) ok, want false", bad)
		}
	}
}

func TestValidateTarget(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*policy.TargetRef)
		wantErr string
	}{
		{name: "valid", mutate: func(*policy.TargetRef) {}},
		{name: "no cluster", mutate: func(t *policy.TargetRef) { t.Cluster = "" }, wantErr: "cluster"},
		{name: "no service", mutate: func(t *policy.TargetRef) { t.Service = "" }, wantErr: "service"},
		{name: "no region", mutate: func(t *policy.TargetRef) { t.Region = "" }, wantErr: "region"},
		{name: "short account", mutate: func(t *policy.TargetRef) { t.Account = "1234" }, wantErr: "12 digits"},
		{name: "alpha account", mutate: func(t *policy.TargetRef) { t.Account = "12345678901a" }, wantErr: "12 digits"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := target()
			tt.mutate(&tr)
			err := ValidateTarget(tr)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("ValidateTarget() = %v, want nil", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("ValidateTarget() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRegistered(t *testing.T) {
	if err := runtime.ValidateTarget(policy.TargetRef{Runtime: "ecs"}); err == nil {
		t.Error("ValidateTarget(empty ecs target) = nil, want error")
	}
	if err := runtime.ValidateTarget(target()); err != nil {
		t.Errorf("ValidateTarget(valid) = %v", err)
	}
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{name: "metadata uri", env: map[string]string{"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4"}, want: true},
		{name: "exec env", env: map[string]string{"AWS_EXECUTION_ENV": "AWS_ECS_FARGATE"}, want: true},
		{name: "lambda", env: map[string]string{"AWS_EXECUTION_ENV": "AWS_Lambda_go"}},
		{name: "none", env: map[string]string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt, err := runtime.Select(configAuto(), func(k string) string { return tt.env[k] })
			if tt.want && (err != nil || rt != RuntimeType) {
				t.Errorf("Select() = %q, %v, want ecs", rt, err)
			}
			if !tt.want && err == nil && rt == RuntimeType {
				t.Errorf("Select() = ecs, want no match")
			}
		})
	}
}

func TestQueueKey(t *testing.T) {
	a := New(NewSimulatedECS())
	got, err := a.QueueKey(target())
	if err != nil {
		t.Fatal(err)
	}
	if want := ServiceKey(ServiceARN(SeedRegion, SeedAccount, SeedCluster, SeedService)); got != want {
		t.Errorf("QueueKey() = %q, want %q", got, want)
	}
	if _, err := a.QueueKey(policy.TargetRef{Runtime: "ecs"}); err == nil {
		t.Error("QueueKey(invalid) = nil error")
	}
}

func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name     string
		target   func(*policy.TargetRef)
		status   string
		tags     map[string]string
		wantEnv  string
		notFound bool
	}{
		{name: "found uses cluster as environment", target: func(*policy.TargetRef) {}, wantEnv: "production"},
		{name: "tag environment", target: func(*policy.TargetRef) {}, tags: map[string]string{"environment": "prod-eu"}, wantEnv: "prod-eu"},
		{name: "tag Environment", target: func(*policy.TargetRef) {}, tags: map[string]string{"Environment": "Staging"}, wantEnv: "Staging"},
		{name: "target environment wins", target: func(t *policy.TargetRef) { t.Environment = "explicit" }, tags: map[string]string{"environment": "tag"}, wantEnv: "explicit"},
		{name: "missing service", target: func(t *policy.TargetRef) { t.Service = "nope" }, notFound: true},
		{name: "missing cluster", target: func(t *policy.TargetRef) { t.Cluster = "nope" }, notFound: true},
		{name: "inactive", target: func(*policy.TargetRef) {}, status: "INACTIVE", notFound: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim, arn := newSim(t, ServiceSpec{Tags: tt.tags, Containers: []types.ContainerDefinition{app("app", awslogs())}})
			if tt.status != "" {
				if err := sim.SetServiceStatus(arn, tt.status); err != nil {
					t.Fatal(err)
				}
			}
			tr := target()
			tt.target(&tr)
			wl, err := New(sim).ResolveTarget(context.Background(), tr)
			if tt.notFound {
				if !errors.Is(err, runtime.ErrNotFound) {
					t.Fatalf("ResolveTarget() error = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := state.WorkloadIdentity{
				Account: SeedAccount, Region: SeedRegion, Environment: tt.wantEnv,
				Service: SeedService, Deployment: SeedService, Workload: SeedService,
				Runtime: "ecs", RuntimeID: arn,
			}
			if wl.Identity != want {
				t.Errorf("Identity = %+v, want %+v", wl.Identity, want)
			}
			if wl.Revision != "payments-api:1" {
				t.Errorf("Revision = %q, want payments-api:1", wl.Revision)
			}
		})
	}
}

func TestObserve(t *testing.T) {
	otel := types.KeyValuePair{Name: aws.String(OTLPEndpointEnv), Value: aws.String("http://x:4317")}
	sidecar := func(name string) types.ContainerDefinition {
		return types.ContainerDefinition{Name: aws.String(name), Essential: aws.Bool(false)}
	}
	labelled := app("app", awslogs())
	labelled.DockerLabels = map[string]string{MetricsLabel: MetricsLabelEnabled}
	nonEssential := types.ContainerDefinition{Name: aws.String("migrate"), Essential: aws.Bool(false)}

	tests := []struct {
		name                  string
		containers            []types.ContainerDefinition
		logs, metrics, traces bool
		driver                string
	}{
		{name: "nothing", containers: []types.ContainerDefinition{app("app", nil)}, driver: "none"},
		{name: "logs enabled", containers: []types.ContainerDefinition{app("app", awslogs())}, logs: true, driver: "awslogs"},
		{name: "one of two essentials lacks logs", containers: []types.ContainerDefinition{app("a", awslogs()), app("b", nil)}, driver: "awslogs"},
		{name: "mixed drivers", containers: []types.ContainerDefinition{app("a", awslogs()), app("b", &types.LogConfiguration{LogDriver: types.LogDriverSplunk})}, logs: true, driver: "mixed"},
		{name: "non-essential ignored for logs", containers: []types.ContainerDefinition{app("a", awslogs()), nonEssential}, logs: true, driver: "awslogs"},
		{name: "sidecar ignored for logs", containers: []types.ContainerDefinition{app("a", awslogs()), sidecar("aws-otel-collector")}, logs: true, metrics: true, driver: "awslogs"},
		{name: "datadog metrics", containers: []types.ContainerDefinition{app("a", nil), sidecar("datadog-agent")}, metrics: true, driver: "none"},
		{name: "label metrics", containers: []types.ContainerDefinition{labelled}, logs: true, metrics: true, driver: "awslogs"},
		{name: "otel env traces", containers: []types.ContainerDefinition{app("a", awslogs(), otel)}, logs: true, traces: true, driver: "awslogs"},
		{name: "xray sidecar traces", containers: []types.ContainerDefinition{app("a", nil), sidecar("xray-daemon")}, traces: true, driver: "none"},
		{name: "no app containers", containers: []types.ContainerDefinition{sidecar("aws-otel-collector")}, metrics: true, driver: "none"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim, _ := newSim(t, ServiceSpec{Containers: tt.containers, LaunchType: types.LaunchTypeFargate})
			a := New(sim)
			wl, err := a.ResolveTarget(context.Background(), target())
			if err != nil {
				t.Fatal(err)
			}
			obs, err := a.Observe(context.Background(), wl)
			if err != nil {
				t.Fatal(err)
			}
			if obs.Logs != tt.logs || obs.Metrics != tt.metrics || obs.Traces != tt.traces {
				t.Errorf("observed logs/metrics/traces = %v/%v/%v, want %v/%v/%v", obs.Logs, obs.Metrics, obs.Traces, tt.logs, tt.metrics, tt.traces)
			}
			wantDetails := map[string]string{
				DetailLaunchType: "FARGATE", DetailLogDriver: tt.driver, DetailRevision: "1",
				DetailRolloutState: "COMPLETED", DetailDeploymentID: "ecs-svc/1",
				DetailRunningCount: "1", DetailDesiredCount: "1",
			}
			for k, want := range wantDetails {
				if got := obs.Details[k]; got != want {
					t.Errorf("Details[%s] = %q, want %q", k, got, want)
				}
			}
			if obs.Revision != "payments-api:1" {
				t.Errorf("Revision = %q", obs.Revision)
			}
			if !strings.HasSuffix(obs.Details[DetailTaskDefinition], "task-definition/payments-api:1") {
				t.Errorf("Details[taskDefinition] = %q", obs.Details[DetailTaskDefinition])
			}
		})
	}
}

func TestObserveCustomCollectorPattern(t *testing.T) {
	sim, _ := newSim(t, ServiceSpec{Containers: []types.ContainerDefinition{
		app("a", awslogs()), {Name: aws.String("my-agent"), Essential: aws.Bool(false)},
	}})
	a := New(sim, WithCollectorPatterns("my-agent"))
	wl, _ := a.ResolveTarget(context.Background(), target())
	obs, err := a.Observe(context.Background(), wl)
	if err != nil || !obs.Metrics {
		t.Errorf("Observe() metrics = %v, err = %v, want true", obs.Metrics, err)
	}
}

func TestObserveIgnoresRaw(t *testing.T) {
	sim := NewSeededSimulation()
	a := New(sim)
	wl, err := a.ResolveTarget(context.Background(), target())
	if err != nil {
		t.Fatal(err)
	}
	wl.Raw = "poisoned"
	obs, err := a.Observe(context.Background(), wl)
	if err != nil || obs.Logs {
		t.Errorf("Observe() logs = %v, err = %v, want false/nil", obs.Logs, err)
	}
}

func TestCapabilities(t *testing.T) {
	if c := New(NewSimulatedECS()).Capabilities(); c.HostTelemetry || c.NetworkTelemetry || !c.TelemetryConfig {
		t.Errorf("default Capabilities() = %+v, want Fargate-conservative", c)
	}
	if c := New(NewSimulatedECS(), WithCapabilities(EC2Capabilities())).Capabilities(); !c.HostTelemetry {
		t.Errorf("EC2 Capabilities() = %+v, want HostTelemetry", c)
	}
}

func driftedAdapter(t *testing.T) (*SimulatedECS, *Adapter, runtime.Workload, string) {
	t.Helper()
	sim := NewSeededSimulation()
	a := New(sim, WithRemediator(sim.Remediator()))
	wl, err := a.ResolveTarget(context.Background(), target())
	if err != nil {
		t.Fatal(err)
	}
	return sim, a, wl, wl.Identity.RuntimeID
}

func TestReconcileSimulatedRemediation(t *testing.T) {
	ctx := context.Background()
	sim, a, wl, arn := driftedAdapter(t)
	desired := state.DesiredState{Logs: true, Metrics: true, Traces: true}

	obs, err := a.Observe(ctx, wl)
	if err != nil {
		t.Fatal(err)
	}
	diff := state.Diff(desired, obs, a.Capabilities())
	if len(diff.Actions) != 1 || diff.Actions[0].Name != state.ActionEnableLogs {
		t.Fatalf("diff actions = %+v, want enable_logs", diff.Actions)
	}
	res, err := a.Reconcile(ctx, desired, obs, diff.Actions)
	if err != nil || !res.Changed {
		t.Fatalf("Reconcile() = %+v, %v, want Changed", res, err)
	}
	after, err := a.Observe(ctx, wl)
	if err != nil {
		t.Fatal(err)
	}
	if d := state.Diff(desired, after, a.Capabilities()); !d.InSync() {
		t.Errorf("not compliant after reconcile: %+v", d)
	}
	if after.Revision != "payments-api:2" || obs.Revision != "payments-api:1" {
		t.Errorf("revisions before/after = %q/%q, want payments-api:1/payments-api:2", obs.Revision, after.Revision)
	}
	if after.Details[DetailDeploymentID] == obs.Details[DetailDeploymentID] {
		t.Errorf("deployment id unchanged: %q", after.Details[DetailDeploymentID])
	}
	if rev, _ := sim.Revision(arn); rev != 2 {
		t.Errorf("Revision() = %d, want 2", rev)
	}
	if n := sim.CallCount(CallUpdateService); n != 1 {
		t.Errorf("UpdateService calls = %d, want 1", n)
	}
}

func TestReconcileNoActionsDoesNotRemediate(t *testing.T) {
	sim, a, wl, _ := driftedAdapter(t)
	before := sim.Calls()
	obs, _ := a.Observe(context.Background(), wl)
	res, err := a.Reconcile(context.Background(), state.DesiredState{}, obs, nil)
	if err != nil || res.Changed {
		t.Errorf("Reconcile(no actions) = %+v, %v", res, err)
	}
	after := sim.Calls()
	if after[CallRegisterTaskDefinition] != before[CallRegisterTaskDefinition] || after[CallUpdateService] != 0 {
		t.Errorf("remediation calls changed: before %v after %v", before, after)
	}
}

func TestLogRemediatorChangesNothing(t *testing.T) {
	ctx := context.Background()
	sim := NewSeededSimulation()
	var buf strings.Builder
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	a := New(sim, WithLogger(logger)) // default remediator: LogRemediator
	wl, err := a.ResolveTarget(ctx, target())
	if err != nil {
		t.Fatal(err)
	}
	obs, _ := a.Observe(ctx, wl)
	actions := []state.Action{{Name: state.ActionEnableLogs, Signal: state.SignalLogs, Reason: state.ReasonDrift}}
	res, err := a.Reconcile(ctx, state.DesiredState{Logs: true}, obs, actions)
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed || res.Message == "" {
		t.Errorf("Reconcile() = %+v, want Changed=false with message", res)
	}
	after, _ := a.Observe(ctx, wl)
	if after.Logs || after.Revision != obs.Revision {
		t.Errorf("state changed: logs=%v revision=%q", after.Logs, after.Revision)
	}
	if sim.CallCount(CallRegisterTaskDefinition) != 1 || sim.CallCount(CallUpdateService) != 0 {
		t.Errorf("modifying ECS calls were made: %v", sim.Calls())
	}
	for _, want := range []string{`"resource":"payments-api"`, `"runtime":"ecs"`, `"action":"enable_logs"`, state.ReasonDrift} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log output missing %s: %s", want, buf.String())
		}
	}
}

func TestSimulatedTelemetryRoundTrip(t *testing.T) {
	ctx := context.Background()
	sim := NewSeededSimulation()
	a := New(sim)
	wl, _ := a.ResolveTarget(ctx, target())
	f, tr := false, true
	if err := sim.SetTelemetry(wl.Identity.RuntimeID, &tr, &f, &f); err != nil {
		t.Fatal(err)
	}
	obs, _ := a.Observe(ctx, wl)
	if !obs.Logs || obs.Metrics || obs.Traces {
		t.Errorf("after SetTelemetry(true,false,false): %v/%v/%v", obs.Logs, obs.Metrics, obs.Traces)
	}
	if err := sim.SetTelemetry(wl.Identity.RuntimeID, nil, &tr, &tr); err != nil {
		t.Fatal(err)
	}
	obs, _ = a.Observe(ctx, wl)
	if !obs.Logs || !obs.Metrics || !obs.Traces {
		t.Errorf("after enabling all: %v/%v/%v", obs.Logs, obs.Metrics, obs.Traces)
	}
	if err := sim.SetLogging("arn:aws:ecs:x:1:service/c/s", true); err == nil {
		t.Error("SetLogging(unknown) = nil")
	}
}
