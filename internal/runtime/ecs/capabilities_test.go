package ecs

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/alerting/memory"
	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
)

func TestLaunchType(t *testing.T) {
	cps := func(names ...string) []types.CapacityProviderStrategyItem {
		var out []types.CapacityProviderStrategyItem
		for _, n := range names {
			out = append(out, types.CapacityProviderStrategyItem{CapacityProvider: aws.String(n)})
		}
		return out
	}
	tests := []struct {
		name string
		svc  types.Service
		want string
	}{
		{"fargate", types.Service{LaunchType: types.LaunchTypeFargate}, "FARGATE"},
		{"ec2", types.Service{LaunchType: types.LaunchTypeEc2}, "EC2"},
		{"fargate spot provider", types.Service{CapacityProviderStrategy: cps("FARGATE_SPOT")}, "FARGATE"},
		{"asg provider", types.Service{CapacityProviderStrategy: cps("my-asg")}, "EC2"},
		{"mixed prefers fargate", types.Service{CapacityProviderStrategy: cps("my-asg", "FARGATE")}, "FARGATE"},
		{"unknown", types.Service{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := launchType(tt.svc); got != tt.want {
				t.Errorf("launchType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveTargetCapabilities(t *testing.T) {
	sim := NewSimulatedECS()
	for name, lt := range map[string]types.LaunchType{"fg": types.LaunchTypeFargate, "ec": types.LaunchTypeEc2, "ext": types.LaunchTypeExternal} {
		sim.AddService(ServiceSpec{Account: SeedAccount, Region: SeedRegion, Cluster: SeedCluster, Name: name, LaunchType: lt})
	}
	tgt := func(name string) policy.TargetRef {
		return policy.TargetRef{Runtime: "ecs", Account: SeedAccount, Region: SeedRegion, Cluster: SeedCluster, Service: name}
	}
	resolve := func(a *Adapter, name string) runtime.Workload {
		t.Helper()
		wl, err := a.ResolveTarget(context.Background(), tgt(name))
		if err != nil {
			t.Fatal(err)
		}
		return wl
	}

	a := New(sim)
	if c := resolve(a, "fg").EffectiveCapabilities(a.Capabilities()); c != FargateCapabilities() {
		t.Errorf("fargate caps = %+v", c)
	}
	if c := resolve(a, "ec").EffectiveCapabilities(a.Capabilities()); c != EC2Capabilities() {
		t.Errorf("ec2 caps = %+v", c)
	}
	if wl := resolve(a, "ext"); wl.Capabilities != nil {
		t.Errorf("unknown launch type caps = %+v, want nil (adapter default)", wl.Capabilities)
	}

	fixed := New(sim, WithCapabilities(FargateCapabilities()))
	if wl := resolve(fixed, "ec"); wl.Capabilities != nil {
		t.Errorf("WithCapabilities must disable detection, got %+v", wl.Capabilities)
	}
}

// TestHostCPUAlertByLaunchType: the host CPU rule is Unsupported on Fargate and
// reconciled on EC2, through the real engine with a single adapter.
func TestHostCPUAlertByLaunchType(t *testing.T) {
	sim := NewSimulatedECS()
	for name, lt := range map[string]types.LaunchType{"fg": types.LaunchTypeFargate, "ec": types.LaunchTypeEc2} {
		sim.AddService(ServiceSpec{
			Account: SeedAccount, Region: SeedRegion, Cluster: SeedCluster, Name: name, LaunchType: lt,
			Containers: []types.ContainerDefinition{app("app", awslogs())},
		})
	}
	be := memory.New()
	var pols []policy.ObservabilityPolicy
	for _, name := range []string{"fg", "ec"} {
		p := loadPolicy(t)
		p.Metadata.Name = name
		p.Target.Service = name
		p.Spec.Telemetry.Logs.Enabled = true
		p.Spec.Telemetry.Metrics.Enabled = false
		p.Spec.Telemetry.Traces.Enabled = false
		p.Spec.Alerts.Profile = "standard"
		pols = append(pols, p)
	}
	engine := controller.NewEngine(New(sim, WithRemediator(sim.Remediator())), controller.NewMemoryPolicyStore(pols...),
		controller.NewStatusStore(), nil, controller.WithSteps(alerting.NewStep(be)))

	hostCPU := func(name string) bool {
		for _, r := range be.Rules() {
			if r.Owner.PolicyName == name && r.ID == alerting.RuleHostCPU {
				return true
			}
		}
		return false
	}
	for name, want := range map[string]bool{"fg": false, "ec": true} {
		outs, err := engine.ReconcileOnce(context.Background(), policy.Key(name))
		if err != nil || len(outs) != 1 {
			t.Fatalf("ReconcileOnce(%s) = %+v, %v", name, outs, err)
		}
		if got := hostCPU(name); got != want {
			t.Errorf("%s: host CPU rule present = %v, want %v", name, got, want)
		}
		var hostUnsupported bool
		for _, u := range outs[0].Unsupported {
			hostUnsupported = hostUnsupported || strings.Contains(u, alerting.RuleHostCPU)
		}
		if hostUnsupported == want {
			t.Errorf("%s: Unsupported = %v, want host CPU listed only for fargate", name, outs[0].Unsupported)
		}
	}
}
