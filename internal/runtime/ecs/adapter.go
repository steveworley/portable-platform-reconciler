// Package ecs implements the ECS runtime adapter. It discovers ECS services,
// derives their canonical identity and observes telemetry configuration by
// reading authoritative state from the ECS API (DescribeServices and
// DescribeTaskDefinition). It never trusts event payloads or Workload.Raw.
package ecs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// RuntimeType is the runtime name of this adapter.
const RuntimeType runtime.RuntimeType = "ecs"

// API is the subset of the ECS client the adapter depends on.
// *ecs.Client satisfies it.
type API interface {
	DescribeServices(ctx context.Context, in *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error)
	DescribeTaskDefinition(ctx context.Context, in *ecs.DescribeTaskDefinitionInput, opts ...func(*ecs.Options)) (*ecs.DescribeTaskDefinitionOutput, error)
}

var _ API = (*ecs.Client)(nil)

// Detail keys reported in ObservedState.Details.
const (
	DetailLaunchType     = "launchType"
	DetailTaskDefinition = "taskDefinition"
	DetailRevision       = "revision"
	DetailLogDriver      = "logDriver"
	DetailDeploymentID   = "deploymentID"
	DetailRolloutState   = "rolloutState"
	DetailRunningCount   = "runningCount"
	DetailDesiredCount   = "desiredCount"
)

// Adapter is the ECS RuntimeAdapter.
type Adapter struct {
	api        API
	logger     *slog.Logger
	remediator Remediator
	caps       runtime.Capabilities
	cls        classifier
}

var _ runtime.RuntimeAdapter = (*Adapter)(nil)

// Option configures an Adapter.
type Option func(*Adapter)

// WithLogger sets the logger. Default discards.
func WithLogger(l *slog.Logger) Option {
	return func(a *Adapter) {
		if l != nil {
			a.logger = l
		}
	}
}

// WithRemediator sets how Reconcile applies actions. Default is a LogRemediator.
func WithRemediator(r Remediator) Option {
	return func(a *Adapter) {
		if r != nil {
			a.remediator = r
		}
	}
}

// WithCapabilities overrides the reported capabilities. The default is
// Fargate-conservative: no host or network telemetry. Use
// EC2Capabilities for EC2-backed clusters.
func WithCapabilities(c runtime.Capabilities) Option { return func(a *Adapter) { a.caps = c } }

// WithCollectorPatterns replaces the container-name fragments that identify a
// metrics collector sidecar (DefaultCollectorPatterns).
func WithCollectorPatterns(p ...string) Option { return func(a *Adapter) { a.cls.collectors = p } }

// WithXRayPatterns replaces the container-name fragments that identify an
// X-Ray daemon sidecar (DefaultXRayPatterns).
func WithXRayPatterns(p ...string) Option { return func(a *Adapter) { a.cls.xray = p } }

// FargateCapabilities are the default capabilities: no host or network telemetry.
func FargateCapabilities() runtime.Capabilities {
	return runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, AlertReconciliation: true}
}

// EC2Capabilities are the capabilities for EC2-backed clusters, which add host telemetry.
func EC2Capabilities() runtime.Capabilities {
	c := FargateCapabilities()
	c.HostTelemetry = true
	return c
}

// New creates an adapter reading from api.
func New(api API, opts ...Option) *Adapter {
	a := &Adapter{
		api:    api,
		logger: slog.New(slog.DiscardHandler),
		caps:   FargateCapabilities(),
		cls:    classifier{collectors: DefaultCollectorPatterns, xray: DefaultXRayPatterns},
	}
	for _, o := range opts {
		o(a)
	}
	if a.remediator == nil {
		a.remediator = NewLogRemediator(a.logger)
	}
	return a
}

// Runtime implements runtime.RuntimeAdapter.
func (a *Adapter) Runtime() runtime.RuntimeType { return RuntimeType }

// Capabilities implements runtime.RuntimeAdapter. The value is adapter-wide,
// not per workload; see WithCapabilities.
func (a *Adapter) Capabilities() runtime.Capabilities { return a.caps }

var accountRE = regexp.MustCompile(`^[0-9]{12}$`)

// ValidateTarget checks that target has the fields the ECS adapter needs:
// cluster, service, region and a 12-digit account.
func ValidateTarget(t policy.TargetRef) error {
	var errs []error
	if t.Cluster == "" {
		errs = append(errs, errors.New("ecs target requires cluster"))
	}
	if t.Service == "" {
		errs = append(errs, errors.New("ecs target requires service"))
	}
	if t.Region == "" {
		errs = append(errs, errors.New("ecs target requires region"))
	}
	if !accountRE.MatchString(t.Account) {
		errs = append(errs, fmt.Errorf("ecs target account %q must be 12 digits", t.Account))
	}
	return errors.Join(errs...)
}

// QueueKey implements runtime.RuntimeAdapter: "ECSService:<service ARN>".
func (a *Adapter) QueueKey(t policy.TargetRef) (string, error) {
	if err := ValidateTarget(t); err != nil {
		return "", err
	}
	return ServiceKey(ServiceARN(t.Region, t.Account, t.Cluster, t.Service)), nil
}

// describeService reads the service; it wraps runtime.ErrNotFound when the
// service is missing or INACTIVE.
func (a *Adapter) describeService(ctx context.Context, t policy.TargetRef) (types.Service, error) {
	out, err := a.api.DescribeServices(ctx, &ecs.DescribeServicesInput{
		Cluster:  aws.String(t.Cluster),
		Services: []string{t.Service},
		Include:  []types.ServiceField{types.ServiceFieldTags},
	})
	if err != nil {
		return types.Service{}, fmt.Errorf("describe service %s/%s: %w", t.Cluster, t.Service, err)
	}
	for _, s := range out.Services {
		if deref(s.ServiceName) != t.Service {
			continue
		}
		if strings.EqualFold(deref(s.Status), "INACTIVE") {
			return types.Service{}, fmt.Errorf("ecs service %s/%s is INACTIVE: %w", t.Cluster, t.Service, runtime.ErrNotFound)
		}
		return s, nil
	}
	reason := "not returned"
	if len(out.Failures) > 0 {
		reason = deref(out.Failures[0].Reason)
	}
	return types.Service{}, fmt.Errorf("ecs service %s/%s: %s: %w", t.Cluster, t.Service, reason, runtime.ErrNotFound)
}

// environment resolves the canonical environment. Precedence: the target's
// Environment, then the service tag "environment" or "Environment", then the
// cluster name.
func environment(t policy.TargetRef, svc types.Service) string {
	if t.Environment != "" {
		return t.Environment
	}
	for _, tag := range svc.Tags {
		if k := deref(tag.Key); (k == "environment" || k == "Environment") && deref(tag.Value) != "" {
			return deref(tag.Value)
		}
	}
	return t.Cluster
}

// ResolveTarget implements runtime.RuntimeAdapter. DescribeServices is
// authoritative: a missing, INACTIVE or failed service wraps runtime.ErrNotFound.
func (a *Adapter) ResolveTarget(ctx context.Context, t policy.TargetRef) (runtime.Workload, error) {
	if err := ValidateTarget(t); err != nil {
		return runtime.Workload{}, err
	}
	svc, err := a.describeService(ctx, t)
	if err != nil {
		return runtime.Workload{}, err
	}
	arn := deref(svc.ServiceArn)
	if arn == "" {
		arn = ServiceARN(t.Region, t.Account, t.Cluster, t.Service)
	}
	return runtime.Workload{
		Identity: state.WorkloadIdentity{
			Account:     t.Account,
			Region:      t.Region,
			Environment: environment(t, svc),
			Service:     t.Service,
			Deployment:  t.Service,
			Workload:    t.Service,
			Runtime:     string(RuntimeType),
			RuntimeID:   arn,
		},
		Ref:      t,
		Revision: taskDefRevision(deref(svc.TaskDefinition)),
	}, nil
}

// taskDefRevision turns a task definition ARN into "family:revision".
func taskDefRevision(arn string) string {
	if i := strings.LastIndex(arn, "task-definition/"); i >= 0 {
		return arn[i+len("task-definition/"):]
	}
	return arn
}

func primaryDeployment(svc types.Service) (types.Deployment, bool) {
	for _, d := range svc.Deployments {
		if strings.EqualFold(deref(d.Status), "PRIMARY") {
			return d, true
		}
	}
	return types.Deployment{}, false
}

// Observe implements runtime.RuntimeAdapter. It re-reads the service and its
// task definition from the API; Workload.Raw and event payloads are not used.
func (a *Adapter) Observe(ctx context.Context, wl runtime.Workload) (state.ObservedState, error) {
	svc, err := a.describeService(ctx, wl.Ref)
	if err != nil {
		return state.ObservedState{}, err
	}
	tdARN := deref(svc.TaskDefinition)
	if d, ok := primaryDeployment(svc); ok && deref(d.TaskDefinition) != "" {
		tdARN = deref(d.TaskDefinition)
	}
	td, err := a.api.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String(tdARN)})
	if err != nil {
		return state.ObservedState{}, fmt.Errorf("describe task definition %s: %w", tdARN, err)
	}
	if td.TaskDefinition == nil {
		return state.ObservedState{}, fmt.Errorf("describe task definition %s: empty response: %w", tdARN, runtime.ErrNotFound)
	}
	tel := a.cls.derive(td.TaskDefinition.ContainerDefinitions)
	revision := taskDefRevision(tdARN)

	launch := string(svc.LaunchType)
	if launch == "" {
		for _, cp := range svc.CapacityProviderStrategy {
			if strings.HasPrefix(deref(cp.CapacityProvider), "FARGATE") {
				launch = string(types.LaunchTypeFargate)
			}
		}
	}
	details := map[string]string{
		DetailLaunchType:     launch,
		DetailTaskDefinition: tdARN,
		DetailRevision:       strconv.Itoa(int(td.TaskDefinition.Revision)),
		DetailLogDriver:      tel.logDriver,
		DetailRunningCount:   strconv.Itoa(int(svc.RunningCount)),
		DetailDesiredCount:   strconv.Itoa(int(svc.DesiredCount)),
	}
	if d, ok := primaryDeployment(svc); ok {
		details[DetailDeploymentID] = deref(d.Id)
		details[DetailRolloutState] = string(d.RolloutState)
	}
	return state.ObservedState{
		Workload: wl.Identity,
		Revision: revision,
		Logs:     tel.logs,
		Metrics:  tel.metrics,
		Traces:   tel.traces,
		Details:  details,
	}, nil
}

// Reconcile implements runtime.RuntimeAdapter. It never calls ECS to modify
// anything; the configured Remediator decides what applying actions means.
func (a *Adapter) Reconcile(ctx context.Context, _ state.DesiredState, observed state.ObservedState, actions []state.Action) (state.ReconcileResult, error) {
	if len(actions) == 0 {
		return state.ReconcileResult{Message: "no actions required"}, nil
	}
	return a.remediator.Remediate(ctx, observed.Workload, actions)
}
