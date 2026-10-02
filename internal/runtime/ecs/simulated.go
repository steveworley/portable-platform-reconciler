package ecs

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/example/portable-platform-reconciler/internal/state"
)

// Names of the ECS APIs counted by SimulatedECS.Calls.
const (
	CallDescribeServices       = "DescribeServices"
	CallDescribeTaskDefinition = "DescribeTaskDefinition"
	CallRegisterTaskDefinition = "RegisterTaskDefinition"
	CallUpdateService          = "UpdateService"
)

// Seeded sample service identity (see NewSeededSimulation).
const (
	SeedAccount = "123456789012"
	SeedRegion  = "ap-southeast-2"
	SeedCluster = "production"
	SeedService = "payments-api"
)

// ServiceSpec describes a service added to a SimulatedECS.
type ServiceSpec struct {
	Account      string
	Region       string
	Cluster      string
	Name         string
	LaunchType   types.LaunchType // default FARGATE
	Tags         map[string]string
	Containers   []types.ContainerDefinition
	DesiredCount int32 // default 1
}

type simService struct {
	spec       ServiceSpec
	arn        string
	status     string
	family     string
	revision   int32 // task definition revision the service points at
	deployment int   // deployment sequence number
}

// SimulatedECS is an in-memory ECS model that satisfies API. It is used by
// "simulated" mode, the demo and tests. Task definitions are immutable, like
// real ECS: every change registers a new revision and points the service at it.
// Safe for concurrent use.
type SimulatedECS struct {
	mu       sync.Mutex
	services map[string]*simService // by service ARN
	taskDefs map[string][]types.TaskDefinition
	calls    map[string]int
	cls      classifier
}

var _ API = (*SimulatedECS)(nil)

// NewSimulatedECS returns an empty simulation.
func NewSimulatedECS() *SimulatedECS {
	return &SimulatedECS{
		services: map[string]*simService{},
		taskDefs: map[string][]types.TaskDefinition{},
		calls:    map[string]int{},
		cls:      classifier{collectors: DefaultCollectorPatterns, xray: DefaultXRayPatterns},
	}
}

// NewSeededSimulation returns a simulation containing production/payments-api
// (account 123456789012, ap-southeast-2, FARGATE) with metrics (collector
// sidecar) and traces (OTLP env) enabled but logging DISABLED, so scenario A
// shows drift for logs only.
func NewSeededSimulation() *SimulatedECS {
	s := NewSimulatedECS()
	s.AddService(ServiceSpec{
		Account:    SeedAccount,
		Region:     SeedRegion,
		Cluster:    SeedCluster,
		Name:       SeedService,
		LaunchType: types.LaunchTypeFargate,
		Containers: []types.ContainerDefinition{
			{
				Name:      aws.String("payments-api"),
				Image:     aws.String("example/payments-api:1.0.0"),
				Essential: aws.Bool(true),
				Environment: []types.KeyValuePair{
					{Name: aws.String(OTLPEndpointEnv), Value: aws.String("http://localhost:4317")},
				},
			},
			{
				Name:      aws.String("aws-otel-collector"),
				Image:     aws.String("public.ecr.aws/aws-observability/aws-otel-collector:latest"),
				Essential: aws.Bool(false),
				LogConfiguration: &types.LogConfiguration{
					LogDriver: types.LogDriverAwslogs,
					Options:   map[string]string{"awslogs-group": "/ecs/otel"},
				},
			},
		},
		DesiredCount: 2,
	})
	return s
}

// AddService registers a service at task definition revision 1 and returns its ARN.
func (s *SimulatedECS) AddService(spec ServiceSpec) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if spec.LaunchType == "" {
		spec.LaunchType = types.LaunchTypeFargate
	}
	if spec.DesiredCount == 0 {
		spec.DesiredCount = 1
	}
	arn := ServiceARN(spec.Region, spec.Account, spec.Cluster, spec.Name)
	svc := &simService{spec: spec, arn: arn, status: "ACTIVE", family: spec.Name, deployment: 1}
	s.services[arn] = svc
	svc.revision = s.registerLocked(svc, cloneContainers(spec.Containers))
	return arn
}

// SetServiceStatus sets the service status (e.g. "INACTIVE", "DRAINING").
func (s *SimulatedECS) SetServiceStatus(serviceARN, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc, ok := s.services[serviceARN]
	if !ok {
		return fmt.Errorf("simulated ecs: unknown service %q", serviceARN)
	}
	svc.status = status
	return nil
}

// SetLogging changes logging out-of-band, simulating drift that produced no
// event (scenario C). It registers a new task definition revision.
func (s *SimulatedECS) SetLogging(serviceARN string, enabled bool) error {
	return s.SetTelemetry(serviceARN, &enabled, nil, nil)
}

// SetTelemetry changes telemetry out-of-band. A nil pointer leaves that signal
// unchanged. It registers a new task definition revision.
func (s *SimulatedECS) SetTelemetry(serviceARN string, logs, metrics, traces *bool) error {
	_, err := s.mutate(serviceARN, func(c classifier, defs []types.ContainerDefinition) []types.ContainerDefinition {
		if logs != nil {
			defs = setLogs(c, defs, *logs)
		}
		if metrics != nil {
			defs = setMetrics(c, defs, *metrics)
		}
		if traces != nil {
			defs = setTraces(c, defs, *traces)
		}
		return defs
	})
	return err
}

// Calls returns a copy of the per-API call counters (see the Call* constants).
func (s *SimulatedECS) Calls() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.calls))
	for k, v := range s.calls {
		out[k] = v
	}
	return out
}

// CallCount returns how many times the named API was called.
func (s *SimulatedECS) CallCount(api string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[api]
}

// Revision returns the task definition revision the service currently uses.
func (s *SimulatedECS) Revision(serviceARN string) (int32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc, ok := s.services[serviceARN]
	if !ok {
		return 0, false
	}
	return svc.revision, true
}

// Remediator returns a Remediator that applies actions to this simulation by
// registering a new task definition revision and pointing the service at it,
// mimicking what a real rollout would do.
func (s *SimulatedECS) Remediator() Remediator { return simRemediator{sim: s} }

type simRemediator struct{ sim *SimulatedECS }

func (r simRemediator) Remediate(_ context.Context, id state.WorkloadIdentity, actions []state.Action) (state.ReconcileResult, error) {
	rev, err := r.sim.mutate(id.RuntimeID, func(c classifier, defs []types.ContainerDefinition) []types.ContainerDefinition {
		for _, a := range actions {
			on := strings.HasPrefix(string(a.Name), "enable_")
			switch a.Signal {
			case state.SignalLogs:
				defs = setLogs(c, defs, on)
			case state.SignalMetrics:
				defs = setMetrics(c, defs, on)
			case state.SignalTraces:
				defs = setTraces(c, defs, on)
			}
		}
		return defs
	})
	if err != nil {
		return state.ReconcileResult{}, err
	}
	return state.ReconcileResult{
		Applied: actions,
		Changed: true,
		Message: fmt.Sprintf("simulated: registered task definition revision %d and started a new deployment", rev),
	}, nil
}

// mutate registers a new revision of the service's task definition with fn
// applied and points the service at it. It returns the new revision.
func (s *SimulatedECS) mutate(serviceARN string, fn func(classifier, []types.ContainerDefinition) []types.ContainerDefinition) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc, ok := s.services[serviceARN]
	if !ok {
		return 0, fmt.Errorf("simulated ecs: unknown service %q", serviceARN)
	}
	cur := s.taskDefs[svc.family][svc.revision-1]
	defs := fn(s.cls, cloneContainers(cur.ContainerDefinitions))
	svc.revision = s.registerLocked(svc, defs)
	s.calls[CallUpdateService]++
	svc.deployment++
	return svc.revision, nil
}

// registerLocked registers a new revision of svc's family; s.mu must be held.
func (s *SimulatedECS) registerLocked(svc *simService, defs []types.ContainerDefinition) int32 {
	s.calls[CallRegisterTaskDefinition]++
	rev := int32(len(s.taskDefs[svc.family]) + 1)
	s.taskDefs[svc.family] = append(s.taskDefs[svc.family], types.TaskDefinition{
		Family:               aws.String(svc.family),
		Revision:             rev,
		TaskDefinitionArn:    aws.String(fmt.Sprintf("arn:aws:ecs:%s:%s:task-definition/%s:%d", svc.spec.Region, svc.spec.Account, svc.family, rev)),
		ContainerDefinitions: defs,
	})
	return rev
}

// DescribeServices implements API.
func (s *SimulatedECS) DescribeServices(_ context.Context, in *ecs.DescribeServicesInput, _ ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[CallDescribeServices]++
	cluster := aws.ToString(in.Cluster)
	if i := strings.LastIndex(cluster, "/"); i >= 0 {
		cluster = cluster[i+1:]
	}
	withTags := false
	for _, f := range in.Include {
		if f == types.ServiceFieldTags {
			withTags = true
		}
	}
	out := &ecs.DescribeServicesOutput{}
	for _, name := range in.Services {
		if i := strings.LastIndex(name, "/"); i >= 0 {
			name = name[i+1:]
		}
		svc := s.find(cluster, name)
		if svc == nil {
			out.Failures = append(out.Failures, types.Failure{
				Arn:    aws.String(name),
				Reason: aws.String("MISSING"),
			})
			continue
		}
		td := s.taskDefs[svc.family][svc.revision-1]
		dep := types.Deployment{
			Id:             aws.String(fmt.Sprintf("ecs-svc/%d", svc.deployment)),
			Status:         aws.String("PRIMARY"),
			RolloutState:   types.DeploymentRolloutStateCompleted,
			TaskDefinition: td.TaskDefinitionArn,
		}
		ts := types.Service{
			ServiceName:    aws.String(svc.spec.Name),
			ServiceArn:     aws.String(svc.arn),
			ClusterArn:     aws.String(fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", svc.spec.Region, svc.spec.Account, svc.spec.Cluster)),
			Status:         aws.String(svc.status),
			LaunchType:     svc.spec.LaunchType,
			TaskDefinition: td.TaskDefinitionArn,
			DesiredCount:   svc.spec.DesiredCount,
			RunningCount:   svc.spec.DesiredCount,
			Deployments:    []types.Deployment{dep},
		}
		if withTags {
			keys := make([]string, 0, len(svc.spec.Tags))
			for k := range svc.spec.Tags {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				ts.Tags = append(ts.Tags, types.Tag{Key: aws.String(k), Value: aws.String(svc.spec.Tags[k])})
			}
		}
		out.Services = append(out.Services, ts)
	}
	return out, nil
}

func (s *SimulatedECS) find(cluster, name string) *simService {
	for _, svc := range s.services {
		if svc.spec.Cluster == cluster && svc.spec.Name == name {
			return svc
		}
	}
	return nil
}

// DescribeTaskDefinition implements API. The input may be a task definition
// ARN, "family:revision" or a bare family (latest revision).
func (s *SimulatedECS) DescribeTaskDefinition(_ context.Context, in *ecs.DescribeTaskDefinitionInput, _ ...func(*ecs.Options)) (*ecs.DescribeTaskDefinitionOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[CallDescribeTaskDefinition]++
	ref := taskDefRevision(aws.ToString(in.TaskDefinition))
	family, rev := ref, int32(0)
	if i := strings.LastIndex(ref, ":"); i >= 0 {
		n, err := strconv.Atoi(ref[i+1:])
		if err != nil {
			return nil, fmt.Errorf("simulated ecs: invalid task definition %q", ref)
		}
		family, rev = ref[:i], int32(n)
	}
	defs := s.taskDefs[family]
	if len(defs) == 0 || rev < 0 || int(rev) > len(defs) {
		return nil, fmt.Errorf("simulated ecs: task definition %q not found", ref)
	}
	if rev == 0 {
		rev = int32(len(defs))
	}
	td := defs[rev-1]
	td.ContainerDefinitions = cloneContainers(td.ContainerDefinitions)
	return &ecs.DescribeTaskDefinitionOutput{TaskDefinition: &td}, nil
}

func cloneContainer(c types.ContainerDefinition) types.ContainerDefinition {
	if c.LogConfiguration != nil {
		lc := *c.LogConfiguration
		c.LogConfiguration = &lc
	}
	c.Environment = append([]types.KeyValuePair(nil), c.Environment...)
	if c.DockerLabels != nil {
		m := make(map[string]string, len(c.DockerLabels))
		for k, v := range c.DockerLabels {
			m[k] = v
		}
		c.DockerLabels = m
	}
	return c
}

func cloneContainers(in []types.ContainerDefinition) []types.ContainerDefinition {
	out := make([]types.ContainerDefinition, len(in))
	for i, c := range in {
		out[i] = cloneContainer(c)
	}
	return out
}

func setLogs(c classifier, defs []types.ContainerDefinition, on bool) []types.ContainerDefinition {
	for i := range defs {
		if c.isSidecar(defs[i]) {
			continue
		}
		if !on {
			defs[i].LogConfiguration = nil
			continue
		}
		if defs[i].LogConfiguration == nil {
			defs[i].LogConfiguration = &types.LogConfiguration{
				LogDriver: types.LogDriverAwslogs,
				Options:   map[string]string{"awslogs-group": "/ecs/" + deref(defs[i].Name)},
			}
		}
	}
	return defs
}

func setMetrics(c classifier, defs []types.ContainerDefinition, on bool) []types.ContainerDefinition {
	if !on {
		out := defs[:0]
		for _, d := range defs {
			if c.isCollector(d) {
				continue
			}
			delete(d.DockerLabels, MetricsLabel)
			out = append(out, d)
		}
		return out
	}
	for i := range defs {
		if c.isSidecar(defs[i]) {
			continue
		}
		if defs[i].DockerLabels == nil {
			defs[i].DockerLabels = map[string]string{}
		}
		defs[i].DockerLabels[MetricsLabel] = MetricsLabelEnabled
		break
	}
	return defs
}

func setTraces(c classifier, defs []types.ContainerDefinition, on bool) []types.ContainerDefinition {
	out := defs[:0]
	for _, d := range defs {
		if c.isXRay(d) && !on {
			continue
		}
		if !c.isSidecar(d) {
			env := d.Environment[:0]
			for _, e := range d.Environment {
				if deref(e.Name) != OTLPEndpointEnv {
					env = append(env, e)
				}
			}
			d.Environment = env
			if on {
				d.Environment = append(d.Environment, types.KeyValuePair{
					Name: aws.String(OTLPEndpointEnv), Value: aws.String("http://localhost:4317"),
				})
			}
		}
		out = append(out, d)
	}
	return out
}
