package alerting

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/example/portable-platform-reconciler/internal/state"
)

// ErrUnknownProfile is returned for an alert profile that is not registered.
var ErrUnknownProfile = errors.New("unknown alert profile")

// Profile is a named, centrally defined set of rule templates.
type Profile struct {
	Name  string
	Rules []RuleTemplate
}

// Profiles is a registry of alert profiles.
type Profiles struct {
	mu sync.RWMutex
	m  map[string]Profile
}

// NewProfiles returns an empty registry.
func NewProfiles() *Profiles { return &Profiles{m: map[string]Profile{}} }

// Register adds or replaces a profile.
func (p *Profiles) Register(pr Profile) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m[pr.Name] = pr
}

// Get returns the named profile or an error wrapping ErrUnknownProfile.
func (p *Profiles) Get(name string) (Profile, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pr, ok := p.m[name]
	if !ok {
		return Profile{}, fmt.Errorf("%w %q (known: %v)", ErrUnknownProfile, name, p.namesLocked())
	}
	return pr, nil
}

// Names returns the registered profile names, sorted.
func (p *Profiles) Names() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.namesLocked()
}

func (p *Profiles) namesLocked() []string {
	out := make([]string, 0, len(p.m))
	for n := range p.m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Built-in profile names.
const (
	ProfileStandard = "standard"
	ProfileMinimal  = "minimal"
)

// Rule IDs of the built-in golden-signal rules.
const (
	RuleLatencyP95      = "golden.latency.p95"
	RuleErrorRate       = "golden.errors.rate"
	RuleTraffic         = "golden.traffic.throughput"
	RuleContainerCPU    = "golden.saturation.container.cpu"
	RuleContainerMemory = "golden.saturation.container.memory"
	RuleHostCPU         = "golden.saturation.host.cpu"
)

var (
	ruleLatency = Rule{
		ID: RuleLatencyP95, Signal: SignalLatency, Name: "High latency (p95)",
		Description: "p95 request latency is above 500ms for 5 minutes",
		Aggregation: AggP95, Operator: OpAbove, Threshold: 500, Unit: UnitMilliseconds,
		Window: 5 * time.Minute, Severity: SeverityCritical,
		RequiresTelemetry: []string{TelemetryMetrics, TelemetryTraces},
	}
	ruleErrors = Rule{
		ID: RuleErrorRate, Signal: SignalErrors, Name: "High error rate",
		Description: "error rate is above 5% for 5 minutes",
		Aggregation: AggRate, Operator: OpAbove, Threshold: 5, Unit: UnitPercent,
		Window: 5 * time.Minute, Severity: SeverityCritical,
		RequiresTelemetry: []string{TelemetryMetrics, TelemetryTraces},
	}
	ruleTraffic = Rule{
		ID: RuleTraffic, Signal: SignalTraffic, Name: "Traffic drop",
		Description: "throughput is below 1 request per minute for 10 minutes",
		Aggregation: AggRate, Operator: OpBelow, Threshold: 1, Unit: UnitRequestsPerMin,
		Window: 10 * time.Minute, Severity: SeverityWarning,
		RequiresTelemetry: []string{TelemetryMetrics, TelemetryTraces},
	}
	ruleContainerCPU = Rule{
		ID: RuleContainerCPU, Signal: SignalSaturation, Name: "High container CPU",
		Description: "container CPU utilisation is above 80% for 10 minutes",
		Aggregation: AggAvg, Operator: OpAbove, Threshold: 80, Unit: UnitPercent,
		Window: 10 * time.Minute, Severity: SeverityWarning,
		Scope: ScopeContainer, Resource: ResourceCPU,
	}
	ruleContainerMemory = Rule{
		ID: RuleContainerMemory, Signal: SignalSaturation, Name: "High container memory",
		Description: "container memory utilisation is above 85% for 10 minutes",
		Aggregation: AggAvg, Operator: OpAbove, Threshold: 85, Unit: UnitPercent,
		Window: 10 * time.Minute, Severity: SeverityWarning,
		Scope: ScopeContainer, Resource: ResourceMemory,
	}
	ruleHostCPU = Rule{
		ID: RuleHostCPU, Signal: SignalSaturation, Name: "High host CPU",
		Description: "host CPU utilisation is above 90% for 10 minutes",
		Aggregation: AggAvg, Operator: OpAbove, Threshold: 90, Unit: UnitPercent,
		Window: 10 * time.Minute, Severity: SeverityWarning,
		Scope: ScopeHost, Resource: ResourceCPU,
		RequiresCapability: state.CapHostTelemetry,
	}
)

// DefaultProfiles holds the built-in profiles (standard, minimal).
var DefaultProfiles = func() *Profiles {
	p := NewProfiles()
	p.Register(Profile{Name: ProfileStandard, Rules: []RuleTemplate{
		ruleLatency, ruleErrors, ruleTraffic, ruleContainerCPU, ruleContainerMemory, ruleHostCPU,
	}})
	p.Register(Profile{Name: ProfileMinimal, Rules: []RuleTemplate{ruleErrors, ruleLatency}})
	return p
}()
