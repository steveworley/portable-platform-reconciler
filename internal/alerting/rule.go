// Package alerting reconciles a default golden-signal alert set for each
// workload into a pluggable alert backend. It is runtime- and backend-neutral:
// rules are expressed against the canonical workload identity, and backends
// (see the memory and newrelic subpackages) translate them to their own
// definitions.
package alerting

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/example/portable-platform-reconciler/internal/state"
)

// Signal is a golden signal. It is an open string.
type Signal string

// Golden signals.
const (
	SignalLatency    Signal = "latency"
	SignalTraffic    Signal = "traffic"
	SignalErrors     Signal = "errors"
	SignalSaturation Signal = "saturation"
)

// Aggregation is how samples are reduced over the rule window.
type Aggregation string

// Aggregations.
const (
	AggP95   Aggregation = "p95"
	AggRate  Aggregation = "rate"
	AggCount Aggregation = "count"
	AggAvg   Aggregation = "avg"
)

// Operator compares the aggregated value with the threshold.
type Operator string

// Operators.
const (
	OpAbove Operator = "above"
	OpBelow Operator = "below"
)

// Severity is the alert priority.
type Severity string

// Severities.
const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
)

// Unit is the unit of Rule.Threshold.
type Unit string

// Units.
const (
	UnitMilliseconds   Unit = "ms"
	UnitPercent        Unit = "percent"
	UnitRequestsPerMin Unit = "rpm"
)

// Scope says what a saturation rule measures.
const (
	ScopeContainer = "container"
	ScopeHost      = "host"
)

// Resource says which resource a saturation rule measures.
const (
	ResourceCPU    = "cpu"
	ResourceMemory = "memory"
)

// Telemetry names used by Rule.RequiresTelemetry.
const (
	TelemetryMetrics = "metrics"
	TelemetryTraces  = "traces"
)

// Rule is a runtime- and backend-neutral alert rule. Profiles hold Rules as
// templates; DesiredRules binds them to a workload as ManagedRules.
type Rule struct {
	// ID is stable across releases and unique within a profile, e.g. "golden.latency.p95".
	ID          string
	Signal      Signal
	Name        string
	Description string
	Aggregation Aggregation
	Operator    Operator
	Threshold   float64
	Unit        Unit
	Window      time.Duration
	Severity    Severity

	// Scope and Resource refine saturation rules (container|host, cpu|memory).
	Scope    string
	Resource string

	// RequiresCapability is a runtime capability name (a runtime.Capabilities
	// field, e.g. "HostTelemetry"). Empty means none.
	RequiresCapability string
	// RequiresTelemetry lists telemetry signals of which at least one must be
	// enabled in the policy (e.g. metrics or traces). Empty means none.
	RequiresTelemetry []string
}

// RuleTemplate is a Rule that has not been bound to a workload.
type RuleTemplate = Rule

// Owner identifies who a managed rule belongs to.
type Owner struct {
	PolicyName string
	// WorkloadKey is the canonical identity key (identity.String()).
	WorkloadKey string
	Runtime     string
}

// OwnerFor returns the owner of the rules reconciled for policyName on identity.
func OwnerFor(policyName string, identity state.WorkloadIdentity) Owner {
	return Owner{PolicyName: policyName, WorkloadKey: identity.String(), Runtime: identity.Runtime}
}

// ManagedRule is a Rule bound to a workload and an owner.
type ManagedRule struct {
	Rule
	Owner    Owner
	Identity state.WorkloadIdentity
	// Divergent is set by a backend that observes the stored rule differs from
	// what it would itself have written (e.g. its query was edited by hand) in
	// a way the neutral fields cannot express. It forces a fingerprint mismatch.
	Divergent bool
}

// Fingerprint returns a deterministic hash of the normalised rule content
// (not of backend-specific state). Equal fingerprints mean no update is needed.
func (r ManagedRule) Fingerprint() string {
	id := r.Identity
	parts := []string{
		"id=" + r.ID,
		"signal=" + string(r.Signal),
		"scope=" + r.Scope,
		"resource=" + r.Resource,
		"name=" + r.Name,
		"description=" + r.Description,
		"aggregation=" + string(r.Aggregation),
		"operator=" + string(r.Operator),
		"threshold=" + strconv.FormatFloat(r.Threshold, 'g', -1, 64),
		"unit=" + string(r.Unit),
		"window=" + r.Window.String(),
		"severity=" + string(r.Severity),
		"identity=" + strings.Join([]string{id.Account, id.Region, id.Environment, id.Service, id.Deployment, id.Workload}, "|"),
		fmt.Sprintf("divergent=%t", r.Divergent),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:8])
}

// Ownership tag keys and values.
const (
	TagManagedBy   = "managed-by"
	ManagedByValue = "portable-reconciler"
	TagPolicy      = "policy"
	TagWorkload    = "workload"
	TagRuntime     = "runtime"
	TagRuleID      = "rule-id"
	TagFingerprint = "fingerprint"
	workloadKeySep = ":"
)

// Tags returns the ownership tags a backend must persist with the rule.
// "workload" is the runtime ID; together with "runtime" it rebuilds Owner.WorkloadKey.
func (r ManagedRule) Tags() map[string]string {
	return map[string]string{
		TagManagedBy:   ManagedByValue,
		TagPolicy:      r.Owner.PolicyName,
		TagWorkload:    strings.TrimPrefix(r.Owner.WorkloadKey, r.Owner.Runtime+workloadKeySep),
		TagRuntime:     r.Owner.Runtime,
		TagRuleID:      r.ID,
		TagFingerprint: r.Fingerprint(),
	}
}

// OwnerFromTags rebuilds the owner and rule ID from persisted tags. ok is
// false when the tags do not mark the rule as managed by this controller.
func OwnerFromTags(tags map[string]string) (owner Owner, ruleID string, ok bool) {
	if tags[TagManagedBy] != ManagedByValue || tags[TagRuleID] == "" {
		return Owner{}, "", false
	}
	rt := tags[TagRuntime]
	return Owner{
		PolicyName:  tags[TagPolicy],
		WorkloadKey: rt + workloadKeySep + tags[TagWorkload],
		Runtime:     rt,
	}, tags[TagRuleID], true
}
