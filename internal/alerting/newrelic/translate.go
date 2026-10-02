// Package newrelic is the New Relic alerting.Backend. It talks NerdGraph
// through a Client; the same Backend code runs against the real HTTPClient and
// the DryRunClient. Everything New Relic specific (NRQL, event types,
// attribute names, GraphQL shapes) lives in this package.
package newrelic

import (
	"fmt"
	"math"
	"strings"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// NamePrefix prefixes every condition name this controller creates; List
// searches on it.
const NamePrefix = "[portable-reconciler]"

// aggregationWindowSeconds is the NRQL signal aggregation window.
const aggregationWindowSeconds = 60

// Condition is the New Relic NRQL static alert condition a rule translates to.
// JSON tags match both the NerdGraph input and the search result shape.
type Condition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
	NRQL        NRQL   `json:"nrql"`
	Signal      Signal `json:"signal"`
	Terms       []Term `json:"terms"`
	Value       string `json:"valueFunction,omitempty"`
	ID          string `json:"id,omitempty"` // read side only
	PolicyID    string `json:"policyId,omitempty"`
	Type        string `json:"type,omitempty"`
}

// NRQL holds the query.
type NRQL struct {
	Query string `json:"query"`
}

// Signal configures how NRQL results are aggregated.
type Signal struct {
	AggregationWindow int `json:"aggregationWindow"`
}

// Term is one threshold of a condition.
type Term struct {
	Threshold            float64 `json:"threshold"`
	ThresholdDuration    int     `json:"thresholdDuration"`
	ThresholdOccurrences string  `json:"thresholdOccurrences"`
	Operator             string  `json:"operator"`
	Priority             string  `json:"priority"`
}

// ConditionName returns the New Relic condition name for r.
func ConditionName(r alerting.ManagedRule) string {
	return fmt.Sprintf("%s %s %s", NamePrefix, r.Identity.Service, r.Name)
}

// Translate converts r to a New Relic condition. The description carries the
// ownership encoding (see EncodeDescription).
func Translate(r alerting.ManagedRule) (Condition, error) {
	q, err := nrql(r)
	if err != nil {
		return Condition{}, err
	}
	secs := int(r.Window.Seconds())
	if secs < 60 || secs%aggregationWindowSeconds != 0 {
		return Condition{}, fmt.Errorf("rule %s: window %s must be a whole number of minutes", r.ID, r.Window)
	}
	return Condition{
		Name:        ConditionName(r),
		Description: EncodeDescription(r),
		Enabled:     true,
		NRQL:        NRQL{Query: q},
		Signal:      Signal{AggregationWindow: aggregationWindowSeconds},
		Value:       "SINGLE_VALUE",
		Terms: []Term{{
			Threshold:            toNRThreshold(r),
			ThresholdDuration:    secs,
			ThresholdOccurrences: "ALL",
			Operator:             strings.ToUpper(string(r.Operator)),
			Priority:             strings.ToUpper(string(r.Severity)),
		}},
	}, nil
}

// APM latency is stored in seconds; the neutral threshold is milliseconds.
func toNRThreshold(r alerting.ManagedRule) float64 {
	if r.Signal == alerting.SignalLatency && r.Unit == alerting.UnitMilliseconds {
		return r.Threshold / 1000
	}
	return r.Threshold
}

func fromNRThreshold(r alerting.ManagedRule, v float64) float64 {
	if r.Signal == alerting.SignalLatency && r.Unit == alerting.UnitMilliseconds {
		v *= 1000
	}
	return math.Round(v*1e6) / 1e6
}

func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func nrql(r alerting.ManagedRule) (string, error) {
	id := r.Identity
	apm := "appName = " + quote(id.Service)
	switch r.Signal {
	case alerting.SignalLatency:
		if r.Aggregation != alerting.AggP95 {
			return "", fmt.Errorf("rule %s: unsupported latency aggregation %q", r.ID, r.Aggregation)
		}
		return "SELECT percentile(duration, 95) FROM Transaction WHERE " + apm, nil
	case alerting.SignalErrors:
		return "SELECT percentage(count(*), WHERE error IS true) FROM Transaction WHERE " + apm, nil
	case alerting.SignalTraffic:
		return "SELECT rate(count(*), 1 minute) FROM Transaction WHERE " + apm, nil
	case alerting.SignalSaturation:
		return saturationNRQL(r)
	}
	return "", fmt.Errorf("rule %s: unsupported signal %q", r.ID, r.Signal)
}

func saturationNRQL(r alerting.ManagedRule) (string, error) {
	id := r.Identity
	if r.Scope == alerting.ScopeHost {
		if r.Resource != alerting.ResourceCPU {
			return "", fmt.Errorf("rule %s: unsupported host resource %q", r.ID, r.Resource)
		}
		return "SELECT average(cpuPercent) FROM SystemSample WHERE apmApplicationNames LIKE " + quote("%|"+id.Service+"|%"), nil
	}
	var metric string
	switch id.Runtime {
	case "kubernetes":
		ns, dep := k8sNames(id)
		switch r.Resource {
		case alerting.ResourceCPU:
			metric = "cpuCoresUtilization"
		case alerting.ResourceMemory:
			metric = "memoryWorkingSetUtilization"
		default:
			return "", fmt.Errorf("rule %s: unsupported resource %q", r.ID, r.Resource)
		}
		return fmt.Sprintf("SELECT average(%s) FROM K8sContainerSample WHERE deploymentName = %s AND namespaceName = %s",
			metric, quote(dep), quote(ns)), nil
	case "ecs":
		switch r.Resource {
		case alerting.ResourceCPU:
			metric = "cpuPercent"
		case alerting.ResourceMemory:
			metric = "memoryUsageBytes / memoryLimitBytes * 100"
		default:
			return "", fmt.Errorf("rule %s: unsupported resource %q", r.ID, r.Resource)
		}
		return fmt.Sprintf("SELECT average(%s) FROM ContainerSample WHERE %s", metric, ecsFilter(id.RuntimeID, id.Workload)), nil
	}
	return "", fmt.Errorf("rule %s: no New Relic saturation source for runtime %q", r.ID, id.Runtime)
}

// k8sNames returns the namespace and deployment name. RuntimeID is "namespace/name".
func k8sNames(id state.WorkloadIdentity) (namespace, deployment string) {
	ns, name, found := strings.Cut(id.RuntimeID, "/")
	if !found {
		ns, name = "", id.RuntimeID
	}
	if id.Deployment != "" {
		name = id.Deployment
	}
	return ns, name
}

// ecsFilter filters ContainerSample by ECS service. The cluster is taken from
// a service ARN (arn:...:service/<cluster>/<name>); without one only the
// service name is matched.
func ecsFilter(runtimeID, service string) string {
	if _, res, ok := strings.Cut(runtimeID, ":service/"); ok {
		if cluster, name, ok := strings.Cut(res, "/"); ok {
			return fmt.Sprintf("ecsClusterName = %s AND ecsServiceName = %s", quote(cluster), quote(name))
		}
	}
	return "ecsServiceName = " + quote(service)
}
