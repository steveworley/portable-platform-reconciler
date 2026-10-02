// Package policy defines the runtime-neutral ObservabilityPolicy model and its parser.
package policy

const (
	// APIVersion is the only supported policy apiVersion.
	APIVersion = "observability.platform.example/v1alpha1"
	// Kind is the only supported policy kind.
	Kind = "ObservabilityPolicy"
)

// KeyPrefix prefixes work-queue keys that refer to a policy.
const KeyPrefix = "ObservabilityPolicy:"

// Key returns the work-queue key for the named policy.
func Key(name string) string { return KeyPrefix + name }

// ParseKey extracts the policy name from a policy work-queue key.
func ParseKey(key string) (name string, ok bool) {
	if len(key) <= len(KeyPrefix) || key[:len(KeyPrefix)] != KeyPrefix {
		return "", false
	}
	return key[len(KeyPrefix):], true
}

// ObservabilityPolicy declares the desired observability configuration for one workload.
type ObservabilityPolicy struct {
	APIVersion string    `json:"apiVersion" mapstructure:"apiVersion"`
	Kind       string    `json:"kind" mapstructure:"kind"`
	Metadata   Metadata  `json:"metadata" mapstructure:"metadata"`
	Target     TargetRef `json:"target" mapstructure:"target"`
	Spec       Spec      `json:"spec" mapstructure:"spec"`
}

// Metadata identifies a policy.
type Metadata struct {
	Name string `json:"name" mapstructure:"name"`
	// Generation increases whenever the spec changes. Defaults to 1.
	Generation int64 `json:"generation,omitempty" mapstructure:"generation"`
}

// TargetRef is a runtime-neutral reference to a workload. Runtime names the
// adapter that interprets it; only the fields that adapter needs are populated.
// Which fields are required is decided by the adapter (see
// runtime.Registration.ValidateTarget), not by this package.
type TargetRef struct {
	Runtime     string `json:"runtime" mapstructure:"runtime"`
	Environment string `json:"environment,omitempty" mapstructure:"environment"`

	// Commonly used by AWS-style runtimes (e.g. ECS).
	Account string `json:"account,omitempty" mapstructure:"account"`
	Region  string `json:"region,omitempty" mapstructure:"region"`
	Cluster string `json:"cluster,omitempty" mapstructure:"cluster"`
	Service string `json:"service,omitempty" mapstructure:"service"`

	// Commonly used by namespaced runtimes (e.g. Kubernetes).
	Namespace string `json:"namespace,omitempty" mapstructure:"namespace"`
	Kind      string `json:"kind,omitempty" mapstructure:"kind"`
	Name      string `json:"name,omitempty" mapstructure:"name"`

	// Attributes carries fields for platforms the fields above do not fit
	// (e.g. a Nomad job or Cloud Run service).
	Attributes map[string]string `json:"attributes,omitempty" mapstructure:"attributes"`
}

// Spec is the desired observability configuration.
type Spec struct {
	Profile   string    `json:"profile,omitempty" mapstructure:"profile"`
	Telemetry Telemetry `json:"telemetry" mapstructure:"telemetry"`
	Alerts    Alerts    `json:"alerts,omitempty" mapstructure:"alerts"`
}

// Telemetry selects which signals must be enabled.
type Telemetry struct {
	Logs    SignalSpec `json:"logs" mapstructure:"logs"`
	Metrics SignalSpec `json:"metrics" mapstructure:"metrics"`
	Traces  SignalSpec `json:"traces" mapstructure:"traces"`
}

// SignalSpec configures a single telemetry signal.
type SignalSpec struct {
	Enabled bool `json:"enabled" mapstructure:"enabled"`
}

// Alerts selects the alerting profile.
type Alerts struct {
	Profile string `json:"profile,omitempty" mapstructure:"profile"`
}
