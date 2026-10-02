// Package config loads and validates the controller configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// RuntimeAuto selects the runtime by environment detection.
const RuntimeAuto = "auto"

// ECS mode values for ECSConfig.Mode.
const (
	ECSModeSimulated = "simulated"
	ECSModeAWS       = "aws"
)

// EnvRuntime overrides runtime.type when set.
const EnvRuntime = "RECONCILER_RUNTIME"

// Config is the controller configuration.
type Config struct {
	Runtime        RuntimeConfig        `json:"runtime" mapstructure:"runtime"`
	Reconciliation ReconciliationConfig `json:"reconciliation" mapstructure:"reconciliation"`
	Policies       PoliciesConfig       `json:"policies" mapstructure:"policies"`
	Kubernetes     KubernetesConfig     `json:"kubernetes" mapstructure:"kubernetes"`
	ECS            ECSConfig            `json:"ecs" mapstructure:"ecs"`
	EventBridge    EventBridgeConfig    `json:"eventbridge" mapstructure:"eventbridge"`
	Status         StatusConfig         `json:"status" mapstructure:"status"`
	Alerting       AlertingConfig       `json:"alerting" mapstructure:"alerting"`
}

// New Relic regions for NewRelicConfig.Region.
const (
	RegionUS = "US"
	RegionEU = "EU"
)

// AlertingConfig configures alert management. Switching Backend is how the
// platform team migrates alert engines without touching any policy.
type AlertingConfig struct {
	// Enabled turns the alerts reconcile step on. Default true.
	Enabled bool `json:"enabled" mapstructure:"enabled"`
	// Backend is the name of a registered alert backend (e.g. "newrelic",
	// "memory"). Default "newrelic", which runs in dry-run mode unless
	// newrelic.dryRun is false, so the default never touches the network.
	// Whether the name is registered is checked at construction, not here.
	Backend  string         `json:"backend" mapstructure:"backend"`
	NewRelic NewRelicConfig `json:"newrelic" mapstructure:"newrelic"`
}

// NewRelicConfig configures the New Relic alert backend. The API key is never
// stored here; it is read from the environment variable named by APIKeyEnv.
// Constraints for dryRun=false (account, policy, key present) are enforced
// when the backend is constructed.
type NewRelicConfig struct {
	AccountID int64 `json:"accountId" mapstructure:"accountId"`
	// PolicyID is the shared New Relic alert policy all conditions are created in.
	PolicyID int64 `json:"policyId" mapstructure:"policyId"`
	// Region is US or EU.
	Region    string `json:"region" mapstructure:"region"`
	APIKeyEnv string `json:"apiKeyEnv" mapstructure:"apiKeyEnv"`
	// DryRun logs the NerdGraph requests instead of sending them. Default true.
	DryRun bool `json:"dryRun" mapstructure:"dryRun"`
}

// RuntimeConfig selects the runtime adapter.
type RuntimeConfig struct {
	// Type is "auto" or the name of a registered runtime (e.g. "kubernetes",
	// "ecs"). An explicit type beats auto-detection. Whether the name is
	// registered is checked at selection time, not here.
	Type string `json:"type" mapstructure:"type"`
}

// ReconciliationConfig tunes the engine.
type ReconciliationConfig struct {
	// ResyncInterval is a Go duration string, e.g. "5m".
	ResyncInterval string `json:"resyncInterval" mapstructure:"resyncInterval"`
	Workers        int    `json:"workers" mapstructure:"workers"`
	MaxRetries     int    `json:"maxRetries" mapstructure:"maxRetries"`
}

// PoliciesConfig locates policy files.
type PoliciesConfig struct {
	Dir string `json:"dir" mapstructure:"dir"`
}

// KubernetesConfig configures the Kubernetes adapter.
type KubernetesConfig struct {
	Kubeconfig       string `json:"kubeconfig,omitempty" mapstructure:"kubeconfig"`
	ApplyAnnotations bool   `json:"applyAnnotations" mapstructure:"applyAnnotations"`
}

// ECSConfig configures the ECS adapter.
type ECSConfig struct {
	Mode   string `json:"mode" mapstructure:"mode"` // simulated or aws
	Region string `json:"region,omitempty" mapstructure:"region"`
}

// EventBridgeConfig configures the EventBridge HTTP event source.
type EventBridgeConfig struct {
	ListenAddr string `json:"listenAddr" mapstructure:"listenAddr"`
}

// StatusConfig configures the status endpoint. Empty disables it.
type StatusConfig struct {
	ListenAddr string `json:"listenAddr,omitempty" mapstructure:"listenAddr"`
}

// Default returns the default configuration.
func Default() Config {
	return Config{
		Runtime:        RuntimeConfig{Type: RuntimeAuto},
		Reconciliation: ReconciliationConfig{ResyncInterval: "5m", Workers: 2, MaxRetries: 5},
		Policies:       PoliciesConfig{Dir: "./examples"},
		ECS:            ECSConfig{Mode: ECSModeSimulated},
		EventBridge:    EventBridgeConfig{ListenAddr: ":8080"},
		Alerting: AlertingConfig{
			Enabled: true,
			Backend: "newrelic",
			NewRelic: NewRelicConfig{
				Region:    RegionUS,
				APIKeyEnv: "NEW_RELIC_API_KEY",
				DryRun:    true,
			},
		},
	}
}

// ResyncDuration returns the parsed resync interval. Valid after Validate.
func (c Config) ResyncDuration() time.Duration {
	d, err := time.ParseDuration(c.Reconciliation.ResyncInterval)
	if err != nil {
		return 0
	}
	return d
}

// Parse decodes YAML over the defaults, applies environment overrides via
// getenv (os.Getenv when nil) and validates the result. Unknown keys are rejected.
func Parse(data []byte, getenv func(string) string) (Config, error) {
	cfg := Default()
	if len(data) > 0 {
		if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("decode config: %w", err)
		}
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	if v := getenv(EnvRuntime); v != "" {
		cfg.Runtime.Type = v
	}
	cfg.Runtime.Type = strings.ToLower(cfg.Runtime.Type)
	cfg.Alerting.Backend = strings.ToLower(cfg.Alerting.Backend)
	cfg.Alerting.NewRelic.Region = strings.ToUpper(cfg.Alerting.NewRelic.Region)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Load reads path and parses it. An empty path yields defaults plus environment overrides.
func Load(path string, getenv func(string) string) (Config, error) {
	var data []byte
	if path != "" {
		var err error
		if data, err = os.ReadFile(path); err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
	}
	return Parse(data, getenv)
}

// Validate checks the configuration.
func (c Config) Validate() error {
	var errs []error
	if c.Runtime.Type == "" {
		errs = append(errs, errors.New("runtime.type is required (use \"auto\" to detect)"))
	}
	if d, err := time.ParseDuration(c.Reconciliation.ResyncInterval); err != nil {
		errs = append(errs, fmt.Errorf("reconciliation.resyncInterval: %w", err))
	} else if d <= 0 {
		errs = append(errs, errors.New("reconciliation.resyncInterval must be positive"))
	}
	if c.Reconciliation.Workers < 1 {
		errs = append(errs, errors.New("reconciliation.workers must be at least 1"))
	}
	if c.Reconciliation.MaxRetries < 0 {
		errs = append(errs, errors.New("reconciliation.maxRetries must not be negative"))
	}
	if c.Policies.Dir == "" {
		errs = append(errs, errors.New("policies.dir is required"))
	}
	switch c.ECS.Mode {
	case ECSModeSimulated, ECSModeAWS:
	default:
		errs = append(errs, fmt.Errorf("ecs.mode %q must be simulated or aws", c.ECS.Mode))
	}
	if c.Alerting.Enabled {
		if c.Alerting.Backend == "" {
			errs = append(errs, errors.New("alerting.backend is required when alerting is enabled"))
		}
		nr := c.Alerting.NewRelic
		if nr.Region != RegionUS && nr.Region != RegionEU {
			errs = append(errs, fmt.Errorf("alerting.newrelic.region %q must be US or EU", nr.Region))
		}
		if nr.AccountID < 0 || nr.PolicyID < 0 {
			errs = append(errs, errors.New("alerting.newrelic.accountId and policyId must not be negative"))
		}
		if nr.APIKeyEnv == "" {
			errs = append(errs, errors.New("alerting.newrelic.apiKeyEnv is required (name of the env var holding the key)"))
		}
	}
	return errors.Join(errs...)
}
