package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(nil, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg != Default() {
		t.Errorf("Parse(nil) = %+v, want defaults", cfg)
	}
	if got := cfg.ResyncDuration(); got != 5*time.Minute {
		t.Errorf("ResyncDuration() = %v, want 5m", got)
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		env     map[string]string
		check   func(Config) bool
		wantErr string
	}{
		{name: "override", yaml: "runtime: {type: ecs}\nreconciliation: {resyncInterval: 10s, workers: 4}\n",
			check: func(c Config) bool {
				return c.Runtime.Type == "ecs" && c.ResyncDuration() == 10*time.Second && c.Reconciliation.Workers == 4 && c.Reconciliation.MaxRetries == 5
			}},
		{name: "env wins", yaml: "runtime: {type: ecs}\n", env: map[string]string{EnvRuntime: "Kubernetes"},
			check: func(c Config) bool { return c.Runtime.Type == "kubernetes" }},
		{name: "empty runtime", yaml: "runtime: {type: \"\"}\n", wantErr: "runtime.type"},
		{name: "custom runtime", yaml: "runtime: {type: nomad}\n", check: func(c Config) bool { return c.Runtime.Type == "nomad" }},
		{name: "bad duration", yaml: "reconciliation: {resyncInterval: soon}\n", wantErr: "resyncInterval"},
		{name: "zero workers", yaml: "reconciliation: {workers: 0}\n", wantErr: "workers"},
		{name: "bad ecs mode", yaml: "ecs: {mode: fargate}\n", wantErr: "ecs.mode"},
		{name: "alerting defaults", yaml: "", check: func(c Config) bool {
			a := c.Alerting
			return a.Enabled && a.Backend == "newrelic" && a.NewRelic.DryRun && a.NewRelic.Region == "US" && a.NewRelic.APIKeyEnv == "NEW_RELIC_API_KEY"
		}},
		{name: "alerting override", yaml: "alerting: {backend: Memory, newrelic: {accountId: 7, policyId: 9, region: eu, dryRun: false}}\n",
			check: func(c Config) bool {
				a := c.Alerting
				return a.Enabled && a.Backend == "memory" && a.NewRelic.AccountID == 7 && a.NewRelic.PolicyID == 9 &&
					a.NewRelic.Region == "EU" && !a.NewRelic.DryRun && a.NewRelic.APIKeyEnv == "NEW_RELIC_API_KEY"
			}},
		{name: "alerting disabled skips checks", yaml: "alerting: {enabled: false, backend: \"\"}\n", check: func(c Config) bool { return !c.Alerting.Enabled }},
		{name: "alerting empty backend", yaml: "alerting: {backend: \"\"}\n", wantErr: "alerting.backend"},
		{name: "alerting bad region", yaml: "alerting: {newrelic: {region: AP}}\n", wantErr: "alerting.newrelic.region"},
		{name: "alerting empty key env", yaml: "alerting: {newrelic: {apiKeyEnv: \"\"}}\n", wantErr: "apiKeyEnv"},
		{name: "alerting negative id", yaml: "alerting: {newrelic: {accountId: -1}}\n", wantErr: "must not be negative"},
		{name: "unknown key", yaml: "bogus: 1\n", wantErr: "decode config"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tt.yaml), env(tt.env))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Parse() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if !tt.check(cfg) {
				t.Errorf("Parse() = %+v", cfg)
			}
		})
	}
}
