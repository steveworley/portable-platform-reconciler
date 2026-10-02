package policy

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validECS = `
apiVersion: observability.platform.example/v1alpha1
kind: ObservabilityPolicy
metadata: {name: payments-api}
target: {runtime: ecs, account: "123456789012", region: ap-southeast-2, cluster: production, service: payments-api}
spec:
  profile: standard
  telemetry:
    logs: {enabled: true}
    metrics: {enabled: false}
    traces: {enabled: true}
  alerts: {profile: standard}
`

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{name: "valid ecs", in: validECS},
		{name: "valid kubernetes", in: strings.Replace(validECS,
			`{runtime: ecs, account: "123456789012", region: ap-southeast-2, cluster: production, service: payments-api}`,
			`{runtime: kubernetes, namespace: payments, kind: Deployment, name: payments-api}`, 1)},
		{name: "bad apiVersion", in: strings.Replace(validECS, "v1alpha1", "v9", 1), wantErr: "apiVersion"},
		{name: "bad kind", in: strings.Replace(validECS, "kind: ObservabilityPolicy", "kind: Other", 1), wantErr: "kind must be"},
		{name: "missing name", in: strings.Replace(validECS, "{name: payments-api}", "{}", 1), wantErr: "metadata.name"},
		{name: "missing runtime", in: strings.Replace(validECS, "runtime: ecs, ", "", 1), wantErr: "target.runtime is required"},
		{name: "unknown field", in: validECS + "extra: 1\n", wantErr: "unknown field"},
		{name: "not yaml", in: "{{{", wantErr: "decode policy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse([]byte(tt.in))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Parse() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if p.Metadata.Generation != 1 {
				t.Errorf("Generation = %d, want 1", p.Metadata.Generation)
			}
		})
	}
}

func TestParseFields(t *testing.T) {
	p, err := Parse([]byte(validECS))
	if err != nil {
		t.Fatal(err)
	}
	if p.Target.Account != "123456789012" || p.Target.Region != "ap-southeast-2" {
		t.Errorf("Target = %+v", p.Target)
	}
	if !p.Spec.Telemetry.Logs.Enabled || p.Spec.Telemetry.Metrics.Enabled || !p.Spec.Telemetry.Traces.Enabled {
		t.Errorf("Telemetry = %+v", p.Spec.Telemetry)
	}
	if p.Spec.Alerts.Profile != "standard" {
		t.Errorf("Alerts.Profile = %q, want standard", p.Spec.Alerts.Profile)
	}
}

func TestTargetValidatorOption(t *testing.T) {
	validator := func(tr TargetRef) error {
		if tr.Cluster == "" {
			return errors.New("cluster required")
		}
		return nil
	}
	if _, err := Parse([]byte(validECS), WithTargetValidator(validator)); err != nil {
		t.Errorf("Parse() error = %v", err)
	}
	noCluster := strings.Replace(validECS, "cluster: production, ", "", 1)
	if _, err := Parse([]byte(noCluster), WithTargetValidator(validator)); err == nil || !strings.Contains(err.Error(), "cluster required") {
		t.Errorf("Parse() error = %v, want cluster required", err)
	}
	if _, err := Parse([]byte(noCluster)); err != nil {
		t.Errorf("Parse() without validator error = %v", err)
	}
}

func TestLoadDir(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("a.yaml", validECS)
	write("b.yml", strings.Replace(validECS, "name: payments-api}", "name: other}", 1))
	write("ignored.txt", "nope")

	got, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}
	if len(got) != 2 || got[0].Metadata.Name != "payments-api" || got[1].Metadata.Name != "other" {
		t.Errorf("LoadDir() = %+v", got)
	}

	write("c.yaml", validECS)
	if _, err := LoadDir(dir); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("LoadDir() error = %v, want duplicate", err)
	}
}

func TestLoadExamples(t *testing.T) {
	for _, dir := range []string{"../../examples/ecs", "../../examples/kubernetes"} {
		got, err := LoadDir(dir)
		if err != nil || len(got) != 1 {
			t.Errorf("LoadDir(%s) = %d policies, error %v; want 1, nil", dir, len(got), err)
		}
	}
}

func TestKey(t *testing.T) {
	if got := Key("x"); got != "ObservabilityPolicy:x" {
		t.Errorf("Key() = %q", got)
	}
	if n, ok := ParseKey("ObservabilityPolicy:x"); !ok || n != "x" {
		t.Errorf("ParseKey() = %q, %v", n, ok)
	}
	if _, ok := ParseKey("ECSService:arn"); ok {
		t.Error("ParseKey(non-policy) ok = true")
	}
}
