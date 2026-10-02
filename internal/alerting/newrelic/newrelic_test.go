package newrelic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

var (
	k8sID = state.WorkloadIdentity{Service: "payments-api", Deployment: "payments-api", Workload: "payments-api", Runtime: "kubernetes", RuntimeID: "payments/payments-api"}
	ecsID = state.WorkloadIdentity{Service: "checkout", Workload: "checkout", Runtime: "ecs", RuntimeID: "arn:aws:ecs:ap-southeast-2:123456789012:service/prod-cluster/checkout"}
)

func rulesFor(t *testing.T, id state.WorkloadIdentity) map[string]alerting.ManagedRule {
	t.Helper()
	p := policy.ObservabilityPolicy{
		Metadata: policy.Metadata{Name: "pol"},
		Spec: policy.Spec{
			Telemetry: policy.Telemetry{Metrics: policy.SignalSpec{Enabled: true}},
			Alerts:    policy.Alerts{Profile: "standard"},
		},
	}
	rules, unsupported, err := alerting.DesiredRules(p, id, runtime.Capabilities{HostTelemetry: true, AlertReconciliation: true})
	if err != nil || len(unsupported) != 0 {
		t.Fatalf("DesiredRules() = %v, %v", unsupported, err)
	}
	m := map[string]alerting.ManagedRule{}
	for _, r := range rules {
		m[r.ID] = r
	}
	return m
}

func TestNRQLGolden(t *testing.T) {
	tests := []struct {
		id   state.WorkloadIdentity
		rule string
		want string
	}{
		{k8sID, alerting.RuleLatencyP95, "SELECT percentile(duration, 95) FROM Transaction WHERE appName = 'payments-api'"},
		{k8sID, alerting.RuleErrorRate, "SELECT percentage(count(*), WHERE error IS true) FROM Transaction WHERE appName = 'payments-api'"},
		{k8sID, alerting.RuleTraffic, "SELECT rate(count(*), 1 minute) FROM Transaction WHERE appName = 'payments-api'"},
		{k8sID, alerting.RuleContainerCPU, "SELECT average(cpuCoresUtilization) FROM K8sContainerSample WHERE deploymentName = 'payments-api' AND namespaceName = 'payments'"},
		{k8sID, alerting.RuleContainerMemory, "SELECT average(memoryWorkingSetUtilization) FROM K8sContainerSample WHERE deploymentName = 'payments-api' AND namespaceName = 'payments'"},
		{k8sID, alerting.RuleHostCPU, "SELECT average(cpuPercent) FROM SystemSample WHERE apmApplicationNames LIKE '%|payments-api|%'"},
		{ecsID, alerting.RuleLatencyP95, "SELECT percentile(duration, 95) FROM Transaction WHERE appName = 'checkout'"},
		{ecsID, alerting.RuleContainerCPU, "SELECT average(cpuPercent) FROM ContainerSample WHERE ecsClusterName = 'prod-cluster' AND ecsServiceName = 'checkout'"},
		{ecsID, alerting.RuleContainerMemory, "SELECT average(memoryUsageBytes / memoryLimitBytes * 100) FROM ContainerSample WHERE ecsClusterName = 'prod-cluster' AND ecsServiceName = 'checkout'"},
	}
	for _, tt := range tests {
		t.Run(tt.id.Runtime+"/"+tt.rule, func(t *testing.T) {
			got, err := nrql(rulesFor(t, tt.id)[tt.rule])
			if err != nil || got != tt.want {
				t.Errorf("nrql() = %q, %v\nwant %q", got, err, tt.want)
			}
		})
	}
}

func TestNRQLEcsWithoutARNAndQuoting(t *testing.T) {
	id := state.WorkloadIdentity{Service: "o'brien", Workload: "svc", Runtime: "ecs", RuntimeID: "svc"}
	got, err := nrql(rulesFor(t, id)[alerting.RuleContainerCPU])
	if err != nil || got != "SELECT average(cpuPercent) FROM ContainerSample WHERE ecsServiceName = 'svc'" {
		t.Errorf("nrql() = %q, %v", got, err)
	}
	got, _ = nrql(rulesFor(t, id)[alerting.RuleErrorRate])
	if !strings.Contains(got, `appName = 'o\'brien'`) {
		t.Errorf("quote not escaped: %q", got)
	}
}

func TestNRQLUnknownRuntime(t *testing.T) {
	id := state.WorkloadIdentity{Service: "x", Workload: "x", Runtime: "nomad", RuntimeID: "x"}
	rules := rulesFor(t, id)
	if _, err := nrql(rules[alerting.RuleContainerCPU]); err == nil {
		t.Error("saturation on unknown runtime must error")
	}
	for _, rid := range []string{alerting.RuleLatencyP95, alerting.RuleErrorRate, alerting.RuleTraffic, alerting.RuleHostCPU} {
		if _, err := nrql(rules[rid]); err != nil {
			t.Errorf("%s on unknown runtime: %v", rid, err)
		}
	}
}

func TestTranslateCondition(t *testing.T) {
	c, err := Translate(rulesFor(t, k8sID)[alerting.RuleLatencyP95])
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "[portable-reconciler] payments-api High latency (p95)" {
		t.Errorf("Name = %q", c.Name)
	}
	want := Term{Threshold: 0.5, ThresholdDuration: 300, ThresholdOccurrences: "ALL", Operator: "ABOVE", Priority: "CRITICAL"}
	if len(c.Terms) != 1 || c.Terms[0] != want {
		t.Errorf("Terms = %+v, want %+v", c.Terms, want)
	}
	tr, _ := Translate(rulesFor(t, k8sID)[alerting.RuleTraffic])
	if tr.Terms[0].Operator != "BELOW" || tr.Terms[0].Priority != "WARNING" || tr.Terms[0].ThresholdDuration != 600 {
		t.Errorf("traffic term = %+v", tr.Terms[0])
	}
}

func TestDescriptionRoundTrip(t *testing.T) {
	for id, r := range rulesFor(t, ecsID) {
		t.Run(id, func(t *testing.T) {
			desc := EncodeDescription(r)
			for _, line := range []string{"managed-by=portable-reconciler", "policy=pol", "rule-id=" + id, "runtime=ecs", "fingerprint=" + r.Fingerprint()} {
				if !strings.Contains(desc, line+"\n") && !strings.HasSuffix(desc, line) {
					t.Errorf("description lacks %q:\n%s", line, desc)
				}
			}
			got, fp, ok, err := DecodeDescription(desc)
			if err != nil || !ok {
				t.Fatalf("DecodeDescription() = %v, %v", ok, err)
			}
			if got.Fingerprint() != r.Fingerprint() || fp != r.Fingerprint() || got.Owner != r.Owner || got.Identity != r.Identity {
				t.Errorf("round trip changed rule:\n got %+v\nwant %+v", got, r)
			}
		})
	}
}

func TestDecodeDescriptionForeign(t *testing.T) {
	for _, d := range []string{"", "hand written condition", "managed-by=someone-else\nrule-id=x"} {
		if _, _, ok, err := DecodeDescription(d); ok || err != nil {
			t.Errorf("DecodeDescription(%q) = ok %v, err %v, want ignored", d, ok, err)
		}
	}
	if _, _, ok, err := DecodeDescription("managed-by=portable-reconciler\nrule-id=x\nspec=not-json"); !ok || err == nil {
		t.Errorf("corrupt managed description: ok=%v err=%v, want ok with error", ok, err)
	}
}

func newDryRun(t *testing.T) (*Backend, *DryRunClient) {
	t.Helper()
	c := NewDryRunClient(nil)
	b, err := New(config.NewRelicConfig{AccountID: 1, PolicyID: 2, DryRun: true}, WithClient(c))
	if err != nil {
		t.Fatal(err)
	}
	return b, c
}

func apply(t *testing.T, b alerting.Backend, owner alerting.Owner, desired []alerting.ManagedRule) alerting.Plan {
	t.Helper()
	ctx := context.Background()
	actual, err := b.List(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	p := alerting.PlanFor(desired, actual)
	for _, r := range p.Create {
		if err := b.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range p.Update {
		if err := b.Update(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range p.Delete {
		if err := b.Delete(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func desiredList(t *testing.T, id state.WorkloadIdentity) ([]alerting.ManagedRule, alerting.Owner) {
	t.Helper()
	var out []alerting.ManagedRule
	for _, r := range rulesFor(t, id) {
		out = append(out, r)
	}
	return out, alerting.OwnerFor("pol", id)
}

func TestDryRunIdempotentDriftAndCleanup(t *testing.T) {
	b, client := newDryRun(t)
	desired, owner := desiredList(t, k8sID)

	if p := apply(t, b, owner, desired); len(p.Create) != 6 {
		t.Fatalf("first plan creates = %d, want 6", len(p.Create))
	}
	if n := len(client.Conditions()); n != 6 {
		t.Errorf("shadow conditions = %d, want 6", n)
	}
	if p := apply(t, b, owner, desired); !p.Empty() {
		t.Fatalf("second plan = %+v, want empty", p)
	}

	// Threshold edited in the NR UI.
	for id, c := range client.Conditions() {
		if strings.Contains(c.Description, "rule-id="+alerting.RuleLatencyP95+"\n") {
			c.Terms[0].Threshold = 3
			client.conds[id] = c
		}
	}
	if p := apply(t, b, owner, desired); len(p.Update) != 1 || p.Update[0].ID != alerting.RuleLatencyP95 || len(p.Create)+len(p.Delete) != 0 {
		t.Fatalf("after threshold edit plan = %+v, want one latency update", p)
	}
	// NRQL edited in the NR UI.
	for id, c := range client.Conditions() {
		if strings.Contains(c.Description, "rule-id="+alerting.RuleTraffic+"\n") {
			c.NRQL.Query = "SELECT count(*) FROM Transaction"
			client.conds[id] = c
		}
	}
	if p := apply(t, b, owner, desired); len(p.Update) != 1 || p.Update[0].ID != alerting.RuleTraffic {
		t.Fatalf("after nrql edit plan = %+v, want one traffic update", p)
	}
	if p := apply(t, b, owner, desired); !p.Empty() {
		t.Fatalf("plan after repair = %+v, want empty", p)
	}
	// Deleted in the UI.
	for id, c := range client.Conditions() {
		if strings.Contains(c.Description, "rule-id="+alerting.RuleHostCPU+"\n") {
			delete(client.conds, id)
		}
	}
	if p := apply(t, b, owner, desired); len(p.Create) != 1 {
		t.Fatalf("after UI delete plan = %+v, want one create", p)
	}
	// Stale rules are deleted; foreign owners are untouched.
	other, otherOwner := desiredList(t, ecsID)
	apply(t, b, otherOwner, other)
	if p := apply(t, b, owner, nil); len(p.Delete) != 6 {
		t.Fatalf("cleanup plan deletes = %d, want 6", len(p.Delete))
	}
	if got, _ := b.List(context.Background(), otherOwner); len(got) != 6 {
		t.Errorf("other owner rules = %d, want 6", len(got))
	}
}

func TestListIgnoresUnmanagedConditions(t *testing.T) {
	b, client := newDryRun(t)
	client.conds["99"] = Condition{ID: "99", PolicyID: "2", Name: "[portable-reconciler] hand made", Description: "just a human", Enabled: true}
	got, err := b.List(context.Background(), alerting.Owner{})
	if err != nil || len(got) != 0 {
		t.Errorf("List() = %v, %v, want none", got, err)
	}
}

type recordingClient struct {
	ops  []string
	vars []map[string]any
	resp map[string]string
}

func (r *recordingClient) Query(_ context.Context, gql string, vars map[string]any) (json.RawMessage, error) {
	r.ops = append(r.ops, gql)
	r.vars = append(r.vars, vars)
	for k, v := range r.resp {
		if strings.Contains(gql, k) {
			return json.RawMessage(v), nil
		}
	}
	return json.RawMessage(`{}`), nil
}

func TestBackendRequestShape(t *testing.T) {
	rc := &recordingClient{resp: map[string]string{
		"nrqlConditionsSearch": `{"actor":{"account":{"alerts":{"nrqlConditionsSearch":{"nextCursor":"","nrqlConditions":[]}}}}}`,
	}}
	b, err := New(config.NewRelicConfig{AccountID: 42, PolicyID: 7, DryRun: true}, WithClient(rc))
	if err != nil {
		t.Fatal(err)
	}
	desired, owner := desiredList(t, k8sID)
	if _, err := b.List(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	if err := b.Create(context.Background(), desired[0]); err != nil {
		t.Fatal(err)
	}
	if len(rc.ops) != 2 || !strings.Contains(rc.ops[0], "nrqlConditionsSearch") || !strings.Contains(rc.ops[1], "alertsNrqlConditionStaticCreate") {
		t.Fatalf("ops = %v", rc.ops)
	}
	if rc.vars[0]["accountId"] != int64(42) || rc.vars[0]["policyId"] != "7" || rc.vars[0]["nameLike"] != "[portable-reconciler]" {
		t.Errorf("search vars = %v", rc.vars[0])
	}
	cond, _ := rc.vars[1]["condition"].(map[string]any)
	if rc.vars[1]["policyId"] != "7" || cond["enabled"] != true || !strings.HasPrefix(cond["name"].(string), NamePrefix) {
		t.Errorf("create vars = %v", rc.vars[1])
	}
	if _, has := cond["id"]; has {
		t.Error("create input must not carry read-only fields")
	}
}

func TestHTTPClientRequest(t *testing.T) {
	var gotKey, gotCT, gotMethod, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotCT, gotMethod, gotPath = r.Header.Get("API-Key"), r.Header.Get("Content-Type"), r.Method, r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		_, _ = w.Write([]byte(`{"data":{"ok":true}}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL+"/graphql", "secret", srv.Client())
	data, err := c.Query(context.Background(), "query { x }", map[string]any{"a": 1})
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("Query() = %s, %v", data, err)
	}
	if gotKey != "secret" || gotCT != "application/json" || gotMethod != http.MethodPost || gotPath != "/graphql" {
		t.Errorf("request: key=%q ct=%q method=%q path=%q", gotKey, gotCT, gotMethod, gotPath)
	}
	if gotBody["query"] != "query { x }" || gotBody["variables"].(map[string]any)["a"] != float64(1) {
		t.Errorf("body = %v", gotBody)
	}
}

func TestHTTPClientErrors(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{"graphql errors", 200, `{"errors":[{"message":"bad nrql"},{"message":"worse"}]}`, "bad nrql; worse"},
		{"http status", 401, `unauthorized`, "status 401"},
		{"bad json", 200, `<html>`, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()
			_, err := NewHTTPClient(srv.URL, "k", nil).Query(context.Background(), "q", nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestEndpointFor(t *testing.T) {
	for in, want := range map[string]string{"US": EndpointUS, "": EndpointUS, "eu": EndpointEU} {
		if got, err := EndpointFor(in); err != nil || got != want {
			t.Errorf("EndpointFor(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	if _, err := EndpointFor("AP"); err == nil {
		t.Error("EndpointFor(AP) must fail")
	}
}

func TestNewValidation(t *testing.T) {
	env := func(m map[string]string) Option { return WithGetenv(func(k string) string { return m[k] }) }
	tests := []struct {
		name    string
		cfg     config.NewRelicConfig
		env     map[string]string
		wantErr []string
	}{
		{"dry run needs nothing", config.NewRelicConfig{DryRun: true}, nil, nil},
		{"live missing everything", config.NewRelicConfig{Region: "US", APIKeyEnv: "K"}, nil, []string{"accountId", "policyId", "K"}},
		{"live missing key", config.NewRelicConfig{AccountID: 1, PolicyID: 2, Region: "US", APIKeyEnv: "K"}, nil, []string{"K"}},
		{"live ok", config.NewRelicConfig{AccountID: 1, PolicyID: 2, Region: "EU", APIKeyEnv: "K"}, map[string]string{"K": "v"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, env(tt.env))
			if len(tt.wantErr) == 0 {
				if err != nil {
					t.Fatalf("New() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("New() error = nil")
			}
			for _, w := range tt.wantErr {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q lacks %q", err, w)
				}
			}
		})
	}
}

func TestRegisteredAsNewRelicDryRunByDefault(t *testing.T) {
	b, err := alerting.New("newrelic", config.Default(), nil)
	if err != nil || b.Name() != "newrelic" {
		t.Fatalf("New(newrelic) = %v, %v", b, err)
	}
	desired, owner := desiredList(t, k8sID)
	if p := apply(t, b, owner, desired); len(p.Create) != 6 {
		t.Errorf("plan = %+v", p)
	}
	if p := apply(t, b, owner, desired); !p.Empty() {
		t.Errorf("second plan = %+v, want empty", p)
	}
}
