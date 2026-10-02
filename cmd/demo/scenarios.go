package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/alerting/memory"
	"github.com/example/portable-platform-reconciler/internal/alerting/newrelic"
	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/events/eventbridge"
	k8ssource "github.com/example/portable-platform-reconciler/internal/events/kubernetes"
	"github.com/example/portable-platform-reconciler/internal/policy"
	ecsruntime "github.com/example/portable-platform-reconciler/internal/runtime/ecs"
	k8sruntime "github.com/example/portable-platform-reconciler/internal/runtime/kubernetes"
	"github.com/example/portable-platform-reconciler/internal/state"
)

const settle = 10 * time.Second

var scenarios = map[string]func(context.Context, *env) error{
	"A": scenarioA, "B": scenarioB, "C": scenarioC, "D": scenarioD, "E": scenarioE, "F": scenarioF,
}

// ecsStack is the ECS runtime wired to the engine: simulated ECS, real ECS
// adapter, real EventBridge HTTP source on a random localhost port and an
// in-memory alert backend.
type ecsStack struct {
	sim     *ecsruntime.SimulatedECS
	adapter *ecsruntime.Adapter
	backend *memory.Backend
	h       *harness
	pol     policy.ObservabilityPolicy
	url     string
	arn     string
	seeded  bool
}

func newECSStack(ctx context.Context, e *env, label, backendName string, resyncEvery time.Duration) (*ecsStack, error) {
	pol, err := e.loadPolicy("ecs/ecs-policy.yaml")
	if err != nil {
		return nil, err
	}
	sim := ecsruntime.NewSeededSimulation()
	adapter := ecsruntime.New(sim, ecsruntime.WithLogger(e.logger), ecsruntime.WithRemediator(sim.Remediator()))
	backend := memory.New(memory.WithName(backendName))
	h := newHarness(e, label, adapter, backend)
	eb := eventbridge.NewSource("127.0.0.1:0", eventbridge.WithLogger(e.logger))
	if err := h.start(ctx, resyncEvery, eb); err != nil {
		return nil, err
	}
	select {
	case <-eb.Ready():
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("eventbridge source did not start")
	}
	return &ecsStack{
		sim: sim, adapter: adapter, backend: backend, h: h, pol: pol,
		url: "http://" + eb.Addr() + eventbridge.EventsPath,
		arn: ecsruntime.ServiceARN(ecsruntime.SeedRegion, ecsruntime.SeedAccount, ecsruntime.SeedCluster, ecsruntime.SeedService),
	}, nil
}

// seed adds the policy (a policy-change event) and waits for the first full reconcile.
func (s *ecsStack) seed() error {
	s.seeded = true
	s.h.store.Upsert(s.pol)
	if !waitFor(settle, func() bool { return s.h.alertsReconciled(s.pol.Metadata.Name) }) {
		return fmt.Errorf("initial ECS reconcile did not complete")
	}
	return nil
}

// sharedECS returns the ECS stack used by A, B and C, seeding it (quietly) if
// an earlier scenario has not.
func (e *env) sharedECS(ctx context.Context) (*ecsStack, error) {
	if e.shared == nil {
		s, err := newECSStack(ctx, e, "ecs", "a", e.resync)
		if err != nil {
			return nil, err
		}
		e.shared = s
	}
	return e.shared, nil
}

func (e *env) ecsSeeded(ctx context.Context) (*ecsStack, error) {
	s, err := e.sharedECS(ctx)
	if err != nil {
		return nil, err
	}
	if !s.seeded {
		if err := s.seed(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *ecsStack) post(event map[string]any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	resp, err := http.Post(s.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("eventbridge source returned %s", resp.Status)
	}
	return nil
}

func (e *env) printRules(rules []alerting.ManagedRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tw := tabwriter.NewWriter(e.out, 4, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "    RULE ID\tSIGNAL\tTHRESHOLD\tWINDOW\tSEVERITY")
	for _, r := range rules {
		fmt.Fprintf(tw, "    %s\t%s\t%s %g %s\t%s\t%s\n", r.ID, r.Signal, r.Operator, r.Threshold, r.Unit, r.Window, r.Severity)
	}
	_ = tw.Flush()
}

func ruleIDs(rules []alerting.ManagedRule) []string {
	ids := make([]string, len(rules))
	for i, r := range rules {
		ids[i] = r.ID
	}
	sort.Strings(ids) // backends need not list in a stable order
	return ids
}

func ruleFingerprints(rules []alerting.ManagedRule) []string {
	fps := make([]string, len(rules))
	for i, r := range rules {
		fps[i] = r.ID + "=" + r.Fingerprint()
	}
	return fps
}

func sameStrings(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

// statusYAML renders a policy status in the shape of docs/poc-spec.md section 8.
func statusYAML(st controller.PolicyStatus) (string, error) {
	type cond struct {
		Type    string `json:"type"`
		Status  string `json:"status"`
		Reason  string `json:"reason,omitempty"`
		Message string `json:"message,omitempty"`
	}
	type body struct {
		Phase              string            `json:"phase"`
		ObservedGeneration int64             `json:"observedGeneration"`
		Conditions         []cond            `json:"conditions"`
		LastReconciledAt   string            `json:"lastReconciledAt,omitempty"`
		Details            map[string]string `json:"details,omitempty"`
	}
	b := body{Phase: string(st.Phase), ObservedGeneration: st.ObservedGeneration, Details: st.Details}
	if st.LastReconciledAt != nil {
		b.LastReconciledAt = st.LastReconciledAt.UTC().Format(time.RFC3339)
	}
	for _, c := range st.Conditions {
		b.Conditions = append(b.Conditions, cond{string(c.Type), string(c.Status), c.Reason, c.Message})
	}
	out, err := yaml.Marshal(map[string]body{"status": b})
	return string(out), err
}

func indent(s, prefix string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n") + "\n"
}

func scenarioA(ctx context.Context, e *env) error {
	e.header("A", "initial reconciliation (ECS): drift detected, simulated remediation, alert rules created")
	s, err := e.sharedECS(ctx)
	if err != nil {
		return err
	}
	name := s.pol.Metadata.Name
	wl, err := s.adapter.ResolveTarget(ctx, s.pol.Target)
	if err != nil {
		return err
	}
	before, err := s.adapter.Observe(ctx, wl)
	if err != nil {
		return err
	}
	revBefore, _ := s.sim.Revision(s.arn)
	e.printf("  Seeded ECS service %s (Fargate), task definition revision %d\n", wl.Identity.RuntimeID, revBefore)
	e.printf("  Observed:  logs=%t metrics=%t traces=%t\n", before.Logs, before.Metrics, before.Traces)
	e.printf("  Policy:    logs=%t metrics=%t traces=%t, alerts.profile=%s\n",
		s.pol.Spec.Telemetry.Logs.Enabled, s.pol.Spec.Telemetry.Metrics.Enabled, s.pol.Spec.Telemetry.Traces.Enabled, s.pol.Spec.Alerts.Profile)
	e.printf("  Adding the policy (policy-change event) ...\n")

	mark := s.h.count()
	if err := s.seed(); err != nil {
		return err
	}
	revAfter, _ := s.sim.Revision(s.arn)
	var first *controller.Outcome
	for _, o := range s.h.outcomesFrom(mark, "") {
		if hasAction(o, state.ActionEnableLogs) {
			first = &o
			break
		}
	}
	if first == nil {
		e.check(false, "an outcome with the enable_logs action was recorded")
		return nil
	}
	e.printf("\n  Drift detected, actions taken:\n")
	for _, a := range first.Actions {
		e.printf("    - %-18s %s\n", a.Name, a.Signal)
	}
	e.printf("  ECS task definition revision: %d -> %d (simulated remediation registered a new revision)\n", revBefore, revAfter)

	rules := s.backend.Rules()
	e.printf("\n  Alert rules in backend %q for %s (profile standard):\n", s.backend.Name(), wl.Identity.Workload)
	e.printRules(rules)
	e.printf("  Reported UNSUPPORTED (not failed):\n")
	for _, u := range first.Unsupported {
		e.printf("    - %s\n", u)
	}
	st, _ := s.h.statuses.Get(name)
	y, err := statusYAML(st)
	if err != nil {
		return err
	}
	e.printf("\n  Policy status (docs/poc-spec.md section 8 shape):\n%s", indent(y, "    "))

	obs, err := s.adapter.Observe(ctx, wl)
	if err != nil {
		return err
	}
	ids := ruleIDs(rules)
	e.check(first.Workload.Runtime == "ecs", "workload resolved to canonical identity runtime=ecs")
	e.check(revAfter == revBefore+1 && obs.Logs, "logs now enabled in ECS (re-read via Describe*), revision %d -> %d", revBefore, revAfter)
	e.check(len(rules) == 5 && !contains(ids, alerting.RuleHostCPU), "5 golden-signal rules created; host CPU not created")
	e.check(anyContains(first.Unsupported, alerting.RuleHostCPU), "host CPU rule reported Unsupported on Fargate")
	e.check(s.backend.Writes().Creates == 5, "backend received exactly 5 creates (got %d)", s.backend.Writes().Creates)
	cnd, _ := st.Condition(alerting.ConditionAlertsReconciled)
	e.check(cnd.Status == controller.ConditionTrue && st.Details[alerting.DetailManagedRules] == "5", "status: AlertsReconciled=True, alerts.managedRules=5")
	return nil
}

func contains(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}

func anyContains(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func scenarioB(ctx context.Context, e *env) error {
	e.header("B", "duplicate event: second delivery is a no-op, event payload is ignored")
	s, err := e.ecsSeeded(ctx)
	if err != nil {
		return err
	}
	event := map[string]any{
		"version":     "0",
		"id":          "demo-dup-1",
		"source":      "aws.ecs",
		"detail-type": "ECS Deployment State Change",
		"detail": map[string]any{
			"clusterArn": "arn:aws:ecs:ap-southeast-2:123456789012:cluster/production",
			"serviceArn": s.arn,
			"eventName":  "SERVICE_DEPLOYMENT_IN_PROGRESS",
			// Deliberately misleading: the controller must not trust any of this.
			"desiredCount":   0,
			"loggingEnabled": false,
		},
	}
	s.backend.ResetWrites()
	callsBefore := s.sim.Calls()
	revBefore, _ := s.sim.Revision(s.arn)
	e.printf("  POSTing the same ECS Deployment State Change event twice to %s\n", s.url)
	e.printf("  (payload claims desiredCount=0 and loggingEnabled=false; reality differs)\n")

	var outs []controller.Outcome
	for i := 1; i <= 2; i++ {
		mark := s.h.count()
		if err := s.post(event); err != nil {
			return err
		}
		if !waitFor(settle, func() bool { return len(s.h.outcomesFrom(mark, "ECSService:")) >= 1 }) {
			e.check(false, "event %d produced a reconcile", i)
			return nil
		}
		outs = append(outs, s.h.outcomesFrom(mark, "ECSService:")...)
	}
	calls := s.sim.Calls()
	revAfter, _ := s.sim.Revision(s.arn)
	dDescSvc := calls[ecsruntime.CallDescribeServices] - callsBefore[ecsruntime.CallDescribeServices]
	dDescTD := calls[ecsruntime.CallDescribeTaskDefinition] - callsBefore[ecsruntime.CallDescribeTaskDefinition]
	dReg := calls[ecsruntime.CallRegisterTaskDefinition] - callsBefore[ecsruntime.CallRegisterTaskDefinition]
	dUpd := calls[ecsruntime.CallUpdateService] - callsBefore[ecsruntime.CallUpdateService]
	w := s.backend.Writes()

	e.printf("\n  ECS API calls caused by the 2 events:\n")
	e.printf("    DescribeServices        +%d   (authoritative reads)\n", dDescSvc)
	e.printf("    DescribeTaskDefinition  +%d   (authoritative reads)\n", dDescTD)
	e.printf("    RegisterTaskDefinition  +%d   (remediation)\n", dReg)
	e.printf("    UpdateService           +%d   (remediation)\n", dUpd)
	e.printf("  Alert backend writes: creates=%d updates=%d deletes=%d\n", w.Creates, w.Updates, w.Deletes)

	allNoop := len(outs) >= 2
	for _, o := range outs {
		allNoop = allNoop && o.NoOp && o.Err == nil
	}
	e.check(allNoop, "both events reconciled and both were NoOp (state already converged)")
	e.check(dDescSvc >= 2 && dDescTD >= 2, "Describe* were called for each event: state was read, not taken from the payload")
	e.check(dReg == 0 && dUpd == 0 && revBefore == revAfter, "zero remediation calls (revision stays %d)", revAfter)
	e.check(w.Total() == 0, "zero alert backend writes")
	return nil
}

func scenarioC(ctx context.Context, e *env) error {
	e.header("C", "missed event: out-of-band drift is corrected by the periodic resync")
	s, err := e.ecsSeeded(ctx)
	if err != nil {
		return err
	}
	s.backend.ResetWrites()
	revBefore, _ := s.sim.Revision(s.arn)
	if err := s.sim.SetLogging(s.arn, false); err != nil {
		return err
	}
	n := s.backend.Tamper(alerting.RuleLatencyP95, 999)
	e.printf("  Out-of-band, with NO event sent: logging disabled in ECS; latency rule threshold edited to 999 (%d rule)\n", n)
	e.printf("  Waiting for the next resync tick (every %s) ...\n", e.resync)
	start := time.Now()
	fixed := waitFor(3*e.resync+settle, func() bool {
		for _, r := range s.backend.Rules() {
			if r.ID == alerting.RuleLatencyP95 && r.Threshold != 500 {
				return false
			}
		}
		rev, _ := s.sim.Revision(s.arn)
		return rev >= revBefore+2 && s.h.alertsReconciled(s.pol.Metadata.Name)
	})
	elapsed := time.Since(start).Round(10 * time.Millisecond)
	w := s.backend.Writes()
	revAfter, _ := s.sim.Revision(s.arn)
	wl, err := s.adapter.ResolveTarget(ctx, s.pol.Target)
	if err != nil {
		return err
	}
	obs, err := s.adapter.Observe(ctx, wl)
	if err != nil {
		return err
	}
	e.printf("\n  Drift corrected after %s. ECS revision %d -> %d; backend writes: creates=%d updates=%d deletes=%d\n",
		elapsed, revBefore, revAfter, w.Creates, w.Updates, w.Deletes)
	e.check(fixed, "drift detected and fixed with no event, only the resync")
	e.check(obs.Logs, "logging re-enabled in ECS")
	e.check(w.Updates == 1 && w.Creates == 0 && w.Deletes == 0, "exactly 1 alert update (the tampered rule), no other writes")
	e.check(elapsed <= 3*e.resync+2*time.Second, "corrected within about one resync interval (%s)", elapsed)
	return nil
}

func scenarioD(ctx context.Context, e *env) error {
	e.header("D", "different runtime: same policy and engine, Kubernetes adapter")
	pol, err := e.loadPolicy("kubernetes/kubernetes-policy.yaml")
	if err != nil {
		return err
	}
	client := fake.NewClientset()
	adapter := k8sruntime.New(client, k8sruntime.WithLogger(e.logger), k8sruntime.WithApplyAnnotations(true))
	backend := memory.New(memory.WithName("k8s"))
	// A resync that never fires proves the informer event drives this scenario.
	const never = time.Hour
	h := newHarness(e, "k8s", adapter, backend)
	if err := h.start(ctx, never, k8ssource.New(client, k8ssource.WithLogger(e.logger))); err != nil {
		return err
	}
	e.printf("  Engine type:   %T\n", h.engine)
	e.printf("  Adapter type:  %T (runtime=%s)\n", h.adapter, h.adapter.Runtime())
	e.printf("  Alert step:    %T into backend %q\n", alerting.NewStep(backend), backend.Name())
	e.printf("  Policy:        examples/kubernetes/kubernetes-policy.yaml (spec identical to the ECS policy; only target differs)\n")
	e.printf("  Adding the policy before the workload exists: it stays Pending (a WARN log below is expected).\n")

	h.store.Upsert(pol)
	time.Sleep(500 * time.Millisecond) // let the informer sync before the Deployment appears
	e.printf("  Creating Deployment %s/%s WITHOUT annotations; the informer should wake the engine ...\n", pol.Target.Namespace, pol.Target.Name)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Namespace: pol.Target.Namespace,
		Name:      pol.Target.Name,
		Labels:    map[string]string{k8sruntime.LabelAppName: pol.Target.Name, k8sruntime.LabelEnvironment: "production"},
	}}
	mark := h.count()
	if _, err := client.AppsV1().Deployments(dep.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		return err
	}
	ok := waitFor(settle, func() bool { return h.alertsReconciled(pol.Metadata.Name) })
	if !ok {
		e.check(false, "Kubernetes deployment event led to a reconcile")
		return nil
	}
	got, err := client.AppsV1().Deployments(dep.Namespace).Get(ctx, dep.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	var first *controller.Outcome
	for _, o := range h.outcomesFrom(mark, "KubernetesDeployment:") {
		if hasAction(o, state.ActionEnableLogs) {
			first = &o
			break
		}
	}
	rules := backend.Rules()
	e.printf("\n  Annotations applied by the adapter:\n")
	for _, k := range []string{k8sruntime.AnnotationLogs, k8sruntime.AnnotationMetrics, k8sruntime.AnnotationTraces, k8sruntime.AnnotationReconciled} {
		e.printf("    %s: %s\n", k, got.Annotations[k])
	}
	e.printf("\n  Alert rules in backend %q (host CPU IS supported on Kubernetes nodes):\n", backend.Name())
	e.printRules(rules)

	st, _ := h.statuses.Get(pol.Metadata.Name)
	e.check(first != nil, "reconcile was triggered by the KubernetesDeployment key from the informer (resync disabled)")
	e.check(got.Annotations[k8sruntime.AnnotationLogs] == k8sruntime.AnnotationEnabled &&
		got.Annotations[k8sruntime.AnnotationMetrics] == k8sruntime.AnnotationEnabled &&
		got.Annotations[k8sruntime.AnnotationTraces] == k8sruntime.AnnotationEnabled, "annotations applied for logs, metrics and traces")
	e.check(len(rules) == 6 && contains(ruleIDs(rules), alerting.RuleHostCPU), "6 golden-signal rules incl. host CPU (capability differs per runtime, engine unchanged)")
	e.check(st.Workload != nil && st.Workload.Runtime == "kubernetes" && st.Workload.RuntimeID == "payments/payments-api",
		"canonical identity is runtime=kubernetes runtimeID=payments/payments-api")
	e.check(st.Phase == controller.PhaseReady, "phase Ready (no unsupported features on Kubernetes), got %s", st.Phase)
	return nil
}

func scenarioE(ctx context.Context, e *env) error {
	e.header("E", "backend migration: same policies, new alert backend, no team-side change")
	pol, err := e.loadPolicy("ecs/ecs-policy.yaml")
	if err != nil {
		return err
	}
	sim := ecsruntime.NewSeededSimulation()
	adapter := ecsruntime.New(sim, ecsruntime.WithLogger(e.logger), ecsruntime.WithRemediator(sim.Remediator()))
	store := controller.NewMemoryPolicyStore(pol)
	key := policy.Key(pol.Metadata.Name)

	engineFor := func(b alerting.Backend) *controller.Engine {
		return controller.NewEngine(adapter, store, controller.NewStatusStore(), e.logger,
			controller.WithSteps(alerting.NewStep(b, alerting.WithLogger(e.logger))))
	}
	reconcile := func(label string, b alerting.Backend) error {
		outs, err := engineFor(b).ReconcileOnce(ctx, key)
		if err != nil {
			return fmt.Errorf("reconcile into %s: %w", label, err)
		}
		e.printf("  Reconciled policy %q into backend %q: %d action(s)\n", pol.Metadata.Name, label, len(outs[0].Actions))
		return nil
	}

	a := memory.New(memory.WithName("a"))
	b := memory.New(memory.WithName("b"))
	e.printf("  Step 1: the platform runs on backend \"a\"\n")
	if err := reconcile("a", a); err != nil {
		return err
	}
	e.printf("  Step 2: platform team switches alerting.backend to \"b\" (policy %q unchanged, generation %d)\n", pol.Metadata.Name, pol.Metadata.Generation)
	if err := reconcile("b", b); err != nil {
		return err
	}
	e.printf("\n  Backend \"b\" now holds:\n")
	e.printRules(b.Rules())

	cfg := config.Default()
	cfg.Alerting.NewRelic.DryRun = true
	nr, err := alerting.New("newrelic", cfg, e.logger)
	if err != nil {
		return err
	}
	e.printf("\n  Step 3: or switch to New Relic (dry-run: nothing leaves this process)\n")
	if err := reconcile("newrelic", nr); err != nil {
		return err
	}
	nrRules, err := nr.List(ctx, alerting.OwnerFor(pol.Metadata.Name, a.Rules()[0].Identity))
	if err != nil {
		return err
	}
	e.printf("  Generated NRQL conditions (what a live run would create):\n")
	for _, r := range a.Rules() {
		if r.ID != alerting.RuleLatencyP95 && r.ID != alerting.RuleContainerCPU {
			continue
		}
		c, err := newrelic.Translate(r)
		if err != nil {
			return err
		}
		e.printf("    %s\n      NRQL: %s\n      term: %s %g for %ds (%s)\n", c.Name, c.NRQL.Query, c.Terms[0].Operator, c.Terms[0].Threshold, c.Terms[0].ThresholdDuration, c.Terms[0].Priority)
	}

	e.check(sameStrings(ruleIDs(a.Rules()), ruleIDs(b.Rules())) && len(b.Rules()) == 5, "backend b holds the same 5 rule IDs as backend a")
	e.check(sameStrings(ruleFingerprints(a.Rules()), ruleFingerprints(b.Rules())), "rule fingerprints are identical across backends (same desired set)")
	e.check(sameStrings(ruleIDs(a.Rules()), ruleIDs(nrRules)), "New Relic (dry-run) holds the same rule IDs, translated to NRQL")

	e.printf("\n  Step 4: opt-in cleanup of the old backend \"a\" (alerting.Prune, keep nothing)\n")
	rep, err := alerting.Prune(ctx, a, nil, true)
	if err != nil {
		return err
	}
	e.printf("  Dry run would delete %d rule(s) from \"a\":\n", len(rep.Pruned))
	e.printRules(rep.Pruned)
	e.check(rep.DryRun && len(rep.Pruned) == 5 && len(a.Rules()) == 5, "dry-run reports 5 rules and deletes nothing")
	rep, err = alerting.Prune(ctx, a, nil, false)
	if err != nil {
		return err
	}
	e.printf("  Real prune deleted %d rule(s) from \"a\"\n", len(rep.Pruned))
	e.check(len(rep.Pruned) == 5 && len(a.Rules()) == 0, "old backend a is empty after the real prune")
	e.check(len(b.Rules()) == 5, "new backend b is untouched (5 rules)")
	return nil
}

func scenarioF(ctx context.Context, e *env) error {
	e.header("F", "policy deleted: the finalizer removes its alert rules")
	s, err := newECSStack(ctx, e, "ecs-f", "f", e.resync)
	if err != nil {
		return err
	}
	if err := s.seed(); err != nil {
		return err
	}
	name := s.pol.Metadata.Name
	e.printf("  Policy %q reconciled; backend holds %d rule(s). Deleting the policy ...\n", name, len(s.backend.Rules()))
	s.h.store.Delete(name)
	gone := waitFor(settle, func() bool {
		_, has := s.h.statuses.Get(name)
		return !has && len(s.backend.Rules()) == 0
	})
	w := s.backend.Writes()
	e.printf("  Backend writes: creates=%d updates=%d deletes=%d\n", w.Creates, w.Updates, w.Deletes)
	e.check(gone, "alert rules removed and status dropped after the policy was deleted")
	e.check(w.Deletes == 5, "exactly 5 delete writes (got %d)", w.Deletes)
	return nil
}
