// Package kubernetes implements runtime.RuntimeAdapter for Kubernetes
// Deployments. Telemetry intent is recorded as annotations on the Deployment's
// own metadata (never the pod template, so no rollout is triggered); the
// adapter always reads the Deployment from the API server before deciding.
package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// RuntimeType is the runtime name served by this adapter.
const RuntimeType runtime.RuntimeType = "kubernetes"

// KindDeployment is the only supported target kind.
const KindDeployment = "Deployment"

// KeyPrefix prefixes queue keys produced for Deployments.
const KeyPrefix = "KubernetesDeployment:"

// Annotations written to and read from the Deployment metadata.
const (
	AnnotationLogs       = "observability.platform.example/logs"
	AnnotationMetrics    = "observability.platform.example/metrics"
	AnnotationTraces     = "observability.platform.example/traces"
	AnnotationReconciled = "observability.platform.example/reconciled"

	// AnnotationEnabled is the value that marks a signal as enabled.
	AnnotationEnabled = "enabled"

	// AnnotationRevision is set by the Deployment controller.
	AnnotationRevision = "deployment.kubernetes.io/revision"
)

// Labels consulted when building the canonical identity.
const (
	LabelAppName        = "app.kubernetes.io/name"
	LabelApp            = "app"
	LabelEnvironment    = "environment"
	LabelAppEnvironment = "app.kubernetes.io/environment"
)

// DeploymentKey returns the work-queue key for a Deployment. The event source
// uses the same function so keys match RuntimeAdapter.QueueKey exactly.
func DeploymentKey(namespace, name string) string {
	return KeyPrefix + namespace + "/" + name
}

// Adapter is the Kubernetes RuntimeAdapter. Safe for concurrent use.
type Adapter struct {
	client           kubernetes.Interface
	logger           *slog.Logger
	applyAnnotations bool
	environment      string
}

var _ runtime.RuntimeAdapter = (*Adapter)(nil)

// Option configures an Adapter.
type Option func(*Adapter)

// WithLogger sets the logger. Default discards.
func WithLogger(l *slog.Logger) Option {
	return func(a *Adapter) {
		if l != nil {
			a.logger = l
		}
	}
}

// WithApplyAnnotations enables writing annotations. When false (the default)
// Reconcile is a dry run: it logs the actions and returns Changed=false, so
// DryRun is set, so the engine reports the policy Degraded (reason DryRun)
// without retrying.
func WithApplyAnnotations(apply bool) Option {
	return func(a *Adapter) { a.applyAnnotations = apply }
}

// WithEnvironment sets the default environment used when neither the
// Deployment labels nor the target specify one.
func WithEnvironment(env string) Option {
	return func(a *Adapter) { a.environment = env }
}

// New creates an Adapter using client.
func New(client kubernetes.Interface, opts ...Option) *Adapter {
	a := &Adapter{client: client, logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Runtime implements runtime.RuntimeAdapter.
func (a *Adapter) Runtime() runtime.RuntimeType { return RuntimeType }

// Capabilities implements runtime.RuntimeAdapter. Nodes are EC2-backed, so
// host and network telemetry are available.
func (a *Adapter) Capabilities() runtime.Capabilities {
	return runtime.Capabilities{
		WorkloadDiscovery:   true,
		TelemetryConfig:     true,
		HostTelemetry:       true,
		NetworkTelemetry:    true,
		AlertReconciliation: true,
	}
}

// ValidateTarget checks that target names a supported Kubernetes workload.
func ValidateTarget(t policy.TargetRef) error {
	if t.Namespace == "" {
		return fmt.Errorf("kubernetes target requires namespace")
	}
	if t.Name == "" {
		return fmt.Errorf("kubernetes target requires name")
	}
	if t.Kind != "" && t.Kind != KindDeployment {
		return fmt.Errorf("kubernetes target kind %q is not supported (only %s)", t.Kind, KindDeployment)
	}
	return nil
}

// QueueKey implements runtime.RuntimeAdapter.
func (a *Adapter) QueueKey(t policy.TargetRef) (string, error) {
	if err := ValidateTarget(t); err != nil {
		return "", err
	}
	return DeploymentKey(t.Namespace, t.Name), nil
}

// ResolveTarget implements runtime.RuntimeAdapter. It GETs the Deployment.
//
// Identity precedence:
//   - Service: label app.kubernetes.io/name, then app, then the Deployment name.
//   - Environment: label environment, then app.kubernetes.io/environment, then
//     target.Environment, then the adapter default (WithEnvironment), then the namespace.
func (a *Adapter) ResolveTarget(ctx context.Context, t policy.TargetRef) (runtime.Workload, error) {
	if err := ValidateTarget(t); err != nil {
		return runtime.Workload{}, err
	}
	d, err := a.getDeployment(ctx, t.Namespace, t.Name)
	if err != nil {
		return runtime.Workload{}, err
	}
	return runtime.Workload{
		Identity: a.identity(t, d),
		Ref:      t,
		Revision: revision(d),
		Raw:      d,
	}, nil
}

func (a *Adapter) getDeployment(ctx context.Context, ns, name string) (*appsv1.Deployment, error) {
	d, err := a.client.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("deployment %s/%s: %w", ns, name, runtime.ErrNotFound)
		}
		return nil, fmt.Errorf("get deployment %s/%s: %w", ns, name, err)
	}
	return d, nil
}

func (a *Adapter) identity(t policy.TargetRef, d *appsv1.Deployment) runtime.WorkloadIdentity {
	return runtime.WorkloadIdentity{
		Environment: firstNonEmpty(d.Labels[LabelEnvironment], d.Labels[LabelAppEnvironment], t.Environment, a.environment, d.Namespace),
		Service:     firstNonEmpty(d.Labels[LabelAppName], d.Labels[LabelApp], d.Name),
		Deployment:  d.Name,
		Workload:    d.Name,
		Runtime:     string(RuntimeType),
		RuntimeID:   d.Namespace + "/" + d.Name,
	}
}

func revision(d *appsv1.Deployment) string {
	s := fmt.Sprintf("generation=%d,observedGeneration=%d", d.Generation, d.Status.ObservedGeneration)
	if r := d.Annotations[AnnotationRevision]; r != "" {
		s += ",revision=" + r
	}
	return s
}

// Observe implements runtime.RuntimeAdapter. It re-reads the Deployment and
// lists its Pods; Workload.Raw is never used for state.
func (a *Adapter) Observe(ctx context.Context, wl runtime.Workload) (state.ObservedState, error) {
	ns, name := wl.Ref.Namespace, wl.Ref.Name
	d, err := a.getDeployment(ctx, ns, name)
	if err != nil {
		return state.ObservedState{}, err
	}
	details := map[string]string{
		"replicas.desired":   strconv.Itoa(int(desiredReplicas(d))),
		"replicas.ready":     strconv.Itoa(int(d.Status.ReadyReplicas)),
		"replicas.available": strconv.Itoa(int(d.Status.AvailableReplicas)),
		"replicas.updated":   strconv.Itoa(int(d.Status.UpdatedReplicas)),
	}
	if d.Spec.Selector != nil {
		sel, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
		if err != nil {
			return state.ObservedState{}, fmt.Errorf("deployment %s/%s selector: %w", ns, name, err)
		}
		pods, err := a.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
		if err != nil {
			return state.ObservedState{}, fmt.Errorf("list pods for %s/%s: %w", ns, name, err)
		}
		ready := 0
		for i := range pods.Items {
			if podReady(&pods.Items[i]) {
				ready++
			}
		}
		details["pods.total"] = strconv.Itoa(len(pods.Items))
		details["pods.ready"] = strconv.Itoa(ready)
	}
	return state.ObservedState{
		Workload: a.identity(wl.Ref, d),
		Revision: revision(d),
		Logs:     d.Annotations[AnnotationLogs] == AnnotationEnabled,
		Metrics:  d.Annotations[AnnotationMetrics] == AnnotationEnabled,
		Traces:   d.Annotations[AnnotationTraces] == AnnotationEnabled,
		Details:  details,
	}, nil
}

func desiredReplicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}

// Reconcile implements runtime.RuntimeAdapter. Actions become a JSON merge
// patch on the Deployment's metadata annotations; the pod template is never
// touched. Enable actions set the signal annotation to "enabled", disable
// actions remove it. Actions already reflected on the live Deployment are
// skipped, so a compliant Deployment receives no write.
//
// With applyAnnotations false this is a dry run: actions are logged and
// Changed is false and DryRun is true.
func (a *Adapter) Reconcile(ctx context.Context, _ state.DesiredState, observed state.ObservedState, actions []state.Action) (state.ReconcileResult, error) {
	id := observed.Workload
	resource := id.Workload
	if !a.applyAnnotations {
		for _, act := range actions {
			a.logger.Info("dry-run: would apply action", "resource", resource, "runtime", id.Runtime, "action", string(act.Name), "reason", act.Reason)
		}
		return state.ReconcileResult{Changed: false, DryRun: true, Message: "dry-run: applyAnnotations is disabled"}, nil
	}

	ns, name, err := splitRuntimeID(id.RuntimeID)
	if err != nil {
		return state.ReconcileResult{}, err
	}
	d, err := a.getDeployment(ctx, ns, name)
	if err != nil {
		return state.ReconcileResult{}, err
	}

	patch := map[string]any{}
	var applied []state.Action
	for _, act := range actions {
		key, enable, ok := annotationFor(act)
		if !ok {
			return state.ReconcileResult{}, fmt.Errorf("unsupported action %q", act.Name)
		}
		cur, present := d.Annotations[key]
		switch {
		case enable && cur != AnnotationEnabled:
			patch[key] = AnnotationEnabled
		case !enable && present:
			patch[key] = nil
		default:
			continue
		}
		applied = append(applied, act)
	}
	if len(applied) == 0 {
		return state.ReconcileResult{Changed: false, Message: "already compliant"}, nil
	}
	patch[AnnotationReconciled] = "true"

	body, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": patch}})
	if err != nil {
		return state.ReconcileResult{}, fmt.Errorf("encode patch: %w", err)
	}
	// A merge patch carries no resourceVersion, so it cannot conflict with
	// concurrent updates to unrelated fields; no retry loop is needed.
	if _, err := a.client.AppsV1().Deployments(ns).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return state.ReconcileResult{}, fmt.Errorf("deployment %s/%s: %w", ns, name, runtime.ErrNotFound)
		}
		return state.ReconcileResult{}, fmt.Errorf("patch deployment %s/%s: %w", ns, name, err)
	}
	for _, act := range applied {
		a.logger.Info("applied action", "resource", resource, "runtime", id.Runtime, "action", string(act.Name), "reason", act.Reason)
	}
	return state.ReconcileResult{Applied: applied, Changed: true, Message: "annotations patched"}, nil
}

func annotationFor(act state.Action) (key string, enable, ok bool) {
	switch act.Name {
	case state.ActionEnableLogs:
		return AnnotationLogs, true, true
	case state.ActionDisableLogs:
		return AnnotationLogs, false, true
	case state.ActionEnableMetrics:
		return AnnotationMetrics, true, true
	case state.ActionDisableMetrics:
		return AnnotationMetrics, false, true
	case state.ActionEnableTraces:
		return AnnotationTraces, true, true
	case state.ActionDisableTraces:
		return AnnotationTraces, false, true
	}
	return "", false, false
}

func splitRuntimeID(id string) (ns, name string, err error) {
	for i := 0; i < len(id); i++ {
		if id[i] == '/' {
			return id[:i], id[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid kubernetes runtime id %q", id)
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}
