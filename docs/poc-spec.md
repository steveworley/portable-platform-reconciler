# POC: Portable Platform Reconciliation Controller

Build a proof-of-concept for a **runtime-neutral platform controller** that can reconcile observability policy across Kubernetes and AWS ECS/Fargate.

The purpose of this POC is to validate the architecture, not to build a production observability platform.

## Core architectural principle

> **Events trigger reconciliation; authoritative APIs determine state. Reconciliation is periodic and idempotent, so correctness does not depend on receiving every event.**

The controller must separate:

1. Desired state / policy
2. Change notification
3. Authoritative state observation
4. Reconciliation
5. Runtime-specific actions

Do NOT build the system around EventBridge or Kubernetes watches directly. Those are event sources that wake the same reconciliation engine.

## Goals

A single controller application with: a common reconciliation engine; a runtime adapter abstraction; a Kubernetes adapter; an ECS adapter; a generic work queue; event-driven reconciliation; periodic reconciliation; idempotent reconciliation; structured reconciliation status; a simple ObservabilityPolicy model; clear separation between controller logic and runtime APIs.

The same controller binary should run against either Kubernetes or ECS. Runtime is determined from configuration, explicit configuration preferred over automatic detection.

## Non-goals

Full OpenTelemetry collection, eBPF, Alertmanager, Grafana, VictoriaMetrics, Tempo, ClickHouse, production IAM automation, automatic modification of production ECS task definitions, full CRD generation (unless trivial), a complete platform API, distributed controller cluster, HA, auth beyond what's necessary.

## Technology

Go, standard library preferred, lightweight deps. client-go where useful, AWS SDK for Go v2, structured logging (log/slog), unit tests, Dockerfile, local dev instructions.

## Architecture

```text
Kubernetes Watch ───────┐
ECS EventBridge ────────┤
Periodic Resync ────────├──► Work Queue ───► Reconcile ──► RuntimeAdapter (k8s | ecs) ──► runtime APIs
Policy Change ──────────┘
```

**Notification does not contain authoritative state.** EventBridge event → enqueue ECS service → reconcile → DescribeServices / DescribeTaskDefinition → determine actual state → compare desired → act if required. Never treat the event payload as the source of truth.

## 1. Policy model

```yaml
apiVersion: observability.platform.example/v1alpha1
kind: ObservabilityPolicy
metadata:
  name: payments-api
target:
  runtime: ecs
  account: "123456789012"
  region: ap-southeast-2
  cluster: production
  service: payments-api
spec:
  profile: standard
  telemetry:
    logs: {enabled: true}
    metrics: {enabled: true}
    traces: {enabled: true}
  alerts:
    profile: standard
```

Kubernetes target:

```yaml
target:
  runtime: kubernetes
  namespace: payments
  kind: Deployment
  name: payments-api
```

Policy is runtime-neutral beyond the target reference.

## 2. Runtime adapter interface

```go
type RuntimeAdapter interface {
    Runtime() RuntimeType
    ResolveTarget(ctx context.Context, target TargetRef) (Workload, error)
    Observe(ctx context.Context, workload Workload) (ObservedState, error)
    Reconcile(ctx context.Context, desired DesiredState, observed ObservedState) (ReconcileResult, error)
}
```

The reconciliation engine must not contain ECS or Kubernetes API calls.

## 3. Canonical workload identity

ECS: account, region, environment, service, deployment, workload, runtime=ecs, runtimeID=arn:aws:ecs:ap-southeast-2:123456789012:service/production/payments-api
Kubernetes: environment, service, deployment, workload, runtime=kubernetes, runtimeID=payments/payments-api

The rest of the platform should not need to know whether the workload is a Deployment or ECS Service.

## 4. Work queue

Keys like `ECSService:arn:aws:ecs:...`, `KubernetesDeployment:payments/payments-api`, `ObservabilityPolicy:payments-api`. Provide dedup, retry, backoff, structured logging, graceful shutdown. In-memory only.

## 5. Event sources

- Kubernetes: watch/informer on Deployments → enqueue workload. Never reconcile directly.
- ECS: EventBridge consumer abstraction. HTTP endpoint accepting EventBridge-compatible events and/or simulated source. Handler only enqueues. Example:
  `{"source":"aws.ecs","detail-type":"ECS Deployment State Change","detail":{"clusterArn":"...","serviceArn":"..."}}`
- Periodic resync: configurable (`reconciliation.resyncInterval = 5m`), enqueues known resources.

## 6. Reconciliation lifecycle

Receive queue item → resolve policy → resolve runtime target → read authoritative current state → calculate desired → compare → apply → re-read if appropriate → record status. Must be idempotent.

## 7. POC action

Simulated. Desired logs.enabled=true, observed false → action "enable_logs". Log:
`{"resource":"payments-api","runtime":"ecs","action":"enable_logs","reason":"desired state differs from observed state"}`
Kubernetes may optionally apply a harmless annotation `observability.platform.example/reconciled: "true"`. ECS stays simulated.

## 8. Reconciliation status

```yaml
status:
  phase: Ready
  observedGeneration: 1
  conditions:
    - {type: TargetResolved, status: "True"}
    - {type: ConfigurationCompliant, status: "True"}
    - {type: Reconciled, status: "True"}
  lastReconciledAt: "..."
```
Runtime-neutral model.

## 9. Runtime detection

`runtime.type: auto | kubernetes | ecs`. Explicit wins. Auto uses env signals (e.g. KUBERNETES_SERVICE_HOST, ECS_CONTAINER_METADATA_URI_V4). Happens once at startup.

## 10. ECS adapter

Resolve cluster/service; retrieve service; retrieve task definition; determine deployment/task-def revision; convert to canonical identity; produce ObservedState; compute compliance. ECS Service → Task Definition → Containers → Logging/telemetry config → ObservedState. Adapter does not know how alert rules are evaluated.

## 11. Kubernetes adapter

Resolve Deployment; observe Deployment and relevant Pods; canonical identity; ObservedState; reconcile. K8s API interaction stays in adapter.

## 12. Demonstration scenarios (must be reproducible)

- A — initial reconciliation: policy logs enabled, observed disabled → drift detected, simulated remediation.
- B — duplicate event: same event twice → first reconciles, second determines state already correct, no action.
- C — missed event: modify simulated observed state without event, wait for periodic resync → drift detected → reconcile.
- D — different runtime: same policy/reconciliation logic against k8s and ECS adapters; core engine unchanged.

## 13. Testing

Unit tests: policy parsing, target resolution, canonical identity, desired/actual comparison, idempotent reconciliation, queue behaviour, retry behaviour, runtime adapter selection. Adapter tests with mocks/fakes. Minimum:
desired==observed → no action; desired!=observed → action; same reconcile twice → second no-op; event received → enqueued; no event → periodic resync still reconciles.

## 14. Repository structure (approximate)

```
cmd/controller/
internal/controller/{reconcile.go,queue.go,status.go}
internal/policy/{model.go,parser.go}
internal/runtime/{adapter.go,identity.go,kubernetes/,ecs/}
internal/events/{source.go,kubernetes/,eventbridge/}
internal/state/{desired.go,observed.go,diff.go}
internal/config/config.go
examples/{ecs-policy.yaml,kubernetes-policy.yaml}
docs/architecture.md
Dockerfile
README.md
```

## 15. docs/architecture.md

Mermaid diagrams for controller architecture and reconciliation lifecycle. State explicitly:
> Event sources are not authoritative state. They only trigger reconciliation. The runtime adapter reads authoritative state before deciding what action is required.

## 16. Design constraints

Do not introduce: separate controller for ECS; separate engine for k8s; event-specific business logic; direct ECS/k8s API calls from the policy engine; assumptions EventBridge events are complete; assumptions k8s watches are permanent; a requirement that every runtime exposes identical capabilities. Use capability-oriented interfaces:

```go
type Capabilities struct {
    WorkloadDiscovery   bool
    TelemetryConfig     bool
    HostTelemetry       bool
    NetworkTelemetry    bool
    AlertReconciliation bool
}
```
(ECS Fargate lacks host-level capabilities of an EC2-backed k8s node.)

## 17. Definition of success

1. One controller architecture supports Kubernetes and ECS.
2. Kubernetes watches and EventBridge events feed the same work queue.
3. Neither event source is treated as authoritative state.
4. The runtime adapter reads authoritative state.
5. Reconciliation is idempotent.
6. Periodic reconciliation catches missed changes.
7. Runtime-specific API logic remains inside adapters.
8. Canonical workload identity is runtime-independent.
9. The same ObservabilityPolicy model works across both runtimes.

---

## 18. Primary intent: centrally managed alerting (added 2026-10-02)

The controller's main purpose is **alert management**. Deploy the controller; it discovers workloads on any runtime and reconciles a **default alert set** for each (aligned to the golden signals: latency, traffic, errors, saturation) into an **alert backend**. New Relic is the first backend; the backend must be swappable (e.g. Prometheus/Alertmanager, Datadog, Grafana) via configuration.

Business goal: centrally manage the alerting integration and support migrations. If the business moves off New Relic, 500 teams should not have to change Terraform — the platform team switches the backend adapter and the controller reconciles the same policies into the new engine.

Two independent adapter axes:

```text
                 ObservabilityPolicy (alerts.profile: standard)
                              │
                      Reconciliation Engine
                     ┌────────┴─────────┐
             RuntimeAdapter         AlertBackend
          (where workloads run)   (where alerts live)
           k8s | ecs | ...       newrelic | fake | prometheus | ...
```

- **Alert profiles** (`standard`, etc.) expand into runtime-neutral `AlertRule`s: signal (latency/traffic/errors/saturation), threshold, window, severity. Profiles are defined centrally; a policy may override thresholds later (not required for POC).
- **Desired alert set** = profile rules × canonical workload identity. Rules requiring capabilities the runtime lacks are reported as unsupported, not failed.
- **AlertBackend interface**: list managed rules for a workload (authoritative read), create/update/delete rules. The backend translates runtime-neutral rules into backend-specific definitions (NRQL conditions for New Relic), using the canonical identity (and runtime type only where telemetry source differs, e.g. K8sContainerSample vs ECS container metrics).
- **Ownership**: every backend-managed rule is tagged (managed-by=portable-reconciler, policy, workload runtimeID, rule id) so the controller only touches its own alerts and can find them again. The backend is observed as authoritative state, same principle as runtimes — never assume a previous create succeeded.
- **Idempotency**: reconciling twice yields no backend changes; drift (rule edited/deleted in NR UI) is corrected on the next resync.
- **Migration**: switching `alerting.backend` reconciles the same desired set into the new backend. Cleanup of the old backend is an explicit, opt-in mode (POC: documented, optional).
- **POC scope**: New Relic adapter builds NerdGraph mutations/NRQL and runs in **dry-run** by default (logs what it would send, keeps an in-memory record so idempotency works). Real NerdGraph calls only when an API key + account ID are configured and `alerting.newrelic.dryRun=false`. An in-memory `fake` backend is used for tests and the demo.
- Status gains an `AlertsReconciled` condition and a count of managed rules.

Additional demo scenario E — backend migration: the same policies reconciled into backend A, then config switched to backend B; B ends up with the equivalent rule set, no policy changes.
