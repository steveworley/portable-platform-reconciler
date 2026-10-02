# Portable Platform Reconciler (POC)

A runtime-neutral platform controller that **centrally reconciles a golden-signal alert set for every workload into a swappable alert backend**. New Relic is the first backend.

The business case: if the company moves off New Relic, 500 teams should not have to change Terraform. Teams declare an `ObservabilityPolicy` once. The platform team changes `alerting.backend` and the controller reconciles the same policies into the new engine.

```text
                 ObservabilityPolicy (alerts.profile: standard)
                              |
                      Reconciliation Engine
                     +--------+---------+
             RuntimeAdapter         AlertBackend
          (where workloads run)   (where alerts live)
           kubernetes | ecs         newrelic | memory | (prometheus, datadog, ...)
```

The same binary runs against Kubernetes or ECS/Fargate. The two adapter axes (runtime and alert backend) are independent and are selected by configuration.

> **Events trigger reconciliation; authoritative APIs determine state. Reconciliation is periodic and idempotent, so correctness does not depend on receiving every event.**

Kubernetes watches, EventBridge events, policy file changes and a periodic resync all feed one work queue. None of them is treated as state: the runtime adapter and the alert backend are always re-read before anything is changed.

Design docs: [docs/architecture.md](docs/architecture.md), [docs/extending.md](docs/extending.md) (adding a runtime or an alert backend), [docs/poc-spec.md](docs/poc-spec.md) (the spec).

## Quick start

No cluster, AWS account or New Relic account is needed.

```sh
make demo            # or: go run ./cmd/demo
go run ./cmd/demo --scenario E     # one scenario (A|B|C|D|E|F|all)
go run ./cmd/demo -v               # include controller info logs
```

The demo runs the real engine, adapters and event sources against fakes (simulated ECS, `client-go` fake clientset, in-memory alert backends), prints what happens, checks its own expectations and exits non-zero if any fail.

| Scenario | Shows |
|---|---|
| A | Initial reconciliation (ECS): logging drift detected, simulated remediation (new task definition revision), 5 golden-signal alert rules created, host CPU reported **Unsupported** on Fargate, status in the spec section 8 shape |
| B | Duplicate EventBridge event posted twice: both reconciles are no-ops, zero backend writes, zero remediation calls; `Describe*` calls prove state is read, not taken from the payload |
| C | Missed event: logging disabled and an alert threshold edited out-of-band with no event; the next resync fixes both (1 alert update) |
| D | Same policy spec on Kubernetes via the real informer: annotations applied, same rules plus host CPU (supported on nodes); engine code unchanged |
| E | Backend migration: same policies reconciled into backend B (identical rule IDs and fingerprints), New Relic dry-run NRQL shown, `Prune` dry-run then real prune of the old backend |
| F | Policy deleted: the finalizer removes its alert rules |

## Running the controller

```sh
go build -o bin/controller ./cmd/controller      # or: make build
bin/controller --config examples/config/ecs-simulated.yaml --log-format text
```

Subcommands: `run` (default), `prune`, `version`. Flags: `--config path` (empty = defaults), `--log-format json|text`, `--log-level debug|info|warn|error`.

### ECS (simulated, no AWS)

```sh
go run ./cmd/controller --config examples/config/ecs-simulated.yaml --log-format text

# in another shell: send an EventBridge-shaped event
curl -XPOST localhost:8080/events -d '{
  "source":"aws.ecs","detail-type":"ECS Deployment State Change",
  "detail":{"serviceArn":"arn:aws:ecs:ap-southeast-2:123456789012:service/production/payments-api"}}'
curl localhost:8081/status
curl localhost:8081/status/payments-api
curl localhost:8081/healthz
```

With `ecs.mode: aws` the adapter reads real ECS with the default AWS credential chain. It only reads; remediation is logged, not applied.

### Kubernetes

```sh
go run ./cmd/controller --config examples/config/kubernetes.yaml --log-format text
```

Uses in-cluster config, else `kubernetes.kubeconfig` / `$KUBECONFIG` / `~/.kube/config`. With `kubernetes.applyAnnotations: true` it patches `observability.platform.example/{logs,metrics,traces,reconciled}` annotations on the Deployment; with `false` it only reports drift. Example manifests are in `deploy/kubernetes/` (examples only; nothing applies them).

### Event sources

All of these call `engine.Enqueue` and nothing else:

| Source | Active | Trigger |
|---|---|---|
| resync | always | every `reconciliation.resyncInterval`, enqueues every policy |
| policyfile | always | policy files in `policies.dir` added, changed or removed |
| kubernetes informer | runtime is `kubernetes` | Deployment add/update/delete |
| eventbridge HTTP | runtime is `ecs` | `POST /events` on `eventbridge.listenAddr` |

Shutdown on SIGINT/SIGTERM cancels the sources and drains in-flight reconciles.

### Status endpoint

If `status.listenAddr` is set: `GET /status` (JSON list of policy statuses), `GET /status/{name}`, `GET /healthz`. Each status has `phase`, `conditions` (`TargetResolved`, `CapabilitiesSatisfied`, `ConfigurationCompliant`, `AlertsReconciled`, `Reconciled`), the canonical workload identity, last actions and `details` (`alerts.backend`, `alerts.managedRules`, `alerts.profile`).

## Configuration reference

YAML, decoded strictly (unknown keys are rejected). Defaults apply for anything omitted.

| Key | Default | Meaning |
|---|---|---|
| `runtime.type` | `auto` | `kubernetes`, `ecs` or `auto` (detects from `KUBERNETES_SERVICE_HOST` / `ECS_CONTAINER_METADATA_URI_V4`). Explicit wins. Env `RECONCILER_RUNTIME` overrides. Selected once at startup |
| `reconciliation.resyncInterval` | `5m` | Go duration for the periodic resync |
| `reconciliation.workers` | `2` | Concurrent reconcile workers |
| `reconciliation.maxRetries` | `5` | Retries (with backoff) before a key is dropped until the next event or resync |
| `policies.dir` | `./examples` | Directory of `ObservabilityPolicy` YAML files (`*.yaml`, `*.yml`) |
| `kubernetes.kubeconfig` | empty | Kubeconfig path; empty = in-cluster, then default loading rules |
| `kubernetes.applyAnnotations` | `false` | Apply the annotation remediation; `false` is report-only |
| `ecs.mode` | `simulated` | `simulated` (seeded in-memory ECS) or `aws` |
| `ecs.region` | empty | AWS region for `aws` mode |
| `eventbridge.listenAddr` | `:8080` | EventBridge HTTP source (ECS runtime only) |
| `status.listenAddr` | empty | Status server; empty disables it |
| `alerting.enabled` | `true` | Reconcile alert rules |
| `alerting.backend` | `newrelic` | Registered backend name: `newrelic`, `memory` |
| `alerting.newrelic.dryRun` | `true` | `true` = log NerdGraph requests, keep an in-memory shadow, never touch the network |
| `alerting.newrelic.accountId` | `0` | New Relic account ID (required when live) |
| `alerting.newrelic.policyId` | `0` | Existing alert policy that holds the conditions (required when live) |
| `alerting.newrelic.region` | `US` | `US` or `EU` |
| `alerting.newrelic.apiKeyEnv` | `NEW_RELIC_API_KEY` | Name of the env var holding the API key. The key is never in the config file |

Examples: `examples/config/ecs-simulated.yaml`, `examples/config/kubernetes.yaml`, `examples/config/newrelic-live.yaml`.

## Alerts: how they work

- A policy says `alerts.profile: standard`. Centrally defined profiles expand into runtime-neutral rules for the golden signals: latency (p95), traffic, errors and saturation (container CPU and memory, host CPU).
- Desired alert set = profile rules x canonical workload identity. A rule that needs a capability the runtime lacks (host CPU on Fargate) is reported as **unsupported**, not failed.
- Every backend rule is tagged `managed-by=portable-reconciler`, `policy`, `workload`, `runtime`, `rule-id`, so the controller only touches its own rules and can find them again.
- The backend is read as authoritative state on every reconcile. A second reconcile makes no writes; a rule edited or deleted in the vendor UI is corrected on the next resync.

### New Relic: dry-run vs live

- **Dry-run (default, `dryRun: true`)**: the backend builds the exact NerdGraph mutations and NRQL, logs them, and applies them to an in-memory shadow store so idempotency still works. Nothing is sent.
- **Live (`dryRun: false`)**: requires `accountId`, `policyId` and the API key in the env var named by `apiKeyEnv`; construction fails otherwise. Region selects the US or EU NerdGraph endpoint. Start from `examples/config/newrelic-live.yaml` and try a non-production account first.

### Migration and prune workflow

1. Add the new backend (implementation plus registration) and set `alerting.backend` to it. The controller reconciles the same policies into the new engine. Teams change nothing.
2. Verify the new backend's rules (scenario E shows identical rule IDs and fingerprints).
3. Clean up the old backend, which is explicit and opt-in:

```sh
# Dry run (default): list what would be deleted from the old backend
controller prune --config cfg.yaml --backend newrelic --keep-none
# Really delete every rule this controller manages there
controller prune --config cfg.yaml --backend newrelic --keep-none --dry-run=false
# Orphan sweep: keep rules whose policy still exists in policies.dir, delete the rest
controller prune --config cfg.yaml --backend newrelic --orphans --dry-run=false
```

Only rules carrying this controller's ownership tags are ever listed, so hand-made alerts are never touched. Note that the process builds the named backend from the config: the `memory` backend is per-process and always empty, and `newrelic` in dry-run only sees its own shadow store. Pruning real New Relic conditions needs a live config.

## Project layout

```text
cmd/controller/            the controller binary (run, prune, version)
cmd/demo/                  in-process scenario demo and end-to-end test
internal/config/           config loading, defaults, validation
internal/policy/           ObservabilityPolicy model and parser
internal/state/            desired/observed state, diff, canonical identity
internal/controller/       engine, work queue, status store, reconcile steps
internal/runtime/          RuntimeAdapter, capabilities, registry
internal/runtime/{kubernetes,ecs}/   adapters (ECS includes the simulation)
internal/events/           event sources: resync, policyfile, kubernetes, eventbridge
internal/alerting/         profiles, rules, plan, Step, Prune, backend registry
internal/alerting/{memory,newrelic}/ alert backends
examples/                  sample policies and configs
deploy/                    example Kubernetes manifests and ECS task definition (not applied)
docs/                      spec, architecture, extending
```

## Build, test, container

```sh
make build        # bin/controller and bin/demo
make test         # go test -race -count=1 ./... (includes the scenario demo)
make vet
make lint         # gofmt check
make docker-build # distroless static, nonroot; entrypoint /controller
docker run --rm --entrypoint /demo portable-platform-reconciler:dev --examples /examples
```

`go test -short ./...` skips the multi-second scenario demo test.

## POC limitations

- Policy state, controller status and (for `memory` and dry-run New Relic) alert state are **in memory**. A restart starts from the policy files and re-reads backends.
- No HA and no leader election. Run exactly one replica.
- **ECS remediation is simulated** (in `simulated` mode) or only logged (`aws` mode). The controller never modifies real task definitions.
- **New Relic NRQL and attribute names are unvalidated** against a live account. Treat them as a starting point and check the conditions in a test account before going live.
- Finalizers rely on the in-memory status, so a policy **deleted while the controller is down is not finalized**. Use `prune --orphans` as the sweep.
- Policies are read from files only; there is no CRD or API server for policies.
- One alert policy ID per New Relic config, and thresholds come from the central profile (no per-policy overrides yet).
