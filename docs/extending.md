# Extending the controller

Guides for the four common extensions. Read [architecture.md](architecture.md) first for the model. Paths are relative to the repository root; skeletons use the real interfaces and registries, with the module path `github.com/example/portable-platform-reconciler`.

## 1. Adding a runtime (for example Nomad or Cloud Run)

No change is needed in `internal/controller`, `internal/state`, `internal/alerting` or the queue. A runtime is one adapter package, one event source, and one blank import in the binary.

### Steps

1. **Decide the target shape.** `policy.TargetRef` has explicit fields for AWS-style and namespaced runtimes plus `Attributes map[string]string` for anything else. A Nomad target can use `Attributes["job"]` and `Namespace`.
2. **Create `internal/runtime/<name>/adapter.go`** implementing `runtime.RuntimeAdapter`:
   - `Runtime()` returns a `runtime.RuntimeType` constant (an open string).
   - `Capabilities()` returns the runtime-wide defaults. Set only what is true. Override per workload through `Workload.Capabilities` when it varies (as ECS does for Fargate vs EC2).
   - `QueueKey(target)` returns `<Prefix>:<runtimeID>`. The event source must call the same helper.
   - `ResolveTarget` reads the authoritative API, returns a canonical `state.WorkloadIdentity`, and wraps `runtime.ErrNotFound` when the workload is absent.
   - `Observe` re-reads the API. It must not use `Workload.Raw` or any event data for state.
   - `Reconcile` applies the engine-computed `[]state.Action`. Return `DryRun: true` when you deliberately change nothing.
3. **Register from `init()`** with a detector and a target validator.
4. **Write the key helper** (like `ecs.ServiceKey` and `kubernetes.DeploymentKey`).
5. **Write an event source** implementing `events.Source`. It only computes keys and calls `Enqueue`.
6. **Blank-import** the adapter package in `cmd/` so `init()` runs. The runtime is then selectable with `runtime.type: <name>`, or by auto-detection if the detector matches.

### Skeleton

```go
package nomad

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

const RuntimeType runtime.RuntimeType = "nomad"
const KeyPrefix = "NomadJob:"

// JobKey is shared by Adapter.QueueKey and the event source.
func JobKey(namespace, job string) string { return KeyPrefix + namespace + "/" + job }

func init() {
	runtime.Register(RuntimeType, factory,
		runtime.EnvSet("NOMAD_ALLOC_ID"), // detector, may be nil (explicit selection only)
		runtime.WithTargetValidator(ValidateTarget),
	)
}

func factory(cfg config.Config, logger *slog.Logger) (runtime.RuntimeAdapter, error) {
	// Build the API client from cfg. Add a config section in internal/config if needed.
	return New(newClient(cfg), logger), nil
}

// ValidateTarget runs at policy load time through policy.WithTargetValidator(runtime.ValidateTarget).
func ValidateTarget(t policy.TargetRef) error {
	if t.Attributes["job"] == "" {
		return fmt.Errorf("nomad target requires attributes.job")
	}
	return nil
}

type Adapter struct { /* client, logger */ }

var _ runtime.RuntimeAdapter = (*Adapter)(nil)

func (a *Adapter) Runtime() runtime.RuntimeType { return RuntimeType }

func (a *Adapter) Capabilities() runtime.Capabilities {
	return runtime.Capabilities{WorkloadDiscovery: true, TelemetryConfig: true, AlertReconciliation: true}
}

func (a *Adapter) QueueKey(t policy.TargetRef) (string, error) {
	if err := ValidateTarget(t); err != nil {
		return "", err
	}
	return JobKey(t.Namespace, t.Attributes["job"]), nil
}

func (a *Adapter) ResolveTarget(ctx context.Context, t policy.TargetRef) (runtime.Workload, error) {
	job, err := a.client.Job(ctx, t.Namespace, t.Attributes["job"])
	if err != nil {
		if isNotFound(err) {
			return runtime.Workload{}, fmt.Errorf("job %s: %w", t.Attributes["job"], runtime.ErrNotFound)
		}
		return runtime.Workload{}, err
	}
	return runtime.Workload{
		Identity: state.WorkloadIdentity{
			Environment: t.Environment,
			Service:     job.Name,
			Deployment:  job.Name,
			Workload:    job.Name,
			Runtime:     string(RuntimeType),
			RuntimeID:   t.Namespace + "/" + job.Name,
		},
		Ref:      t,
		Revision: fmt.Sprint(job.Version),
	}, nil
}

func (a *Adapter) Observe(ctx context.Context, wl runtime.Workload) (state.ObservedState, error) {
	// Re-read from the API using wl.Ref. Derive Logs, Metrics, Traces from the real config.
	return state.ObservedState{Workload: wl.Identity /* , Logs: ..., Metrics: ..., Traces: ... */}, nil
}

func (a *Adapter) Reconcile(ctx context.Context, desired state.DesiredState, observed state.ObservedState, actions []state.Action) (state.ReconcileResult, error) {
	// Apply actions idempotently. Return DryRun: true if you intentionally change nothing.
	return state.ReconcileResult{Applied: actions, Changed: true}, nil
}
```

### Event source skeleton

```go
package nomadevents

import (
	"context"

	"github.com/example/portable-platform-reconciler/internal/events"
	"github.com/example/portable-platform-reconciler/internal/runtime/nomad"
)

type Source struct{ /* client */ }

var _ events.Source = (*Source)(nil)

func (s *Source) Name() string { return "nomad" }

// Start blocks until ctx is done and then returns nil.
func (s *Source) Start(ctx context.Context, q events.Enqueuer) error {
	for ev := range s.stream(ctx) {
		// Extract identifiers only. Never copy state out of the event.
		q.Enqueue(nomad.JobKey(ev.Namespace, ev.JobID))
	}
	return nil
}
```

Wiring: the binary starts each source with `Start(ctx, engine)`. `controller.Engine` satisfies `events.Enqueuer`. Keep the periodic `resync.Source` running; it is what makes a lossy event stream safe.

### Tests to write

Use `internal/runtime/fake/fake.go` as the reference for a minimal adapter, and `internal/runtime/ecs/adapter_test.go` for an adapter over a mockable client interface (`ecs.API`).

- Registration: `Select` with `runtime.type` set explicitly, and with the detector under auto (see `TestRegisterThirdRuntime` and `TestSelect` in `internal/runtime/runtime_test.go`). Duplicate registration panics.
- `ValidateTarget` table test with valid and invalid targets.
- `QueueKey` equals the key produced by the event source for the same workload (see `TestKeysMatchAdapterQueueKey` in `internal/events/eventbridge`).
- `ResolveTarget`: found, not found (`errors.Is(err, runtime.ErrNotFound)`), API error, identity fields.
- `Observe` reads from the API and ignores `Workload.Raw` (see `TestObserveIgnoresRaw`).
- `Capabilities` and any per-workload override.
- `Reconcile`: applies actions, is a no-op when already compliant, reports `DryRun` when it does not change anything.
- Event source: an event produces an enqueued key, a duplicate enqueues again (the queue dedupes), shutdown on context cancel returns nil.
- Engine-level checks already exist and should keep passing without modification (`internal/controller/reconcile_test.go`). If you needed to edit the engine, the abstraction leaked; fix the adapter instead.

## 2. Adding an alert backend (Alertmanager, Grafana, Datadog)

Implement `alerting.Backend` in a new package under `internal/alerting/<name>/`. Optionally implement `alerting.Lister` so `alerting.Prune` can clean the backend. The engine, the step, profiles and runtimes do not change.

### Contract

```go
type Backend interface {
	Name() string
	List(ctx context.Context, owner alerting.Owner) ([]alerting.ManagedRule, error)
	Create(ctx context.Context, r alerting.ManagedRule) error
	Update(ctx context.Context, r alerting.ManagedRule) error
	Delete(ctx context.Context, r alerting.ManagedRule) error
}

type Lister interface {
	ListAll(ctx context.Context) ([]alerting.ManagedRule, error)
}
```

Rules the backend must follow:

- `List` is the authoritative read. It must return only rules tagged as managed by this controller for the given owner. It must be accurate after every write, because `Step.Reconcile` re-lists to verify convergence.
- **Ownership tags.** Persist `ManagedRule.Tags()` with each rule (`managed-by`, `policy`, `workload`, `runtime`, `rule-id`, `fingerprint`). In `List`, rebuild the owner with `alerting.OwnerFromTags`. Skip anything that is not managed. Use native labels or tags where the engine has them. If it does not, encode them in a description or annotation as `internal/alerting/newrelic/description.go` does.
- **Fingerprint round trip.** The plan compares `ManagedRule.Fingerprint()` of the desired rule with the one computed from the rule you return from `List`. Rebuild the neutral `Rule` fields (threshold, operator, window, severity, name, description, identity) from what is stored. If the stored definition differs from what you would have written (someone edited the query by hand), set `ManagedRule.Divergent = true` so the fingerprint mismatches and the rule is updated. The New Relic `ruleFromCondition` shows this.
- **Translation.** Convert runtime-neutral `Rule` fields into the engine's definition inside your package only. Use the canonical `Identity` for selectors. Branch on `Identity.Runtime` only where the telemetry source genuinely differs (New Relic does this for container samples). Return an error for a rule you cannot express; it surfaces as `BackendFailed`.
- **Idempotent writes.** `Update` and `Delete` find the existing object by owner and rule ID, not by an ID cached in memory. A restart must not lose track of rules.
- **Dry run.** Follow the New Relic approach: put the transport behind a small interface (`newrelic.Client`), provide a real implementation and a recording implementation (`DryRunClient`) that logs writes and keeps an in-memory store so list, create, update and delete behave consistently. Make dry run the default in config and require explicit settings (and fail at construction) to send real requests.
- **Pruning.** `ListAll` returns every rule tagged as managed regardless of owner, never unmanaged ones.

### Skeleton

```go
package grafana

import (
	"context"
	"log/slog"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/config"
)

const BackendName = "grafana"

func init() {
	alerting.Register(BackendName, func(cfg config.Config, logger *slog.Logger) (alerting.Backend, error) {
		return New(cfg.Alerting.Grafana, logger) // add GrafanaConfig to internal/config.AlertingConfig
	})
}

type Backend struct{ client Client }

var (
	_ alerting.Backend = (*Backend)(nil)
	_ alerting.Lister  = (*Backend)(nil)
)

func (b *Backend) Name() string { return BackendName }

func (b *Backend) List(ctx context.Context, owner alerting.Owner) ([]alerting.ManagedRule, error) {
	all, err := b.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	var out []alerting.ManagedRule
	for _, r := range all {
		if r.Owner == owner {
			out = append(out, r)
		}
	}
	return out, nil
}

func (b *Backend) ListAll(ctx context.Context) ([]alerting.ManagedRule, error) {
	// Query the engine, keep only objects where alerting.OwnerFromTags(labels) reports ok,
	// rebuild ManagedRule (set Divergent when the stored definition was edited).
	return nil, nil
}

func (b *Backend) Create(ctx context.Context, r alerting.ManagedRule) error { /* translate, write with r.Tags() */ return nil }
func (b *Backend) Update(ctx context.Context, r alerting.ManagedRule) error { /* find by owner and r.ID */ return nil }
func (b *Backend) Delete(ctx context.Context, r alerting.ManagedRule) error { /* find by owner and r.ID */ return nil }
```

Then:

1. Add a config struct for the backend to `internal/config.AlertingConfig` (secrets by environment variable name, as `NewRelicConfig.APIKeyEnv` does), plus validation.
2. Blank-import the package in `cmd/`.
3. Select it with `alerting.backend: grafana`. `alerting.New` returns `ErrUnknownBackend` listing registered names if the name is wrong.

### Tests

Copy the approach in `internal/alerting/newrelic/newrelic_test.go` and `internal/alerting/memory/memory_test.go`.

- **Golden translation tests.** Table of (identity, rule ID) to expected query string, for both runtimes (`TestNRQLGolden`). Golden strings catch accidental changes to what gets deployed to the alert engine.
- **Description or tag round trip** (`TestDescriptionRoundTrip`) and a foreign object that must be ignored (`TestDecodeDescriptionForeign`, `TestListIgnoresUnmanagedConditions`).
- **Idempotency and drift against the recording client** (`TestDryRunIdempotentDriftAndCleanup`): reconcile twice and assert no writes on the second run, edit a stored rule and assert an update, delete one and assert a create, remove a rule from the profile and assert a delete.
- **Request shape** (`TestBackendRequestShape`), HTTP client error handling, construction validation (`TestNewValidation`) and registration with dry run default (`TestRegisteredAsNewRelicDryRunByDefault`).
- **Prune** never touches unmanaged objects (`TestListAllAndPruneNeverTouchUnmanaged`).
- A migration test using two backends (`TestMigrationBetweenBackends` in `internal/alerting/step_test.go` uses `memory.New(memory.WithName(...))`).

## 3. Adding or changing an alert profile

Profiles are defined centrally in `internal/alerting/profile.go`. A profile is a `Profile{Name, Rules []RuleTemplate}` registered in `DefaultProfiles`. A policy selects one with `spec.alerts.profile`.

### Adding a rule

1. Add a `Rule` value with a stable, unique `ID` (`golden.<signal>.<name>`). The ID is part of the ownership tags and of the backend lookup key, so do not rename it later; a rename deletes the old rule and creates a new one.
2. Fill in `Signal` (`SignalLatency`, `SignalTraffic`, `SignalErrors`, `SignalSaturation`), `Aggregation`, `Operator`, `Threshold`, `Unit`, `Window`, `Severity`, and for saturation `Scope` and `Resource`.
3. Add it to the `Rules` list of the profiles that should have it, or register a new `Profile` in the `DefaultProfiles` initialiser.
4. Teach each backend to translate it. For New Relic that is `nrql` / `saturationNRQL` in `internal/alerting/newrelic/translate.go`. A rule a backend cannot express returns an error, which surfaces as `BackendFailed`, so update every registered backend in the same change.
5. Add golden tests (`TestNRQLGolden`) and a case in `TestDesiredRules`.

### Capability gating

Set what the rule depends on:

- `RequiresCapability`: a `state.Cap*` value. If `runtime.Capabilities.Supports` returns false (using the workload's effective capabilities), `Profiles.Desired` omits the rule and reports `alert <id>: runtime lacks capability <cap>`. Example: `golden.saturation.host.cpu` requires `state.CapHostTelemetry`, so it is created for Kubernetes and ECS on EC2 and reported as unsupported for Fargate. The phase becomes `Degraded` and the other rules are still reconciled.
- `RequiresTelemetry`: a list of `TelemetryMetrics` and `TelemetryTraces`. At least one must be enabled in the policy's `spec.telemetry`, otherwise the rule is unsupported.

A new capability needs a `state.Capability` constant, a field on `runtime.Capabilities`, and a case in `Capabilities.Supports`. Adapters then set it.

### Changing thresholds of an existing rule

Edit the template. Fingerprints change, so the next reconcile of every workload produces an `update_alert_rule` action and the backend is updated. Roll this out knowing it touches every workload's alerts at the next resync. Changing a profile in a backend that stores thresholds in a translated form must keep `List` able to decode the stored values (the New Relic backend converts thresholds with `toNRThreshold` and `fromNRThreshold`).

Removing a rule from a profile deletes it from the backend on the next reconcile, because anything owned and not desired is planned for deletion (`TestStepProfileChangeDeletesStaleRules`).

Per-policy threshold overrides are not implemented. They would apply when expanding templates in `Profiles.Desired`.

## 4. Runbook: migrating alert backends

Scenario: move from backend A (for example `newrelic`) to backend B (for example `grafana`). No policy file changes. The controller is deployed once per runtime environment, so repeat per environment.

### Before

1. Confirm backend B is implemented, registered, and blank-imported in the binary. Check `alerting.Names()` output or the startup error if the name is unknown.
2. Configure B's credentials in the environment and its settings in the controller config. Keep B in dry run (if the backend supports it) first.
3. Record the current state: `alerts.managedRules` in each policy's status and the rule count in A.

### Option 1: stage in dry run, then switch

1. Deploy the controller with `alerting.backend: B` and B's dry-run setting on. The controller logs every create it would send (`alert rule write` entries with `backend`, `action`, `rule`). Review the volume and rule IDs against the count in A.
2. Turn B's dry run off. On the next resync every policy's rules are created in B.

### Option 2: run both for a period

The controller has one `alerting.backend` per instance. To run A and B in parallel, deploy a second controller instance for the same environment configured with backend B and the same policies, leaving the first on A. Both read the same workloads and write to different engines. Be aware of duplicate notifications if both engines page the same people, and of double the API load on the runtime.

### Verify

1. Policy statuses show `AlertsReconciled=True` and `alerts.backend: B` with `alerts.managedRules` equal to the previous count, minus rules that B reports as unsupported.
2. A second resync produces no `alert rule write` log lines (idempotent).
3. In B, rules carry the ownership tags and match the expected set per workload.
4. Edit or delete one rule in B manually, wait for a resync, and confirm it is corrected.

### Prune A (explicit and opt-in)

Switching the backend does not touch A. Its rules stay until pruned. `alerting.Prune(ctx, backendA, keep, dryRun)` is the function that does it. A needs to implement `alerting.Lister`; otherwise it returns `ErrPruneUnsupported`.

Use the `prune` subcommand of the controller binary (it wraps `alerting.Prune` in `internal/alerting/prune.go`). `--dry-run` defaults to `true`.

1. Dry run, keeping nothing (empties A of rules the controller created). Review the report; only rules tagged `managed-by=portable-reconciler` appear, unmanaged rules in A are never listed.

   ```sh
   controller prune --config cfg-backend-a.yaml --backend <A> --keep-none
   ```

2. Run for real. Deletion continues past individual failures; the returned error joins them and the report lists only the successes. Re-run until the report is empty.

   ```sh
   controller prune --config cfg-backend-a.yaml --backend <A> --keep-none --dry-run=false
   ```

3. After a successful prune, decommission A's credentials and configuration from the controller.

The same command with `--orphans` (instead of `--keep-none`) keeps rules whose owning policy still exists in `policies.dir` and deletes the rest; use it as the sweep for policies deleted while the controller was down. Pruning is never triggered automatically.

### Rollback

Set `alerting.backend` back to A. If A was not pruned, its rules are still present and the next reconcile finds them in sync. If A was pruned, the controller recreates them. B's rules remain until pruned in the same way.
