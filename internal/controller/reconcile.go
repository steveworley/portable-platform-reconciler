package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// errNotConverged is returned when drift remains after the adapter applied actions.
var errNotConverged = errors.New("drift remains after reconcile")

// Outcome describes the result of reconciling one policy for one queue key.
type Outcome struct {
	Key     string
	Policy  string
	Runtime string
	// Workload is the resolved identity; zero if the target was not resolved.
	Workload state.WorkloadIdentity
	// Actions are the actions the adapter was asked to apply.
	Actions []state.Action
	// NoOp is true when no action was required (already compliant) or the
	// policy was skipped.
	NoOp bool
	// DryRun is true when the adapter deliberately changed nothing (simulated
	// or dry-run mode). The phase is Degraded with reason DryRun and the
	// reconcile is not retried.
	DryRun bool
	// Unsupported lists drifted features the runtime cannot satisfy.
	Unsupported []string
	Phase       Phase
	Err         error
}

// Option configures an Engine.
type Option func(*Engine)

// WithMaxRetries sets how many times a failing key is retried before it is dropped. Default 5.
func WithMaxRetries(n int) Option { return func(e *Engine) { e.maxRetries = n } }

// WithBackoff sets the exponential retry backoff bounds.
func WithBackoff(base, max time.Duration) Option {
	return func(e *Engine) { e.queueOpts = QueueOptions{BaseDelay: base, MaxDelay: max} }
}

// WithObserver registers a callback invoked after every policy reconcile
// (including failures). It runs on the worker goroutine and must not block.
func WithObserver(fn func(Outcome)) Option { return func(e *Engine) { e.observer = fn } }

// WithClock overrides the clock used for LastReconciledAt.
func WithClock(now func() time.Time) Option { return func(e *Engine) { e.now = now } }

// Engine is the single, runtime-neutral reconciliation engine. All event
// sources feed it through Enqueue; it never trusts event payloads and always
// asks the runtime adapter for authoritative state.
type Engine struct {
	adapter  runtime.RuntimeAdapter
	policies PolicyStore
	statuses *StatusStore
	logger   *slog.Logger
	queue    *Queue

	maxRetries int
	queueOpts  QueueOptions
	observer   func(Outcome)
	steps      []ReconcileStep
	now        func() time.Time
}

// NewEngine creates an Engine. If policies supports change notifications
// (like MemoryPolicyStore) policy changes are enqueued automatically.
func NewEngine(adapter runtime.RuntimeAdapter, policies PolicyStore, statuses *StatusStore, logger *slog.Logger, opts ...Option) *Engine {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	e := &Engine{
		adapter:    adapter,
		policies:   policies,
		statuses:   statuses,
		logger:     logger,
		maxRetries: 5,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	e.queue = NewQueue(e.queueOpts)
	if s, ok := policies.(subscribable); ok {
		s.Subscribe(func(name string) { e.Enqueue(policy.Key(name)) })
	}
	return e
}

// Enqueue is the single entrypoint for event sources. Keys are either
// "ObservabilityPolicy:<name>" or an adapter queue key (see RuntimeAdapter.QueueKey).
func (e *Engine) Enqueue(key string) {
	e.logger.Debug("enqueue", "key", key)
	e.queue.Add(key)
}

// Run processes the queue with the given number of workers until ctx is
// cancelled, then stops accepting keys and waits for in-flight work.
func (e *Engine) Run(ctx context.Context, workers int) error {
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e.processNext(ctx) {
			}
		}()
	}
	<-ctx.Done()
	e.logger.Info("shutting down, draining in-flight reconciles")
	e.queue.ShutDownWithDrain()
	wg.Wait()
	return nil
}

func (e *Engine) processNext(ctx context.Context) bool {
	key, shutdown := e.queue.Get()
	if shutdown {
		return false
	}
	defer e.queue.Done(key)
	if ctx.Err() != nil {
		return true
	}
	_, err := e.ReconcileOnce(ctx, key)
	if err == nil {
		e.queue.Forget(key)
		return true
	}
	if n := e.queue.NumRequeues(key); n < e.maxRetries {
		e.logger.Warn("reconcile failed, will retry", "key", key, "attempt", n+1, "error", err)
		e.queue.AddRateLimited(key)
		return true
	}
	e.logger.Error("reconcile failed, giving up until next event or resync", "key", key, "error", err)
	e.queue.Forget(key)
	return true
}

// ReconcileOnce synchronously reconciles every policy that key maps to and
// returns one Outcome per policy. A key that maps to no policy is logged and
// dropped without error.
func (e *Engine) ReconcileOnce(ctx context.Context, key string) ([]Outcome, error) {
	var names []string
	if name, ok := policy.ParseKey(key); ok {
		names = []string{name}
	} else {
		names = e.policiesFor(key)
		if len(names) == 0 {
			e.logger.Info("no policy maps to key, dropping", "key", key)
			return nil, nil
		}
	}
	var (
		outs []Outcome
		errs []error
	)
	for _, name := range names {
		out, err := e.reconcilePolicy(ctx, key, name)
		out.Err = err
		if e.observer != nil {
			e.observer(out)
		}
		outs = append(outs, out)
		if err != nil {
			errs = append(errs, fmt.Errorf("policy %s: %w", name, err))
		}
	}
	return outs, errors.Join(errs...)
}

// policiesFor maps an adapter queue key to the names of policies targeting it.
func (e *Engine) policiesFor(key string) []string {
	var names []string
	for _, p := range e.policies.List() {
		if p.Target.Runtime != string(e.adapter.Runtime()) {
			e.logger.Debug("skipping policy for other runtime", "policy", p.Metadata.Name, "policyRuntime", p.Target.Runtime, "runtime", e.adapter.Runtime())
			continue
		}
		k, err := e.adapter.QueueKey(p.Target)
		if err != nil {
			e.logger.Warn("policy target has no queue key", "policy", p.Metadata.Name, "error", err)
			continue
		}
		if k == key {
			names = append(names, p.Metadata.Name)
		}
	}
	return names
}

func (e *Engine) reconcilePolicy(ctx context.Context, key, name string) (Outcome, error) {
	rt := string(e.adapter.Runtime())
	out := Outcome{Key: key, Policy: name, Runtime: rt}

	p, ok := e.policies.Get(name)
	if !ok {
		out.NoOp = true
		if err := e.finalize(ctx, name); err != nil {
			out.Phase = PhaseError
			return out, err
		}
		e.statuses.Delete(name)
		e.logger.Info("policy not found, dropping", "policy", name)
		return out, nil
	}
	if p.Target.Runtime != rt {
		e.logger.Info("policy targets another runtime, skipping", "policy", name, "policyRuntime", p.Target.Runtime, "runtime", rt)
		out.NoOp = true
		return out, nil
	}

	prev, _ := e.statuses.Get(name)
	st := PolicyStatus{PolicyName: name, ObservedGeneration: p.Metadata.Generation, LastActions: prev.LastActions}
	finish := func(phase Phase, err error) (Outcome, error) {
		st.Phase = phase
		out.Phase = phase
		if err == nil {
			t := e.now()
			st.LastReconciledAt = &t
		} else {
			st.LastReconciledAt = prev.LastReconciledAt
		}
		e.statuses.Set(st)
		return out, err
	}

	// Resolve target.
	wl, err := e.adapter.ResolveTarget(ctx, p.Target)
	if err != nil {
		if errors.Is(err, runtime.ErrNotFound) {
			e.logger.Warn("target not found; will retry on next event or resync", "policy", name, "runtime", rt, "error", err)
			st.setCondition(ConditionTargetResolved, ConditionFalse, "NotFound", err.Error())
			return finish(PhasePending, nil)
		}
		st.setCondition(ConditionTargetResolved, ConditionUnknown, "ResolveFailed", err.Error())
		return finish(PhaseError, fmt.Errorf("resolve target: %w", err))
	}
	out.Workload = wl.Identity
	st.Workload = &wl.Identity
	st.setCondition(ConditionTargetResolved, ConditionTrue, "Resolved", "")
	resource := wl.Identity.Workload

	// Read authoritative state, never event payloads.
	observed, err := e.adapter.Observe(ctx, wl)
	if err != nil {
		st.setCondition(ConditionReconciled, ConditionFalse, "ObserveFailed", err.Error())
		return finish(PhaseError, fmt.Errorf("observe: %w", err))
	}

	desired := state.FromPolicy(p)
	caps := wl.EffectiveCapabilities(e.adapter.Capabilities())
	diff := state.Diff(desired, observed, caps)

	var dryRunMsg string
	if diff.InSync() {
		out.NoOp = true
		e.logger.Info("already compliant, no action", "resource", resource, "runtime", rt, "policy", name)
	} else {
		out.Actions = diff.Actions
		for _, a := range diff.Actions {
			e.logger.Info("reconcile action", "resource", resource, "runtime", rt, "action", string(a.Name), "reason", a.Reason)
		}
		res, err := e.adapter.Reconcile(ctx, desired, observed, diff.Actions)
		if err != nil {
			st.setCondition(ConditionReconciled, ConditionFalse, "ReconcileFailed", err.Error())
			return finish(PhaseError, fmt.Errorf("reconcile: %w", err))
		}
		st.LastActions = diff.Actions
		e.logger.Debug("adapter reconcile result", "resource", resource, "changed", res.Changed, "message", res.Message)

		if res.DryRun {
			// Nothing was changed on purpose; do not treat remaining drift as
			// a failure. The periodic resync re-evaluates.
			out.DryRun = true
			dryRunMsg = "dry run, would apply: " + describeActions(diff.Actions)
		} else {
			// Re-read and re-diff to confirm convergence.
			observed, err = e.adapter.Observe(ctx, wl)
			if err != nil {
				st.setCondition(ConditionReconciled, ConditionFalse, "ObserveFailed", err.Error())
				return finish(PhaseError, fmt.Errorf("re-observe: %w", err))
			}
			diff = state.Diff(desired, observed, caps)
			if !diff.InSync() {
				st.setCondition(ConditionCapabilitiesSatisfied, ConditionTrue, "Supported", "")
				st.setCondition(ConditionConfigurationCompliant, ConditionFalse, "DriftRemains", fmt.Sprintf("%d action(s) still required after reconcile", len(diff.Actions)))
				st.setCondition(ConditionReconciled, ConditionFalse, "NotConverged", errNotConverged.Error())
				return finish(PhaseDegraded, errNotConverged)
			}
		}
	}

	out.Unsupported = diff.Unsupported
	phase := PhaseReady
	if len(diff.Unsupported) > 0 {
		phase = PhaseDegraded
		msg := fmt.Sprint(diff.Unsupported)
		e.logger.Warn("desired state not supported by runtime", "resource", resource, "runtime", rt, "unsupported", diff.Unsupported)
		st.setCondition(ConditionCapabilitiesSatisfied, ConditionFalse, "Unsupported", msg)
		st.setCondition(ConditionConfigurationCompliant, ConditionFalse, "Unsupported", msg)
	} else {
		st.setCondition(ConditionCapabilitiesSatisfied, ConditionTrue, "Supported", "")
		st.setCondition(ConditionConfigurationCompliant, ConditionTrue, "Compliant", "")
	}
	if out.DryRun {
		phase = PhaseDegraded
		st.setCondition(ConditionConfigurationCompliant, ConditionFalse, "DryRun", dryRunMsg)
	}

	// Additional steps (e.g. alert backend reconciliation).
	in := StepInput{Policy: p, Desired: desired, Workload: wl, Observed: observed, Capabilities: caps}
	var stepErrs []error
	for _, step := range e.steps {
		res, err := step.Reconcile(ctx, in)
		for _, c := range res.Conditions {
			st.setCondition(c.Type, c.Status, c.Reason, c.Message)
		}
		out.Actions = append(out.Actions, res.Actions...)
		if len(res.Actions) > 0 {
			st.LastActions = append(append([]state.Action(nil), st.LastActions...), res.Actions...)
		}
		out.Unsupported = append(out.Unsupported, res.Unsupported...)
		if res.Degraded || len(res.Unsupported) > 0 {
			phase = PhaseDegraded
		}
		for k, v := range res.Details {
			if st.Details == nil {
				st.Details = map[string]string{}
			}
			st.Details[k] = v
		}
		if err != nil {
			if _, has := st.Condition(ConditionType(step.Name())); !has {
				st.setCondition(ConditionType(step.Name()), ConditionFalse, "StepFailed", err.Error())
			}
			stepErrs = append(stepErrs, fmt.Errorf("step %s: %w", step.Name(), err))
		}
	}
	out.NoOp = len(out.Actions) == 0
	if len(stepErrs) > 0 {
		st.setCondition(ConditionReconciled, ConditionFalse, "StepFailed", "one or more reconcile steps failed")
		return finish(PhaseError, errors.Join(stepErrs...))
	}
	st.setCondition(ConditionReconciled, ConditionTrue, "Reconciled", "")
	return finish(phase, nil)
}

// finalize runs every Finalizer step for a deleted policy whose workload is
// known from its last recorded status. A policy that never resolved a workload
// owns nothing to clean up.
func (e *Engine) finalize(ctx context.Context, name string) error {
	prev, ok := e.statuses.Get(name)
	if !ok || prev.Workload == nil {
		return nil
	}
	in := FinalizeInput{PolicyName: name, Workload: *prev.Workload, Capabilities: e.adapter.Capabilities()}
	var errs []error
	for _, step := range e.steps {
		f, ok := step.(Finalizer)
		if !ok {
			continue
		}
		if err := f.Finalize(ctx, in); err != nil {
			errs = append(errs, fmt.Errorf("finalize step %s: %w", step.Name(), err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		prev.Phase = PhaseError
		prev.setCondition(ConditionFinalized, ConditionFalse, "FinalizeFailed", err.Error())
		e.statuses.Set(prev)
		return err
	}
	return nil
}

func describeActions(actions []state.Action) string {
	names := make([]string, len(actions))
	for i, a := range actions {
		names[i] = string(a.Name)
	}
	return strings.Join(names, ", ")
}

func (s *PolicyStatus) setCondition(t ConditionType, status ConditionStatus, reason, msg string) {
	c := Condition{Type: t, Status: status, Reason: reason, Message: msg}
	for i := range s.Conditions {
		if s.Conditions[i].Type == t {
			s.Conditions[i] = c
			return
		}
	}
	s.Conditions = append(s.Conditions, c)
}
