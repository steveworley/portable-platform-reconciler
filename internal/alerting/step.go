package alerting

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"

	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// StepName is the name of the alerts reconcile step.
const StepName = "alerts"

// ConditionAlertsReconciled is the condition the step reports.
const ConditionAlertsReconciled controller.ConditionType = "AlertsReconciled"

// Condition reasons.
const (
	ReasonDisabled       = "Disabled"
	ReasonReconciled     = "Reconciled"
	ReasonUnsupported    = "Unsupported"
	ReasonInvalidProfile = "InvalidProfile"
	ReasonBackendFailed  = "BackendFailed"
	ReasonNotConverged   = "NotConverged"
)

// Action names produced by the step.
const (
	ActionCreateRule state.ActionName = "create_alert_rule"
	ActionUpdateRule state.ActionName = "update_alert_rule"
	ActionDeleteRule state.ActionName = "delete_alert_rule"
)

// ReasonRuleDrift is the reason attached to every alert action.
const ReasonRuleDrift = "desired alert rule differs from backend"

// Detail keys the step contributes to the policy status.
const (
	DetailBackend      = "alerts.backend"
	DetailManagedRules = "alerts.managedRules"
	DetailProfile      = "alerts.profile"
)

var errNotConverged = errors.New("alert rules differ from backend after reconcile")

var _ controller.ReconcileStep = (*Step)(nil)

// Step reconciles the policy's alert profile into a Backend. It holds no
// per-workload state: the backend is re-read on every run.
type Step struct {
	backend  Backend
	profiles *Profiles
	logger   *slog.Logger
}

// StepOption customises a Step.
type StepOption func(*Step)

// WithProfiles overrides the profile registry (default DefaultProfiles).
func WithProfiles(p *Profiles) StepOption { return func(s *Step) { s.profiles = p } }

// WithLogger sets the logger used to record each backend write.
func WithLogger(l *slog.Logger) StepOption { return func(s *Step) { s.logger = l } }

// NewStep returns the alerts step for backend.
func NewStep(backend Backend, opts ...StepOption) *Step {
	s := &Step{backend: backend, profiles: DefaultProfiles, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Name implements controller.ReconcileStep.
func (s *Step) Name() string { return StepName }

func cond(status controller.ConditionStatus, reason, msg string) controller.Condition {
	return controller.Condition{Type: ConditionAlertsReconciled, Status: status, Reason: reason, Message: msg}
}

// Reconcile implements controller.ReconcileStep.
func (s *Step) Reconcile(ctx context.Context, in controller.StepInput) (controller.StepResult, error) {
	profile := in.Policy.Spec.Alerts.Profile
	if profile == "" {
		return controller.StepResult{Conditions: []controller.Condition{
			cond(controller.ConditionTrue, ReasonDisabled, "policy has no alerts profile"),
		}}, nil
	}
	if !in.Capabilities.AlertReconciliation {
		msg := "runtime does not support alert reconciliation"
		return controller.StepResult{
			Conditions:  []controller.Condition{cond(controller.ConditionFalse, ReasonUnsupported, msg)},
			Unsupported: []string{"alerts: " + msg},
		}, nil
	}

	id := in.Workload.Identity
	desired, unsupported, err := s.profiles.Desired(in.Policy, id, in.Capabilities)
	if err != nil {
		// A bad profile name is permanent: report it, do not retry.
		return controller.StepResult{
			Conditions: []controller.Condition{cond(controller.ConditionFalse, ReasonInvalidProfile, err.Error())},
			Degraded:   true,
			Details:    map[string]string{DetailBackend: s.backend.Name(), DetailProfile: profile},
		}, nil
	}

	owner := OwnerFor(in.Policy.Metadata.Name, id)
	res := controller.StepResult{Unsupported: unsupported}
	fail := func(reason string, err error) (controller.StepResult, error) {
		res.Conditions = []controller.Condition{cond(controller.ConditionFalse, reason, err.Error())}
		return res, err
	}

	actual, err := s.backend.List(ctx, owner)
	if err != nil {
		return fail(ReasonBackendFailed, fmt.Errorf("list alert rules: %w", err))
	}
	plan := PlanFor(desired, actual)
	if err := s.apply(ctx, in, plan, &res); err != nil {
		return fail(ReasonBackendFailed, err)
	}

	// Re-read to confirm; never trust that the writes took effect.
	actual, err = s.backend.List(ctx, owner)
	if err != nil {
		return fail(ReasonBackendFailed, fmt.Errorf("re-list alert rules: %w", err))
	}
	res.Details = map[string]string{
		DetailBackend:      s.backend.Name(),
		DetailManagedRules: strconv.Itoa(len(actual)),
		DetailProfile:      profile,
	}
	if rest := PlanFor(desired, actual); !rest.Empty() {
		return fail(ReasonNotConverged, fmt.Errorf("%w: %d create, %d update, %d delete remaining",
			errNotConverged, len(rest.Create), len(rest.Update), len(rest.Delete)))
	}
	msg := fmt.Sprintf("%d alert rule(s) reconciled into %s", len(actual), s.backend.Name())
	res.Conditions = []controller.Condition{cond(controller.ConditionTrue, ReasonReconciled, msg)}
	return res, nil
}

func (s *Step) apply(ctx context.Context, in controller.StepInput, plan Plan, res *controller.StepResult) error {
	type write struct {
		name  state.ActionName
		rules []ManagedRule
		do    func(context.Context, ManagedRule) error
	}
	for _, w := range []write{
		{ActionCreateRule, plan.Create, s.backend.Create},
		{ActionUpdateRule, plan.Update, s.backend.Update},
		{ActionDeleteRule, plan.Delete, s.backend.Delete},
	} {
		for _, r := range w.rules {
			s.logger.Info("alert rule write",
				"resource", in.Workload.Identity.Workload, "runtime", in.Workload.Identity.Runtime,
				"backend", s.backend.Name(), "action", string(w.name), "rule", r.ID)
			if err := w.do(ctx, r); err != nil {
				return fmt.Errorf("%s %s: %w", w.name, r.ID, err)
			}
			res.Actions = append(res.Actions, state.Action{Name: w.name, Signal: state.Signal(r.ID), Reason: ReasonRuleDrift})
		}
	}
	return nil
}
