package newrelic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/config"
)

// BackendName is the registered backend name.
const BackendName = "newrelic"

func init() {
	alerting.Register(BackendName, func(cfg config.Config, logger *slog.Logger) (alerting.Backend, error) {
		return New(cfg.Alerting.NewRelic, WithLogger(logger))
	})
}

// NerdGraph operations. Dry-run and the real client both receive exactly these.
const (
	searchQuery = `query($accountId: Int!, $policyId: ID!, $nameLike: String!, $cursor: String) {
  actor { account(id: $accountId) { alerts {
    nrqlConditionsSearch(searchCriteria: {policyId: $policyId, nameLike: $nameLike}, cursor: $cursor) {
      nextCursor
      nrqlConditions { id name description enabled policyId type nrql { query } signal { aggregationWindow } terms { threshold thresholdDuration thresholdOccurrences operator priority } }
    }
  } } }
}`
	createMutation = `mutation($accountId: Int!, $policyId: ID!, $condition: AlertsNrqlConditionStaticInput!) {
  alertsNrqlConditionStaticCreate(accountId: $accountId, policyId: $policyId, condition: $condition) { id name }
}`
	updateMutation = `mutation($accountId: Int!, $id: ID!, $condition: AlertsNrqlConditionUpdateStaticInput!) {
  alertsNrqlConditionStaticUpdate(accountId: $accountId, id: $id, condition: $condition) { id name }
}`
	deleteMutation = `mutation($accountId: Int!, $id: ID!) {
  alertsConditionDelete(accountId: $accountId, id: $id) { id }
}`
)

// Backend implements alerting.Backend on New Relic.
type Backend struct {
	client    Client
	accountID int64
	policyID  string
}

var (
	_ alerting.Backend = (*Backend)(nil)
	_ alerting.Lister  = (*Backend)(nil)
)

type options struct {
	client Client
	logger *slog.Logger
	getenv func(string) string
}

// Option customises New.
type Option func(*options)

// WithClient injects the NerdGraph client, bypassing dry-run/HTTP selection.
func WithClient(c Client) Option { return func(o *options) { o.client = c } }

// WithLogger sets the logger (used by the dry-run client).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithGetenv overrides environment lookup (default os.Getenv).
func WithGetenv(f func(string) string) Option { return func(o *options) { o.getenv = f } }

// New builds the backend. With cfg.DryRun it uses a DryRunClient; otherwise it
// requires accountId, policyId and the API key (read from the env var named by
// cfg.APIKeyEnv) and uses a real HTTPClient.
func New(cfg config.NewRelicConfig, opts ...Option) (*Backend, error) {
	o := options{getenv: os.Getenv}
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	client := o.client
	if client == nil {
		if cfg.DryRun {
			client = NewDryRunClient(o.logger)
		} else {
			var errs []error
			if cfg.AccountID <= 0 {
				errs = append(errs, errors.New("alerting.newrelic.accountId is required when dryRun is false"))
			}
			if cfg.PolicyID <= 0 {
				errs = append(errs, errors.New("alerting.newrelic.policyId is required when dryRun is false"))
			}
			key := ""
			if cfg.APIKeyEnv == "" {
				errs = append(errs, errors.New("alerting.newrelic.apiKeyEnv is required when dryRun is false"))
			} else if key = o.getenv(cfg.APIKeyEnv); key == "" {
				errs = append(errs, fmt.Errorf("environment variable %s (alerting.newrelic.apiKeyEnv) is not set", cfg.APIKeyEnv))
			}
			endpoint, err := EndpointFor(cfg.Region)
			if err != nil {
				errs = append(errs, err)
			}
			if err := errors.Join(errs...); err != nil {
				return nil, fmt.Errorf("newrelic backend: %w", err)
			}
			client = NewHTTPClient(endpoint, key, nil)
		}
	}
	return &Backend{client: client, accountID: cfg.AccountID, policyID: strconv.FormatInt(cfg.PolicyID, 10)}, nil
}

// Name implements alerting.Backend.
func (b *Backend) Name() string { return BackendName }

// search returns every condition in the shared policy whose name carries NamePrefix.
func (b *Backend) search(ctx context.Context) ([]Condition, error) {
	var all []Condition
	cursor := ""
	for {
		vars := map[string]any{"accountId": b.accountID, "policyId": b.policyID, "nameLike": NamePrefix}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		raw, err := b.client.Query(ctx, searchQuery, vars)
		if err != nil {
			return nil, err
		}
		var resp struct {
			Actor struct {
				Account struct {
					Alerts struct {
						Search struct {
							NextCursor string      `json:"nextCursor"`
							Conditions []Condition `json:"nrqlConditions"`
						} `json:"nrqlConditionsSearch"`
					} `json:"alerts"`
				} `json:"account"`
			} `json:"actor"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("decode condition search: %w", err)
		}
		s := resp.Actor.Account.Alerts.Search
		all = append(all, s.Conditions...)
		if s.NextCursor == "" {
			return all, nil
		}
		cursor = s.NextCursor
	}
}

// List implements alerting.Backend. Conditions without our ownership tags, and
// those owned by someone else, are ignored.
func (b *Backend) List(ctx context.Context, owner alerting.Owner) ([]alerting.ManagedRule, error) {
	conds, err := b.search(ctx)
	if err != nil {
		return nil, fmt.Errorf("list conditions: %w", err)
	}
	var out []alerting.ManagedRule
	for _, c := range conds {
		r, ok, err := ruleFromCondition(c)
		if err != nil {
			return nil, err
		}
		if ok && r.Owner == owner {
			out = append(out, r)
		}
	}
	return out, nil
}

// ListAll implements alerting.Lister. Conditions without our ownership tags
// are skipped, so they can never be pruned.
func (b *Backend) ListAll(ctx context.Context) ([]alerting.ManagedRule, error) {
	conds, err := b.search(ctx)
	if err != nil {
		return nil, fmt.Errorf("list conditions: %w", err)
	}
	var out []alerting.ManagedRule
	for _, c := range conds {
		r, ok, err := ruleFromCondition(c)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, r)
		}
	}
	return out, nil
}

// ruleFromCondition rebuilds the rule from the description, then overlays what
// New Relic actually holds (threshold, operator, window, severity). If the
// query, name or enabled flag differ from what we would write, the rule is
// marked Divergent so the next plan updates it.
func ruleFromCondition(c Condition) (alerting.ManagedRule, bool, error) {
	r, _, ok, err := DecodeDescription(c.Description)
	if !ok || err != nil {
		return alerting.ManagedRule{}, ok, err
	}
	if len(c.Terms) != 1 {
		r.Divergent = true
		return r, true, nil
	}
	t := c.Terms[0]
	r.Threshold = fromNRThreshold(r, t.Threshold)
	r.Operator = alerting.Operator(strings.ToLower(t.Operator))
	r.Severity = alerting.Severity(strings.ToLower(t.Priority))
	r.Window = time.Duration(t.ThresholdDuration) * time.Second
	want, terr := Translate(r)
	if terr != nil || !c.Enabled || c.NRQL.Query != want.NRQL.Query || c.Name != want.Name ||
		c.Signal.AggregationWindow != want.Signal.AggregationWindow || t.ThresholdOccurrences != "ALL" {
		r.Divergent = true
	}
	return r, true, nil
}

// find locates the New Relic condition ID for r, so writes never rely on cached state.
func (b *Backend) find(ctx context.Context, r alerting.ManagedRule) (string, error) {
	conds, err := b.search(ctx)
	if err != nil {
		return "", fmt.Errorf("find condition: %w", err)
	}
	for _, c := range conds {
		got, ok, err := ruleFromCondition(c)
		if err == nil && ok && got.Owner == r.Owner && got.ID == r.ID {
			return c.ID, nil
		}
	}
	return "", fmt.Errorf("alert condition for rule %s not found", r.ID)
}

// Create implements alerting.Backend.
func (b *Backend) Create(ctx context.Context, r alerting.ManagedRule) error {
	c, err := Translate(r)
	if err != nil {
		return err
	}
	_, err = b.client.Query(ctx, createMutation, map[string]any{
		"accountId": b.accountID, "policyId": b.policyID, "condition": c.input(),
	})
	return err
}

// Update implements alerting.Backend.
func (b *Backend) Update(ctx context.Context, r alerting.ManagedRule) error {
	c, err := Translate(r)
	if err != nil {
		return err
	}
	id, err := b.find(ctx, r)
	if err != nil {
		return err
	}
	_, err = b.client.Query(ctx, updateMutation, map[string]any{
		"accountId": b.accountID, "id": id, "condition": c.input(),
	})
	return err
}

// Delete implements alerting.Backend.
func (b *Backend) Delete(ctx context.Context, r alerting.ManagedRule) error {
	id, err := b.find(ctx, r)
	if err != nil {
		return err
	}
	_, err = b.client.Query(ctx, deleteMutation, map[string]any{"accountId": b.accountID, "id": id})
	return err
}

// input is the write-side shape: the read-only fields are dropped.
func (c Condition) input() map[string]any {
	return map[string]any{
		"name": c.Name, "description": c.Description, "enabled": c.Enabled,
		"nrql": c.NRQL, "signal": c.Signal, "terms": c.Terms, "valueFunction": c.Value,
	}
}
