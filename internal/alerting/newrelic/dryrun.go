package newrelic

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
)

// DryRunClient is a Client that never touches the network. It logs the exact
// GraphQL and variables it is given and interprets the search/create/update/
// delete operations against an in-memory shadow store, so Backend.List
// reflects earlier dry-run writes and reconciliation stays idempotent.
type DryRunClient struct {
	logger *slog.Logger

	mu     sync.Mutex
	nextID int
	conds  map[string]Condition
}

var _ Client = (*DryRunClient)(nil)

// NewDryRunClient returns an empty shadow store. A nil logger discards logs.
func NewDryRunClient(logger *slog.Logger) *DryRunClient {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &DryRunClient{logger: logger, conds: map[string]Condition{}}
}

// Conditions returns a snapshot of the shadow store keyed by condition ID.
func (d *DryRunClient) Conditions() map[string]Condition {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]Condition, len(d.conds))
	for k, v := range d.conds {
		out[k] = v
	}
	return out
}

// Query implements Client.
func (d *DryRunClient) Query(_ context.Context, gql string, vars map[string]any) (json.RawMessage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case strings.Contains(gql, "nrqlConditionsSearch"):
		d.logger.Debug("dry-run: nerdgraph search", "variables", jsonString(vars))
		return d.search(vars)
	case strings.Contains(gql, "alertsNrqlConditionStaticCreate"):
		d.logWrite("alertsNrqlConditionStaticCreate", gql, vars)
		return d.upsert("alertsNrqlConditionStaticCreate", "", vars)
	case strings.Contains(gql, "alertsNrqlConditionStaticUpdate"):
		d.logWrite("alertsNrqlConditionStaticUpdate", gql, vars)
		id, _ := vars["id"].(string)
		if _, ok := d.conds[id]; !ok {
			return nil, fmt.Errorf("dry-run: condition %q not found", id)
		}
		return d.upsert("alertsNrqlConditionStaticUpdate", id, vars)
	case strings.Contains(gql, "alertsConditionDelete"):
		d.logWrite("alertsConditionDelete", gql, vars)
		id, _ := vars["id"].(string)
		if _, ok := d.conds[id]; !ok {
			return nil, fmt.Errorf("dry-run: condition %q not found", id)
		}
		delete(d.conds, id)
		return json.RawMessage(fmt.Sprintf(`{"alertsConditionDelete":{"id":%q}}`, id)), nil
	}
	return nil, fmt.Errorf("dry-run: unsupported operation: %s", truncate(gql, 80))
}

func (d *DryRunClient) logWrite(op, gql string, vars map[string]any) {
	d.logger.Info("dry-run: would send NerdGraph mutation", "operation", op, "mutation", gql, "variables", jsonString(vars))
}

func (d *DryRunClient) upsert(op, id string, vars map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(vars["condition"])
	if err != nil {
		return nil, fmt.Errorf("dry-run: encode condition: %w", err)
	}
	var c Condition
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("dry-run: decode condition: %w", err)
	}
	if id == "" {
		d.nextID++
		id = strconv.Itoa(d.nextID)
		c.PolicyID = fmt.Sprint(vars["policyId"])
	} else {
		c.PolicyID = d.conds[id].PolicyID
	}
	c.ID, c.Type = id, "STATIC"
	d.conds[id] = c
	return json.RawMessage(fmt.Sprintf(`{%q:{"id":%q,"name":%q}}`, op, id, c.Name)), nil
}

func (d *DryRunClient) search(vars map[string]any) (json.RawMessage, error) {
	policyID := fmt.Sprint(vars["policyId"])
	nameLike := fmt.Sprint(vars["nameLike"])
	out := []Condition{}
	for _, c := range d.conds {
		if c.PolicyID == policyID && strings.Contains(strings.ToLower(c.Name), strings.ToLower(nameLike)) {
			out = append(out, c)
		}
	}
	resp := map[string]any{"actor": map[string]any{"account": map[string]any{"alerts": map[string]any{
		"nrqlConditionsSearch": map[string]any{"nextCursor": "", "nrqlConditions": out},
	}}}}
	return json.Marshal(resp)
}

func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<unencodable: %v>", err)
	}
	return string(b)
}
