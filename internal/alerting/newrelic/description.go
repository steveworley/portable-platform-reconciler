package newrelic

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/state"
)

// Ownership encoding (POC choice): NRQL conditions support entity tags via
// taggingAddTagsToEntity, but that is a second, non-atomic mutation per
// condition. For the POC the ownership tags, the fingerprint and the neutral
// rule spec are stored in the condition description as `key=value` lines:
//
//	managed-by=portable-reconciler
//	policy=payments-api
//	...
//	fingerprint=0123456789abcdef
//	spec={...json...}
//	identity={...json...}
//
// List parses them back, so a rule can be rebuilt from New Relic alone. The
// description is human-visible in the UI; do not hand-edit it. Moving to
// entity tags later only changes this file.

const (
	lineSpec     = "spec"
	lineIdentity = "identity"
)

type specDoc struct {
	ID            string  `json:"id"`
	Signal        string  `json:"signal"`
	Scope         string  `json:"scope,omitempty"`
	Resource      string  `json:"resource,omitempty"`
	Name          string  `json:"name"`
	Description   string  `json:"description"`
	Aggregation   string  `json:"aggregation"`
	Operator      string  `json:"operator"`
	Threshold     float64 `json:"threshold"`
	Unit          string  `json:"unit"`
	WindowSeconds int     `json:"windowSeconds"`
	Severity      string  `json:"severity"`
}

type identityDoc struct {
	Account     string `json:"account,omitempty"`
	Region      string `json:"region,omitempty"`
	Environment string `json:"environment,omitempty"`
	Service     string `json:"service"`
	Deployment  string `json:"deployment,omitempty"`
	Workload    string `json:"workload"`
	Runtime     string `json:"runtime"`
	RuntimeID   string `json:"runtimeID"`
}

var oneLine = strings.NewReplacer("\n", " ", "\r", " ")

// EncodeDescription renders the ownership tags and rule spec as key=value lines.
func EncodeDescription(r alerting.ManagedRule) string {
	tags := r.Tags()
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys)+2)
	for _, k := range keys {
		lines = append(lines, k+"="+oneLine.Replace(tags[k]))
	}
	// json.Marshal output is single-line and cannot fail for these types.
	spec, _ := json.Marshal(specDoc{
		ID: r.ID, Signal: string(r.Signal), Scope: r.Scope, Resource: r.Resource, Name: r.Name,
		Description: r.Description, Aggregation: string(r.Aggregation), Operator: string(r.Operator),
		Threshold: r.Threshold, Unit: string(r.Unit), WindowSeconds: int(r.Window.Seconds()), Severity: string(r.Severity),
	})
	id := r.Identity
	ident, _ := json.Marshal(identityDoc{
		Account: id.Account, Region: id.Region, Environment: id.Environment, Service: id.Service,
		Deployment: id.Deployment, Workload: id.Workload, Runtime: id.Runtime, RuntimeID: id.RuntimeID,
	})
	lines = append(lines, lineSpec+"="+string(spec), lineIdentity+"="+string(ident))
	return strings.Join(lines, "\n")
}

// DecodeDescription rebuilds a rule from a description written by
// EncodeDescription. ok is false when the description does not mark a rule as
// managed by this controller; err reports a managed-looking but corrupt one.
// The returned rule carries the stored (not recomputed) neutral fields.
func DecodeDescription(desc string) (r alerting.ManagedRule, storedFingerprint string, ok bool, err error) {
	kv := map[string]string{}
	for _, line := range strings.Split(desc, "\n") {
		if k, v, found := strings.Cut(line, "="); found {
			kv[k] = v
		}
	}
	owner, ruleID, managed := alerting.OwnerFromTags(kv)
	if !managed {
		return alerting.ManagedRule{}, "", false, nil
	}
	var sd specDoc
	if err := json.Unmarshal([]byte(kv[lineSpec]), &sd); err != nil {
		return alerting.ManagedRule{}, "", true, fmt.Errorf("rule %s: decode spec: %w", ruleID, err)
	}
	var idd identityDoc
	if err := json.Unmarshal([]byte(kv[lineIdentity]), &idd); err != nil {
		return alerting.ManagedRule{}, "", true, fmt.Errorf("rule %s: decode identity: %w", ruleID, err)
	}
	return alerting.ManagedRule{
		Rule: alerting.Rule{
			ID: sd.ID, Signal: alerting.Signal(sd.Signal), Scope: sd.Scope, Resource: sd.Resource,
			Name: sd.Name, Description: sd.Description, Aggregation: alerting.Aggregation(sd.Aggregation),
			Operator: alerting.Operator(sd.Operator), Threshold: sd.Threshold, Unit: alerting.Unit(sd.Unit),
			Window: time.Duration(sd.WindowSeconds) * time.Second, Severity: alerting.Severity(sd.Severity),
		},
		Owner: owner,
		Identity: state.WorkloadIdentity{
			Account: idd.Account, Region: idd.Region, Environment: idd.Environment, Service: idd.Service,
			Deployment: idd.Deployment, Workload: idd.Workload, Runtime: idd.Runtime, RuntimeID: idd.RuntimeID,
		},
	}, kv[alerting.TagFingerprint], true, nil
}
