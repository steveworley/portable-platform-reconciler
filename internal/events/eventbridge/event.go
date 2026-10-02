// Package eventbridge is the ECS event source. It accepts EventBridge-shaped
// events over HTTP and enqueues the affected ECS service keys.
//
// Events are notifications only. Nothing in an event payload is treated as
// state: the engine re-reads the service through the ECS adapter.
package eventbridge

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/example/portable-platform-reconciler/internal/runtime/ecs"
)

// Event is the EventBridge event envelope.
type Event struct {
	Version    string          `json:"version,omitempty"`
	ID         string          `json:"id,omitempty"`
	DetailType string          `json:"detail-type"`
	Source     string          `json:"source"`
	Account    string          `json:"account,omitempty"`
	Time       string          `json:"time,omitempty"`
	Region     string          `json:"region,omitempty"`
	Resources  []string        `json:"resources,omitempty"`
	Detail     json.RawMessage `json:"detail,omitempty"`
}

// ECS event identifiers handled by KeysForEvent.
const (
	SourceECS                  = "aws.ecs"
	DetailTypeDeploymentChange = "ECS Deployment State Change"
	DetailTypeServiceAction    = "ECS Service Action"
	DetailTypeTaskChange       = "ECS Task State Change"
)

// eventDetail holds only the fields needed to identify a resource.
type eventDetail struct {
	ServiceArn string `json:"serviceArn"`
	ClusterArn string `json:"clusterArn"`
	Group      string `json:"group"`
}

// KeysForEvent returns the ECSService work-queue keys an event refers to.
//
// It only identifies WHICH resource to re-read. It deliberately does not
// extract, interpret or trust any state from the payload (task status,
// deployment status, desired counts, ...): events can be duplicated, delayed,
// reordered or lost, so the ECS adapter reads authoritative state from the ECS
// API when the key is reconciled.
//
// Events whose source is not "aws.ecs", unknown detail types, and events from
// which no service can be identified yield no keys. Keys are de-duplicated
// within one event and built with the ecs package helpers so they match
// Adapter.QueueKey exactly.
func KeysForEvent(ev Event) []string {
	if ev.Source != SourceECS {
		return nil
	}
	var d eventDetail
	if len(ev.Detail) > 0 {
		if err := json.Unmarshal(ev.Detail, &d); err != nil {
			d = eventDetail{}
		}
	}

	var arns []string
	switch ev.DetailType {
	case DetailTypeDeploymentChange, DetailTypeServiceAction:
		// serviceArn is not always in detail; resources[] carries it.
		arns = append(arns, d.ServiceArn)
		arns = append(arns, ev.Resources...)
	case DetailTypeTaskChange:
		name, ok := strings.CutPrefix(d.Group, "service:")
		if !ok || name == "" {
			return nil // standalone tasks are not services
		}
		region, account, cluster, ok := ecs.ParseClusterARN(d.ClusterArn)
		if !ok {
			return nil
		}
		arns = append(arns, ecs.ServiceARN(region, account, cluster, name))
	default:
		return nil
	}

	var keys []string
	seen := map[string]struct{}{}
	for _, arn := range arns {
		if _, _, _, _, ok := ecs.ParseServiceARN(arn); !ok {
			continue
		}
		if _, dup := seen[arn]; dup {
			continue
		}
		seen[arn] = struct{}{}
		keys = append(keys, ecs.ServiceKey(arn))
	}
	return keys
}

// Consumer is a pluggable pull-based event feed, such as an SQS queue that an
// EventBridge rule targets. An implementation would block in Consume, calling
// handle for each event, and return when ctx is done. Pass events to
// KeysForEvent and enqueue the result, as the HTTP handler does. No
// implementation exists yet; the HTTP endpoint covers the POC.
type Consumer interface {
	// Name identifies the consumer in logs.
	Name() string
	// Consume delivers events to handle until ctx is cancelled or a fatal error occurs.
	Consume(ctx context.Context, handle func(ctx context.Context, ev Event) error) error
}
