package ecs

import (
	"fmt"
	"strings"
)

// KeyPrefix prefixes work-queue keys that refer to an ECS service.
const KeyPrefix = "ECSService:"

// ServiceARN builds the ARN of an ECS service in the new (cluster-qualified)
// format: arn:aws:ecs:<region>:<account>:service/<cluster>/<service>.
func ServiceARN(region, account, cluster, service string) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:service/%s/%s", region, account, cluster, service)
}

// ServiceKey returns the work-queue key for a service ARN: "ECSService:<arn>".
// Both the adapter (QueueKey) and the EventBridge source use it so keys match
// exactly.
func ServiceKey(arn string) string { return KeyPrefix + arn }

// ParseServiceARN splits a cluster-qualified service ARN into its parts. It
// reports false for any other shape, including the legacy format that has no
// cluster segment.
func ParseServiceARN(arn string) (region, account, cluster, service string, ok bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "ecs" || parts[3] == "" || parts[4] == "" {
		return "", "", "", "", false
	}
	res := strings.Split(parts[5], "/")
	if len(res) != 3 || res[0] != "service" || res[1] == "" || res[2] == "" {
		return "", "", "", "", false
	}
	return parts[3], parts[4], res[1], res[2], true
}

// ParseClusterARN splits a cluster ARN (arn:aws:ecs:<region>:<account>:cluster/<name>).
func ParseClusterARN(arn string) (region, account, cluster string, ok bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "ecs" || parts[3] == "" || parts[4] == "" {
		return "", "", "", false
	}
	res := strings.Split(parts[5], "/")
	if len(res) != 2 || res[0] != "cluster" || res[1] == "" {
		return "", "", "", false
	}
	return parts[3], parts[4], res[1], true
}
