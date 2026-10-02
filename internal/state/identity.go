// Package state holds the runtime-neutral desired state, observed state and
// the pure diff between them. It never talks to a runtime.
package state

// WorkloadIdentity is the canonical, runtime-independent identity of a workload.
// The rest of the platform need not know whether it is a Deployment or an ECS service.
type WorkloadIdentity struct {
	Account     string `json:"account,omitempty"`
	Region      string `json:"region,omitempty"`
	Environment string `json:"environment,omitempty"`
	Service     string `json:"service"`
	Deployment  string `json:"deployment,omitempty"`
	Workload    string `json:"workload"`
	Runtime     string `json:"runtime"`   // open string, e.g. "ecs"
	RuntimeID   string `json:"runtimeID"` // ECS service ARN or "namespace/name"
}

// String returns a stable unique key such as "kubernetes:payments/payments-api".
func (i WorkloadIdentity) String() string { return i.Runtime + ":" + i.RuntimeID }
