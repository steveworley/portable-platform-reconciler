package state

// ObservedState is what an adapter read from the authoritative runtime API.
type ObservedState struct {
	Workload WorkloadIdentity `json:"workload"`
	// Revision is the task-definition revision or deployment generation observed.
	Revision string `json:"revision,omitempty"`
	Logs     bool   `json:"logs"`
	Metrics  bool   `json:"metrics"`
	Traces   bool   `json:"traces"`
	// Details holds adapter-reported facts, e.g. "logDriver".
	Details map[string]string `json:"details,omitempty"`
}
