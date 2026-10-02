package runtime

import "github.com/example/portable-platform-reconciler/internal/state"

// WorkloadIdentity is the canonical runtime-independent workload identity.
// It is defined in package state (to avoid an import cycle) and aliased here.
type WorkloadIdentity = state.WorkloadIdentity
