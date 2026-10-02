package ecs

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// Telemetry conventions used when deriving observed state.
const (
	// MetricsLabel is the docker label that marks a container as exporting metrics.
	MetricsLabel = "observability.platform.example/metrics"
	// MetricsLabelEnabled is the MetricsLabel value that enables metrics.
	MetricsLabelEnabled = "enabled"
	// OTLPEndpointEnv is the environment variable that marks OTLP trace export.
	OTLPEndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"
)

// DefaultCollectorPatterns are container-name fragments identifying a metrics collector sidecar.
var DefaultCollectorPatterns = []string{"otel-collector", "aws-otel", "datadog-agent"}

// DefaultXRayPatterns are container-name fragments identifying an X-Ray daemon sidecar.
var DefaultXRayPatterns = []string{"xray-daemon", "aws-xray"}

// telemetry is the telemetry configuration derived from a task definition.
type telemetry struct {
	logs, metrics, traces bool
	logDriver             string
}

type classifier struct {
	collectors []string
	xray       []string
}

func matchesAny(name string, patterns []string) bool {
	name = strings.ToLower(name)
	for _, p := range patterns {
		if p != "" && strings.Contains(name, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

func (c classifier) isCollector(ct types.ContainerDefinition) bool {
	return matchesAny(deref(ct.Name), c.collectors)
}

func (c classifier) isXRay(ct types.ContainerDefinition) bool {
	return matchesAny(deref(ct.Name), c.xray)
}

// isSidecar reports whether the container is infrastructure (collector,
// x-ray daemon or FireLens router) rather than application code.
func (c classifier) isSidecar(ct types.ContainerDefinition) bool {
	return c.isCollector(ct) || c.isXRay(ct) || ct.FirelensConfiguration != nil
}

// derive computes telemetry from the container definitions.
//
//   - logs: every essential non-sidecar container has a logConfiguration
//     (and at least one such container exists).
//   - metrics: a collector sidecar exists, or any container carries the
//     MetricsLabel=enabled docker label.
//   - traces: any app container sets OTLPEndpointEnv, or an x-ray sidecar exists.
func (c classifier) derive(defs []types.ContainerDefinition) telemetry {
	var t telemetry
	apps, logged := 0, 0
	drivers := map[string]struct{}{}
	for _, ct := range defs {
		if c.isCollector(ct) || ct.DockerLabels[MetricsLabel] == MetricsLabelEnabled {
			t.metrics = true
		}
		if c.isXRay(ct) {
			t.traces = true
		}
		if c.isSidecar(ct) {
			continue
		}
		for _, e := range ct.Environment {
			if deref(e.Name) == OTLPEndpointEnv && deref(e.Value) != "" {
				t.traces = true
			}
		}
		if ct.Essential != nil && !*ct.Essential {
			continue
		}
		apps++
		if ct.LogConfiguration != nil && ct.LogConfiguration.LogDriver != "" {
			logged++
			drivers[string(ct.LogConfiguration.LogDriver)] = struct{}{}
		}
	}
	t.logs = apps > 0 && logged == apps
	switch len(drivers) {
	case 0:
		t.logDriver = "none"
	case 1:
		for d := range drivers {
			t.logDriver = d
		}
	default:
		t.logDriver = "mixed"
	}
	return t
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
