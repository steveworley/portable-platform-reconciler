package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/controller"
)

func TestExecuteVersion(t *testing.T) {
	var out bytes.Buffer
	if err := execute(context.Background(), []string{"version"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatalf("execute(version) = %v", err)
	}
	if got := out.String(); !strings.Contains(got, "controller") {
		t.Errorf("version output = %q, want it to name the binary", got)
	}
}

func TestExecuteUnknownCommand(t *testing.T) {
	if err := execute(context.Background(), []string{"bogus"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Error("execute(bogus) = nil, want error")
	}
}

func TestPruneRejectsConflictingModes(t *testing.T) {
	err := execute(context.Background(), []string{"prune", "--keep-none", "--orphans"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Error("execute(prune --keep-none --orphans) = nil, want error")
	}
}

func TestPruneDryRunOnEmptyBackend(t *testing.T) {
	var out bytes.Buffer
	args := []string{"prune", "--backend", "memory", "--log-level", "error"}
	if err := execute(context.Background(), args, &out, &bytes.Buffer{}); err != nil {
		t.Fatalf("execute(prune) = %v", err)
	}
	if got := out.String(); !strings.Contains(got, "dryRun=true") || !strings.Contains(got, "would delete 0") {
		t.Errorf("prune output = %q, want a dry run deleting 0 rules", got)
	}
}

func TestStatusHandler(t *testing.T) {
	statuses := controller.NewStatusStore()
	statuses.Set(controller.PolicyStatus{PolicyName: "payments-api", Phase: controller.PhaseReady})
	h := statusHandler(statuses)

	tests := []struct {
		path string
		want int
	}{
		{"/healthz", http.StatusOK},
		{"/status", http.StatusOK},
		{"/status/payments-api", http.StatusOK},
		{"/status/missing", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if rec.Code != tt.want {
				t.Errorf("GET %s = %d, want %d", tt.path, rec.Code, tt.want)
			}
		})
	}
}
