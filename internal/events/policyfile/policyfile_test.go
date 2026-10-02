package policyfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/controller"
)

const doc = `apiVersion: observability.platform.example/v1alpha1
kind: ObservabilityPolicy
metadata: {name: NAME}
target: {runtime: fake, name: NAME}
spec:
  telemetry:
    logs: {enabled: LOGS}
`

func write(t *testing.T, dir, file, name, logs string) {
	t.Helper()
	body := strings.NewReplacer("NAME", name, "LOGS", logs).Replace(doc)
	if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSync(t *testing.T) {
	dir := t.TempDir()
	store := controller.NewMemoryPolicyStore()
	var notified []string
	store.Subscribe(func(n string) { notified = append(notified, n) })
	src, err := New(dir, 1, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	write(t, dir, "a.yaml", "a", "true")
	src.Sync()
	if p, ok := store.Get("a"); !ok || p.Metadata.Generation != 1 {
		t.Fatalf("after add: %+v, %v", p, ok)
	}

	src.Sync() // unchanged: no notification
	if len(notified) != 1 {
		t.Errorf("notifications = %v, want 1", notified)
	}

	write(t, dir, "a.yaml", "a", "false")
	src.Sync()
	if p, _ := store.Get("a"); p.Metadata.Generation != 2 || p.Spec.Telemetry.Logs.Enabled {
		t.Errorf("after change: %+v", p)
	}

	if err := os.Remove(filepath.Join(dir, "a.yaml")); err != nil {
		t.Fatal(err)
	}
	src.Sync()
	if _, ok := store.Get("a"); ok {
		t.Error("policy still present after file removed")
	}

	write(t, dir, "bad.yaml", "", "true")
	write(t, dir, "b.yaml", "b", "true")
	src.Sync() // load error: store untouched
	if _, ok := store.Get("b"); ok {
		t.Error("store updated despite load error")
	}
}
