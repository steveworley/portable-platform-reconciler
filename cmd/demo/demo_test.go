package main

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"
)

// TestDemoScenarios runs every scenario end to end; each scenario checks its
// own expectations, so this doubles as the repository's end-to-end test.
func TestDemoScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("scenario demo takes several seconds; skipped with -short")
	}
	var out bytes.Buffer
	logger := slog.New(slog.DiscardHandler)
	cfg := options{scenario: "all", resync: time.Second}
	if err := run(context.Background(), &out, logger, cfg); err != nil {
		t.Fatalf("run() = %v\n%s", err, out.String())
	}
	t.Log(out.String())
}

func TestUnknownScenario(t *testing.T) {
	err := run(context.Background(), &bytes.Buffer{}, slog.New(slog.DiscardHandler), options{scenario: "Z"})
	if err == nil {
		t.Fatal("run() = nil, want error for unknown scenario")
	}
}
