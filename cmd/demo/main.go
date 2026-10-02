// Command demo runs the reconciliation scenarios from docs/poc-spec.md
// (A-D, plus the alerting scenario E and policy deletion F) in-process, using
// the real engine, adapters and event sources against fakes: a simulated ECS,
// a fake Kubernetes clientset and in-memory alert backends. It needs no
// cluster, AWS account or New Relic account, checks its own expectations and
// exits non-zero if any fail, so it doubles as an end-to-end smoke test.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	scenario := flag.String("scenario", "all", "scenario to run: A|B|C|D|E|F|all")
	verbose := flag.Bool("v", false, "show controller info logs")
	examples := flag.String("examples", "", "path to the examples directory (default: found by walking up from the working directory)")
	resync := flag.Duration("resync", 2*time.Second, "resync interval used for the scenarios")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	level := slog.LevelWarn
	if *verbose {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := run(ctx, os.Stdout, logger, options{scenario: *scenario, examples: *examples, resync: *resync}); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

type options struct {
	scenario string
	examples string
	resync   time.Duration
}

var scenarioOrder = []string{"A", "B", "C", "D", "E", "F"}

// run executes the selected scenarios and returns an error if any check fails.
func run(ctx context.Context, out io.Writer, logger *slog.Logger, cfg options) error {
	sel := strings.ToUpper(cfg.scenario)
	var ids []string
	if sel == "ALL" {
		ids = scenarioOrder
	} else if _, ok := scenarios[sel]; ok {
		ids = []string{sel}
	} else {
		return fmt.Errorf("unknown scenario %q (want A-F or all)", cfg.scenario)
	}
	dir := cfg.examples
	if dir == "" {
		var err error
		if dir, err = findExamples(); err != nil {
			return err
		}
	}
	e := newEnv(out, logger, dir, cfg.resync)
	defer e.close()

	e.printf("Portable platform reconciler: scenario demo (resync every %s)\n", cfg.resync)
	e.printf("Everything below runs in-process against fakes; the engine and adapters are the real ones.\n")

	var failed []string
	for _, id := range ids {
		start := e.failures()
		if err := scenarios[id](ctx, e); err != nil {
			e.printf("  [ERROR] scenario %s: %v\n", id, err)
			e.fail(fmt.Sprintf("scenario %s: %v", id, err))
		}
		if e.failures() > start {
			failed = append(failed, id)
		}
	}

	e.printf("\n=== Summary ===\n")
	for _, id := range ids {
		status := "PASS"
		for _, f := range failed {
			if f == id {
				status = "FAIL"
			}
		}
		e.printf("  Scenario %s: %s\n", id, status)
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d scenario(s) failed: %s", len(failed), strings.Join(failed, ", "))
	}
	e.printf("All checks passed.\n")
	return nil
}
