package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
)

// runPrune implements the opt-in cleanup of an alert backend: after a
// migration (--keep-none on the old backend) or as an orphan sweep (--orphans).
func runPrune(ctx context.Context, o options, logger *slog.Logger, out io.Writer) error {
	if o.keepNone && o.orphans {
		return errors.New("--keep-none and --orphans are mutually exclusive")
	}
	cfg, err := config.Load(o.configPath, nil)
	if err != nil {
		return err
	}
	name := o.backend
	if name == "" {
		name = cfg.Alerting.Backend
	}
	backend, err := alerting.New(name, cfg, logger)
	if err != nil {
		return fmt.Errorf("create alert backend: %w", err)
	}

	var keep func(alerting.Owner) bool
	mode := "keep-none"
	if o.orphans {
		mode = "orphans"
		policies, err := policy.LoadDir(cfg.Policies.Dir, policy.WithTargetValidator(runtime.ValidateTarget))
		if err != nil {
			return fmt.Errorf("load policies: %w", err)
		}
		live := map[string]struct{}{}
		for _, p := range policies {
			live[p.Metadata.Name] = struct{}{}
		}
		keep = func(ow alerting.Owner) bool { _, ok := live[ow.PolicyName]; return ok }
	}

	rep, err := alerting.Prune(ctx, backend, keep, o.dryRun)
	verb := "deleted"
	if rep.DryRun {
		verb = "would delete"
	}
	fmt.Fprintf(out, "backend=%s mode=%s dryRun=%t: %s %d rule(s), kept %d\n", backend.Name(), mode, rep.DryRun, verb, len(rep.Pruned), rep.Kept)
	for _, r := range rep.Pruned {
		fmt.Fprintf(out, "  %s  policy=%s workload=%s\n", r.ID, r.Owner.PolicyName, r.Owner.WorkloadKey)
	}
	if rep.DryRun {
		fmt.Fprintln(out, "dry run: re-run with --dry-run=false to delete")
	}
	return err
}
