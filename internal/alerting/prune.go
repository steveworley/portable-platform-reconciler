package alerting

import (
	"context"
	"errors"
	"fmt"
)

// Lister is an optional Backend capability: list every rule tagged as managed
// by this controller, whatever its owner. Rules without our ownership tags
// must never be returned.
type Lister interface {
	ListAll(ctx context.Context) ([]ManagedRule, error)
}

// ErrPruneUnsupported is returned by Prune for a backend that is not a Lister.
var ErrPruneUnsupported = errors.New("alert backend cannot list all managed rules")

// PruneReport describes a Prune run.
type PruneReport struct {
	// DryRun is true when nothing was deleted.
	DryRun bool
	// Pruned are the rules deleted, or in a dry run the rules that would be.
	Pruned []ManagedRule
	// Kept is the number of managed rules whose owner was kept.
	Kept int
}

// Prune deletes the managed rules in backend whose owner keep rejects. A nil
// keep keeps nothing, which empties a backend of everything this controller
// created (e.g. the old backend after a migration). With a keep that accepts
// the live policies' owners it is an orphan sweep. In a dry run it only
// reports. Rules not tagged as managed are never listed, so never touched.
// Deletion continues past failures; Pruned holds only successful deletes and
// the returned error joins the failures.
func Prune(ctx context.Context, backend Backend, keep func(Owner) bool, dryRun bool) (PruneReport, error) {
	rep := PruneReport{DryRun: dryRun}
	l, ok := backend.(Lister)
	if !ok {
		return rep, fmt.Errorf("%w: %s", ErrPruneUnsupported, backend.Name())
	}
	rules, err := l.ListAll(ctx)
	if err != nil {
		return rep, fmt.Errorf("list managed rules: %w", err)
	}
	var errs []error
	for _, r := range rules {
		if keep != nil && keep(r.Owner) {
			rep.Kept++
			continue
		}
		if !dryRun {
			if err := backend.Delete(ctx, r); err != nil {
				errs = append(errs, fmt.Errorf("delete %s: %w", r.ID, err))
				continue
			}
		}
		rep.Pruned = append(rep.Pruned, r)
	}
	return rep, errors.Join(errs...)
}
