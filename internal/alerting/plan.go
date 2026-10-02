package alerting

import "sort"

// Plan is the set of writes that moves a backend from actual to desired.
type Plan struct {
	Create []ManagedRule
	Update []ManagedRule
	Delete []ManagedRule
}

// Empty reports whether the plan has no writes.
func (p Plan) Empty() bool { return len(p.Create)+len(p.Update)+len(p.Delete) == 0 }

// PlanFor diffs desired against actual, keyed by rule ID. Rules only in desired
// are created, rules whose fingerprint differs are updated (with the desired
// content), and rules only in actual are deleted. actual must contain only
// rules this controller owns (Backend.List guarantees that). Each list is
// sorted by rule ID.
func PlanFor(desired, actual []ManagedRule) Plan {
	want := make(map[string]ManagedRule, len(desired))
	for _, r := range desired {
		want[r.ID] = r
	}
	have := make(map[string]ManagedRule, len(actual))
	for _, r := range actual {
		have[r.ID] = r
	}
	var p Plan
	for id, d := range want {
		a, ok := have[id]
		switch {
		case !ok:
			p.Create = append(p.Create, d)
		case a.Fingerprint() != d.Fingerprint():
			p.Update = append(p.Update, d)
		}
	}
	for id, a := range have {
		if _, ok := want[id]; !ok {
			p.Delete = append(p.Delete, a)
		}
	}
	for _, l := range [][]ManagedRule{p.Create, p.Update, p.Delete} {
		sort.Slice(l, func(i, j int) bool { return l[i].ID < l[j].ID })
	}
	return p
}
