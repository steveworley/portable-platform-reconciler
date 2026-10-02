package alerting

import (
	"reflect"
	"testing"
)

func mr(id string, threshold float64) ManagedRule {
	r := ManagedRule{Rule: Rule{ID: id, Threshold: threshold, Name: id}, Owner: Owner{PolicyName: "p"}}
	return r
}

func TestPlanFor(t *testing.T) {
	tests := []struct {
		name                         string
		desired, actual              []ManagedRule
		wantCreate, wantUpd, wantDel []string
	}{
		{name: "both empty"},
		{name: "all create", desired: []ManagedRule{mr("b", 1), mr("a", 1)}, wantCreate: []string{"a", "b"}},
		{name: "no-op", desired: []ManagedRule{mr("a", 1), mr("b", 2)}, actual: []ManagedRule{mr("b", 2), mr("a", 1)}},
		{name: "update on fingerprint change", desired: []ManagedRule{mr("a", 1), mr("b", 2)}, actual: []ManagedRule{mr("a", 1), mr("b", 9)}, wantUpd: []string{"b"}},
		{name: "delete stale", actual: []ManagedRule{mr("z", 1), mr("y", 1)}, wantDel: []string{"y", "z"}},
		{name: "mixed", desired: []ManagedRule{mr("a", 1), mr("b", 2), mr("c", 3)}, actual: []ManagedRule{mr("b", 5), mr("c", 3), mr("d", 4)},
			wantCreate: []string{"a"}, wantUpd: []string{"b"}, wantDel: []string{"d"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PlanFor(tt.desired, tt.actual)
			for _, c := range []struct {
				what string
				got  []ManagedRule
				want []string
			}{{"Create", p.Create, tt.wantCreate}, {"Update", p.Update, tt.wantUpd}, {"Delete", p.Delete, tt.wantDel}} {
				if got := ids(c.got); !(len(got) == 0 && len(c.want) == 0) && !reflect.DeepEqual(got, c.want) {
					t.Errorf("%s = %v, want %v", c.what, got, c.want)
				}
			}
			wantEmpty := len(tt.wantCreate)+len(tt.wantUpd)+len(tt.wantDel) == 0
			if p.Empty() != wantEmpty {
				t.Errorf("Empty() = %v, want %v", p.Empty(), wantEmpty)
			}
		})
	}
}

func TestPlanUpdateCarriesDesiredContent(t *testing.T) {
	p := PlanFor([]ManagedRule{mr("a", 1)}, []ManagedRule{mr("a", 9)})
	if len(p.Update) != 1 || p.Update[0].Threshold != 1 {
		t.Errorf("Update = %+v, want desired content (threshold 1)", p.Update)
	}
}

func TestPlanDivergentActualIsUpdated(t *testing.T) {
	a := mr("a", 1)
	a.Divergent = true
	if p := PlanFor([]ManagedRule{mr("a", 1)}, []ManagedRule{a}); len(p.Update) != 1 {
		t.Errorf("Plan = %+v, want one update", p)
	}
}
