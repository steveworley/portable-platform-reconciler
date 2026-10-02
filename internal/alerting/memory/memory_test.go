package memory

import (
	"context"
	"testing"

	"github.com/example/portable-platform-reconciler/internal/alerting"
)

func TestBackend(t *testing.T) {
	ctx := context.Background()
	owner := alerting.Owner{PolicyName: "p", WorkloadKey: "fake:w", Runtime: "fake"}
	r := alerting.ManagedRule{Rule: alerting.Rule{ID: "r1", Threshold: 1}, Owner: owner}
	b := New(WithName("a"))
	if b.Name() != "a" {
		t.Errorf("Name() = %q", b.Name())
	}
	if err := b.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := b.Create(ctx, r); err == nil {
		t.Error("duplicate Create must fail")
	}
	if got, _ := b.List(ctx, alerting.Owner{PolicyName: "other"}); len(got) != 0 {
		t.Errorf("List(other owner) = %v", got)
	}
	if b.Tamper("r1", 5) != 1 {
		t.Error("Tamper found nothing")
	}
	if got, _ := b.List(ctx, owner); len(got) != 1 || got[0].Threshold != 5 {
		t.Errorf("List() = %v", got)
	}
	if err := b.Update(ctx, r); err != nil {
		t.Fatal(err)
	}
	if b.Remove("r1") != 1 || len(b.Rules()) != 0 {
		t.Error("Remove did not remove")
	}
	if err := b.Delete(ctx, r); err == nil {
		t.Error("Delete of missing rule must fail")
	}
	if got := b.Writes(); got != (Writes{Creates: 1, Updates: 1}) {
		t.Errorf("Writes() = %+v (tamper/remove must not count)", got)
	}
}
