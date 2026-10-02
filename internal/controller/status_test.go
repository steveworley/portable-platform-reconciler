package controller

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStatusStoreTransitionTime(t *testing.T) {
	s := NewStatusStore()
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return clock }

	mk := func(status ConditionStatus) PolicyStatus {
		return PolicyStatus{PolicyName: "p", Phase: PhaseReady, Conditions: []Condition{{Type: ConditionReconciled, Status: status}}}
	}
	s.Set(mk(ConditionTrue))
	t0 := clock

	clock = clock.Add(time.Minute)
	s.Set(mk(ConditionTrue))
	got, _ := s.Get("p")
	if c, _ := got.Condition(ConditionReconciled); !c.LastTransitionTime.Equal(t0) {
		t.Errorf("transition time changed without status change: %v, want %v", c.LastTransitionTime, t0)
	}

	clock = clock.Add(time.Minute)
	s.Set(mk(ConditionFalse))
	got, _ = s.Get("p")
	if c, _ := got.Condition(ConditionReconciled); !c.LastTransitionTime.Equal(clock) {
		t.Errorf("transition time = %v, want %v", c.LastTransitionTime, clock)
	}
}

func TestStatusStoreListGetDelete(t *testing.T) {
	s := NewStatusStore()
	s.Set(PolicyStatus{PolicyName: "b"})
	s.Set(PolicyStatus{PolicyName: "a"})
	l := s.List()
	if len(l) != 2 || l[0].PolicyName != "a" {
		t.Errorf("List() = %+v", l)
	}
	if _, ok := s.Get("zzz"); ok {
		t.Error("Get(missing) ok = true")
	}
	s.Delete("a")
	if _, ok := s.Get("a"); ok {
		t.Error("Get(deleted) ok = true")
	}
}

func TestStatusStoreReturnsCopies(t *testing.T) {
	s := NewStatusStore()
	s.Set(PolicyStatus{PolicyName: "p", Conditions: []Condition{{Type: ConditionReconciled, Status: ConditionTrue}}})
	got, _ := s.Get("p")
	got.Conditions[0].Status = ConditionFalse
	again, _ := s.Get("p")
	if again.Conditions[0].Status != ConditionTrue {
		t.Error("store state mutated through returned copy")
	}
}

func TestPolicyStatusJSON(t *testing.T) {
	b, err := json.Marshal(PolicyStatus{PolicyName: "p", Phase: PhaseReady, ObservedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"phase":"Ready"`, `"observedGeneration":1`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("json %s missing %s", b, want)
		}
	}
}
