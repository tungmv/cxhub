package health

import (
	"errors"
	"testing"
)

func TestSetCountsConsecutiveFailures(t *testing.T) {
	registry := New([]string{"a"})
	registry.Set("a", nil)
	registry.Set("a", errors.New("upstream down"))
	registry.Set("a", errors.New("upstream down"))
	if got := registry.Snapshot()["a"].Failures; got != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", got)
	}
	registry.Set("a", nil)
	state := registry.Snapshot()["a"]
	if !state.Healthy || state.Failures != 0 || state.LastError != "" {
		t.Fatalf("success must clear the failure streak, got %+v", state)
	}
}
