package health

import (
	"errors"
	"testing"
	"time"
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

func TestSetTargetTracksPerModelSeparately(t *testing.T) {
	registry := New([]string{"cliproxy"})
	registry.SetTarget("cliproxy", "codex", errors.New("invalidated oauth token"), 0)
	registry.SetTarget("cliproxy", "gemini", nil, 0)
	if got := registry.Target("cliproxy", "codex").Failures; got != 1 {
		t.Fatalf("expected codex failure count 1, got %d", got)
	}
	if got := registry.Target("cliproxy", "gemini"); !got.Healthy || got.Failures != 0 {
		t.Fatalf("a healthy model on the same backend must stay healthy, got %+v", got)
	}
}

func TestSetTargetCooldownExpires(t *testing.T) {
	registry := New([]string{"cliproxy"})
	registry.SetTarget("cliproxy", "gpt", errors.New("rate limited"), 50*time.Millisecond)
	state := registry.Target("cliproxy", "gpt")
	if !state.Cooling(time.Now()) {
		t.Fatal("expected target to be cooling during the cooldown window")
	}
	if state.Cooling(time.Now().Add(time.Minute)) {
		t.Fatal("cooldown must expire")
	}
	registry.SetTarget("cliproxy", "gpt", nil, 0)
	if got := registry.Target("cliproxy", "gpt"); !got.Healthy || got.Cooling(time.Now()) {
		t.Fatalf("success must clear cooldown, got %+v", got)
	}
}
