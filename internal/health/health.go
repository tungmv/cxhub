package health

import (
	"sync"
	"time"
)

type Backend struct {
	Healthy bool `json:"healthy"`
	// Failures is the count of consecutive failed attempts, reset by any
	// success. Routing orders backends by it, so a single success does not
	// erase a backend that has been failing.
	Failures  int       `json:"failures"`
	LastCheck time.Time `json:"last_check,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}

// TargetState tracks one backend/model pair. A model can be unhealthy (for
// example an invalidated account for one provider family) while the backend
// itself is fine, so routing orders and cools down per model rather than per
// backend. A backend-wide failure would otherwise demote every model that
// shares that backend, including healthy ones.
type TargetState struct {
	Healthy bool `json:"healthy"`
	// Failures is the count of consecutive failed attempts, reset by any
	// success.
	Failures  int       `json:"failures"`
	LastCheck time.Time `json:"last_check,omitempty"`
	LastError string    `json:"last_error,omitempty"`
	// CoolingUntil is set after a fallbackable failure when a cooldown is
	// configured for the target. While now < CoolingUntil the target is
	// deprioritized so another model serves the request first.
	CoolingUntil time.Time `json:"cooling_until,omitempty"`
}

// Key identifies a backend/model target; the NUL separator keeps IDs that
// contain a slash or colon from colliding.
func Key(backend, model string) string { return backend + "\x00" + model }

// Cooling reports whether the target is parked until now.
func (t TargetState) Cooling(now time.Time) bool {
	return !t.CoolingUntil.IsZero() && now.Before(t.CoolingUntil)
}

type Registry struct {
	mu       sync.RWMutex
	backends map[string]Backend
	targets  map[string]TargetState
}

func New(names []string) *Registry {
	backends := make(map[string]Backend, len(names))
	for _, name := range names {
		backends[name] = Backend{LastCheck: time.Now().UTC(), LastError: "not checked"}
	}
	return &Registry{backends: backends, targets: make(map[string]TargetState)}
}

// Set records a backend-wide result (used by doctor and the backend summary).
func (r *Registry) Set(name string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.backends[name]
	state := Backend{LastCheck: time.Now().UTC()}
	if err == nil {
		state.Healthy = true
	} else {
		state.Failures = previous.Failures + 1
		state.LastError = err.Error()
	}
	r.backends[name] = state
}

// SetTarget records the result of one backend/model attempt. A positive
// cooldown parks a failed target until the cooldown elapses; a success clears
// both the failure streak and any cooldown.
func (r *Registry) SetTarget(backend, model string, err error, cooldown time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := Key(backend, model)
	previous := r.targets[key]
	state := TargetState{LastCheck: time.Now().UTC()}
	if err == nil {
		state.Healthy = true
	} else {
		state.Failures = previous.Failures + 1
		state.LastError = err.Error()
		if cooldown > 0 {
			state.CoolingUntil = time.Now().UTC().Add(cooldown)
		} else {
			state.CoolingUntil = previous.CoolingUntil
		}
	}
	r.targets[key] = state
}

// Target returns the last known state for a backend/model pair.
func (r *Registry) Target(backend, model string) TargetState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.targets[Key(backend, model)]
}

func (r *Registry) Snapshot() map[string]Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := make(map[string]Backend, len(r.backends))
	for name, state := range r.backends {
		copy[name] = state
	}
	return copy
}

// SnapshotTargets returns a copy of every tracked backend/model state keyed by
// Key(backend, model).
func (r *Registry) SnapshotTargets() map[string]TargetState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := make(map[string]TargetState, len(r.targets))
	for key, state := range r.targets {
		copy[key] = state
	}
	return copy
}
