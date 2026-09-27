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

type Registry struct {
	mu       sync.RWMutex
	backends map[string]Backend
}

func New(names []string) *Registry {
	backends := make(map[string]Backend, len(names))
	for _, name := range names {
		backends[name] = Backend{LastCheck: time.Now().UTC(), LastError: "not checked"}
	}
	return &Registry{backends: backends}
}

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

func (r *Registry) Snapshot() map[string]Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := make(map[string]Backend, len(r.backends))
	for name, state := range r.backends {
		copy[name] = state
	}
	return copy
}
