package routing

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"cxhub/internal/config"
	"cxhub/internal/provider"
)

type Target struct {
	// Profile is the logical profile this target was defined in. It can differ
	// from the requested profile when the target was reached through a
	// same-level fallback.
	Profile  string
	Backend  string
	Model    string
	Timeout  time.Duration
	Retries  int
	Backoff  time.Duration
	Provider provider.Provider
}

type Router struct {
	profiles        map[string][]Target
	levels          map[string][]string
	ownLevel        map[string]string
	profilePriority map[string]int
}

func New(cfg *config.Config, providers map[string]provider.Provider) *Router {
	names := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	profiles := make(map[string][]Target, len(cfg.Profiles))
	levels := make(map[string][]string)
	ownLevel := make(map[string]string, len(cfg.Profiles))
	profilePriority := make(map[string]int, len(cfg.Profiles))
	for _, name := range names {
		profile := cfg.Profiles[name]
		targets := make([]Target, 0, len(profile.Targets))
		for _, target := range profile.Targets {
			targets = append(targets, Target{
				Profile:  name,
				Backend:  target.Backend,
				Model:    target.Model,
				Timeout:  target.TimeoutDuration(),
				Retries:  profile.EffectiveRetries(),
				Backoff:  profile.RetryBackoffDuration(),
				Provider: providers[target.Backend],
			})
		}
		profiles[name] = targets
		profilePriority[name] = profile.Priority
		if level := strings.TrimSpace(profile.Level); level != "" {
			levels[level] = append(levels[level], name)
			ownLevel[name] = level
		}
	}
	return &Router{profiles: profiles, levels: levels, ownLevel: ownLevel, profilePriority: profilePriority}
}

// Resolve returns the requested profile's targets followed by deterministic
// fallback targets from other profiles that declare the same level. Profiles
// without a level never gain cross-profile fallback. Duplicate backend/model
// pairs are attempted only once.
func (r *Router) Resolve(profile string) ([]Target, error) {
	primary, ok := r.profiles[profile]
	if !ok {
		return nil, fmt.Errorf("unknown logical profile %q", profile)
	}
	resolved := make([]Target, 0, len(primary))
	seen := make(map[string]struct{}, len(primary))
	append := func(target Target) {
		key := target.Backend + "\x00" + target.Model
		if _, duplicate := seen[key]; duplicate {
			return
		}
		seen[key] = struct{}{}
		resolved = append(resolved, target)
	}
	for _, target := range primary {
		append(target)
	}
	level := r.ownLevel[profile]
	for _, peer := range r.levelPeers(level, profile) {
		for _, target := range r.profiles[peer] {
			append(target)
		}
	}
	return resolved, nil
}

// levelPeers returns the other profiles of a level ordered by ascending
// priority, then profile name, so high-priority profiles (e.g. the
// orchestrator) are preferred fallbacks.
func (r *Router) levelPeers(level, self string) []string {
	peers := make([]string, 0, len(r.levels[level]))
	for _, peer := range r.levels[level] {
		if peer != self {
			peers = append(peers, peer)
		}
	}
	sort.Slice(peers, func(i, j int) bool {
		left, right := r.profilePriority[peers[i]], r.profilePriority[peers[j]]
		if left != right {
			return left < right
		}
		return peers[i] < peers[j]
	})
	return peers
}

func (r *Router) Profiles() []string {
	result := make([]string, 0, len(r.profiles))
	for profile := range r.profiles {
		result = append(result, profile)
	}
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j] < result[j-1]; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result
}
