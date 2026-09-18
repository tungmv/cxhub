package routing

import (
	"fmt"

	"cxhub/internal/config"
	"cxhub/internal/provider"
)

type Target struct {
	Backend  string
	Model    string
	Provider provider.Provider
}

type Router struct {
	profiles map[string][]Target
}

func New(cfg *config.Config, providers map[string]provider.Provider) *Router {
	profiles := make(map[string][]Target, len(cfg.Profiles))
	for name, profile := range cfg.Profiles {
		targets := make([]Target, 0, len(profile.Targets))
		for _, target := range profile.Targets {
			targets = append(targets, Target{Backend: target.Backend, Model: target.Model, Provider: providers[target.Backend]})
		}
		profiles[name] = targets
	}
	return &Router{profiles: profiles}
}

func (r *Router) Resolve(profile string) ([]Target, error) {
	targets, ok := r.profiles[profile]
	if !ok {
		return nil, fmt.Errorf("unknown logical profile %q", profile)
	}
	return append([]Target(nil), targets...), nil
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
