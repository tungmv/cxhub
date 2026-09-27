package config

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Config struct {
	Gateway  GatewayConfig            `yaml:"gateway"`
	Backends map[string]BackendConfig `yaml:"backends"`
	Profiles map[string]ProfileConfig `yaml:"profiles"`
	Decision DecisionConfig           `yaml:"decision"`
	Logging  LoggingConfig            `yaml:"logging"`
}

type DecisionConfig struct {
	Backend string `yaml:"backend"`
	Model   string `yaml:"model"`
	// DefaultProfile routes the request when selection fails.
	DefaultProfile string `yaml:"default_profile"`
	// Candidates are the logical models selection may choose from. When empty,
	// every configured profile is a candidate.
	Candidates []string `yaml:"candidates"`
	Timeout    string   `yaml:"timeout"`
}

// CandidateProfiles returns the profiles selection may choose from, sorted by
// name. Validate defaults empty Candidates to every profile.
func (d DecisionConfig) CandidateProfiles() []string {
	result := append([]string(nil), d.Candidates...)
	sort.Strings(result)
	return result
}

func (d DecisionConfig) TimeoutDuration() time.Duration {
	if strings.TrimSpace(d.Timeout) == "" {
		return 2 * time.Second
	}
	duration, err := time.ParseDuration(d.Timeout)
	if err != nil || duration <= 0 {
		return 2 * time.Second
	}
	return duration
}

type GatewayConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	RequestTimeout string `yaml:"request_timeout"`
}

type BackendConfig struct {
	Type           string            `yaml:"type"`
	BaseURL        string            `yaml:"base_url"`
	APIKey         string            `yaml:"api_key"`
	RequiresAPIKey bool              `yaml:"requires_api_key"`
	Headers        map[string]string `yaml:"headers"`
}

type ProfileConfig struct {
	// Level groups profiles into a fallback tier. Profiles sharing the same
	// non-empty level serve as each other's fallback targets.
	Level string `yaml:"level"`
	// Priority orders same-level fallback: lower numbers are tried first.
	Priority int `yaml:"priority"`
	// Retries is the total number of passes over the attempt list (1 = no
	// retry, 0 = default of 1).
	Retries int `yaml:"retries"`
	// RetryBackoff is the wait between retry passes.
	RetryBackoff string         `yaml:"retry_backoff"`
	Targets      []TargetConfig `yaml:"targets"`
}

const maxRetries = 100

// EffectiveRetries returns the configured number of passes over the attempt
// list, defaulting to 1 and capped at maxRetries.
func (p ProfileConfig) EffectiveRetries() int {
	if p.Retries < 1 {
		return 1
	}
	if p.Retries > maxRetries {
		return maxRetries
	}
	return p.Retries
}

// RetryBackoffDuration returns the wait between retry passes, or 0 when unset.
func (p ProfileConfig) RetryBackoffDuration() time.Duration {
	if strings.TrimSpace(p.RetryBackoff) == "" {
		return 0
	}
	duration, err := time.ParseDuration(p.RetryBackoff)
	if err != nil || duration <= 0 {
		return 0
	}
	return duration
}

type TargetConfig struct {
	Backend string `yaml:"backend"`
	Model   string `yaml:"model"`
	Timeout string `yaml:"timeout"`
}

// TimeoutDuration returns the per-attempt timeout, or 0 when no timeout is set.
func (t TargetConfig) TimeoutDuration() time.Duration {
	if strings.TrimSpace(t.Timeout) == "" {
		return 0
	}
	duration, err := time.ParseDuration(t.Timeout)
	if err != nil || duration <= 0 {
		return 0
	}
	return duration
}

type LoggingConfig struct {
	Verbose bool `yaml:"verbose"`
}

func (c *Config) Address() string {
	host := c.Gateway.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Gateway.Port
	if port == 0 {
		port = 8787
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func (c *Config) RequestTimeoutDuration() time.Duration {
	if c.Gateway.RequestTimeout == "" {
		return 10 * time.Minute
	}
	duration, err := time.ParseDuration(c.Gateway.RequestTimeout)
	if err != nil || duration <= 0 {
		return 10 * time.Minute
	}
	return duration
}

func (c *Config) Validate() error {
	if c.Gateway.Host == "" {
		c.Gateway.Host = "127.0.0.1"
	}
	if c.Gateway.Port == 0 {
		c.Gateway.Port = 8787
	}
	if c.Gateway.Port < 1 || c.Gateway.Port > 65535 {
		return fmt.Errorf("gateway.port must be between 1 and 65535")
	}
	if c.Gateway.RequestTimeout != "" {
		duration, err := time.ParseDuration(c.Gateway.RequestTimeout)
		if err != nil || duration <= 0 {
			return fmt.Errorf("gateway.request_timeout must be a positive Go duration, got %q", c.Gateway.RequestTimeout)
		}
	}
	if c.Gateway.Host != "127.0.0.1" && c.Gateway.Host != "localhost" && c.Gateway.Host != "::1" {
		// Non-loopback binding is explicit and valid, but is intentionally visible to callers.
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("at least one backend is required")
	}
	backendNames := make([]string, 0, len(c.Backends))
	for name := range c.Backends {
		backendNames = append(backendNames, name)
	}
	sort.Strings(backendNames)
	for _, name := range backendNames {
		b := c.Backends[name]
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("backend name must not be empty")
		}
		if b.Type != "openai-compatible" && b.Type != "openai-chat-compatible" {
			return fmt.Errorf("backend %q has unsupported type %q", name, b.Type)
		}
		u, err := url.Parse(b.BaseURL)
		if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("backend %q has invalid base_url %q", name, b.BaseURL)
		}
		if b.RequiresAPIKey && strings.TrimSpace(b.APIKey) == "" {
			return fmt.Errorf("backend %q requires an API key but api_key is empty", name)
		}
	}
	if len(c.Profiles) == 0 {
		return fmt.Errorf("at least one profile is required")
	}
	decisionConfigured := c.Decision.Backend != "" || c.Decision.Model != "" || c.Decision.DefaultProfile != "" || c.Decision.Timeout != ""
	if decisionConfigured {
		if c.Decision.Backend == "" {
			return fmt.Errorf("decision.backend is required")
		}
		backend, ok := c.Backends[c.Decision.Backend]
		if !ok {
			return fmt.Errorf("decision.backend %q does not exist", c.Decision.Backend)
		}
		if backend.APIKey == "" {
			return fmt.Errorf("decision backend %q requires an API key", c.Decision.Backend)
		}
		decisionURL, _ := url.Parse(backend.BaseURL)
		if !strings.EqualFold(decisionURL.Hostname(), "openrouter.ai") {
			return fmt.Errorf("decision.backend %q must point to openrouter.ai", c.Decision.Backend)
		}
		if c.Decision.Model == "" {
			c.Decision.Model = "respan/span-01-lite"
		}
		if c.Decision.DefaultProfile == "" {
			return fmt.Errorf("decision.default_profile is required")
		}
		if _, ok := c.Profiles[c.Decision.DefaultProfile]; !ok {
			return fmt.Errorf("decision.default_profile %q does not exist", c.Decision.DefaultProfile)
		}
		if strings.TrimSpace(c.Decision.Timeout) != "" {
			duration, err := time.ParseDuration(c.Decision.Timeout)
			if err != nil || duration <= 0 {
				return fmt.Errorf("decision.timeout must be a positive Go duration, got %q", c.Decision.Timeout)
			}
		}
	}
	profileNames := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		profileNames = append(profileNames, name)
	}
	sort.Strings(profileNames)
	if decisionConfigured && len(c.Decision.Candidates) == 0 {
		c.Decision.Candidates = profileNames
	}
	for _, candidate := range c.Decision.Candidates {
		if _, ok := c.Profiles[candidate]; !ok {
			return fmt.Errorf("decision.candidates %q does not exist", candidate)
		}
	}
	for _, name := range profileNames {
		p := c.Profiles[name]
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("profile name must not be empty")
		}
		if name == "auto" {
			return fmt.Errorf("profile name %q is reserved for automatic routing", name)
		}
		if len(p.Targets) == 0 {
			return fmt.Errorf("profile %q must have at least one target", name)
		}
		seen := make(map[string]struct{}, len(p.Targets))
		for i, target := range p.Targets {
			if _, ok := c.Backends[target.Backend]; !ok {
				return fmt.Errorf("profile %q target %d references backend %q, which does not exist", name, i+1, target.Backend)
			}
			if strings.TrimSpace(target.Model) == "" {
				return fmt.Errorf("profile %q target %d is missing model", name, i+1)
			}
			if strings.TrimSpace(target.Timeout) != "" {
				duration, err := time.ParseDuration(target.Timeout)
				if err != nil || duration <= 0 {
					return fmt.Errorf("profile %q target %d has invalid timeout %q: must be a positive Go duration", name, i+1, target.Timeout)
				}
			}
			key := target.Backend + "\x00" + target.Model
			if _, ok := seen[key]; ok {
				return fmt.Errorf("profile %q contains duplicate target %s/%s", name, target.Backend, target.Model)
			}
			seen[key] = struct{}{}
		}
		if p.Retries < 0 || p.Retries > maxRetries {
			return fmt.Errorf("profile %q retries must be between 0 and %d", name, maxRetries)
		}
		if strings.TrimSpace(p.RetryBackoff) != "" {
			duration, err := time.ParseDuration(p.RetryBackoff)
			if err != nil || duration <= 0 {
				return fmt.Errorf("profile %q has invalid retry_backoff %q: must be a positive Go duration", name, p.RetryBackoff)
			}
		}
	}
	return nil
}
