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
	Logging  LoggingConfig            `yaml:"logging"`
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
	Targets []TargetConfig `yaml:"targets"`
}

type TargetConfig struct {
	Backend string `yaml:"backend"`
	Model   string `yaml:"model"`
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
		if b.Type != "openai-compatible" {
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
	profileNames := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		profileNames = append(profileNames, name)
	}
	sort.Strings(profileNames)
	for _, name := range profileNames {
		p := c.Profiles[name]
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("profile name must not be empty")
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
			key := target.Backend + "\x00" + target.Model
			if _, ok := seen[key]; ok {
				return fmt.Errorf("profile %q contains duplicate target %s/%s", name, target.Backend, target.Model)
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}
