package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	text := os.Expand(string(b), func(key string) string { return os.Getenv(key) })
	var cfg Config
	if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config %q: %w", path, err)
	}
	return &cfg, nil
}

func DefaultPath() string {
	if value := os.Getenv("CXHUB_CONFIG"); value != "" {
		return value
	}
	if dir, err := os.UserConfigDir(); err == nil {
		return dir + "/cxhub/config.yaml"
	}
	return "config.yaml"
}

func MissingEnvironmentReferences(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var missing []string
	for _, line := range strings.Split(string(b), "\n") {
		for {
			start := strings.Index(line, "${")
			if start < 0 {
				break
			}
			end := strings.IndexByte(line[start+2:], '}')
			if end < 0 {
				break
			}
			name := line[start+2 : start+2+end]
			if os.Getenv(name) == "" {
				missing = append(missing, name)
			}
			line = line[start+3+end:]
		}
	}
	return uniqueStrings(missing)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			out = append(out, value)
		}
	}
	return out
}
