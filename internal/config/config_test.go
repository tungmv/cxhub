package config

import "testing"

func TestValidateRejectsBadTarget(t *testing.T) {
	cfg := &Config{
		Backends: map[string]BackendConfig{"one": {Type: "openai-compatible", BaseURL: "http://127.0.0.1:1/v1"}},
		Profiles: map[string]ProfileConfig{"bad-profile": {Targets: []TargetConfig{{Backend: "missing", Model: "m"}}}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing backend error")
	}
}

func TestValidateRejectsDuplicateTarget(t *testing.T) {
	cfg := &Config{
		Backends: map[string]BackendConfig{"one": {Type: "openai-compatible", BaseURL: "http://127.0.0.1:1/v1"}},
		Profiles: map[string]ProfileConfig{"duplicate-profile": {Targets: []TargetConfig{{Backend: "one", Model: "m"}, {Backend: "one", Model: "m"}}}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected duplicate target error")
	}
}

func TestRequestTimeoutDuration(t *testing.T) {
	cfg := Config{Gateway: GatewayConfig{RequestTimeout: "15s"}}
	if got := cfg.RequestTimeoutDuration().String(); got != "15s" {
		t.Fatalf("timeout = %s", got)
	}
}

func TestValidateRejectsBadTargetTimeout(t *testing.T) {
	cfg := &Config{
		Backends: map[string]BackendConfig{"one": {Type: "openai-compatible", BaseURL: "http://127.0.0.1:1/v1"}},
		Profiles: map[string]ProfileConfig{"bad-timeout": {Targets: []TargetConfig{{Backend: "one", Model: "m", Timeout: "fast"}}}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid timeout error")
	}
	cfg.Profiles["bad-timeout"].Targets[0].Timeout = "0s"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected non-positive timeout error")
	}
}

func TestTargetTimeoutDuration(t *testing.T) {
	if got := (TargetConfig{Timeout: "45s"}).TimeoutDuration(); got.String() != "45s" {
		t.Fatalf("timeout = %s", got)
	}
	for _, invalid := range []string{"", "  ", "fast", "-5s", "0s"} {
		if got := (TargetConfig{Timeout: invalid}).TimeoutDuration(); got != 0 {
			t.Fatalf("timeout %q parsed as %s", invalid, got)
		}
	}
}
