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
