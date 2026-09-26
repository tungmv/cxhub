package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cxhub/internal/config"
	"cxhub/internal/provider"
	"cxhub/internal/responses"
)

func TestSpanDecisionUsesOpenRouterDecisionsAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/alpha/decisions" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("authorization header was not set")
		}
		var payload struct {
			Model     string                     `json:"model"`
			State     map[string]json.RawMessage `json:"state"`
			Questions map[string]struct {
				Type string `json:"type"`
			} `json:"questions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			return
		}
		if payload.Model != "respan/span-01-lite" || len(payload.Questions) != 3 {
			t.Errorf("model/questions = %q/%d", payload.Model, len(payload.Questions))
		}
		for name, question := range payload.Questions {
			if question.Type != "noul" {
				t.Errorf("question %q type = %q", name, question.Type)
			}
		}
		var state struct {
			Input []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"input"`
			Output struct {
				Role string `json:"role"`
			} `json:"output"`
		}
		if err := json.Unmarshal(payload.State["input"], &state.Input); err != nil {
			t.Error(err)
		}
		if err := json.Unmarshal(payload.State["output"], &state.Output); err != nil {
			t.Error(err)
		}
		if len(state.Input) != 1 || state.Input[0].Role != "user" || state.Input[0].Content != "Fix the login race." || state.Output.Role != "assistant" {
			t.Errorf("unexpected decision state: %+v", state)
		}
		_, _ = io.WriteString(w, `{"answers":{"speed":{"type":"noul","noul":0.8},"quality":{"type":"noul","noul":0.1},"high_effort":{"type":"noul","noul":0.2}}}`)
	}))
	defer server.Close()
	decider := &spanDecision{model: "respan/span-01-lite", apiKey: "test-key", endpoint: server.URL + "/api/alpha/decisions", client: server.Client()}
	scores, err := decider.Score(context.Background(), "Fix the login race.")
	if err != nil {
		t.Fatal(err)
	}
	if scores.Speed != .8 || scores.Quality != .1 || scores.HighEffort != .2 {
		t.Fatalf("scores = %+v", scores)
	}
}

type fixedDecision decisionScores

func (f fixedDecision) Score(context.Context, string) (decisionScores, error) {
	return decisionScores(f), nil
}

func TestAutomaticRoutePicksTierAndEffort(t *testing.T) {
	cfg := &config.Config{
		Decision: config.DecisionConfig{DefaultProfile: "default"},
		Profiles: map[string]config.ProfileConfig{
			"fast":    {AutoTier: "speed"},
			"default": {AutoTier: "balanced"},
			"strong":  {AutoTier: "quality"},
		},
	}
	state := &runtimeState{config: cfg, decider: fixedDecision{Speed: .84, Quality: .03, HighEffort: .02}}
	s := &Server{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	req, _ := responses.Parse([]byte(`{"model":"auto","input":"What is 2+2?"}`))
	profile, effort, _, err := s.automaticRoute(context.Background(), state, req)
	if err != nil {
		t.Fatal(err)
	}
	if profile != "fast" || effort != "low" {
		t.Fatalf("route = %q/%q", profile, effort)
	}
	state.decider = fixedDecision{Speed: .1, Quality: .9, HighEffort: .85}
	profile, effort, _, err = s.automaticRoute(context.Background(), state, req)
	if err != nil {
		t.Fatal(err)
	}
	if profile != "strong" || effort != "high" {
		t.Fatalf("route = %q/%q", profile, effort)
	}
}

func TestAutoRequestUsesChosenProfileAndEffort(t *testing.T) {
	var gotModel, gotEffort string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model     string `json:"model"`
			Reasoning struct {
				Effort string `json:"effort"`
			} `json:"reasoning"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		gotModel, gotEffort = body.Model, body.Reasoning.Effort
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"ok","model":%q,"output":[]}`, body.Model)
	}))
	defer upstream.Close()
	backend := config.BackendConfig{Type: "openai-compatible", BaseURL: upstream.URL + "/v1"}
	cfg := &config.Config{
		Backends: map[string]config.BackendConfig{"one": backend},
		Profiles: map[string]config.ProfileConfig{
			"fast":    {AutoTier: "speed", Targets: []config.TargetConfig{{Backend: "one", Model: "fast-model"}}},
			"default": {AutoTier: "balanced", Targets: []config.TargetConfig{{Backend: "one", Model: "default-model"}}},
		},
		Decision: config.DecisionConfig{DefaultProfile: "default"},
	}
	backendProvider := provider.NewOpenAICompatible("one", backend, upstream.Client())
	s := NewServer(cfg, map[string]provider.Provider{"one": backendProvider}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.state.Load().decider = fixedDecision{Speed: .9, Quality: .05, HighEffort: .03}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	resp, err := http.Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"auto","input":"What is 2+2?","reasoning":{"summary":"auto"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || gotModel != "fast-model" || gotEffort != "low" {
		t.Fatalf("HTTP=%d upstream model/effort=%q/%q", resp.StatusCode, gotModel, gotEffort)
	}
}

func TestDecisionTextExcludesNonUserAndNonTextPayloads(t *testing.T) {
	req, err := responses.Parse([]byte(`{"model":"auto","instructions":"private system text","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Review this diff"},{"type":"input_image","image_url":"data:secret"}]},{"type":"function_call_output","output":"secret tool result"},{"type":"message","role":"assistant","content":"secret history"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	got := decisionText(req)
	if !strings.Contains(got, "Review this diff") || !strings.Contains(got, "[multimodal input]") {
		t.Fatalf("missing task text: %q", got)
	}
	for _, secret := range []string{"private system text", "data:secret", "secret tool result", "secret history"} {
		if strings.Contains(got, secret) {
			t.Fatalf("decision text leaked %q: %q", secret, got)
		}
	}
}
