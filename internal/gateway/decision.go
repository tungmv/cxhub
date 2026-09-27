package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cxhub/internal/config"
	"cxhub/internal/responses"
)

const openRouterDecisionsURL = "https://openrouter.ai/api/alpha/decisions"

// decisionScores holds one noul score per candidate model plus the two
// reasoning-effort answers.
type decisionScores struct {
	Models     map[string]float64
	HighEffort float64
	LowEffort  float64
}

type decisionScorer interface {
	Score(context.Context, string, []string) (decisionScores, error)
}

type spanDecision struct {
	model    string
	apiKey   string
	endpoint string
	client   *http.Client
}

func newSpanDecision(cfg config.DecisionConfig, backend config.BackendConfig) *spanDecision {
	return &spanDecision{
		model:    cfg.Model,
		apiKey:   backend.APIKey,
		endpoint: openRouterDecisionsURL,
		client:   &http.Client{Timeout: cfg.TimeoutDuration()},
	}
}

// modelQuestionPrefix keeps per-model answers addressable in the flat answer
// map without colliding with the effort questions.
const modelQuestionPrefix = "model:"

func (d *spanDecision) Score(ctx context.Context, task string, candidates []string) (decisionScores, error) {
	if strings.TrimSpace(task) == "" {
		return decisionScores{}, fmt.Errorf("request has no text to classify")
	}
	if len(candidates) == 0 {
		return decisionScores{}, fmt.Errorf("automatic routing has no candidate models")
	}
	questions := map[string]any{
		"high_effort": map[string]string{"type": "noul", "instructions": "A high reasoning effort is needed to answer the user request well."},
		"low_effort":  map[string]string{"type": "noul", "instructions": "The user request can be answered well with little reasoning."},
	}
	for _, candidate := range candidates {
		questions[modelQuestionPrefix+candidate] = map[string]string{
			"type":         "noul",
			"instructions": fmt.Sprintf("The %q model is the best choice to answer this user request.", candidate),
		}
	}
	payload := map[string]any{
		"model": d.model,
		"state": map[string]any{
			"input":  []map[string]string{{"role": "user", "content": task}},
			"output": map[string]string{"role": "assistant", "content": ""},
		},
		"questions": questions,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return decisionScores{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(body))
	if err != nil {
		return decisionScores{}, err
	}
	req.Header.Set("Authorization", "Bearer "+d.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return decisionScores{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return decisionScores{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decisionScores{}, fmt.Errorf("OpenRouter Decisions API returned HTTP %d", resp.StatusCode)
	}
	var result struct {
		Answers map[string]struct {
			Type string  `json:"type"`
			Noul float64 `json:"noul"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return decisionScores{}, fmt.Errorf("decode OpenRouter decision: %w", err)
	}
	read := func(name string) (float64, error) {
		answer, ok := result.Answers[name]
		if !ok || answer.Type != "noul" || answer.Noul < 0 || answer.Noul > 1 {
			return 0, fmt.Errorf("OpenRouter returned an invalid %q decision", name)
		}
		return answer.Noul, nil
	}
	var scores decisionScores
	scores.Models = make(map[string]float64, len(candidates))
	for _, candidate := range candidates {
		score, err := read(modelQuestionPrefix + candidate)
		if err != nil {
			return decisionScores{}, err
		}
		scores.Models[candidate] = score
	}
	if scores.HighEffort, err = read("high_effort"); err != nil {
		return decisionScores{}, err
	}
	if scores.LowEffort, err = read("low_effort"); err != nil {
		return decisionScores{}, err
	}
	return scores, nil
}

func (s *Server) automaticRoute(ctx context.Context, state *runtimeState, req *responses.Request) (string, string, decisionScores, error) {
	if state.decider == nil {
		return "", "", decisionScores{}, fmt.Errorf("automatic routing is not configured")
	}
	candidates := state.config.Decision.CandidateProfiles()
	scores, err := state.decider.Score(ctx, decisionText(req), candidates)
	if err != nil {
		return "", "", scores, err
	}
	// ponytail: ties resolve to the first candidate in sorted order and effort uses fixed thresholds, so selection stays deterministic; tune from observed outcomes if routing misfires.
	selected := ""
	best := -1.0
	for _, candidate := range candidates {
		if score := scores.Models[candidate]; selected == "" || score > best {
			selected, best = candidate, score
		}
	}
	if selected == "" {
		selected = state.config.Decision.DefaultProfile
	}
	effort := "medium"
	if scores.HighEffort >= 0.6 {
		effort = "high"
	} else if scores.LowEffort >= 0.75 {
		effort = "low"
	}
	return selected, effort, scores, nil
}

func decisionText(req *responses.Request) string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(req.Body, &fields) != nil {
		return ""
	}
	input := fields["input"]
	var plain string
	if json.Unmarshal(input, &plain) == nil {
		return truncateDecisionText(plain)
	} else {
		var items []json.RawMessage
		if json.Unmarshal(input, &items) == nil {
			lastUserMessage := ""
			for _, raw := range items {
				var item map[string]json.RawMessage
				if json.Unmarshal(raw, &item) != nil {
					continue
				}
				var role, kind string
				_ = json.Unmarshal(item["role"], &role)
				_ = json.Unmarshal(item["type"], &kind)
				if role != "" && role != "user" {
					continue
				}
				if kind == "function_call_output" || kind == "function_call" || kind == "reasoning" {
					continue
				}
				var text strings.Builder
				appendDecisionContent(&text, item["content"])
				if kind == "input_text" {
					appendDecisionContent(&text, item["text"])
				}
				if message := strings.TrimSpace(text.String()); message != "" {
					lastUserMessage = message
				}
			}
			return truncateDecisionText(lastUserMessage)
		}
	}
	return ""
}

func truncateDecisionText(text string) string {
	value := []rune(strings.TrimSpace(text))
	if len(value) > 12000 {
		value = value[:12000]
	}
	return string(value)
}

func appendDecisionContent(dst *strings.Builder, raw json.RawMessage) {
	var plain string
	if json.Unmarshal(raw, &plain) == nil {
		if plain != "" {
			dst.WriteString(plain)
			dst.WriteByte('\n')
		}
		return
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return
	}
	for _, part := range parts {
		var item map[string]json.RawMessage
		if json.Unmarshal(part, &item) != nil {
			continue
		}
		var kind string
		_ = json.Unmarshal(item["type"], &kind)
		if kind == "input_image" || strings.Contains(kind, "audio") {
			dst.WriteString("[multimodal input]\n")
			continue
		}
		if kind == "input_text" || kind == "text" || kind == "refusal" {
			appendDecisionContent(dst, item["text"])
		}
	}
}
