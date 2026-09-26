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

type decisionScores struct {
	Speed      float64
	Quality    float64
	HighEffort float64
}

type decisionScorer interface {
	Score(context.Context, string) (decisionScores, error)
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

func (d *spanDecision) Score(ctx context.Context, task string) (decisionScores, error) {
	if strings.TrimSpace(task) == "" {
		return decisionScores{}, fmt.Errorf("request has no text to classify")
	}
	payload := map[string]any{
		"model": d.model,
		"state": map[string]any{
			"input":  []map[string]string{{"role": "user", "content": task}},
			"output": map[string]string{"role": "assistant", "content": ""},
		},
		"questions": map[string]any{
			"speed":       map[string]string{"type": "noul", "instructions": "The user request is straightforward, low-risk, and can be handled well by a fast, low-cost model with little reasoning."},
			"quality":     map[string]string{"type": "noul", "instructions": "The user request is difficult, ambiguous, multi-step, or costly to get wrong, so it benefits from a stronger model."},
			"high_effort": map[string]string{"type": "noul", "instructions": "A high reasoning effort is needed to answer the user request well."},
		},
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
	if scores.Speed, err = read("speed"); err != nil {
		return decisionScores{}, err
	}
	if scores.Quality, err = read("quality"); err != nil {
		return decisionScores{}, err
	}
	if scores.HighEffort, err = read("high_effort"); err != nil {
		return decisionScores{}, err
	}
	return scores, nil
}

func (s *Server) automaticRoute(ctx context.Context, req *responses.Request) (string, string, decisionScores, error) {
	if s.Decider == nil {
		return "", "", decisionScores{}, fmt.Errorf("automatic routing is not configured")
	}
	scores, err := s.Decider.Score(ctx, decisionText(req))
	if err != nil {
		return "", "", scores, err
	}
	tier := "balanced"
	// ponytail: fixed score thresholds keep routing deterministic; tune them from observed outcomes if they misclassify tasks.
	if scores.Quality >= 0.6 && scores.Quality > scores.Speed {
		tier = "quality"
	} else if scores.Speed >= 0.6 && scores.Speed > scores.Quality {
		tier = "speed"
	}
	profile := ""
	for name, definition := range s.Config.Profiles {
		if definition.AutoTier == tier {
			profile = name
			break
		}
	}
	if profile == "" && tier != "balanced" {
		for name, definition := range s.Config.Profiles {
			if definition.AutoTier == "balanced" {
				profile = name
				break
			}
		}
	}
	if profile == "" {
		profile = s.Config.Decision.DefaultProfile
	}
	effort := "medium"
	if scores.HighEffort >= 0.6 || scores.Quality >= 0.6 {
		effort = "high"
	} else if scores.Speed >= 0.75 {
		effort = "low"
	}
	return profile, effort, scores, nil
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
