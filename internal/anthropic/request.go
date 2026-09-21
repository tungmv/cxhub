// Package anthropic adapts the Anthropic Messages wire format (Claude Code)
// to the OpenAI Responses format routed by cxhub. The logical profile is
// carried in the Messages request's "model" field, exactly like Codex does.
package anthropic

import (
	"encoding/json"
	"fmt"
	"strings"

	"cxhub/internal/responses"
)

// ToResponses converts an Anthropic Messages request body into a Responses
// request for the given logical profile.
func ToResponses(body []byte) (*responses.Request, error) {
	var source struct {
		Model       string          `json:"model"`
		MaxTokens   int             `json:"max_tokens"`
		Stream      bool            `json:"stream"`
		System      json.RawMessage `json:"system"`
		Temperature *float64        `json:"temperature"`
		TopP        *float64        `json:"top_p"`
		ToolChoice  json.RawMessage `json:"tool_choice"`
		Messages    []message       `json:"messages"`
		Tools       []tool          `json:"tools"`
	}
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, fmt.Errorf("invalid Anthropic Messages request: %w", err)
	}
	if strings.TrimSpace(source.Model) == "" {
		return nil, fmt.Errorf("model is required and must be a configured logical profile")
	}
	var systemParts []string
	out := map[string]any{
		"model":  source.Model,
		"stream": source.Stream,
		// Responses-compatible upstreams must not retain cxhub relays.
		"store": false,
	}
	if instructions, err := systemTexts(source.System); err != nil {
		return nil, err
	} else {
		systemParts = instructions
	}
	input, extraSystem, err := convertMessages(source.Messages)
	if err != nil {
		return nil, err
	}
	systemParts = append(systemParts, extraSystem...)
	out["input"] = input
	if len(systemParts) > 0 {
		out["instructions"] = strings.Join(systemParts, "\n\n")
	}
	if source.MaxTokens > 0 {
		out["max_output_tokens"] = source.MaxTokens
	}
	if source.Temperature != nil {
		out["temperature"] = *source.Temperature
	}
	if source.TopP != nil {
		out["top_p"] = *source.TopP
	}
	if len(source.Tools) > 0 {
		out["tools"] = convertTools(source.Tools)
	}
	if choice, ok, err := convertToolChoice(source.ToolChoice); err != nil {
		return nil, err
	} else if ok {
		out["tool_choice"] = choice
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return &responses.Request{Body: encoded, Model: source.Model, Stream: source.Stream}, nil
}

type message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type block struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
	Source    *source         `json:"source"`
}

type source struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
	URL       string `json:"url"`
}

type tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// systemTexts flattens the Anthropic system prompt (string or text blocks).
func systemTexts(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if text == "" {
			return nil, nil
		}
		return []string{text}, nil
	}
	var blocks []block
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("system prompt is invalid: %w", err)
	}
	parts := make([]string, 0, len(blocks))
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return parts, nil
}

// decodeBlocks accepts Anthropic content as either a plain string or an array
// of content blocks.
func decodeBlocks(raw json.RawMessage) ([]block, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if text == "" {
			return nil, nil
		}
		return []block{{Type: "text", Text: text}}, nil
	}
	var blocks []block
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("message content is invalid: %w", err)
	}
	return blocks, nil
}

// convertMessages maps Anthropic conversation messages to Responses input
// items. Messages with role "system" (Claude Code interleaves them) are
// returned separately so they can be folded into the instructions.
func convertMessages(messages []message) ([]any, []string, error) {
	items := make([]any, 0, len(messages))
	systemParts := make([]string, 0)
	for i, msg := range messages {
		blocks, err := decodeBlocks(msg.Content)
		if err != nil {
			return nil, nil, err
		}
		switch msg.Role {
		case "system":
			for _, b := range blocks {
				if b.Type == "text" && b.Text != "" {
					systemParts = append(systemParts, b.Text)
				}
			}
		case "user":
			userItems, err := userItems(blocks)
			if err != nil {
				return nil, nil, fmt.Errorf("message %d: %w", i+1, err)
			}
			items = append(items, userItems...)
		case "assistant":
			assistantItems, err := assistantItems(blocks)
			if err != nil {
				return nil, nil, fmt.Errorf("message %d: %w", i+1, err)
			}
			items = append(items, assistantItems...)
		default:
			return nil, nil, fmt.Errorf("message %d: unsupported role %q", i+1, msg.Role)
		}
	}
	return items, systemParts, nil
}

func userItems(blocks []block) ([]any, error) {
	content := make([]any, 0, len(blocks))
	items := make([]any, 0)
	for _, b := range blocks {
		switch b.Type {
		case "text":
			content = append(content, map[string]any{"type": "input_text", "text": b.Text})
		case "image":
			if b.Source == nil {
				return nil, fmt.Errorf("image block is missing source")
			}
			url := b.Source.URL
			if b.Source.Type == "base64" {
				url = fmt.Sprintf("data:%s;base64,%s", b.Source.MediaType, b.Source.Data)
			}
			content = append(content, map[string]any{"type": "input_image", "image_url": url})
		case "tool_result":
			output, err := toolResultText(b)
			if err != nil {
				return nil, err
			}
			// A tool result must follow the function_call it answers, so it
			// is emitted as a standalone item before the surrounding text.
			if len(content) > 0 {
				items = append(items, map[string]any{"role": "user", "content": content})
				content = make([]any, 0, len(blocks))
			}
			items = append(items, map[string]any{"type": "function_call_output", "call_id": b.ToolUseID, "output": output})
		case "thinking":
			// Internal reasoning is not replayable input for Responses.
		default:
			return nil, fmt.Errorf("unsupported user content block type %q", b.Type)
		}
	}
	if len(content) > 0 {
		items = append(items, map[string]any{"role": "user", "content": content})
	}
	return items, nil
}

func assistantItems(blocks []block) ([]any, error) {
	text := make([]string, 0, len(blocks))
	items := make([]any, 0)
	flush := func() {
		if len(text) == 0 {
			return
		}
		joined := strings.Join(text, "")
		items = append(items, map[string]any{
			"role":    "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": joined, "annotations": []any{}}},
		})
		text = text[:0]
	}
	for _, b := range blocks {
		switch b.Type {
		case "text":
			text = append(text, b.Text)
		case "tool_use":
			flush()
			arguments := "{}"
			if len(b.Input) > 0 && string(b.Input) != "null" {
				arguments = string(b.Input)
			}
			items = append(items, map[string]any{
				"type": "function_call", "call_id": b.ID, "name": b.Name, "arguments": arguments,
			})
		case "thinking":
			// Internal reasoning is not replayable input for Responses.
		default:
			return nil, fmt.Errorf("unsupported assistant content block type %q", b.Type)
		}
	}
	flush()
	return items, nil
}

// toolResultText flattens a tool_result block into a string output value.
func toolResultText(b block) (string, error) {
	blocks, err := decodeBlocks(b.Content)
	if err != nil {
		return "", fmt.Errorf("tool_result %q: %w", b.ToolUseID, err)
	}
	parts := make([]string, 0, len(blocks))
	for _, inner := range blocks {
		if inner.Type == "text" && inner.Text != "" {
			parts = append(parts, inner.Text)
		}
	}
	output := strings.Join(parts, "\n")
	if output == "" && b.IsError {
		output = "tool execution failed"
	}
	return output, nil
}

func convertTools(tools []tool) []any {
	converted := make([]any, 0, len(tools))
	for _, t := range tools {
		entry := map[string]any{"type": "function", "name": t.Name}
		if t.Description != "" {
			entry["description"] = t.Description
		}
		parameters := t.InputSchema
		if len(parameters) == 0 || string(parameters) == "null" {
			parameters = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		entry["parameters"] = parameters
		converted = append(converted, entry)
	}
	return converted
}

func convertToolChoice(raw json.RawMessage) (any, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	var choice struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &choice); err != nil {
		return nil, false, fmt.Errorf("tool_choice is invalid: %w", err)
	}
	switch choice.Type {
	case "", "auto":
		return "auto", true, nil
	case "any":
		return "required", true, nil
	case "none":
		return "none", true, nil
	case "tool":
		if choice.Name == "" {
			return nil, false, fmt.Errorf("tool_choice of type %q requires a name", choice.Type)
		}
		return map[string]any{"type": "function", "name": choice.Name}, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported tool_choice type %q", choice.Type)
	}
}
