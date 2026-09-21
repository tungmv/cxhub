package anthropic

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestToResponsesBasicMessages(t *testing.T) {
	req, err := ToResponses([]byte(`{
		"model": "orchestrator",
		"max_tokens": 1024,
		"stream": true,
		"system": "You are Claude Code.",
		"messages": [
			{"role": "user", "content": [{"type": "text", "text": "hello"}]},
			{"role": "assistant", "content": [
				{"type": "text", "text": "let me look"},
				{"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {"path": "a.go"}}
			]},
			{"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "contents"}]}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "orchestrator" || !req.Stream {
		t.Fatalf("unexpected request metadata: model=%q stream=%v", req.Model, req.Stream)
	}
	var out struct {
		Instructions string `json:"instructions"`
		Input        []struct {
			Role      string `json:"role"`
			Type      string `json:"type"`
			CallID    string `json:"call_id"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
			Output    string `json:"output"`
		} `json:"input"`
		MaxOutputTokens int `json:"max_output_tokens"`
	}
	if err := json.Unmarshal(req.Body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Instructions != "You are Claude Code." {
		t.Fatalf("instructions = %q", out.Instructions)
	}
	if out.MaxOutputTokens != 1024 {
		t.Fatalf("max_output_tokens = %d", out.MaxOutputTokens)
	}
	if len(out.Input) != 4 {
		t.Fatalf("expected 4 input items, got %d: %s", len(out.Input), req.Body)
	}
	first := out.Input[0]
	if first.Role != "user" {
		t.Fatalf("first item role = %q", first.Role)
	}
	if out.Input[1].Role != "assistant" {
		t.Fatalf("assistant text item missing: %+v", out.Input[1])
	}
	if out.Input[2].Type != "function_call" || out.Input[2].Name != "read_file" || out.Input[2].CallID != "toolu_1" {
		t.Fatalf("tool_use not converted: %+v", out.Input[2])
	}
	if !strings.Contains(out.Input[2].Arguments, `"path"`) {
		t.Fatalf("arguments must keep the raw input object, got %q", out.Input[2].Arguments)
	}
	if out.Input[3].Type != "function_call_output" || out.Input[3].CallID != "toolu_1" || out.Input[3].Output != "contents" {
		t.Fatalf("tool_result not converted: %+v", out.Input[3])
	}
}

func TestToResponsesSystemBlocksAndTools(t *testing.T) {
	req, err := ToResponses([]byte(`{
		"model": "orchestrator",
		"system": [{"type": "text", "text": "part one"}, {"type": "text", "text": "part two"}],
		"tools": [{"name": "bash", "description": "run shell", "input_schema": {"type": "object", "properties": {"cmd": {"type": "string"}}}}],
		"tool_choice": {"type": "tool", "name": "bash"},
		"messages": [{"role": "user", "content": "hi"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Instructions string `json:"instructions"`
		Tools        []struct {
			Type       string          `json:"type"`
			Name       string          `json:"name"`
			Parameters json.RawMessage `json:"parameters"`
		} `json:"tools"`
		ToolChoice json.RawMessage `json:"tool_choice"`
		Input      []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(req.Body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Instructions != "part one\n\npart two" {
		t.Fatalf("instructions = %q", out.Instructions)
	}
	if len(out.Tools) != 1 || out.Tools[0].Type != "function" || out.Tools[0].Name != "bash" {
		t.Fatalf("tools not converted: %s", req.Body)
	}
	if !strings.Contains(string(out.ToolChoice), `"bash"`) {
		t.Fatalf("tool_choice not converted: %s", out.ToolChoice)
	}
	var content []map[string]any
	if err := json.Unmarshal(out.Input[0].Content, &content); err != nil {
		t.Fatal(err)
	}
	if len(content) != 1 || content[0]["type"] != "input_text" || content[0]["text"] != "hi" {
		t.Fatalf("plain string content not converted: %v", out.Input[0])
	}
}

func TestToResponsesToolResultStringContent(t *testing.T) {
	req, err := ToResponses([]byte(`{
		"model": "fast",
		"messages": [
			{"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": [{"type": "text", "text": "line1"}, {"type": "text", "text": "line2"}]}]}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Input []struct {
			Type   string `json:"type"`
			Output string `json:"output"`
			CallID string `json:"call_id"`
		} `json:"input"`
	}
	if err := json.Unmarshal(req.Body, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Input) != 1 || out.Input[0].Output != "line1\nline2" || out.Input[0].CallID != "t1" {
		t.Fatalf("tool_result flattening failed: %s", req.Body)
	}
}

func TestToResponsesRequiresModel(t *testing.T) {
	if _, err := ToResponses([]byte(`{"max_tokens": 1, "messages": []}`)); err == nil {
		t.Fatal("expected error for missing model")
	}
}

func TestResponseConversionTextAndToolUse(t *testing.T) {
	upstream := []byte(`{
		"id": "resp_123", "status": "completed", "model": "real-model",
		"output": [
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]},
			{"type": "function_call", "call_id": "call_1", "name": "bash", "arguments": "{\"cmd\":\"ls\"}"}
		],
		"usage": {"input_tokens": 10, "output_tokens": 5}
	}`)
	converted, err := response(upstream, "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		ID         string           `json:"id"`
		Type       string           `json:"type"`
		Role       string           `json:"role"`
		Model      string           `json:"model"`
		StopReason string           `json:"stop_reason"`
		Content    []map[string]any `json:"content"`
		Usage      map[string]int   `json:"usage"`
	}
	if err := json.Unmarshal(converted, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != "msg_123" || out.Type != "message" || out.Model != "orchestrator" {
		t.Fatalf("unexpected header: %s", converted)
	}
	if out.StopReason != "tool_use" {
		t.Fatalf("stop_reason = %q, want tool_use", out.StopReason)
	}
	if len(out.Content) != 2 || out.Content[0]["type"] != "text" || out.Content[1]["type"] != "tool_use" {
		t.Fatalf("unexpected content: %v", out.Content)
	}
	if out.Usage["input_tokens"] != 10 || out.Usage["output_tokens"] != 5 {
		t.Fatalf("usage not mapped: %v", out.Usage)
	}
}

func TestResponseConversionRejectsUpstreamError(t *testing.T) {
	if _, err := response([]byte(`{"id":"resp_1","error":{"message":"boom"}}`), "p"); err == nil {
		t.Fatal("expected error for upstream error payload")
	}
}

// Claude Code interleaves a role "system" message into the conversation; it
// must fold into instructions, not error out.
func TestToResponsesInterleavedSystemMessage(t *testing.T) {
	req, err := ToResponses([]byte(`{
		"model": "orchestrator",
		"system": [{"type": "text", "text": "base prompt"}],
		"messages": [
			{"role": "user", "content": [{"type": "text", "text": "hi"}]},
			{"role": "system", "content": [{"type": "text", "text": "contextual note"}]},
			{"role": "assistant", "content": [{"type": "text", "text": "hello"}]}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Instructions string `json:"instructions"`
		Input        []struct {
			Role string `json:"role"`
		} `json:"input"`
	}
	if err := json.Unmarshal(req.Body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Instructions != "base prompt\n\ncontextual note" {
		t.Fatalf("instructions = %q", out.Instructions)
	}
	if len(out.Input) != 2 || out.Input[0].Role != "user" || out.Input[1].Role != "assistant" {
		t.Fatalf("system message must not become an input item: %s", req.Body)
	}
}
