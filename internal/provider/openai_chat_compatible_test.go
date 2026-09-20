package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cxhub/internal/config"
	"cxhub/internal/responses"
)

func TestChatRequestConvertsResponsesInputAndTools(t *testing.T) {
	req, err := responses.Parse([]byte(`{
		"model":"coder",
		"instructions":"Be concise",
		"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}],
		"tools":[{"type":"function","name":"shell","description":"run a command","parameters":{"type":"object"}}],
		"tool_choice":{"type":"function","name":"shell"},
		"max_output_tokens":128,
		"stream":true
	}`))
	if err != nil {
		t.Fatal(err)
	}
	body, err := chatRequest(req, "deepseek-ai/deepseek-v4-flash-0731")
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Model      string           `json:"model"`
		Stream     bool             `json:"stream"`
		Messages   []map[string]any `json:"messages"`
		Tools      []map[string]any `json:"tools"`
		ToolChoice map[string]any   `json:"tool_choice"`
		MaxTokens  int              `json:"max_tokens"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Model != "deepseek-ai/deepseek-v4-flash-0731" || !payload.Stream || payload.MaxTokens != 128 {
		t.Fatalf("request routing fields were not converted: %+v", payload)
	}
	if len(payload.Messages) != 2 || payload.Messages[0]["role"] != "system" || payload.Messages[1]["role"] != "user" {
		t.Fatalf("messages = %+v", payload.Messages)
	}
	if len(payload.Tools) != 1 {
		t.Fatalf("tools = %+v", payload.Tools)
	}
	function, ok := payload.Tools[0]["function"].(map[string]any)
	if !ok || function["name"] != "shell" {
		t.Fatalf("tool function = %+v", payload.Tools[0])
	}
	if payload.ToolChoice["type"] != "function" {
		t.Fatalf("tool choice = %+v", payload.ToolChoice)
	}
}

func TestOpenAIChatCompatibleConvertsNonStreamingResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chat-1","created":123,"model":"real","choices":[{"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}`)
	}))
	defer server.Close()

	p := NewOpenAIChatCompatible("nvidia", config.BackendConfig{BaseURL: server.URL + "/v1", APIKey: "secret"}, server.Client())
	req, _ := responses.Parse([]byte(`{"model":"coder","input":"hello","stream":false}`))
	resp, err := p.Responses(context.Background(), req, "real")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var payload struct {
		Object string `json:"object"`
		Status string `json:"status"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Object != "response" || payload.Status != "completed" || len(payload.Output) != 1 || payload.Output[0].Content[0].Text != "OK" {
		t.Fatalf("converted response = %s", body)
	}
}

func TestOpenAIChatCompatibleConvertsStreamingResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"chat-2\",\"model\":\"real\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"id\":\"chat-2\",\"model\":\"real\",\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	p := NewOpenAIChatCompatible("nvidia", config.BackendConfig{BaseURL: server.URL + "/v1"}, server.Client())
	req, _ := responses.Parse([]byte(`{"model":"fast","input":"hello","stream":true}`))
	resp, err := p.Responses(context.Background(), req, "real")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, event := range []string{"response.created", "response.output_text.delta", "response.completed", `"delta":"OK"`, "data: [DONE]"} {
		if !strings.Contains(text, event) {
			t.Fatalf("stream missing %q: %s", event, text)
		}
	}
}
