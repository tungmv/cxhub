package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"cxhub/internal/config"
	"cxhub/internal/responses"
)

// OpenAIChatCompatible adapts the Chat Completions wire format to the
// Responses format exposed by cxhub. NVIDIA's hosted endpoint is compatible
// with Chat Completions, but does not expose a model-backed /responses route.
type OpenAIChatCompatible struct {
	id      string
	baseURL string
	apiKey  string
	headers map[string]string
	client  *http.Client
}

func NewOpenAIChatCompatible(id string, cfg config.BackendConfig, client *http.Client) *OpenAIChatCompatible {
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	headers := make(map[string]string, len(cfg.Headers))
	for key, value := range cfg.Headers {
		headers[key] = value
	}
	return &OpenAIChatCompatible{id: id, baseURL: strings.TrimRight(cfg.BaseURL, "/"), apiKey: cfg.APIKey, headers: headers, client: client}
}

func (p *OpenAIChatCompatible) ID() string { return p.id }

func (p *OpenAIChatCompatible) Responses(ctx context.Context, req *responses.Request, model string) (*http.Response, error) {
	body, err := chatRequest(req, model)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream, application/json")
	if p.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	for key, value := range p.headers {
		httpReq.Header.Set(key, value)
	}
	resp, err := p.client.Do(httpReq)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, err
	}
	if !req.Stream {
		return convertChatResponse(resp, model)
	}
	stream, err := newChatStreamBody(ctx, resp.Body, model)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	resp.Body = stream
	resp.Header.Set("Content-Type", "text/event-stream")
	resp.ContentLength = -1
	return resp, nil
}

func (p *OpenAIChatCompatible) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", nil)
	if err != nil {
		return err
	}
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	for key, value := range p.headers {
		req.Header.Set(key, value)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func chatRequest(req *responses.Request, model string) ([]byte, error) {
	var input map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}
	out := make(map[string]json.RawMessage)
	setString(out, "model", model)
	setBool(out, "stream", req.Stream)

	if raw, ok := input["instructions"]; ok {
		message, err := instructionMessage(raw)
		if err != nil {
			return nil, err
		}
		setMessages(out, []map[string]any{message})
	}
	if raw, ok := input["input"]; ok {
		messages, err := inputMessages(raw)
		if err != nil {
			return nil, err
		}
		appendMessages(out, messages)
	} else if raw, ok := input["messages"]; ok {
		out["messages"] = raw
	}

	for _, key := range []string{
		"temperature", "top_p", "presence_penalty", "frequency_penalty", "stop",
		"seed", "n", "user", "response_format", "parallel_tool_calls",
	} {
		if raw, ok := input[key]; ok {
			out[key] = raw
		}
	}
	if raw, ok := input["max_output_tokens"]; ok {
		out["max_tokens"] = raw
	} else if raw, ok := input["max_tokens"]; ok {
		out["max_tokens"] = raw
	} else if raw, ok := input["max_completion_tokens"]; ok {
		out["max_completion_tokens"] = raw
	}
	if raw, ok := input["tools"]; ok {
		tools, err := chatTools(raw)
		if err != nil {
			return nil, err
		}
		out["tools"] = tools
	}
	if raw, ok := input["tool_choice"]; ok {
		choice, err := chatToolChoice(raw)
		if err != nil {
			return nil, err
		}
		out["tool_choice"] = choice
	}
	return json.Marshal(out)
}

func instructionMessage(raw json.RawMessage) (map[string]any, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return map[string]any{"role": "system", "content": text}, nil
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, fmt.Errorf("instructions are not supported by NVIDIA chat backend: %w", err)
	}
	content, err := chatContent(parts)
	if err != nil {
		return nil, err
	}
	return map[string]any{"role": "system", "content": content}, nil
}

func inputMessages(raw json.RawMessage) ([]map[string]any, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return []map[string]any{{"role": "user", "content": text}}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("input is not supported by NVIDIA chat backend: %w", err)
	}
	messages := make([]map[string]any, 0, len(items))
	for _, item := range items {
		message, err := inputMessage(item)
		if err != nil {
			return nil, err
		}
		if message != nil {
			messages = append(messages, message)
		}
	}
	return messages, nil
}

func inputMessage(raw json.RawMessage) (map[string]any, error) {
	var item map[string]json.RawMessage
	if err := json.Unmarshal(raw, &item); err != nil {
		return nil, fmt.Errorf("input item is invalid: %w", err)
	}
	var itemType string
	_ = json.Unmarshal(item["type"], &itemType)
	if itemType == "function_call_output" {
		var callID, output string
		_ = json.Unmarshal(item["call_id"], &callID)
		if err := json.Unmarshal(item["output"], &output); err != nil {
			output = string(item["output"])
		}
		return map[string]any{"role": "tool", "tool_call_id": callID, "content": output}, nil
	}
	if itemType == "function_call" {
		var callID, name, arguments string
		_ = json.Unmarshal(item["call_id"], &callID)
		_ = json.Unmarshal(item["name"], &name)
		_ = json.Unmarshal(item["arguments"], &arguments)
		return map[string]any{
			"role":       "assistant",
			"tool_calls": []map[string]any{{"id": callID, "type": "function", "function": map[string]string{"name": name, "arguments": arguments}}},
		}, nil
	}
	if itemType == "reasoning" || itemType == "item_reference" {
		return nil, nil
	}
	var role string
	_ = json.Unmarshal(item["role"], &role)
	if role == "" {
		role = "user"
	}
	contentRaw, ok := item["content"]
	if !ok {
		return nil, fmt.Errorf("input item is missing content")
	}
	content, err := chatContentOrText(contentRaw)
	if err != nil {
		return nil, err
	}
	return map[string]any{"role": role, "content": content}, nil
}

func chatContentOrText(raw json.RawMessage) (any, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, fmt.Errorf("message content is invalid: %w", err)
	}
	return chatContent(parts)
}

func chatContent(parts []json.RawMessage) ([]map[string]any, error) {
	content := make([]map[string]any, 0, len(parts))
	for _, raw := range parts {
		var part map[string]json.RawMessage
		if err := json.Unmarshal(raw, &part); err != nil {
			return nil, fmt.Errorf("content part is invalid: %w", err)
		}
		var partType string
		_ = json.Unmarshal(part["type"], &partType)
		switch partType {
		case "input_text", "output_text", "text":
			var text string
			_ = json.Unmarshal(part["text"], &text)
			content = append(content, map[string]any{"type": "text", "text": text})
		case "input_image", "image_url":
			image := map[string]any{"type": "image_url"}
			if imageURL, ok := part["image_url"]; ok {
				var value any
				if json.Unmarshal(imageURL, &value) == nil {
					image["image_url"] = value
				}
			}
			content = append(content, image)
		default:
			return nil, fmt.Errorf("unsupported content part type %q for NVIDIA chat backend", partType)
		}
	}
	return content, nil
}

func chatTools(raw json.RawMessage) (json.RawMessage, error) {
	var tools []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("tools are invalid: %w", err)
	}
	converted := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		var toolType string
		_ = json.Unmarshal(tool["type"], &toolType)
		if toolType != "function" {
			return nil, fmt.Errorf("unsupported tool type %q for NVIDIA chat backend", toolType)
		}
		function := make(map[string]any)
		for _, key := range []string{"name", "description", "parameters", "strict"} {
			if value, ok := tool[key]; ok {
				var decoded any
				if err := json.Unmarshal(value, &decoded); err != nil {
					return nil, err
				}
				function[key] = decoded
			}
		}
		converted = append(converted, map[string]any{"type": "function", "function": function})
	}
	return json.Marshal(converted)
}

func chatToolChoice(raw json.RawMessage) (json.RawMessage, error) {
	var choice string
	if json.Unmarshal(raw, &choice) == nil {
		return raw, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("tool_choice is invalid: %w", err)
	}
	var name string
	_ = json.Unmarshal(object["name"], &name)
	if name == "" {
		return raw, nil
	}
	return json.Marshal(map[string]any{"type": "function", "function": map[string]string{"name": name}})
}

func setMessages(out map[string]json.RawMessage, messages []map[string]any) {
	encoded, _ := json.Marshal(messages)
	out["messages"] = encoded
}

func appendMessages(out map[string]json.RawMessage, messages []map[string]any) {
	var current []map[string]any
	if raw, ok := out["messages"]; ok {
		_ = json.Unmarshal(raw, &current)
	}
	current = append(current, messages...)
	setMessages(out, current)
}

func setString(out map[string]json.RawMessage, key, value string) {
	encoded, _ := json.Marshal(value)
	out[key] = encoded
}

func setBool(out map[string]json.RawMessage, key string, value bool) {
	encoded, _ := json.Marshal(value)
	out[key] = encoded
}

func convertChatResponse(resp *http.Response, model string) (*http.Response, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	converted, err := responseJSON(body, model)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(converted))
	resp.ContentLength = int64(len(converted))
	resp.Header.Set("Content-Type", "application/json")
	return resp, nil
}

func responseJSON(body []byte, model string) ([]byte, error) {
	var source struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role      string          `json:"role"`
				Content   json.RawMessage `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, fmt.Errorf("decode NVIDIA chat response: %w", err)
	}
	if source.Model == "" {
		source.Model = model
	}
	output := make([]any, 0)
	for index, choice := range source.Choices {
		messageID := fmt.Sprintf("msg_%s_%d", source.ID, index)
		content, _ := contentText(choice.Message.Content)
		messageContent := make([]any, 0, 1)
		if content != "" {
			messageContent = append(messageContent, map[string]any{"type": "output_text", "text": content, "annotations": []any{}})
		}
		if len(messageContent) > 0 {
			output = append(output, map[string]any{"id": messageID, "type": "message", "status": "completed", "role": choice.Message.Role, "content": messageContent})
		}
		for _, call := range choice.Message.ToolCalls {
			output = append(output, map[string]any{"id": call.ID, "type": "function_call", "status": "completed", "call_id": call.ID, "name": call.Function.Name, "arguments": call.Function.Arguments})
		}
	}
	status := "completed"
	if len(source.Choices) == 0 {
		status = "incomplete"
	}
	result := map[string]any{
		"id": source.ID, "object": "response", "created_at": source.Created, "status": status,
		"model": source.Model, "output": output,
		"usage": map[string]int{"input_tokens": source.Usage.PromptTokens, "output_tokens": source.Usage.CompletionTokens, "total_tokens": source.Usage.TotalTokens},
	}
	return json.Marshal(result)
}

func contentText(raw json.RawMessage) (string, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	return "", nil
}

type chatStreamBody struct {
	reader *io.PipeReader
	source io.ReadCloser
	once   sync.Once
}

func newChatStreamBody(ctx context.Context, source io.ReadCloser, model string) (io.ReadCloser, error) {
	reader, writer := io.Pipe()
	body := &chatStreamBody{reader: reader, source: source}
	go func() {
		err := writeChatStream(ctx, writer, source, model)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
	}()
	return body, nil
}

func (b *chatStreamBody) Read(p []byte) (int, error) { return b.reader.Read(p) }

func (b *chatStreamBody) Close() error {
	var err error
	b.once.Do(func() {
		err = b.source.Close()
		_ = b.reader.Close()
	})
	return err
}

func writeChatStream(ctx context.Context, dst *io.PipeWriter, source io.ReadCloser, model string) error {
	defer source.Close()
	reader := bufio.NewReaderSize(source, 64*1024)
	started := false
	responseID := ""
	itemID := ""
	textOutput := ""
	toolItems := map[int]string{}
	toolCalls := map[int]string{}
	toolNames := map[int]string{}
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 && strings.HasPrefix(strings.TrimRight(line, "\r\n"), "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(strings.TrimRight(line, "\r\n"), "data:"))
			if data == "[DONE]" {
				if started {
					if itemID != "" {
						if err := writeEvent(ctx, dst, "response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": itemID, "output_index": 0, "content_index": 0, "text": textOutput}); err != nil {
							return err
						}
						if err := writeEvent(ctx, dst, "response.content_part.done", map[string]any{"type": "response.content_part.done", "item_id": itemID, "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": textOutput, "annotations": []any{}}}); err != nil {
							return err
						}
						if err := writeEvent(ctx, dst, "response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": 0, "item": map[string]any{"id": itemID, "type": "message", "status": "completed", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": textOutput, "annotations": []any{}}}}}); err != nil {
							return err
						}
					}
					for index, callID := range toolItems {
						if err := writeEvent(ctx, dst, "response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": index, "item": map[string]any{"type": "function_call", "id": callID, "call_id": callID, "status": "completed", "name": toolNames[index], "arguments": toolCalls[index]}}); err != nil {
							return err
						}
					}
					if err := writeEvent(ctx, dst, "response.completed", map[string]any{"type": "response.completed", "response": map[string]any{"id": responseID, "object": "response", "status": "completed", "model": model}}); err != nil {
						return err
					}
					return writeDone(ctx, dst)
				}
				return nil
			}
			var chunk struct {
				ID      string `json:"id"`
				Model   string `json:"model"`
				Choices []struct {
					Delta struct {
						Role      string `json:"role"`
						Content   string `json:"content"`
						ToolCalls []struct {
							Index    int    `json:"index"`
							ID       string `json:"id"`
							Function struct {
								Name      string `json:"name"`
								Arguments string `json:"arguments"`
							} `json:"function"`
						} `json:"tool_calls"`
					} `json:"delta"`
				} `json:"choices"`
			}
			if json.Unmarshal([]byte(data), &chunk) != nil {
				continue
			}
			if responseID == "" {
				responseID = "resp_" + chunk.ID
			}
			if !started {
				started = true
				if err := writeEvent(ctx, dst, "response.created", map[string]any{"type": "response.created", "response": map[string]any{"id": responseID, "object": "response", "status": "in_progress", "model": model, "output": []any{}}}); err != nil {
					return err
				}
			}
			for _, choice := range chunk.Choices {
				if choice.Delta.Content != "" {
					textOutput += choice.Delta.Content
					if itemID == "" {
						itemID = "msg_" + chunk.ID
						if err := writeEvent(ctx, dst, "response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"id": itemID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}}}); err != nil {
							return err
						}
					}
					if err := writeEvent(ctx, dst, "response.output_text.delta", map[string]any{"type": "response.output_text.delta", "item_id": itemID, "output_index": 0, "content_index": 0, "delta": choice.Delta.Content}); err != nil {
						return err
					}
				}
				for _, call := range choice.Delta.ToolCalls {
					if call.ID != "" {
						toolItems[call.Index] = call.ID
					}
					if _, ok := toolCalls[call.Index]; !ok {
						toolCalls[call.Index] = ""
					}
					toolCalls[call.Index] += call.Function.Arguments
					if call.Function.Name != "" {
						toolNames[call.Index] = call.Function.Name
						if err := writeEvent(ctx, dst, "response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": call.Index, "item": map[string]any{"id": call.ID, "type": "function_call", "status": "in_progress", "call_id": call.ID, "name": call.Function.Name, "arguments": ""}}); err != nil {
							return err
						}
					}
					if call.Function.Arguments != "" {
						if err := writeEvent(ctx, dst, "response.function_call_arguments.delta", map[string]any{"type": "response.function_call_arguments.delta", "item_id": call.ID, "output_index": call.Index, "delta": call.Function.Arguments}); err != nil {
							return err
						}
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func writeEvent(ctx context.Context, dst *io.PipeWriter, eventType string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_, err = fmt.Fprintf(dst, "event: %s\ndata: %s\n\n", eventType, data)
	return err
}

func writeDone(ctx context.Context, dst *io.PipeWriter) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_, err := io.WriteString(dst, "data: [DONE]\n\n")
	return err
}
