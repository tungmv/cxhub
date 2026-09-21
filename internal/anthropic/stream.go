package anthropic

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"cxhub/internal/sse"
)

// response converts a Responses JSON response body into an Anthropic Messages
// response. profile is echoed as the message's model so Claude Code shows the
// logical profile it selected.
func response(body []byte, profile string) ([]byte, error) {
	var source struct {
		ID     string        `json:"id"`
		Status string        `json:"status"`
		Output []outputItem  `json:"output"`
		Usage  usage         `json:"usage"`
		Error  *errorPayload `json:"error"`
	}
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, fmt.Errorf("decode upstream Responses body: %w", err)
	}
	if source.Error != nil && source.Error.Message != "" {
		return nil, fmt.Errorf("%s", source.Error.Message)
	}
	content := make([]any, 0, len(source.Output))
	for _, item := range source.Output {
		switch item.Type {
		case "message":
			for _, part := range item.Content {
				if part.Type == "output_text" && part.Text != "" {
					content = append(content, map[string]any{"type": "text", "text": part.Text})
				}
			}
		case "function_call":
			input := json.RawMessage(item.Arguments)
			if !json.Valid(input) || strings.TrimSpace(item.Arguments) == "" {
				input = json.RawMessage(`{}`)
			}
			content = append(content, map[string]any{"type": "tool_use", "id": item.callID(), "name": item.Name, "input": input})
		}
	}
	stopReason := "end_turn"
	if source.Status == "incomplete" {
		stopReason = "max_tokens"
	}
	for _, block := range content {
		if m, ok := block.(map[string]any); ok && m["type"] == "tool_use" {
			stopReason = "tool_use"
			break
		}
	}
	return json.Marshal(map[string]any{
		"id":            messageID(source.ID),
		"type":          "message",
		"role":          "assistant",
		"model":         profile,
		"content":       content,
		"stop_reason":   stopReason,
		"stop_sequence": nil,
		"usage":         map[string]any{"input_tokens": source.Usage.InputTokens, "output_tokens": source.Usage.OutputTokens},
	})
}

type outputPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type outputItem struct {
	Type      string       `json:"type"`
	ID        string       `json:"id"`
	CallID    string       `json:"call_id"`
	Name      string       `json:"name"`
	Arguments string       `json:"arguments"`
	Content   []outputPart `json:"content"`
}

func (i outputItem) callID() string {
	if i.CallID != "" {
		return i.CallID
	}
	return i.ID
}

type usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type errorPayload struct {
	Message string `json:"message"`
}

func messageID(responseID string) string {
	return "msg_" + strings.TrimPrefix(responseID, "resp_")
}

// streamBody wraps an upstream Responses SSE body and re-emits it as an
// Anthropic Messages SSE stream through a pipe, so the gateway's generic
// passthrough streamer can forward it unchanged.
type streamBody struct {
	reader *io.PipeReader
	source io.ReadCloser
	once   sync.Once
}

func newStreamBody(ctx context.Context, source io.ReadCloser, profile string) io.ReadCloser {
	reader, writer := io.Pipe()
	body := &streamBody{reader: reader, source: source}
	go func() {
		err := translateStream(ctx, writer, source, profile)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
	}()
	return body
}

func (b *streamBody) Read(p []byte) (int, error) { return b.reader.Read(p) }

func (b *streamBody) Close() error {
	var err error
	b.once.Do(func() {
		err = b.source.Close()
		_ = b.reader.Close()
	})
	return err
}

// translateStream consumes upstream Responses SSE events and writes Anthropic
// Messages SSE events. It returns io.EOF only when no event reached the
// client, which lets the gateway retry another target.
func translateStream(ctx context.Context, dst io.Writer, source io.ReadCloser, profile string) error {
	defer source.Close()
	t := &translator{dst: dst, profile: profile, blocks: map[string]int{}, emitted: map[int]int{}}
	parser := sse.NewParser(bufio.NewReaderSize(source, 64*1024))
	sawEvent := false
	for {
		event, err := parser.Next()
		if err == sse.ErrDone || err == io.EOF {
			if !sawEvent {
				return io.EOF
			}
			return t.finalize(ctx)
		}
		if err != nil {
			return err
		}
		sawEvent = true
		if err := t.handle(ctx, event); err != nil {
			return err
		}
	}
}

// translator converts Responses SSE events into Anthropic Messages SSE events.
type translator struct {
	dst       io.Writer
	profile   string
	messageID string
	inputUsed int
	nextIndex int
	blocks    map[string]int // upstream item/call id -> content block index
	emitted   map[int]int    // block index -> bytes already streamed
	toolUse   bool
	finalized bool
}

type streamPayload struct {
	Item     outputItem `json:"item"`
	ItemID   string     `json:"item_id"`
	Delta    string     `json:"delta"`
	Response struct {
		ID     string        `json:"id"`
		Status string        `json:"status"`
		Usage  usage         `json:"usage"`
		Error  *errorPayload `json:"error"`
	} `json:"response"`
}

func (t *translator) handle(ctx context.Context, event sse.Event) error {
	eventType := event.Type
	if eventType == "" {
		var envelope struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal([]byte(event.Data), &envelope)
		eventType = envelope.Type
	}
	switch eventType {
	case "", "ping":
		return nil
	case "response.created":
		var payload streamPayload
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			return nil
		}
		t.messageID = payload.Response.ID
		t.inputUsed = payload.Response.Usage.InputTokens
		return t.emit(ctx, "message_start", map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"id": messageID(t.messageID), "type": "message", "role": "assistant",
				"model": t.profile, "content": []any{}, "stop_reason": nil, "stop_sequence": nil,
				"usage": map[string]any{"input_tokens": t.inputUsed, "output_tokens": 0},
			},
		})
	case "response.output_item.added":
		var payload streamPayload
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			return nil
		}
		switch payload.Item.Type {
		case "message":
			t.blocks[payload.Item.ID] = t.nextIndex
			t.emitted[t.nextIndex] = 0
			t.nextIndex++
			return t.emit(ctx, "content_block_start", map[string]any{
				"type": "content_block_start", "index": t.blocks[payload.Item.ID],
				"content_block": map[string]any{"type": "text", "text": ""},
			})
		case "function_call":
			t.toolUse = true
			callID := payload.Item.callID()
			t.blocks[callID] = t.nextIndex
			t.emitted[t.nextIndex] = 0
			t.nextIndex++
			return t.emit(ctx, "content_block_start", map[string]any{
				"type": "content_block_start", "index": t.blocks[callID],
				"content_block": map[string]any{"type": "tool_use", "id": callID, "name": payload.Item.Name, "input": map[string]any{}},
			})
		}
		return nil
	case "response.output_text.delta":
		index, ok := t.blocks[eventItemID(event.Data)]
		if !ok {
			return nil
		}
		t.emitted[index] += len(eventDelta(event.Data))
		return t.emit(ctx, "content_block_delta", map[string]any{
			"type": "content_block_delta", "index": index,
			"delta": map[string]any{"type": "text_delta", "text": eventDelta(event.Data)},
		})
	case "response.function_call_arguments.delta":
		index, ok := t.blocks[eventItemID(event.Data)]
		if !ok {
			return nil
		}
		t.emitted[index] += len(eventDelta(event.Data))
		return t.emit(ctx, "content_block_delta", map[string]any{
			"type": "content_block_delta", "index": index,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": eventDelta(event.Data)},
		})
	case "response.output_item.done":
		var payload streamPayload
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			return nil
		}
		index, ok := t.blocks[payload.Item.CallID]
		if !ok {
			index, ok = t.blocks[payload.Item.ID]
		}
		if !ok {
			return nil
		}
		if err := t.flushPending(ctx, index, payload.Item); err != nil {
			return err
		}
		return t.emit(ctx, "content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
	case "response.completed":
		var payload streamPayload
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			return nil
		}
		t.inputUsed = payload.Response.Usage.InputTokens
		return t.finalizeWithUsage(ctx, payload.Response.Usage.OutputTokens, payload.Response.Status)
	case "response.failed", "response.incomplete":
		var payload streamPayload
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			return nil
		}
		message := "upstream response failed"
		if payload.Response.Error != nil && payload.Response.Error.Message != "" {
			message = payload.Response.Error.Message
		}
		if err := t.emit(ctx, "error", map[string]any{
			"type":  "error",
			"error": map[string]any{"type": "api_error", "message": message},
		}); err != nil {
			return err
		}
		return t.finalize(ctx)
	}
	return nil
}

func eventItemID(data string) string {
	var payload struct {
		ItemID string `json:"item_id"`
	}
	_ = json.Unmarshal([]byte(data), &payload)
	return payload.ItemID
}

func eventDelta(data string) string {
	var payload struct {
		Delta string `json:"delta"`
	}
	_ = json.Unmarshal([]byte(data), &payload)
	return payload.Delta
}

// flushPending covers upstreams that deliver a whole item without deltas.
func (t *translator) flushPending(ctx context.Context, index int, item outputItem) error {
	if t.emitted[index] > 0 {
		return nil
	}
	switch item.Type {
	case "message":
		text := ""
		for _, part := range item.Content {
			if part.Type == "output_text" {
				text += part.Text
			}
		}
		if text == "" {
			return nil
		}
		t.emitted[index] += len(text)
		return t.emit(ctx, "content_block_delta", map[string]any{
			"type": "content_block_delta", "index": index,
			"delta": map[string]any{"type": "text_delta", "text": text},
		})
	case "function_call":
		arguments := item.Arguments
		if strings.TrimSpace(arguments) == "" {
			arguments = "{}"
		}
		t.emitted[index] += len(arguments)
		return t.emit(ctx, "content_block_delta", map[string]any{
			"type": "content_block_delta", "index": index,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": arguments},
		})
	}
	return nil
}

func (t *translator) finalizeWithUsage(ctx context.Context, outputTokens int, status string) error {
	if t.finalized {
		return nil
	}
	t.finalized = true
	stopReason := "end_turn"
	if t.toolUse {
		stopReason = "tool_use"
	}
	if status == "incomplete" {
		stopReason = "max_tokens"
	}
	if err := t.emit(ctx, "message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": outputTokens},
	}); err != nil {
		return err
	}
	return t.emit(ctx, "message_stop", map[string]any{"type": "message_stop"})
}

func (t *translator) finalize(ctx context.Context) error {
	return t.finalizeWithUsage(ctx, 0, "")
}

func (t *translator) emit(ctx context.Context, eventType string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_, err = fmt.Fprintf(t.dst, "event: %s\ndata: %s\n\n", eventType, data)
	return err
}
