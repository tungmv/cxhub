package anthropic

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"cxhub/internal/sse"
)

func sseParser(r io.Reader) *sse.Parser { return sse.NewParser(r) }

// upstreamStream simulates an upstream Responses SSE stream that terminates
// with a plain EOF (no [DONE] sentinel), like CLIProxyAPI does.
const upstreamEvents = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":7}}}\n\n" +
	"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_1\",\"role\":\"assistant\"}}\n\n" +
	"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"hel\"}\n\n" +
	"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"lo\"}\n\n" +
	"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_9\",\"name\":\"bash\",\"arguments\":\"\"}}\n\n" +
	"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"call_9\",\"delta\":\"{\\\"cmd\\\":\"}\n\n" +
	"event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"call_9\",\"arguments\":\"{\\\"cmd\\\":\\\"ls\\\"}\"}\n\n" +
	"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"call_id\":\"call_9\",\"name\":\"bash\",\"arguments\":\"{\\\"cmd\\\":\\\"ls\\\"}\"}}\n\n" +
	"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":7,\"output_tokens\":3}}}\n\n"

func TestTranslateStreamFullSequence(t *testing.T) {
	var out bytes.Buffer
	err := translateStream(context.Background(), &out, io.NopCloser(strings.NewReader(upstreamEvents)), "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	events, err := parseEvents(out.String())
	if err != nil {
		t.Fatal(err)
	}
	types := make([]string, 0, len(events))
	for _, e := range events {
		types = append(types, e.name)
	}
	want := []string{
		"message_start",
		"content_block_start", "content_block_delta", "content_block_delta",
		"content_block_start", "content_block_delta", "content_block_stop",
		"message_delta", "message_stop",
	}
	if len(types) != len(want) {
		t.Fatalf("event sequence = %v, want %v\n%s", types, want, out.String())
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("event %d = %q, want %q\n%s", i, types[i], want[i], out.String())
		}
	}
	// text deltas carry the streamed text
	if !strings.Contains(events[2].data, `"text":"hel`) {
		t.Fatalf("text delta missing: %s", events[2].data)
	}
	// tool_use block starts with name and empty input
	if !strings.Contains(events[4].data, `"name":"bash"`) || !strings.Contains(events[4].data, `"id":"call_9"`) {
		t.Fatalf("tool_use start wrong: %s", events[4].data)
	}
	// message_stop last, message_delta carries tool_use stop reason and usage
	if !strings.Contains(events[len(events)-2].data, `"stop_reason":"tool_use"`) {
		t.Fatalf("message_delta stop reason wrong: %s", events[len(events)-2].data)
	}
	if !strings.Contains(events[len(events)-2].data, `"output_tokens":3`) {
		t.Fatalf("message_delta usage missing: %s", events[len(events)-2].data)
	}
}

func TestTranslateStreamPlainTextWholeItem(t *testing.T) {
	upstream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_2\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"whole answer\"}]}}\n\n" +
		"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"content\":[{\"type\":\"output_text\",\"text\":\"whole answer\"}]}}\n\n"
	var out bytes.Buffer
	if err := translateStream(context.Background(), &out, io.NopCloser(bytes.NewBufferString(upstream)), "fast"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"text":"whole answer"`) {
		t.Fatalf("whole-item text not flushed: %s", out.String())
	}
	if !strings.Contains(out.String(), `"stop_reason":"end_turn"`) {
		t.Fatalf("expected end_turn: %s", out.String())
	}
	if !strings.HasSuffix(out.String(), "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n") {
		t.Fatalf("stream must end with message_stop: %q", out.String())
	}
}

func TestTranslateStreamNoEventsIsEOF(t *testing.T) {
	if err := translateStream(context.Background(), io.Discard, io.NopCloser(bytes.NewReader(nil)), "p"); err == nil {
		t.Fatal("expected io.EOF so the gateway can retry another target")
	}
}

type parsedEvent struct {
	name string
	data string
}

func parseEvents(raw string) ([]parsedEvent, error) {
	var events []parsedEvent
	parser := sseParser(bytes.NewReader([]byte(raw)))
	for {
		event, err := parser.Next()
		if err == io.EOF {
			return events, nil
		}
		if err != nil {
			return events, err
		}
		events = append(events, parsedEvent{name: event.Type, data: event.Data})
	}
}
