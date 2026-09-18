package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cxhub/internal/config"
	"cxhub/internal/provider"
)

type fakeUpstream struct {
	mu       sync.Mutex
	requests []fakeRequest
	status   int
	delay    time.Duration
	events   []string
}

type fakeRequest struct {
	Model string
}

func (f *fakeUpstream) handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/models" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"list","data":[]}`)
		return
	}
	if r.URL.Path != "/v1/responses" {
		http.NotFound(w, r)
		return
	}
	var payload struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, fakeRequest{Model: payload.Model})
	status := f.status
	delay := f.delay
	events := append([]string(nil), f.events...)
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if status != 0 && status != http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"error":{"message":"fake upstream failure"}}`)
		return
	}
	if r.Header.Get("Accept") == "text/event-stream, application/json" && len(events) > 0 {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, event := range events {
			_, _ = io.WriteString(w, event)
			flusher.Flush()
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, fmt.Sprintf(`{"id":"resp-%s","output":[]}`, payload.Model))
}

func newTestGateway(t *testing.T, backends map[string]*fakeUpstream, profiles map[string]config.ProfileConfig) (*httptest.Server, map[string]*fakeUpstream) {
	t.Helper()
	providers := make(map[string]provider.Provider, len(backends))
	backendConfig := make(map[string]config.BackendConfig, len(backends))
	for name, fake := range backends {
		upstream := httptest.NewServer(http.HandlerFunc(fake.handler))
		t.Cleanup(upstream.Close)
		backendConfig[name] = config.BackendConfig{Type: "openai-compatible", BaseURL: upstream.URL + "/v1"}
		providers[name] = provider.NewOpenAICompatible(name, backendConfig[name], upstream.Client())
	}
	cfg := &config.Config{Gateway: config.GatewayConfig{Host: "127.0.0.1", Port: 8787}, Backends: backendConfig, Profiles: profiles}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(cfg, providers, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(server.Close)
	return server, backends
}

func TestConcurrentFanoutIsolation(t *testing.T) {
	backends := map[string]*fakeUpstream{
		"a": {delay: 20 * time.Millisecond, events: []string{event("response.output_text.delta", `{"delta":"a"}`), "data: [DONE]\n\n"}},
		"b": {delay: 10 * time.Millisecond, events: []string{event("response.output_text.delta", `{"delta":"b"}`), "data: [DONE]\n\n"}},
		"c": {delay: 15 * time.Millisecond, events: []string{event("response.output_text.delta", `{"delta":"c"}`), "data: [DONE]\n\n"}},
	}
	profiles := map[string]config.ProfileConfig{
		"orchestrator": {Targets: []config.TargetConfig{{Backend: "b", Model: "model-orchestrator"}}},
		"coder":        {Targets: []config.TargetConfig{{Backend: "a", Model: "model-code"}}},
		"reviewer":     {Targets: []config.TargetConfig{{Backend: "c", Model: "model-review"}}},
		"fast":         {Targets: []config.TargetConfig{{Backend: "b", Model: "model-fast"}}},
	}
	server, _ := newTestGateway(t, backends, profiles)

	type result struct {
		profile, body string
		status        int
	}
	results := make(chan result, len(profiles))
	var wg sync.WaitGroup
	for profile := range profiles {
		profile := profile
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":[]}`, profile)))
			if err != nil {
				results <- result{profile: profile, body: err.Error(), status: -1}
				return
			}
			response, err := server.Client().Do(request)
			if err != nil {
				results <- result{profile: profile, body: err.Error(), status: -1}
				return
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			results <- result{profile: profile, body: string(body), status: response.StatusCode}
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.status != http.StatusOK || !strings.Contains(result.body, "response.output_text.delta") {
			t.Fatalf("profile %s: status=%d body=%s", result.profile, result.status, result.body)
		}
	}
	for name, fake := range backends {
		fake.mu.Lock()
		requests := append([]fakeRequest(nil), fake.requests...)
		fake.mu.Unlock()
		for _, request := range requests {
			if name == "a" && request.Model != "model-code" || name == "b" && request.Model != "model-orchestrator" && request.Model != "model-fast" || name == "c" && request.Model != "model-review" {
				t.Fatalf("backend %s received wrong model %s", name, request.Model)
			}
		}
	}
}

func TestToolEventsArePassedThrough(t *testing.T) {
	rawEvents := []string{
		event("response.output_item.added", `{"item":{"type":"function_call","call_id":"call-1","name":"shell"}}`),
		event("response.function_call_arguments.delta", `{"item_id":"call-1","delta":"{\\"cmd\\":\\"pwd\\"}"}`),
		event("response.output_item.done", `{"item":{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{\\"cmd\\":\\"pwd\\"}"}}`),
		"data: [DONE]\n\n",
	}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"tools": {events: rawEvents}}, map[string]config.ProfileConfig{"coder": {Targets: []config.TargetConfig{{Backend: "tools", Model: "tool-model"}}}})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true,"tools":[{"type":"function","name":"shell"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range rawEvents {
		if !strings.Contains(string(body), strings.TrimSpace(event)) {
			t.Fatalf("missing event %q in %q", event, body)
		}
	}
}

func TestNonStreamingResponse(t *testing.T) {
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"x": {}}, map[string]config.ProfileConfig{"fast": {Targets: []config.TargetConfig{{Backend: "x", Model: "real-model"}}}})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"fast","stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"id":"resp-real-model"`) {
		t.Fatalf("non-stream response failed: status=%d body=%s", response.StatusCode, body)
	}
}

func TestFallbackBeforeGeneration(t *testing.T) {
	first := &fakeUpstream{status: http.StatusBadGateway}
	second := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"ok"}`), "data: [DONE]\n\n"}}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"first": first, "second": second}, map[string]config.ProfileConfig{"fallback": {Targets: []config.TargetConfig{{Backend: "first", Model: "one"}, {Backend: "second", Model: "two"}}}})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"fallback","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"delta":"ok"`) {
		t.Fatalf("fallback failed: status=%d body=%s", response.StatusCode, body)
	}
}

func TestFallbackWhenStreamEndsBeforeFirstEvent(t *testing.T) {
	first := &fakeUpstream{events: nil}
	second := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"recovered"}`), "data: [DONE]\n\n"}}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"first": first, "second": second}, map[string]config.ProfileConfig{"fallback": {Targets: []config.TargetConfig{{Backend: "first", Model: "one"}, {Backend: "second", Model: "two"}}}})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"fallback","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"delta":"recovered"`) {
		t.Fatalf("stream fallback failed: status=%d body=%s", response.StatusCode, body)
	}
}

func TestSSEParserHandlesMultilineData(t *testing.T) {
	// Exercise the actual stream path with a multiline data field and ensure it is
	// forwarded as one complete SSE record.
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"x": {events: []string{"event: response.test\ndata: one\ndata: two\n\n", "data: [DONE]\n\n"}}}, map[string]config.ProfileConfig{"fast": {Targets: []config.TargetConfig{{Backend: "x", Model: "m"}}}})
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"fast","stream":true}`))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if !strings.Contains(strings.Join(lines, "|"), "data: one|data: two") {
		t.Fatalf("multiline SSE was not preserved: %v", lines)
	}
}

func event(name, data string) string { return "event: " + name + "\ndata: " + data + "\n\n" }
