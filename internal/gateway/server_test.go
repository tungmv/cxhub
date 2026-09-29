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
	"cxhub/internal/health"
	"cxhub/internal/provider"
	"cxhub/internal/responses"
	"cxhub/internal/routing"
)

type fakeProvider struct {
	id string
}

func (p *fakeProvider) ID() string { return p.id }

func (p *fakeProvider) Responses(context.Context, *responses.Request, string) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented")
}

func (p *fakeProvider) Health(context.Context) error { return nil }

type fakeUpstream struct {
	mu        sync.Mutex
	requests  []fakeRequest
	status    int
	delay     time.Duration
	stagger   time.Duration
	failFirst int
	events    []string
}

type fakeRequest struct {
	Model           string
	ReasoningEffort string
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
		Model     string `json:"model"`
		Reasoning struct {
			Effort string `json:"effort"`
		} `json:"reasoning"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, fakeRequest{Model: payload.Model, ReasoningEffort: payload.Reasoning.Effort})
	status := f.status
	delay := f.delay
	stagger := f.stagger
	failFirst := f.failFirst
	events := append([]string(nil), f.events...)
	attempt := len(f.requests)
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if status != 0 && status != http.StatusOK && (failFirst == 0 || attempt <= failFirst) {
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
			if stagger > 0 {
				select {
				case <-time.After(stagger):
				case <-r.Context().Done():
					return
				}
			}
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

func TestUpdateConfigReplacesProfiles(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	backend := config.BackendConfig{Type: "openai-compatible", BaseURL: "http://localhost/v1"}
	providers := map[string]provider.Provider{"one": &fakeProvider{id: "one"}}
	initial := &config.Config{
		Backends: map[string]config.BackendConfig{"one": backend},
		Profiles: map[string]config.ProfileConfig{"old": {Targets: []config.TargetConfig{{Backend: "one", Model: "old-model"}}}},
	}
	server := NewServer(initial, providers, logger)
	updated := &config.Config{
		Backends: map[string]config.BackendConfig{"one": backend},
		Profiles: map[string]config.ProfileConfig{"new": {Targets: []config.TargetConfig{{Backend: "one", Model: "new-model"}}}},
	}
	if err := server.UpdateConfig(updated, providers); err != nil {
		t.Fatal(err)
	}
	changedAddress := &config.Config{
		Gateway: config.GatewayConfig{Port: 8788}, Backends: map[string]config.BackendConfig{"one": backend},
		Profiles: map[string]config.ProfileConfig{"wrong": {Targets: []config.TargetConfig{{Backend: "one", Model: "wrong-model"}}}},
	}
	if err := server.UpdateConfig(changedAddress, providers); err == nil {
		t.Fatal("expected gateway address change to require restart")
	}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"new"`) || strings.Contains(response.Body.String(), `"old"`) {
		t.Fatalf("models after update = %d %s", response.Code, response.Body.String())
	}
	if server.state.Load().config != updated {
		t.Fatal("rejected config replaced the active runtime state")
	}
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

// TestFallbackWhenStreamInjectsFailure reproduces a provider that returns HTTP
// 200, emits the response lifecycle, then injects a response.failed error into
// the SSE stream. The gateway must discard the aborted attempt and serve the
// request from the next target instead of leaking the error to the client.
func TestFallbackWhenStreamInjectsFailure(t *testing.T) {
	first := &fakeUpstream{events: []string{
		event("response.created", `{"type":"response.created","response":{"id":"resp_1"}}`),
		event("response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_1"}}`),
		event("response.failed", `{"type":"response.failed","response":{"id":"resp_1","status":"failed","output":[],"error":{"code":"server_error","message":"Upstream error from Nvidia: Service temporarily overloaded"},"error_type":"provider_overloaded"}}`),
		"data: [DONE]\n\n",
	}}
	second := &fakeUpstream{events: []string{
		event("response.created", `{"type":"response.created","response":{"id":"resp_2"}}`),
		event("response.output_text.delta", `{"delta":"recovered"}`),
		"data: [DONE]\n\n",
	}}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"first": first, "second": second}, map[string]config.ProfileConfig{"fallback": {Targets: []config.TargetConfig{{Backend: "first", Model: "one"}, {Backend: "second", Model: "two"}}}})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"fallback","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.StatusCode, body)
	}
	if !strings.Contains(string(body), `"delta":"recovered"`) {
		t.Fatalf("expected fallback to the next target, body=%s", body)
	}
	if strings.Contains(string(body), "response.failed") || strings.Contains(string(body), "temporarily overloaded") {
		t.Fatalf("injected upstream failure leaked to the client: %s", body)
	}
}

// TestStreamFailureAfterOutputStripsError covers the case where the provider
// fails only after answer output has already been streamed and fallback is no
// longer safe: the terminal event must not carry the error object that clients
// report as an injected JSON error.
func TestStreamFailureAfterOutputStripsError(t *testing.T) {
	first := &fakeUpstream{events: []string{
		event("response.created", `{"type":"response.created","response":{"id":"resp_1"}}`),
		event("response.output_text.delta", `{"delta":"partial"}`),
		event("response.failed", `{"type":"response.failed","response":{"id":"resp_1","status":"failed","output":[],"error":{"code":"server_error","message":"Upstream error from Nvidia: Service temporarily overloaded"},"error_type":"provider_overloaded"}}`),
		"data: [DONE]\n\n",
	}}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"first": first}, map[string]config.ProfileConfig{"solo": {Targets: []config.TargetConfig{{Backend: "first", Model: "one"}}}})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"solo","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), `"delta":"partial"`) {
		t.Fatalf("expected committed output, body=%s", body)
	}
	if strings.Contains(string(body), `"error":{`) || strings.Contains(string(body), "temporarily overloaded") {
		t.Fatalf("error object leaked into the client stream: %s", body)
	}
}

func TestPreferHealthyOrdersFailingTargetLast(t *testing.T) {
	targets := []routing.Target{
		{Backend: "flaky", Model: "a"},
		{Backend: "steady", Model: "b"},
		{Backend: "dead", Model: "c"},
	}
	targetStates := map[string]health.TargetState{
		health.Key("flaky", "a"):  {Failures: 1},
		health.Key("steady", "b"): {Failures: 0},
		health.Key("dead", "c"):   {Failures: 7},
	}
	order := []string{}
	for _, target := range preferHealthy(targetStates, targets) {
		order = append(order, target.Model)
	}
	if strings.Join(order, ",") != "b,a,c" {
		t.Fatalf("expected healthy-first ordering, got %v", order)
	}
	// A failing target must stay reachable, never be dropped.
	if len(preferHealthy(targetStates, targets)) != len(targets) {
		t.Fatal("preferHealthy dropped a target")
	}
}

// TestPreferHealthyIsPerTarget guards the regression where one broken model on a
// shared backend demoted every other model on that backend.
func TestPreferHealthyIsPerTarget(t *testing.T) {
	targets := []routing.Target{
		{Backend: "cliproxy", Model: "codex-broken"},
		{Backend: "cliproxy", Model: "gemini-ok"},
		{Backend: "openrouter", Model: "or-ok"},
	}
	targetStates := map[string]health.TargetState{
		health.Key("cliproxy", "codex-broken"): {Failures: 40},
	}
	order := []string{}
	for _, target := range preferHealthy(targetStates, targets) {
		order = append(order, target.Model)
	}
	if strings.Join(order, ",") != "gemini-ok,or-ok,codex-broken" {
		t.Fatalf("healthy models on a shared backend must stay ahead, got %v", order)
	}
}

func TestPreferHealthyOrdersCoolingTargetLast(t *testing.T) {
	targets := []routing.Target{
		{Backend: "a", Model: "hot"},
		{Backend: "b", Model: "cooled"},
	}
	targetStates := map[string]health.TargetState{
		health.Key("b", "cooled"): {Failures: 0, CoolingUntil: time.Now().Add(time.Minute)},
	}
	if got := preferHealthy(targetStates, targets)[0].Model; got != "hot" {
		t.Fatalf("a cooling target must be tried last, got first %q", got)
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

func TestSameLevelCrossProfileFallback(t *testing.T) {
	primary := &fakeUpstream{status: http.StatusBadGateway}
	backup := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"from-planner"}`), "data: [DONE]\n\n"}}
	otherLevel := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"wrong-tier"}`), "data: [DONE]\n\n"}}
	server, fakes := newTestGateway(t, map[string]*fakeUpstream{"primary": primary, "backup": backup, "other": otherLevel}, map[string]config.ProfileConfig{
		"coder":   {Level: "reasoning", Targets: []config.TargetConfig{{Backend: "primary", Model: "m-coder"}}},
		"planner": {Level: "reasoning", Targets: []config.TargetConfig{{Backend: "backup", Model: "m-planner"}}},
		"fast":    {Level: "speed", Targets: []config.TargetConfig{{Backend: "other", Model: "m-fast"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "from-planner") {
		t.Fatalf("same-level fallback failed: status=%d body=%s", response.StatusCode, body)
	}
	if got := modelRequests(t, fakes, "backup"); len(got) != 1 || got[0] != "m-planner" {
		t.Fatalf("backup backend requests = %v", got)
	}
	if got := modelRequests(t, fakes, "other"); len(got) != 0 {
		t.Fatalf("different-level backend was contacted: %v", got)
	}
}

func TestProfilesWithoutLevelDoNotCrossFallback(t *testing.T) {
	primary := &fakeUpstream{status: http.StatusServiceUnavailable}
	backup := &fakeUpstream{}
	server, fakes := newTestGateway(t, map[string]*fakeUpstream{"primary": primary, "backup": backup}, map[string]config.ProfileConfig{
		"coder":   {Targets: []config.TargetConfig{{Backend: "primary", Model: "m-coder"}}},
		"planner": {Targets: []config.TargetConfig{{Backend: "backup", Model: "m-planner"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if got := modelRequests(t, fakes, "backup"); len(got) != 0 {
		t.Fatalf("level-less profile gained cross-profile fallback: %v", got)
	}
}

func TestSameLevelSkipsDuplicateTargets(t *testing.T) {
	shared := &fakeUpstream{}
	server, fakes := newTestGateway(t, map[string]*fakeUpstream{"shared": shared}, map[string]config.ProfileConfig{
		"coder":   {Level: "reasoning", Targets: []config.TargetConfig{{Backend: "shared", Model: "m"}}},
		"planner": {Level: "reasoning", Targets: []config.TargetConfig{{Backend: "shared", Model: "m"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"id":"resp-m"`) {
		t.Fatalf("duplicate target handling failed: status=%d body=%s", response.StatusCode, body)
	}
	if got := modelRequests(t, fakes, "shared"); len(got) != 1 {
		t.Fatalf("duplicate target was retried: %v", got)
	}
}

func TestTargetTimeoutFallsBack(t *testing.T) {
	slow := &fakeUpstream{delay: 400 * time.Millisecond}
	quick := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"recovered"}`), "data: [DONE]\n\n"}}
	server, fakes := newTestGateway(t, map[string]*fakeUpstream{"slow": slow, "quick": quick}, map[string]config.ProfileConfig{
		"coder": {Targets: []config.TargetConfig{{Backend: "slow", Model: "m-slow", Timeout: "50ms"}, {Backend: "quick", Model: "m-quick"}}},
	})
	started := time.Now()
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "recovered") {
		t.Fatalf("timeout fallback failed: status=%d body=%s", response.StatusCode, body)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("timeout fallback waited too long: %s", elapsed)
	}
	if got := modelRequests(t, fakes, "quick"); len(got) != 1 || got[0] != "m-quick" {
		t.Fatalf("quick backend requests = %v", got)
	}
}

func TestAllTargetsTimedOutReturns504(t *testing.T) {
	slowOne := &fakeUpstream{delay: 300 * time.Millisecond}
	slowTwo := &fakeUpstream{delay: 300 * time.Millisecond}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"slow-one": slowOne, "slow-two": slowTwo}, map[string]config.ProfileConfig{
		"coder": {Targets: []config.TargetConfig{{Backend: "slow-one", Model: "m1", Timeout: "50ms"}, {Backend: "slow-two", Model: "m2", Timeout: "50ms"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusGatewayTimeout || !strings.Contains(string(body), "upstream request timed out") {
		t.Fatalf("expected 504 timeout error, got status=%d body=%s", response.StatusCode, body)
	}
}

func TestTargetTimeoutDisarmedAfterFirstEvent(t *testing.T) {
	first := &fakeUpstream{delay: 10 * time.Millisecond, stagger: 150 * time.Millisecond, events: []string{
		event("response.output_text.delta", `{"delta":"start"}`),
		// This event arrives long after the configured attempt timeout expired.
		event("response.output_text.delta", `{"delta":"-and-finish"}`),
		"data: [DONE]\n\n",
	}}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"x": first}, map[string]config.ProfileConfig{
		"coder": {Targets: []config.TargetConfig{{Backend: "x", Model: "m", Timeout: "50ms"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "start") || !strings.Contains(string(body), "-and-finish") {
		t.Fatalf("healthy generation was cut by attempt timeout: status=%d body=%s", response.StatusCode, body)
	}
}

func modelRequests(t *testing.T, fakes map[string]*fakeUpstream, backend string) []string {
	t.Helper()
	fake := fakes[backend]
	fake.mu.Lock()
	defer fake.mu.Unlock()
	models := make([]string, 0, len(fake.requests))
	for _, request := range fake.requests {
		models = append(models, request.Model)
	}
	return models
}

func TestOrchestratorRetriesUntilSuccess(t *testing.T) {
	flaky := &fakeUpstream{status: http.StatusBadGateway, failFirst: 2}
	server, fakes := newTestGateway(t, map[string]*fakeUpstream{"flaky": flaky}, map[string]config.ProfileConfig{
		"orchestrator": {Retries: 3, RetryBackoff: "20ms", Targets: []config.TargetConfig{{Backend: "flaky", Model: "m-orchestrator"}}},
	})
	started := time.Now()
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"orchestrator","stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"id":"resp-m-orchestrator"`) {
		t.Fatalf("retry-until-success failed: status=%d body=%s", response.StatusCode, body)
	}
	if got := modelRequests(t, fakes, "flaky"); len(got) != 3 {
		t.Fatalf("expected 3 attempts, got %v", got)
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond {
		t.Fatalf("retry backoff was not applied: %s", elapsed)
	}
}

func TestRetriesExhaustedStillReportsFailure(t *testing.T) {
	dead := &fakeUpstream{status: http.StatusBadGateway, failFirst: 2}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"dead": dead}, map[string]config.ProfileConfig{
		"orchestrator": {Retries: 2, Targets: []config.TargetConfig{{Backend: "dead", Model: "m"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"orchestrator","stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestPriorityOrdersSameLevelFallback(t *testing.T) {
	preferred := &fakeUpstream{status: http.StatusBadGateway} // high priority, fails
	backup := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"ok"}`), "data: [DONE]\n\n"}}
	server, fakes := newTestGateway(t, map[string]*fakeUpstream{"preferred": preferred, "backup": backup}, map[string]config.ProfileConfig{
		"z-preferred": {Level: "reasoning", Priority: -1, Targets: []config.TargetConfig{{Backend: "preferred", Model: "m-top"}}},
		"a-backup":    {Level: "reasoning", Priority: 10, Targets: []config.TargetConfig{{Backend: "backup", Model: "m-backup"}}},
		"coder":       {Level: "reasoning", Targets: []config.TargetConfig{{Backend: "preferred", Model: "m-coder"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"delta":"ok"`) {
		t.Fatalf("priority fallback failed: status=%d body=%s", response.StatusCode, body)
	}
	// The high-priority profile must be attempted before the alphabetically
	// first low-priority profile.
	if got := modelRequests(t, fakes, "preferred"); len(got) == 0 {
		t.Fatalf("high-priority profile was never attempted; fallback order is wrong")
	}
	if got := modelRequests(t, fakes, "backup"); len(got) != 1 || got[0] != "m-backup" {
		t.Fatalf("backup backend requests = %v", got)
	}
}

func TestNonFallbackableStatusAdvancesWhenAttemptsRemain(t *testing.T) {
	rejecting := &fakeUpstream{status: http.StatusBadRequest}
	healthy := &fakeUpstream{events: []string{event("response.output_text.delta", `{"delta":"ok"}`), "data: [DONE]\n\n"}}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"rejecting": rejecting, "healthy": healthy}, map[string]config.ProfileConfig{
		"coder": {Targets: []config.TargetConfig{{Backend: "rejecting", Model: "one"}, {Backend: "healthy", Model: "two"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"delta":"ok"`) {
		t.Fatalf("unexpected failure with attempts remaining: status=%d body=%s", response.StatusCode, body)
	}
}

func TestFinalNonFallbackableStatusSurfaces(t *testing.T) {
	rejecting := &fakeUpstream{status: http.StatusBadRequest}
	server, _ := newTestGateway(t, map[string]*fakeUpstream{"rejecting": rejecting}, map[string]config.ProfileConfig{
		"coder": {Targets: []config.TargetConfig{{Backend: "rejecting", Model: "one"}}},
	})
	response, err := server.Client().Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"coder","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func event(name, data string) string { return "event: " + name + "\ndata: " + data + "\n\n" }

// An upstream may terminate a Responses SSE stream with a plain EOF instead of
// a [DONE] sentinel. That is a clean end: the request must succeed and the
// backend must stay healthy.
func TestStreamEndsWithPlainEOFKeepsBackendHealthy(t *testing.T) {
	backends := map[string]*fakeUpstream{
		"a": {events: []string{event("response.output_text.delta", `{"delta":"ok"}`)}},
	}
	profiles := map[string]config.ProfileConfig{
		"orchestrator": {Targets: []config.TargetConfig{{Backend: "a", Model: "model-a"}}},
	}
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
	srv := NewServer(cfg, providers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(srv.Handler())
	t.Cleanup(server.Close)

	resp, err := http.Post(server.URL+"/v1/responses", "application/json",
		strings.NewReader(`{"model":"orchestrator","input":"hi","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "response.output_text.delta") {
		t.Fatalf("stream did not forward delta events: %q", string(body))
	}
	state := srv.state.Load().health.Snapshot()["a"]
	if !state.Healthy {
		t.Fatalf("backend marked unhealthy after clean EOF-terminated stream: %q", state.LastError)
	}
}

func TestAnthropicMessagesStreamEndToEnd(t *testing.T) {
	backends := map[string]*fakeUpstream{
		"a": {events: []string{
			`event: response.created` + "\n" + `data: {"type":"response.created","response":{"id":"resp_1","usage":{"input_tokens":5}}}` + "\n\n",
			`event: response.output_item.added` + "\n" + `data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m1","role":"assistant"}}` + "\n\n",
			`event: response.output_text.delta` + "\n" + `data: {"type":"response.output_text.delta","item_id":"m1","delta":"hi there"}` + "\n\n",
			"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n",
		}},
	}
	profiles := map[string]config.ProfileConfig{
		"orchestrator": {Targets: []config.TargetConfig{{Backend: "a", Model: "model-a"}}},
	}
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
	srv := NewServer(cfg, providers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(srv.Handler())
	t.Cleanup(server.Close)

	body := `{"model":"orchestrator","max_tokens":64,"stream":true,"system":"be brief","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`
	resp, err := http.Post(server.URL+"/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "event: message_start") ||
		!strings.Contains(string(raw), `"text":"hi there"`) ||
		!strings.Contains(string(raw), "event: message_stop") {
		t.Fatalf("unexpected Anthropic stream: %s", raw)
	}
	if !strings.Contains(string(raw), `"model":"orchestrator"`) {
		t.Fatalf("profile not echoed: %s", raw)
	}
	state := srv.state.Load().health.Snapshot()["a"]
	if !state.Healthy {
		t.Fatalf("backend marked unhealthy after translated stream: %q", state.LastError)
	}
}

func TestAnthropicMessagesCountTokens(t *testing.T) {
	backends := map[string]*fakeUpstream{"a": {}}
	profiles := map[string]config.ProfileConfig{
		"fast": {Targets: []config.TargetConfig{{Backend: "a", Model: "m"}}},
	}
	providers := map[string]provider.Provider{}
	backendConfig := map[string]config.BackendConfig{}
	for name, fake := range backends {
		upstream := httptest.NewServer(http.HandlerFunc(fake.handler))
		t.Cleanup(upstream.Close)
		backendConfig[name] = config.BackendConfig{Type: "openai-compatible", BaseURL: upstream.URL + "/v1"}
		providers[name] = provider.NewOpenAICompatible(name, backendConfig[name], upstream.Client())
	}
	cfg := &config.Config{Gateway: config.GatewayConfig{Host: "127.0.0.1", Port: 8787}, Backends: backendConfig, Profiles: profiles}
	server := httptest.NewServer(NewServer(cfg, providers, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(server.Close)

	resp, err := http.Post(server.URL+"/v1/messages/count_tokens", "application/json", strings.NewReader(`{"model":"fast","messages":[{"role":"user","content":"hello world"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		InputTokens int `json:"input_tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.InputTokens < 1 {
		t.Fatalf("input_tokens = %d", out.InputTokens)
	}
}
