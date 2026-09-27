package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"cxhub/internal/config"
	"cxhub/internal/health"
	"cxhub/internal/provider"
	"cxhub/internal/responses"
	"cxhub/internal/routing"
	"cxhub/internal/sse"
)

type Server struct {
	state  atomic.Pointer[runtimeState]
	Logger *slog.Logger
	HTTP   *http.Server
}

type runtimeState struct {
	config    *config.Config
	router    *routing.Router
	health    *health.Registry
	providers map[string]provider.Provider
	decider   decisionScorer
}

func NewServer(cfg *config.Config, providers map[string]provider.Provider, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{Logger: logger}
	s.state.Store(newRuntimeState(cfg, providers))
	return s
}

func newRuntimeState(cfg *config.Config, providers map[string]provider.Provider) *runtimeState {
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	state := &runtimeState{
		config: cfg, router: routing.New(cfg, providers), health: health.New(names), providers: providers,
	}
	if cfg.Decision.Backend != "" {
		state.decider = newSpanDecision(cfg.Decision, cfg.Backends[cfg.Decision.Backend])
	}
	return state
}

// UpdateConfig atomically applies a validated config without interrupting in-flight requests.
func (s *Server) UpdateConfig(cfg *config.Config, providers map[string]provider.Provider) error {
	current := s.state.Load()
	if cfg.Address() != current.config.Address() {
		return fmt.Errorf("gateway address changes require restart")
	}
	s.state.Store(newRuntimeState(cfg, providers))
	return nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("POST /v1/responses", s.handleResponses)
	mux.HandleFunc("POST /v1/messages", s.handleMessages)
	mux.HandleFunc("POST /v1/messages/count_tokens", s.handleCountTokens)
	return requestIDMiddleware(s.Logger, mux)
}

func (s *Server) Start() error {
	s.HTTP = &http.Server{Addr: s.state.Load().config.Address(), Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second}
	return s.HTTP.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.HTTP == nil {
		return nil
	}
	return s.HTTP.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	state := s.state.Load()
	data := make([]map[string]any, 0, len(state.router.Profiles())+1)
	if state.decider != nil {
		data = append(data, map[string]any{"id": "auto", "object": "model", "owned_by": "cxhub"})
	}
	for _, profile := range state.router.Profiles() {
		data = append(data, map[string]any{"id": profile, "object": "model", "owned_by": "cxhub"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	state := s.state.Load()
	backendStates := state.health.Snapshot()
	profileStatus := make(map[string][]map[string]any, len(state.config.Profiles))
	for profile, definition := range state.config.Profiles {
		for _, target := range definition.Targets {
			backendState := backendStates[target.Backend]
			profileStatus[profile] = append(profileStatus[profile], map[string]any{
				"level":    strings.TrimSpace(definition.Level),
				"priority": definition.Priority, "retries": definition.EffectiveRetries(),
				"backend": target.Backend, "model": target.Model, "healthy": backendState.Healthy,
				"last_check": backendState.LastCheck, "last_error": backendState.LastError,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"gateway":  map[string]any{"address": state.config.Address(), "status": "ok"},
		"decision": map[string]any{"enabled": state.decider != nil, "model": state.config.Decision.Model, "candidates": state.config.Decision.CandidateProfiles()},
		"backends": backendStates, "profiles": profileStatus,
	})
}

func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFromContext(r.Context())
	body, ok := readBody(w, r, requestID)
	if !ok {
		return
	}
	req, err := responses.Parse(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	s.serveRequest(w, r, req, nil)
}

// readBody reads and size-limits a request body.
func readBody(w http.ResponseWriter, r *http.Request, requestID string) ([]byte, bool) {
	if r.ContentLength > 64<<20 {
		writeError(w, http.StatusBadRequest, "invalid request body size", requestID)
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "unable to read request body", requestID)
		return nil, false
	}
	if len(body) > 64<<20 {
		writeError(w, http.StatusRequestEntityTooLarge, "request body is too large", requestID)
		return nil, false
	}
	return body, true
}

// responseWrapper converts an upstream Responses success into another wire
// format (e.g. Anthropic Messages) before it reaches the client.
type responseWrapper func(*http.Response) (*http.Response, error)

// serveRequest resolves the profile for req, runs the fallback/retry pipeline,
// and renders the upstream response. A nil wrapper passes Responses bodies
// through untouched.
func (s *Server) serveRequest(w http.ResponseWriter, r *http.Request, req *responses.Request, wrap responseWrapper) {
	started := time.Now()
	requestID := requestIDFromContext(r.Context())
	state := s.state.Load()
	profile := req.Model
	if profile == "" {
		writeError(w, http.StatusBadRequest, "model is required and must be a configured logical profile", requestID)
		return
	}
	if profile == "auto" {
		selected, effort, scores, err := s.automaticRoute(r.Context(), state, req)
		if err != nil {
			selected = state.config.Decision.DefaultProfile
			effort = "medium"
			s.Logger.Warn("automatic route decision failed; using default profile", "request_id", requestID, "profile", selected, "error", err)
		} else {
			s.Logger.Info("automatic route selected", "request_id", requestID, "profile", selected, "reasoning_effort", effort, "model_scores", scores.Models, "high_effort_score", scores.HighEffort, "low_effort_score", scores.LowEffort)
		}
		if err := req.SetReasoningEffort(effort); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), requestID)
			return
		}
		profile = selected
	}
	targets, err := state.router.Resolve(profile)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	if len(targets) == 0 {
		writeError(w, http.StatusServiceUnavailable, "profile has no available targets", requestID)
		return
	}
	targets = preferHealthy(state.health.Snapshot(), targets)
	var lastErr error
	timedOut := false
	rounds, backoff := attemptRounds(targets)
	for round := 0; round < rounds; round++ {
		if round > 0 && !sleepBeforeRetry(r.Context(), backoff) {
			s.writeUpstreamFailure(w, requestID, timedOut, lastErr)
			return
		}
		for index, target := range targets {
			moreAttempts := index+1 < len(targets) || round+1 < rounds
			attemptCtx := r.Context()
			cancelAttempt := func() {}
			var timer *time.Timer
			if target.Timeout > 0 {
				attemptCtx, cancelAttempt = context.WithCancel(r.Context())
				timer = time.AfterFunc(target.Timeout, cancelAttempt)
			}
			resp, callErr := target.Provider.Responses(attemptCtx, req, target.Model)
			if callErr != nil {
				attemptDeadlineHit := attemptTimedOut(attemptCtx, r.Context(), target.Timeout)
				stopTimeoutTimer(timer)
				cancelAttempt()
				timedOut = timedOut || attemptDeadlineHit
				lastErr = callErr
				s.recordHealth(r.Context(), state.health, target.Backend, callErr)
				s.logAttempt(requestID, profile, target, started, time.Time{}, 0, index > 0, round, callErr)
				if moreAttempts {
					continue
				}
				s.writeUpstreamFailure(w, requestID, timedOut, lastErr)
				return
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				stopTimeoutTimer(timer)
				cancelAttempt()
				status, upstreamErr := upstreamError(resp)
				lastErr = upstreamErr
				s.recordHealth(r.Context(), state.health, target.Backend, upstreamErr)
				s.logAttempt(requestID, profile, target, started, time.Time{}, status, index > 0, round, upstreamErr)
				if moreAttempts {
					continue
				}
				writeError(w, mapUpstreamStatus(status), upstreamErr.Error(), requestID)
				return
			}
			if !req.Stream {
				stopTimeoutTimer(timer)
				if wrap != nil {
					converted, wrapErr := wrap(resp)
					if wrapErr != nil {
						_ = resp.Body.Close()
						cancelAttempt()
						s.recordHealth(r.Context(), state.health, target.Backend, wrapErr)
						s.logAttempt(requestID, profile, target, started, time.Now(), resp.StatusCode, index > 0, round, wrapErr)
						writeError(w, http.StatusBadGateway, wrapErr.Error(), requestID)
						return
					}
					resp = converted
				}
				s.recordHealth(r.Context(), state.health, target.Backend, nil)
				defer resp.Body.Close()
				copyHeaders(w.Header(), resp.Header)
				w.WriteHeader(resp.StatusCode)
				_, _ = io.Copy(w, resp.Body)
				cancelAttempt()
				s.logAttempt(requestID, profile, target, started, time.Now(), resp.StatusCode, index > 0, round, nil)
				return
			}
			if wrap != nil {
				converted, wrapErr := wrap(resp)
				if wrapErr != nil {
					_ = resp.Body.Close()
					stopTimeoutTimer(timer)
					cancelAttempt()
					s.recordHealth(r.Context(), state.health, target.Backend, wrapErr)
					s.logAttempt(requestID, profile, target, started, time.Time{}, resp.StatusCode, index > 0, round, wrapErr)
					writeError(w, http.StatusBadGateway, wrapErr.Error(), requestID)
					return
				}
				resp = converted
			}
			streamResult := s.streamResponse(w, r, resp, requestID, profile, target, started, index > 0, round, func() { stopTimeoutTimer(timer) })
			stopTimeoutTimer(timer)
			cancelAttempt()
			s.recordHealth(r.Context(), state.health, target.Backend, streamResult.err)
			if streamResult.err != nil && !streamResult.wrote {
				timedOut = timedOut || attemptTimedOut(attemptCtx, r.Context(), target.Timeout)
				lastErr = streamResult.err
			}
			if !streamResult.wrote && streamResult.err != nil && moreAttempts && r.Context().Err() == nil {
				continue
			}
			if streamResult.err != nil && !streamResult.wrote && r.Context().Err() == nil {
				s.writeUpstreamFailure(w, requestID, timedOut, lastErr)
			}
			return
		}
	}
}

// preferHealthy moves targets whose backend has fewer consecutive failures
// first, so an unstable provider is tried last instead of first. YAML order is
// preserved inside a group of equal failure counts, and no target is dropped:
// a failing backend still serves the request if every healthier one fails.
func preferHealthy(backends map[string]health.Backend, targets []routing.Target) []routing.Target {
	ordered := append([]routing.Target(nil), targets...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return backends[ordered[i].Backend].Failures < backends[ordered[j].Backend].Failures
	})
	return ordered
}

// attemptRounds returns the retry passes over the target list (at least one)
// and the backoff between passes.
func attemptRounds(targets []routing.Target) (rounds int, backoff time.Duration) {
	rounds = 1
	for _, target := range targets {
		if target.Retries > rounds {
			rounds = target.Retries
		}
		if target.Backoff > backoff {
			backoff = target.Backoff
		}
	}
	return rounds, backoff
}

func sleepBeforeRetry(ctx context.Context, backoff time.Duration) bool {
	if backoff <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

// attemptTimedOut reports whether the attempt context was cancelled by its own
// timeout timer rather than by the client.
func attemptTimedOut(attemptCtx, clientCtx context.Context, timeout time.Duration) bool {
	return timeout > 0 && clientCtx.Err() == nil && attemptCtx.Err() != nil
}

func stopTimeoutTimer(timer *time.Timer) {
	if timer != nil {
		timer.Stop()
	}
}

func (s *Server) writeUpstreamFailure(w http.ResponseWriter, requestID string, timedOut bool, err error) {
	status := http.StatusBadGateway
	message := "upstream request failed"
	if timedOut {
		status = http.StatusGatewayTimeout
		message = "upstream request timed out"
	}
	if err != nil {
		message = fmt.Sprintf("%s: %v", message, err)
	}
	writeError(w, status, message, requestID)
}

type streamResult struct {
	wrote bool
	err   error
}

func (s *Server) streamResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, requestID, profile string, target routing.Target, started time.Time, fallback bool, round int, stopTimeout func()) streamResult {
	defer resp.Body.Close()
	flusher, ok := w.(http.Flusher)
	if !ok {
		return streamResult{err: fmt.Errorf("streaming is not supported by response writer")}
	}
	parser := sse.NewParser(resp.Body)
	firstToken := time.Time{}
	wrote := false
	writeEvent := func(raw []byte) {
		if !wrote {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("X-Request-ID", requestID)
			wrote = true
		}
		_, _ = w.Write(raw)
		flusher.Flush()
	}
	for {
		event, err := parser.Next()
		if errors.Is(err, sse.ErrDone) {
			if len(event.Raw) > 0 {
				writeEvent(event.Raw)
			}
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, round, nil)
			return streamResult{wrote: wrote}
		}
		if errors.Is(err, io.EOF) && wrote {
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, round, nil)
			return streamResult{wrote: wrote}
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, round, err)
			return streamResult{wrote: wrote, err: err}
		}
		if err != nil {
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, round, err)
			return streamResult{wrote: wrote, err: err}
		}
		if firstToken.IsZero() {
			if meaningfulEvent(event.Type, event.Data) {
				firstToken = time.Now()
				stopTimeout()
			}
		}
		writeEvent(event.Raw)
		select {
		case <-r.Context().Done():
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, round, r.Context().Err())
			return streamResult{wrote: wrote, err: r.Context().Err()}
		default:
		}
	}
}

func (s *Server) recordHealth(ctx context.Context, registry *health.Registry, backend string, err error) {
	if err != nil && ctx.Err() != nil {
		return
	}
	registry.Set(backend, err)
}

func meaningfulEvent(eventType, data string) bool {
	if eventType == "" {
		var envelope struct {
			Type string `json:"type"`
		}
		if json.Unmarshal([]byte(data), &envelope) == nil {
			eventType = envelope.Type
		}
	}
	return strings.Contains(eventType, "output_text.delta") ||
		strings.Contains(eventType, "function_call_arguments.delta") ||
		strings.Contains(eventType, "custom_tool_call_input.delta") ||
		strings.Contains(eventType, "content_block_delta")
}

func (s *Server) logAttempt(requestID, profile string, target routing.Target, started, firstToken time.Time, status int, fallback bool, round int, err error) {
	attrs := []any{"request_id", requestID, "logical_profile", profile, "backend", target.Backend, "real_model", target.Model, "status", status, "fallback_used", fallback, "attempt_round", round + 1, "total_latency_ms", time.Since(started).Milliseconds()}
	if target.Profile != profile {
		attrs = append(attrs, "resolved_profile", target.Profile)
	}
	if target.Timeout > 0 {
		attrs = append(attrs, "target_timeout", target.Timeout.String())
	}
	if !firstToken.IsZero() {
		attrs = append(attrs, "time_to_first_token_ms", firstToken.Sub(started).Milliseconds())
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
		s.Logger.Error("request completed", attrs...)
		return
	}
	s.Logger.Info("request completed", attrs...)
}

func upstreamError(resp *http.Response) (int, error) {
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	message := fmt.Sprintf("upstream returned HTTP %d", resp.StatusCode)
	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &payload) == nil && payload.Error.Message != "" {
		message = payload.Error.Message
	}
	return resp.StatusCode, errors.New(message)
}

func shouldFallback(status int) bool {
	return status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504
}

func mapUpstreamStatus(status int) int {
	if status >= 400 && status <= 599 {
		return status
	}
	return http.StatusBadGateway
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func requestIDMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = newRequestID()
		}
		ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type requestIDKey struct{}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey{}).(string)
	return requestID
}

func newRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message, requestID string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"message": message}, "request_id": requestID})
}
