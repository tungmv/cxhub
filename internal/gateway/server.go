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
	"strings"
	"time"

	"cxhub/internal/config"
	"cxhub/internal/health"
	"cxhub/internal/provider"
	"cxhub/internal/responses"
	"cxhub/internal/routing"
	"cxhub/internal/sse"
)

type Server struct {
	Config    *config.Config
	Router    *routing.Router
	Providers map[string]provider.Provider
	Health    *health.Registry
	Logger    *slog.Logger
	HTTP      *http.Server
	Decider   decisionScorer
}

func NewServer(cfg *config.Config, providers map[string]provider.Provider, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	server := &Server{Config: cfg, Router: routing.New(cfg, providers), Providers: providers, Health: health.New(names), Logger: logger}
	if cfg.Decision.Backend != "" {
		server.Decider = newSpanDecision(cfg.Decision, cfg.Backends[cfg.Decision.Backend])
	}
	return server
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
	s.HTTP = &http.Server{Addr: s.Config.Address(), Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second}
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
	data := make([]map[string]any, 0, len(s.Router.Profiles()))
	if s.Decider != nil {
		data = append(data, map[string]any{"id": "auto", "object": "model", "owned_by": "cxhub"})
	}
	for _, profile := range s.Router.Profiles() {
		data = append(data, map[string]any{"id": profile, "object": "model", "owned_by": "cxhub"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	backendStates := s.Health.Snapshot()
	profileStatus := make(map[string][]map[string]any, len(s.Config.Profiles))
	for profile, definition := range s.Config.Profiles {
		for _, target := range definition.Targets {
			state := backendStates[target.Backend]
			profileStatus[profile] = append(profileStatus[profile], map[string]any{
				"level":      strings.TrimSpace(definition.Level),
				"auto_tier":  definition.AutoTier,
				"priority":   definition.Priority,
				"retries":    definition.EffectiveRetries(),
				"backend":    target.Backend,
				"model":      target.Model,
				"healthy":    state.Healthy,
				"last_check": state.LastCheck,
				"last_error": state.LastError,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"gateway":  map[string]any{"address": s.Config.Address(), "status": "ok"},
		"decision": map[string]any{"enabled": s.Decider != nil, "model": s.Config.Decision.Model},
		"backends": backendStates,
		"profiles": profileStatus,
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
	profile := req.Model
	if profile == "" {
		writeError(w, http.StatusBadRequest, "model is required and must be a configured logical profile", requestID)
		return
	}
	if profile == "auto" {
		selected, effort, scores, err := s.automaticRoute(r.Context(), req)
		if err != nil {
			selected = s.Config.Decision.DefaultProfile
			effort = "medium"
			s.Logger.Warn("automatic route decision failed; using default profile", "request_id", requestID, "profile", selected, "error", err)
		} else {
			s.Logger.Info("automatic route selected", "request_id", requestID, "profile", selected, "reasoning_effort", effort, "speed_score", scores.Speed, "quality_score", scores.Quality, "high_effort_score", scores.HighEffort)
		}
		if err := req.SetReasoningEffort(effort); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), requestID)
			return
		}
		profile = selected
	}
	targets, err := s.Router.Resolve(profile)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	if len(targets) == 0 {
		writeError(w, http.StatusServiceUnavailable, "profile has no available targets", requestID)
		return
	}
	var lastErr error
	timedOut := false
	rounds, backoff := attemptRounds(targets)
	for round := 0; round < rounds; round++ {
		if round > 0 && !sleepBeforeRetry(r.Context(), backoff) {
			// The client is gone; no further attempt can help.
			s.writeUpstreamFailure(w, requestID, timedOut, lastErr)
			return
		}
		for index, target := range targets {
			moreAttempts := index+1 < len(targets) || round+1 < rounds
			// Each attempt gets its own context. When the target declares a timeout,
			// a timer cancels the attempt before any output so the next target can
			// serve the request; the timer is disarmed on first meaningful output.
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
				s.recordHealth(r.Context(), target.Backend, callErr)
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
				s.recordHealth(r.Context(), target.Backend, upstreamErr)
				s.logAttempt(requestID, profile, target, started, time.Time{}, status, index > 0, round, upstreamErr)
				// While attempts remain, every upstream error status is worth
				// retrying elsewhere; the strict fallback set only governs the
				// final reported error.
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
						lastErr = wrapErr
						s.recordHealth(r.Context(), target.Backend, wrapErr)
						s.logAttempt(requestID, profile, target, started, time.Now(), resp.StatusCode, index > 0, round, wrapErr)
						writeError(w, http.StatusBadGateway, wrapErr.Error(), requestID)
						return
					}
					resp = converted
				}
				s.recordHealth(r.Context(), target.Backend, nil)
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
					lastErr = wrapErr
					s.recordHealth(r.Context(), target.Backend, wrapErr)
					s.logAttempt(requestID, profile, target, started, time.Time{}, resp.StatusCode, index > 0, round, wrapErr)
					writeError(w, http.StatusBadGateway, wrapErr.Error(), requestID)
					return
				}
				resp = converted
			}
			streamResult := s.streamResponse(w, r, resp, requestID, profile, target, started, index > 0, round, func() { stopTimeoutTimer(timer) })
			stopTimeoutTimer(timer)
			cancelAttempt()
			s.recordHealth(r.Context(), target.Backend, streamResult.err)
			if streamResult.err != nil && !streamResult.wrote {
				timedOut = timedOut || attemptTimedOut(attemptCtx, r.Context(), target.Timeout)
				lastErr = streamResult.err
			}
			if !streamResult.wrote && streamResult.err != nil && moreAttempts && r.Context().Err() == nil {
				// No event reached the client, so this is still a safe pre-generation retry.
				continue
			}
			if streamResult.err != nil && !streamResult.wrote && r.Context().Err() == nil {
				s.writeUpstreamFailure(w, requestID, timedOut, lastErr)
			}
			return
		}
	}
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
			// A plain EOF after events were forwarded is how many upstreams
			// terminate a Responses SSE stream; it is a clean end, not a
			// backend failure, so it must not poison backend health.
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
				// The model is producing output; a late timeout must not kill a
				// healthy generation that can no longer be replayed elsewhere.
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

func (s *Server) recordHealth(ctx context.Context, backend string, err error) {
	if err != nil && ctx.Err() != nil {
		return
	}
	s.Health.Set(backend, err)
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

type contextKey string

const requestIDKey contextKey = "request-id"

func requestIDMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = newRequestID()
		}
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(requestIDKey).(string); ok {
		return value
	}
	return "unknown"
}

func newRequestID() string {
	data := make([]byte, 12)
	if _, err := rand.Read(data); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(data)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message, requestID string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"type": "cxhub_error", "message": message, "code": status, "request_id": requestID}})
}
