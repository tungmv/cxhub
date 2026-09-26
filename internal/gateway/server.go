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
	config *config.Config
	router *routing.Router
	health *health.Registry
}

func NewServer(cfg *config.Config, providers map[string]provider.Provider, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	s := &Server{Logger: logger}
	s.state.Store(&runtimeState{config: cfg, router: routing.New(cfg, providers), health: health.New(names)})
	return s
}

// UpdateConfig atomically applies a validated config without interrupting in-flight requests.
func (s *Server) UpdateConfig(cfg *config.Config, providers map[string]provider.Provider) error {
	// Gateway address changes need a listener restart, so do not publish a
	// config that would make /status report a different address than we serve.
	if cfg.Address() != s.state.Load().config.Address() {
		return fmt.Errorf("gateway address changes require restart")
	}
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	s.state.Store(&runtimeState{config: cfg, router: routing.New(cfg, providers), health: health.New(names)})
	return nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("POST /v1/responses", s.handleResponses)
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
	data := make([]map[string]any, 0, len(state.router.Profiles()))
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
			state := backendStates[target.Backend]
			profileStatus[profile] = append(profileStatus[profile], map[string]any{
				"backend":    target.Backend,
				"model":      target.Model,
				"healthy":    state.Healthy,
				"last_check": state.LastCheck,
				"last_error": state.LastError,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"gateway":  map[string]any{"address": s.state.Load().config.Address(), "status": "ok"},
		"backends": backendStates,
		"profiles": profileStatus,
	})
}

func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := requestIDFromContext(r.Context())
	if r.ContentLength > 64<<20 {
		writeError(w, http.StatusBadRequest, "invalid request body size", requestID)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "unable to read request body", requestID)
		return
	}
	if len(body) > 64<<20 {
		writeError(w, http.StatusRequestEntityTooLarge, "request body is too large", requestID)
		return
	}
	req, err := responses.Parse(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	profile := req.Model
	if profile == "" {
		writeError(w, http.StatusBadRequest, "model is required and must be a configured logical profile", requestID)
		return
	}
	state := s.state.Load()
	targets, err := state.router.Resolve(profile)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	if len(targets) == 0 {
		writeError(w, http.StatusServiceUnavailable, "profile has no available targets", requestID)
		return
	}
	for index, target := range targets {
		resp, callErr := target.Provider.Responses(r.Context(), req, target.Model)
		if callErr != nil {
			recordHealth(r.Context(), state.health, target.Backend, callErr)
			s.logAttempt(requestID, profile, target, started, time.Time{}, 0, index > 0, callErr)
			if index+1 < len(targets) {
				continue
			}
			writeError(w, http.StatusBadGateway, "upstream request failed", requestID)
			return
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			status, upstreamErr := upstreamError(resp)
			recordHealth(r.Context(), state.health, target.Backend, upstreamErr)
			s.logAttempt(requestID, profile, target, started, time.Time{}, status, index > 0, upstreamErr)
			if shouldFallback(status) && index+1 < len(targets) {
				continue
			}
			writeError(w, mapUpstreamStatus(status), upstreamErr.Error(), requestID)
			return
		}
		if !req.Stream {
			recordHealth(r.Context(), state.health, target.Backend, nil)
			defer resp.Body.Close()
			copyHeaders(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			s.logAttempt(requestID, profile, target, started, time.Now(), resp.StatusCode, index > 0, nil)
			return
		}
		streamResult := s.streamResponse(w, r, resp, requestID, profile, target, started, index > 0)
		recordHealth(r.Context(), state.health, target.Backend, streamResult.err)
		if !streamResult.wrote && streamResult.err != nil && index+1 < len(targets) && r.Context().Err() == nil {
			// No event reached the client, so this is still a safe pre-generation retry.
			continue
		}
		if streamResult.err != nil && !streamResult.wrote && r.Context().Err() == nil {
			writeError(w, http.StatusBadGateway, "upstream stream failed before producing an event", requestID)
		}
		return
	}
}

type streamResult struct {
	wrote bool
	err   error
}

func (s *Server) streamResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, requestID, profile string, target routing.Target, started time.Time, fallback bool) streamResult {
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
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, nil)
			return streamResult{wrote: wrote}
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, err)
			return streamResult{wrote: wrote, err: err}
		}
		if err != nil {
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, err)
			return streamResult{wrote: wrote, err: err}
		}
		if firstToken.IsZero() {
			if meaningfulEvent(event.Type, event.Data) {
				firstToken = time.Now()
			}
		}
		writeEvent(event.Raw)
		select {
		case <-r.Context().Done():
			s.logAttempt(requestID, profile, target, started, firstToken, resp.StatusCode, fallback, r.Context().Err())
			return streamResult{wrote: wrote, err: r.Context().Err()}
		default:
		}
	}
}

func recordHealth(ctx context.Context, registry *health.Registry, backend string, err error) {
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
		strings.Contains(eventType, "custom_tool_call_input.delta")
}

func (s *Server) logAttempt(requestID, profile string, target routing.Target, started, firstToken time.Time, status int, fallback bool, err error) {
	attrs := []any{"request_id", requestID, "logical_profile", profile, "backend", target.Backend, "real_model", target.Model, "status", status, "fallback_used", fallback, "total_latency_ms", time.Since(started).Milliseconds()}
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
