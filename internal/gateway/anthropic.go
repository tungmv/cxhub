package gateway

import (
	"encoding/json"
	"net/http"

	"cxhub/internal/anthropic"
)

// handleMessages serves Claude Code (Anthropic Messages wire format). The
// request is translated to a Responses request and routed through the exact
// same profile resolution, fallback, retry, and timeout pipeline as Codex.
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFromContext(r.Context())
	body, ok := readBody(w, r, requestID)
	if !ok {
		return
	}
	req, err := anthropic.ToResponses(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	s.serveRequest(w, r, req, func(resp *http.Response) (*http.Response, error) {
		return anthropic.WrapResponse(r.Context(), resp, req.Model)
	})
}

// handleCountTokens answers Claude Code's token preflight with a cheap
// deterministic estimate; cxhub never spends upstream tokens on it.
func (s *Server) handleCountTokens(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFromContext(r.Context())
	body, ok := readBody(w, r, requestID)
	if !ok {
		return
	}
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", requestID)
		return
	}
	encoded, _ := json.Marshal(parsed)
	writeJSON(w, http.StatusOK, map[string]any{"input_tokens": len(encoded)/4 + 1})
}
