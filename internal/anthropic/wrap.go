package anthropic

import (
	"bytes"
	"context"
	"io"
	"net/http"
)

// WrapResponse converts an upstream Responses HTTP response into an Anthropic
// Messages response. Non-streaming bodies are converted in place; streaming
// bodies are replaced by a pipe that translates Responses SSE events into
// Anthropic SSE events, so the gateway can keep forwarding raw bytes.
func WrapResponse(ctx context.Context, resp *http.Response, profile string) (*http.Response, error) {
	if !isEventStream(resp) {
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		converted, err := response(body, profile)
		if err != nil {
			return nil, err
		}
		resp.Body = io.NopCloser(bytes.NewReader(converted))
		resp.ContentLength = int64(len(converted))
		resp.Header.Del("Content-Encoding")
		resp.Header.Set("Content-Type", "application/json")
		return resp, nil
	}
	resp.Body = newStreamBody(ctx, resp.Body, profile)
	resp.Header.Set("Content-Type", "text/event-stream")
	resp.ContentLength = -1
	return resp, nil
}

func isEventStream(resp *http.Response) bool {
	return resp.Header.Get("Content-Type") == "text/event-stream"
}
