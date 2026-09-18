package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cxhub/internal/config"
	"cxhub/internal/responses"
)

type OpenAICompatible struct {
	id      string
	baseURL string
	apiKey  string
	headers map[string]string
	client  *http.Client
}

func NewOpenAICompatible(id string, cfg config.BackendConfig, client *http.Client) *OpenAICompatible {
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	headers := make(map[string]string, len(cfg.Headers))
	for key, value := range cfg.Headers {
		headers[key] = value
	}
	return &OpenAICompatible{id: id, baseURL: strings.TrimRight(cfg.BaseURL, "/"), apiKey: cfg.APIKey, headers: headers, client: client}
}

func (p *OpenAICompatible) ID() string { return p.id }

func (p *OpenAICompatible) Responses(ctx context.Context, req *responses.Request, model string) (*http.Response, error) {
	body, err := req.WithModel(model)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/responses", strings.NewReader(string(body)))
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
	return p.client.Do(httpReq)
}

func (p *OpenAICompatible) Health(ctx context.Context) error {
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

func DefaultHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}
