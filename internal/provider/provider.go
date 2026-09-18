package provider

import (
	"context"
	"net/http"

	"cxhub/internal/responses"
)

type Provider interface {
	ID() string
	Responses(ctx context.Context, req *responses.Request, model string) (*http.Response, error)
	Health(ctx context.Context) error
}
