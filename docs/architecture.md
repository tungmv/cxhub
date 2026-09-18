# Architecture

`cxhub` is a single-user, local HTTP gateway. Codex is its only client.

```text
Codex Responses client
        |
        v
cxhub HTTP server (loopback)
        |
        +-- request-scoped profile resolution
        +-- deterministic target selection/fallback
        v
OpenAI-compatible backend
```

The gateway keeps configuration immutable after startup. Each request parses its
own logical profile, resolves a private target slice, creates an upstream request
with the incoming context, and streams only that request's events to its client.
There is no current-model, current-profile, or active-provider global.

The provider implementation is deliberately small: it sends the original JSON
Responses request with only `model` replaced by the target's real model, injects the
configured backend key/headers, and returns the upstream HTTP response. SSE parsing
is done per request so event order and cancellation remain local to that request.

Endpoints:

- `GET /health`: liveness only.
- `GET /status`: configured profiles and in-memory backend health observations.
- `GET /v1/models`: logical profile names only.
- `POST /v1/responses`: streaming and non-streaming Responses proxy.

Shutdown uses `http.Server.Shutdown`; active requests retain their contexts until
completion or the shutdown deadline.
