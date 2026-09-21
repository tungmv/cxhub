# Claude Code integration

cxhub exposes the Anthropic Messages wire format on the same listener
as the OpenAI Responses API, so Claude Code can use the same logical
profiles and fallback pipeline as Codex.

## Environment setup

```sh
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
export ANTHROPIC_AUTH_TOKEN=not-needed  # any value; cxhub does not gate inbound auth
export ANTHROPIC_MODEL=orchestrator
export ANTHROPIC_SMALL_FAST_MODEL=fast
claude
```

Persist with a launcher alias or wrapper so these variables apply only where needed.

## Request translation

`POST /v1/messages` accepts Anthropic Messages requests on the
gateway. The `model` field maps to a cxhub logical profile
(e.g. `orchestrator`, `coder`, `fast`). Text, images,
`tool_use`/`tool_result` blocks, and tool definitions are translated
to Responses equivalents. Responses are streamed back as Anthropic SSE
events. The translation lives in `internal/anthropic`:

- request conversion — Anthropic Messages request to a Responses request
- stream translator — Responses SSE events to Anthropic SSE events
- gateway handler — routes the translated request through the shared pipeline

`POST /v1/messages/count_tokens` returns a local token estimate without
upstream token expenditure.

## Shared pipeline

Codex and Claude Code share the identical target resolution, fallback,
retry, and timeout pipeline. Profile-level `retries`/`retry_backoff`
and per-target `timeout` apply to both.
