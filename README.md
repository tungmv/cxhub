# cxhub

`cxhub` is a small local OpenAI Responses gateway for Codex CLI. Codex sees a short
logical model roster; `cxhub` privately maps each logical profile to configured
provider/model targets. An optional `auto` model uses OpenRouter's Span-01 Lite
decision API to select a configured speed, balanced, or quality profile and a
reasoning effort for each request.

It is written entirely in Go and has no dashboard, database, catalog synchronizer,
account pool, or tool execution layer.

## Build

```sh
go build ./cmd/cxhub
```

## Configure

Start from [configs/example.yaml](configs/example.yaml):

```sh
mkdir -p ~/.config/cxhub
cp configs/example.yaml ~/.config/cxhub/config.yaml
export CLIPROXY_API_KEY=...
export OPENROUTER_API_KEY=...
```

API keys are expanded from `${ENV_NAME}` at load time and are never sent to Codex
or written to logs. Keep authentication and model IDs as verified local values.

Validate and run:

```sh
./cxhub config validate --config ~/.config/cxhub/config.yaml
./cxhub doctor --config ~/.config/cxhub/config.yaml
./cxhub start --config ~/.config/cxhub/config.yaml
```

The default listener is `127.0.0.1:8787`. `cxhub start` runs in the foreground and
records a private PID file; `cxhub stop` sends SIGTERM to that identified process.
While running, cxhub checks the config file every second and applies valid backend
and profile changes without dropping in-flight requests. Invalid configs are
rejected; changing the gateway address still requires a restart.

## Codex setup

Create a small provider entry in `~/.codex/config.toml` after reviewing your current
file, preserving unrelated settings:

```toml
model_provider = "cxhub"
model = "auto"
model_catalog_json = "~/.codex/model_catalog.json"

[model_providers.cxhub]
name = "cxhub"
base_url = "http://127.0.0.1:8787/v1"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
```

Codex requires metadata for logical profiles that are not in its built-in
catalog. Copy `configs/codex-model-catalog.json` to
`~/.codex/model_catalog.json`; its `orchestrator` entry defines the context
window and supported reasoning levels used by the local profile.

`cxhub init` only creates its own YAML example and deliberately does not rewrite the
existing Codex TOML automatically. The installed Codex CLI was inspected in
[docs/codex-integration.md](docs/codex-integration.md).

## Claude Code setup

cxhub also exposes the Anthropic Messages wire format on the same listener, so
Claude Code can use the same logical profiles and fallback pipeline:

```sh
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
export ANTHROPIC_AUTH_TOKEN=not-needed  # any value; cxhub does not gate inbound auth
claude
```

Or persist it with a launcher alias/wrapper so it only applies where wanted.
`POST /v1/messages` accepts Anthropic Messages requests, maps the `model` field
to a cxhub profile (text, images, `tool_use`/`tool_result` blocks, and tools are
translated to Responses equivalents), and streams back Anthropic SSE events.
`POST /v1/messages/count_tokens` is answered with a local estimate without
spending upstream tokens. Under the hood both Codex and Claude Code share the
identical target resolution, fallback, retry, and timeout pipeline.

## Profiles and fanout

Codex selects a logical profile through the Responses request's `model` value. A
typical configuration can route:

```text
orchestrator -> cliproxy / model-A
coder        -> cliproxy / model-C
reviewer     -> openrouter / model-D
fast         -> openrouter / model-E
```

Each request is isolated, so concurrent child agents cannot change one another's
provider or model. Profiles can declare a `level` so that when every target of
the requested profile fails or times out, other profiles at the same level serve
the request (ordered by `priority`); each target can also set a `timeout` that
triggers fallback when an upstream stalls before producing output, and a profile
can declare `retries`/`retry_backoff` to make a critical profile such as the
orchestrator practically never fail. See
[docs/routing.md](docs/routing.md) and
[docs/architecture.md](docs/architecture.md).

For automatic routing, set `decision.backend`, `decision.model`, and
`decision.default_profile`; set `auto_tier: speed|balanced|quality` on at most one
profile per tier. Codex can then use `model = "auto"` (including in custom agent
TOML files). The selector scores only request text using OpenRouter's
[Decisions API](https://openrouter.ai/docs/api/api-reference/alphadecisions/submit-a-decisions-request).
It sends user text to OpenRouter; image/audio contents are not sent. If selection
fails, cxhub uses `default_profile` with medium reasoning effort. Explicit named
profiles bypass selection.

## Operations

```sh
./cxhub models --config ~/.config/cxhub/config.yaml
curl http://127.0.0.1:8787/health
curl http://127.0.0.1:8787/status
```

`models` prints only logical profiles. `doctor` validates configuration and performs
lightweight `GET /models` checks; it does not spend tokens on model calls. Structured
JSON logs include request ID, profile, backend, real model, status, latency, and
time-to-first-token, but not prompts or keys.

## Tests

The test suite includes an in-process fake OpenAI-compatible upstream for JSON,
streaming SSE, tool-call events, fallback, and four-way concurrent fanout isolation.

```sh
go test ./...
go test -race ./...
go vet ./...
```

## Provider notes

CLIProxyAPI and OpenRouter are represented as generic OpenAI-compatible backends.
The current local Codex inspection found CLIProxyAPI at `http://127.0.0.1:8317/v1`
and OpenRouter at `https://openrouter.ai/api/v1`. NVIDIA's
`https://integrate.api.nvidia.com/v1` endpoint is configured with
`type: openai-chat-compatible`; cxhub translates its Chat Completions requests and
streams back Responses events. If another backend differs from the Responses API,
add an explicit provider adapter rather than silently changing event semantics.
