# cxhub

`cxhub` is a small local OpenAI Responses gateway for Codex CLI. Codex sees a short
logical model roster; `cxhub` privately maps each logical profile to configured
provider/model targets.

It is written entirely in Go and has no dashboard, database, catalog synchronizer,
account pool, or tool execution layer.

## Build

```sh
go build ./cmd/cxhub
```

## Configure

Start from [configs/example.yaml](/Users/tony/build/cxhub/configs/example.yaml):

```sh
mkdir -p ~/.config/cxhub
cp configs/example.yaml ~/.config/cxhub/config.yaml
export CLIPROXY_API_KEY=...
export OPENROUTER_API_KEY=...
```

API keys are expanded from `${ENV_NAME}` at load time and are never sent to Codex
or written to logs. Keep the ckey.vn URL, authentication, and model IDs as verified
local values; this repository intentionally does not guess them.

Validate and run:

```sh
./cxhub config validate --config ~/.config/cxhub/config.yaml
./cxhub doctor --config ~/.config/cxhub/config.yaml
./cxhub start --config ~/.config/cxhub/config.yaml
```

The default listener is `127.0.0.1:8787`. `cxhub start` runs in the foreground and
records a private PID file; `cxhub stop` sends SIGTERM to that identified process.

## Codex setup

Create a small provider entry in `~/.codex/config.toml` after reviewing your current
file, preserving unrelated settings:

```toml
model_provider = "cxhub"
model = "orchestrator"

[model_providers.cxhub]
name = "cxhub"
base_url = "http://127.0.0.1:8787/v1"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
```

`cxhub init` only creates its own YAML example and deliberately does not rewrite the
existing Codex TOML automatically. The installed Codex CLI was inspected in
[docs/codex-integration.md](/Users/tony/build/cxhub/docs/codex-integration.md).

## Profiles and fanout

Codex selects a logical profile through the Responses request's `model` value. A
typical configuration can route:

```text
orchestrator -> cliproxy / model-A
researcher   -> openrouter / model-B
coder        -> cliproxy / model-C
reviewer     -> ckey / model-D
fast         -> openrouter / model-E
```

Each request is isolated, so concurrent child agents cannot change one another's
provider or model. See [docs/routing.md](/Users/tony/build/cxhub/docs/routing.md) and
[docs/architecture.md](/Users/tony/build/cxhub/docs/architecture.md).

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
and OpenRouter at `https://openrouter.ai/api/v1`. ckey.vn remains a placeholder until
its endpoint and authentication contract are verified. If a backend differs from the
Responses API, add an explicit provider adapter rather than silently changing event
semantics.
