# Codex CLI integration

This document records the local Codex CLI inspection performed before implementing
`cxhub`. Inspection was read-only; the existing Codex configuration was not changed.

## Observed installation

- CLI: `codex-cli 0.155.0`
- Binary: `/Users/tony/.local/bin/codex`
- Host: macOS arm64
- Go available for `cxhub`: `go1.27.1 darwin/arm64`
- Existing config: `~/.codex/config.toml`

The current configuration uses the TOML provider mechanism:

```toml
model_provider = "cliproxy"
model = "gpt-5.6-luna"
model_reasoning_effort = "high"

[model_providers.cliproxy]
name = "cliproxy"
base_url = "http://127.0.0.1:8317/v1"
wire_api = "responses"
requires_openai_auth = false
```

The inspected configuration also contains remote provider examples, including an
OpenRouter provider with `wire_api = "responses"`. Secret values are intentionally
omitted here.

The local CLIProxyAPI process was also observed listening on `*:8317`; its `/v1`
endpoint responded to authenticated model discovery. This is upstream state, not
the binding chosen by `cxhub`, which remains loopback-only by default.

## Provider configuration mechanism

Codex selects a provider with the top-level `model_provider` setting. Provider
definitions live under `[model_providers.<id>]`. The relevant fields exposed by the
installed CLI help and configuration are:

- `base_url`: provider API root
- `wire_api`: set to `responses` for the Responses API transport
- `env_key` or an equivalent configured bearer token mechanism for authentication
- `requires_openai_auth`: whether standard OpenAI authentication is required
- `supports_websockets`: whether the provider advertises the optional WebSocket path

The CLI also supports a per-invocation `--model` override and TOML `--profile`
overlays. `cxhub` will use one small Codex provider entry and treat the request's
logical model value as the profile name; it will not synchronize provider catalogs.

## Responses request and response transport

The installed provider configuration explicitly selects the OpenAI Responses wire
API at `/v1`. The gateway therefore exposes:

```text
POST http://127.0.0.1:8787/v1/responses
```

Codex's supported provider transport is HTTP Responses, with streaming enabled for
normal interactive operation. The gateway forwards the JSON request body while
replacing only the upstream implementation model. It preserves unknown JSON fields
so newer Codex fields are not discarded by a narrow gateway schema.

The installed binary embeds request-schema names for `model`, `instructions`,
`input`, `tools`, `tool_choice`, `parallel_tool_calls`, `reasoning`, `store`,
`stream`, `stream_options`, `include`, `service_tier`, and
`previous_response_id`. These names are evidence of supported request surface, not
a claim that every request contains every field.

The request model is the integration key. A request without `model` is rejected;
there is no implicit profile fallback because that could route a child to the wrong
provider.

```json
{"model":"researcher","stream":true,"input":[]}
```

The gateway resolves `model` as a configured logical profile. If the request omits
`model`, it returns a useful client error.

## Streaming format

Streaming uses Server-Sent Events over an HTTP response with:

```text
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

Responses event records are sent as `event: <type>` followed by a JSON `data:` line
and a blank line. The gateway forwards complete upstream SSE records in order and
flushes after each event. A `[DONE]` data marker is preserved as a completion marker
when an upstream emits it. Client cancellation propagates through the request
context to the upstream HTTP request.

Embedded event-name evidence includes `response.created`, `response.completed`,
`response.failed`, `response.function_call`, `response.function_call_output`,
`response.custom_tool_call`, `response.custom_tool_call_output`, and reasoning
events. A live raw Codex-to-upstream capture was not available during inspection;
the gateway therefore treats SSE records as opaque and preserves their framing.

## Tools, function calls, and reasoning

Codex is a coding agent and expects structured Responses items. The gateway does not
execute tools. It forwards tool definitions in the request and forwards structured
tool/function call output events and tool results in the response without flattening
them into text. This keeps tool execution in Codex, where it belongs.

Reasoning configuration and reasoning events are forwarded as opaque JSON. The
gateway does not manufacture reasoning tokens. If a selected upstream omits a
reasoning event or uses a provider-specific shape, that limitation remains an
upstream compatibility limitation and is documented in diagnostics rather than
silently fabricated.

## Root and child agents

The installed CLI exposes stable native multi-agent support and supports model
selection through `--model`, config, and profile overlays. Native child sessions
are separate Codex threads; their request-scoped model selection is expected to
arrive as the Responses `model` value. The gateway therefore makes no attempt to
infer a child profile from process state, thread order, headers, or timing.

The current binary reports `multi_agent` as stable/enabled and exposes native
`spawn_agent` model and reasoning-effort override fields. The documented behavior is
that a child inherits the parent model unless a model override is supplied. No
per-subagent provider/profile map exists in the inspected Codex configuration.

This is the required isolation model:

```text
each HTTP request -> read request.model -> resolve profile -> choose target
```

The local session metadata confirms that child sessions are distinct threads and
carry provider/model metadata independently. The exact upstream wire body is an
implementation detail of Codex and is not available as a public schema in the CLI
help; the gateway consequently preserves unknown request fields and does not depend
on undocumented child-only fields.

If a particular Codex child workflow does not send a distinct logical model, Codex
cannot select different profiles through this integration alone. The supported
workaround is to use the native child model override or start that child with the
intended `--model`/profile override.
`cxhub` does not patch Codex or maintain mutable global routing state.

## Selected strategy

1. Configure one Codex provider whose `base_url` is `http://127.0.0.1:8787/v1` and
   whose `wire_api` is `responses`.
2. Use a minimal logical model roster such as `orchestrator`, `researcher`,
   `coder`, `reviewer`, and `fast`.
3. Resolve each request's model to a profile and then to a deterministic target
   chain in `cxhub` configuration.
4. Forward the original Responses request and stream provider events back to Codex.
5. Permit fallback only before the first meaningful upstream event; never replay a
   request after partial tool/semantic interaction.

## Compatibility limitations and assumptions

- The initial provider implementation assumes CLIProxyAPI, OpenRouter, and ckey.vn
  expose a compatible Responses endpoint. Provider-specific differences must be
  handled by configuration or a later explicit adapter; ckey.vn URL/model values
  remain placeholders until verified.
- Codex may send fields newer than this gateway understands. Unknown JSON is retained
  when forwarding; the gateway only interprets `model`, `stream`, and a small amount
  of request metadata for routing and observability.
- Non-streaming Responses are supported for diagnostics and tests. Streaming is the
  primary production path.
- The gateway does not expose upstream API keys to Codex and binds to loopback by
  default.
- The installed Codex version was inspected locally on 2026-09-18. Re-run this
  inspection after a Codex upgrade because provider and multi-agent behavior can
  change.
