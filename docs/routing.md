# Routing and fallback

The request's `model` is the logical profile name. For example:

```json
{"model":"coder","stream":true,"input":[]}
```

`coder` is looked up in `profiles`, then its targets are attempted in listed
order. Each target contains a backend ID and the real model ID. The real model is
never advertised through `/v1/models`.

## Automatic selection

When decision routing is configured, `model: auto` is advertised as an additional
logical model. cxhub sends the request's user text to OpenRouter's
[Decisions API](https://openrouter.ai/docs/api/api-reference/alphadecisions/submit-a-decisions-request)
using `respan/span-01-lite`. The request asks one `noul` question per candidate
model ("is model X the best choice?"), plus two effort questions. The highest
scoring candidate wins — ties resolve to the first candidate in name order — and
the effort answers pick a `low`, `medium`, or `high` reasoning effort. Image and
audio payloads are not included in the decision request. Ordinary profile names
route unchanged.

`decision.candidates` lists the models selection may choose from; omit it and
every profile competes. Keep it short: each entry costs one question per request.

```yaml
decision:
  backend: openrouter
  model: respan/span-01-lite
  default_profile: orchestrator
  candidates: [fast, orchestrator, reviewer]
  timeout: 2s

profiles:
  fast:
    targets: [{backend: openrouter, model: YOUR_FAST_MODEL}]
  orchestrator:
    targets: [{backend: cliproxy, model: YOUR_DEFAULT_MODEL}]
  reviewer:
    targets: [{backend: openrouter, model: YOUR_STRONG_MODEL}]
```

If classification fails or times out, cxhub routes through `default_profile` with
medium effort. Auto selection adds a network call and transmits user text to
OpenRouter; requests for explicit profiles do neither. The chosen effort is
forwarded normally; reasoning-effort support still depends on the selected
upstream model/provider.

## Same-level fallback across profiles

A profile may declare an optional `level` (for example `reasoning` or `speed`).
Profiles that share the same non-empty level form a fallback tier. When every
target of the requested profile has failed or timed out, the gateway
deterministically appends the targets of the remaining same-level profiles,
ordered by ascending `priority` (then profile name), to the attempt list.
Duplicate backend/model pairs are attempted only once. Profiles without a level
never gain cross-profile fallback.

```yaml
profiles:
  coder:
    level: reasoning
    targets:
      - backend: cliproxy
        model: model-A
  planner:
    level: reasoning
    targets:
      - backend: openrouter
        model: model-D
```

With the configuration above, a request for `coder` that exhausts
`cliproxy/model-A` is retried against `openrouter/model-D` from `planner`. The
attempt log records `resolved_profile` whenever a target came from a fallback
profile.

### Priority within a level

A profile may also declare `priority` (any integer, default `0`). Same-level
fallback targets are ordered by ascending `priority`, then profile name, so a
critical profile such as the orchestrator can be attempted before everything
else in its tier:

```yaml
profiles:
  orchestrator:
    level: reasoning
    priority: 0
    targets: [ ... ]
  planner:
    level: reasoning
    priority: 10
    targets: [ ... ]
```

## Retries

A profile may declare `retries` (total passes over the attempt list, default
`1`, capped at `100`) and `retry_backoff` (wait between passes, default `0`).
Every pass re-attempts the profile's own targets and the same-level fallback
targets in order. While attempts remain, any upstream error status — not just
the fallbackable set — advances to the next attempt; the final attempt's error
is reported to the client unchanged. Retries are bounded by the client's own
request context, so a client that gives up is never waited on. Each attempt log
entry carries `attempt_round`.

For a profile that must practically never fail, combine a wide same-level tier,
per-target `timeout`s, and a high retry budget:

```yaml
profiles:
  orchestrator:
    level: reasoning
    priority: 0
    retries: 5
    retry_backoff: 1s
    targets:
      - backend: cliproxy
        model: model-A
      - backend: openrouter
        model: model-D
        timeout: 45s
```

## Per-target cooldown

Each target accepts an optional `cooldown` (a Go duration, for example `2m`).
After a fallbackable failure — connection error, per-target timeout, or upstream
`408`, `429`, `500`, `502`, `503`, `504` — the backend/model pair is parked for
the cooldown window. While parked it is ordered after every non-cooling target,
so a rate-limited or invalidated account is not retried first. A successful
attempt clears the cooldown.

Tracked state is per backend/model, so one broken model (for example an
expired OAuth token for a single provider family) never demotes the other models
served by the same backend. A cooling target is never dropped: if every
healthier target fails, the gateway still attempts it.

```yaml
profiles:
  coder:
    targets:
      - backend: cliproxy
        model: gpt-5.5
        cooldown: 5m
      - backend: cliproxy
        model: gemini-3.7-flash-high
```

`GET /status` exposes `failures`, `last_error`, and `cooling_until` per target.

## Per-target timeout

Each target accepts an optional `timeout` (a Go duration, for example `45s`).
The timeout bounds one attempt up to its first meaningful streamed event. If the
upstream has not produced output when the timer fires, the attempt is cancelled
and the next target in the attempt list (own targets first, then same-level
fallback targets) serves the request. Once meaningful output has started, the
timer is disarmed so a healthy generation is never cut. When every target in the
attempt list has timed out, the gateway replies with HTTP `504`.

Fallback is deterministic and request-local. Connection failures, per-target
timeouts, and upstream `408`, `429`, `500`, `502`, `503`, and `504` responses may
advance to the next target before a response has produced meaningful output.

A provider may also return HTTP `200` and then inject a failure event
(`response.failed`, or a bare `error` object) into the SSE stream — for example
when the backing provider behind OpenRouter is overloaded. The gateway buffers
the response lifecycle events until the first answer output, so a failure that
arrives before any answer can still be replaced by the next target and the
client never sees the aborted attempt. If the failure arrives after answer
output has already streamed, the gateway forwards a terminal event with the
error object removed; clients report a raw error payload as an injected JSON
error. A successful streamed response is never replayed to another model: once
answer output has been written, the gateway cannot safely cross-model replay a
partially observed response or a tool interaction.

The MVP forwards upstream SSE records rather than interpreting model semantics. It
therefore preserves function-call names, IDs, argument deltas, tool outputs, and
reasoning events as emitted by the upstream. It does not execute tools.

A profile should only contain targets that are actually compatible with the request's
Responses features. Provider-specific translation is intentionally not guessed.
