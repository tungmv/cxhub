# Routing and fallback

The request's `model` is the logical profile name. For example:

```json
{"model":"coder","stream":true,"input":[]}
```

`coder` is looked up in `profiles`, then its targets are attempted in listed
order. Each target contains a backend ID and the real model ID. The real model is
never advertised through `/v1/models`.

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
advance to the next target before a response has produced meaningful output. A
successful streamed response is never replayed to another model. Once an event
has been written, the gateway cannot safely cross-model replay a partially
observed response or a tool interaction.

The MVP forwards upstream SSE records rather than interpreting model semantics. It
therefore preserves function-call names, IDs, argument deltas, tool outputs, and
reasoning events as emitted by the upstream. It does not execute tools.

A profile should only contain targets that are actually compatible with the request's
Responses features. Provider-specific translation is intentionally not guessed.
