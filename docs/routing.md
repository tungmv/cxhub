# Routing and fallback

The request's `model` is the logical profile name. For example:

```json
{"model":"coder","stream":true,"input":[]}
```

`coder` is looked up in `profiles`, then its targets are attempted in listed
order. Each target contains a backend ID and the real model ID. The real model is
never advertised through `/v1/models`.

Fallback is deterministic and request-local. Connection failures and upstream
`408`, `429`, `500`, `502`, `503`, and `504` responses may advance to the next target
before a response has produced meaningful output. A successful streamed response is
never replayed to another model. Once an event has been written, the gateway cannot
safely cross-model replay a partially observed response or a tool interaction.

The MVP forwards upstream SSE records rather than interpreting model semantics. It
therefore preserves function-call names, IDs, argument deltas, tool outputs, and
reasoning events as emitted by the upstream. It does not execute tools.

A profile should only contain targets that are actually compatible with the request's
Responses features. Provider-specific translation is intentionally not guessed.
