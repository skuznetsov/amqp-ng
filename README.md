# amqp

Clean-design AMQP 0-9-1 client for Crystal. Status: **spec-driven design, pre-implementation.**

This shard does not yet have an implementation. The `docs/` directory holds
the complete specification: every public guarantee, every wire-format
detail, and a falsifier matrix that maps each normative claim to the
smallest test that would break it. The intent is that an implementer
(human or LLM) can produce a correct v0 from the docs alone, without
consulting external AMQP references.

## What's in v0

- AMQP 0-9-1 over TCP and TLS (`amqp://`, `amqps://`)
- Publisher confirms (sync + async + fire-and-forget)
- Topology recovery (opt-in)
- Heartbeats (mandatory, fiber-driven)
- Native Crystal: fibers, `Channel(T)`, `Time::Span`, idiomatic exception
  hierarchy, no global singletons
- Target brokers: RabbitMQ 3.13+, LavinMQ 2.x

AMQP 1.0 is architecturally accounted for (see `docs/18-amqp-1-0-forward-plan.md`)
but explicitly deferred to v1. AMQP 0-8 and 0-9 are non-goals.

## Reading order

Start at `docs/00-overview.md`. The numbered prefix is the recommended
reading order — `00-04` set up scope, principles, and the API surface;
`05-wire-0-9-1/` is the byte-level wire codec; `06-12` cover runtime
semantics (connection, channel, confirms, consume, heartbeats, TLS,
recovery); `13-15` are the normative compatibility/performance/reliability
contracts; `16-falsifier-matrix.md` is the test catalogue; `17-20` are
scope, forward plan, observability, risks.

## Status

Pre-implementation. No code under `src/` yet. Specs are live; once they
stabilise, implementation begins (humans or LLMs).
