# amqp

Clean-design AMQP 0-9-1 client for Crystal. Status: **v0.1.0 local
release for `job_hunter` integration**.

This shard now has implementation code under `src/`, executable specs
under `spec/`, and a still-normative design corpus under `docs/`. The
project remains spec-driven: public guarantees should either have a
focused falsifier in the spec suite or be explicitly cut/deferred in
the docs. The current release priority is a small, honest AMQP 0-9-1
surface for RabbitMQ and LavinMQ rather than a complete AMQP kitchen sink.

## What's in v0

- AMQP 0-9-1 over TCP, with `amqps://` TLS support and a live TLS
  broker spec gated by `AMQP_TLS_URL`
- Publisher confirms: confirm mode, sync confirm, async confirm,
  mandatory returns, nack/range settlement, and out-of-order guards
- Topology recovery (opt-in), with v0 new operations during recovery
  failing fast via `RecoveryInProgress`
- Heartbeats (fiber-driven, monotonic-time based)
- Queue/exchange declare, bind, unbind, delete, purge, publish,
  consume, subscribe, get, ack, nack, reject
- Practical `amqp-client.cr` migration helpers: `basic_*` aliases,
  queue/exchange wrappers, work-pool `basic_consume`, return/cancel/close
  callbacks, `channel.flow`, `basic.recover`, transactions, and IO publish
  overloads
- Native Crystal: fibers, `Channel(T)`, `Time::Span`, idiomatic exception
  hierarchy, no global singletons
- Target brokers: RabbitMQ 3.13+, LavinMQ 2.x

The URI query surface is intentionally narrow in v0:
`heartbeat`, `channel_max`, `frame_max`, `connect_timeout`, `recovery`,
`product`, and `information`. TLS policy is supplied through a caller
`OpenSSL::SSL::Context::Client`, not URI query keys. SASL EXTERNAL is
deferred; v0 uses PLAIN credentials.

AMQP 1.0 is architecturally accounted for (see `docs/18-amqp-1-0-forward-plan.md`)
but explicitly deferred to v1. AMQP 0-8 and 0-9 are non-goals.

## Verification status

Current local baseline:

```sh
crystal spec
```

passes with 174 examples, 0 failures, 0 errors, and 4 pending live/chaos
broker specs by default. With `AMQP_TLS_URL` and optional
`AMQP_TLS_CA_CERT`, the TLS pending spec runs; broker chaos fixtures are
gated by `AMQP_CHAOS_DOCKER_CONTAINER`; the invasive backpressure
timing harness remains gated by `AMQP_BACKPRESSURE_LIVE`.
With all three local gates enabled against the project-owned RabbitMQ
containers, the suite last passed before the parity additions with all
live-gated examples enabled. Re-run that opt-in gate before publishing
a public release artifact.
`crystal tool format --check src spec tools/perf_publish.cr`,
`crystal build tools/perf_publish.cr --no-codegen --error-trace`, and
`git diff --check` also pass. `../job_hunter` compiles its CLI entrypoint
against this shard as a local path dependency.

LavinMQ 2.4.0 is locally smoke-verified on plain AMQP from the earlier
release baseline: the default suite and the opt-in backpressure plus
Docker pause/restart chaos suite passed, with only TLS pending. Re-run
the LavinMQ gate after broker-compat changes. LavinMQ TLS is deferred
from v0.1.0.

## Reading order

Start at `docs/00-overview.md`. The numbered prefix is the recommended
reading order — `00-04` set up scope, principles, and the API surface;
`05-wire-0-9-1/` is the byte-level wire codec; `06-12` cover runtime
semantics (connection, channel, confirms, consume, heartbeats, TLS,
recovery); `13-15` are the normative compatibility/performance/reliability
contracts; `16-falsifier-matrix.md` is the test catalogue; `17-20` are
scope, forward plan, observability, risks.

## Status

v0.1.0 local release branch. See `TODO.md` for the working ledger and
`LANDMARKS.md` for verified anchors, refutations, and deferred surface
decisions. Known non-blocking follow-ups include a CI broker matrix,
LavinMQ TLS, a reproducible perf harness, and broader reliability
transcripts.
