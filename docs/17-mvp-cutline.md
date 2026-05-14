# amqp — MVP Cutline

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers planning v0 work; reviewers checking
> that v0 scope is intentional rather than emergent.
> **Companions:** `docs/00-overview.md` (non-goals), `docs/02-public-api.md`
> (the v0 surface), `docs/18-amqp-1-0-forward-plan.md` (where deferred
> work lives), `docs/20-risk-register.md` (known limitations).

This document draws the line between v0 and post-v0. Every numbered
"IN" item below MUST ship in v0; every "OUT" item MUST NOT ship in
v0 (even if convenient).

The cutline is not a wish-list — it is a contract. Adding to IN
requires consensus that the addition is needed for v0 correctness;
adding to OUT (i.e., removing from v0) requires the principle being
deferred to also be removed.

---

## 1. v0 IN

### 1.1 Wire and transport

- **AMQP 0-9-1 over TCP.** Frame encode/decode for method, header,
  body, heartbeat frames. Field types: bit, octet, short, long,
  long-long (signed + unsigned), short-string, long-string,
  field-table (with the type-code subset RabbitMQ and LavinMQ use:
  `t s I l f d S F A T V` — `D` decimal is OUT, see §2.3).
- **TLS via stdlib OpenSSL.** `amqps://` scheme, SNI mandatory,
  caller-controlled cipher and verification policy.
- **Single-host URI.** One peer per `Amqp.connect`. Failover
  composition is the caller's problem.
- **SASL PLAIN and EXTERNAL.** Two mechanisms, no more.

### 1.2 Connection lifecycle

- **Full handshake.** Protocol-header → `connection.start` →
  `connection.start-ok` → `connection.tune` → `connection.tune-ok` →
  `connection.open` → `connection.open-ok`. Negotiated parameters
  exposed via `Connection#heartbeat`, `#channel_max`, `#frame_max`.
- **Heartbeats.** Mandatory, fiber-driven. Send cadence `heartbeat / 2`;
  receive deadline `heartbeat * 2`; timeout surfaces as
  `Amqp::HeartbeatTimeoutError`.
- **Graceful close.** `Connection#close` sends `connection.close`,
  awaits `close-ok`, tears down.
- **Broker-initiated close.** Surfaces as
  `Amqp::ConnectionClosedByBroker` with full reply-code mapping (per
  `docs/03-error-model.md` §3).
- **Idempotent close.** Both caller and broker.

### 1.3 Channel lifecycle

- **Allocation.** `Connection#channel` returns the next free
  channel-id (1..channel_max).
- **Explicit id.** `Connection#channel(id)` for callers who want a
  specific id (rare; useful for AMQP gateway scenarios).
- **Block form.** `Connection#with_channel { |ch| ... }` closes the
  channel on both normal exit and exception escape.
- **Concurrency check.** Concurrent state-changing operations on the
  same channel raise `Amqp::ConcurrencyError`.

### 1.4 Publishing

- **Fire-and-forget.** `Channel#publish` with `mandatory:` /
  `immediate:` flags pass-through.
- **Synchronous confirm.** `Channel#publish_confirm` blocks for
  ack/nack with caller-supplied timeout.
- **Asynchronous confirm.** `Channel#publish_async` returns a
  `{tag, ::Channel(ConfirmOutcome)}` tuple. The Crystal channel
  is `select`-able alongside subscriptions.
- **Confirm mode.** `Channel#confirm_select` enables publisher
  confirms. Idempotent.
- **Returned messages.** `mandatory: true` + unroutable surfaces as
  `Amqp::PublishReturnedError` in `publish_confirm` and as
  `ConfirmOutcome::Kind::Returned` in `publish_async`.

### 1.5 Consuming

- **Block form.** `Channel#consume(queue) { |msg| ... }` runs on
  caller's fiber, returns when consumer is cancelled.
- **Object form.** `Channel#subscribe` returns a `Subscription`
  receiver usable in `select`.
- **Detached form.** `Subscription#spawn_loop { |msg| ... }` spawns
  one fiber.
- **Synchronous get.** `Channel#get(queue) : GetMessage?`.
- **Manual ack/nack/reject.** With `multiple:` (ack/nack) and
  `requeue:` (nack/reject) flags.
- **Prefetch.** `Channel#prefetch(count, global:)`.
- **Backpressure.** Subscription buffer fills → frame-reader blocks
  → TCP backpressure to broker.

### 1.6 Topology

- **Queues.** `queue_declare` / `queue_delete` / `queue_bind` /
  `queue_unbind` / `queue_purge`. Server-named queue via empty name.
- **Exchanges.** `exchange_declare` / `exchange_delete` /
  `exchange_bind` / `exchange_unbind`. Built-in types pass through as
  strings (`"direct"`, `"fanout"`, `"topic"`, `"headers"`); custom
  exchange types (e.g., `x-consistent-hash`) work by string name if
  the broker has the plugin.
- **Arguments.** `Amqp::Arguments` typed wrapper around AMQP field
  tables.

### 1.7 Recovery

- **`Recovery::None`.** Default. Reconnection is the caller's
  responsibility.
- **`Recovery::Full`.** Reconnects, re-opens channels in the order
  they were created, re-declares topology recorded via this
  connection's API, re-installs consumers, re-publishes unconfirmed
  in-flight messages.

### 1.8 Errors

- **Complete exception hierarchy.** Per `docs/03-error-model.md` §1.
- **Reply-code → subclass mapping.** Per `docs/03-error-model.md` §3.
- **Origin tagging.** Caller / Broker / Network / Heartbeat /
  Recovery. Per `docs/03-error-model.md` §4.
- **Recoverable vs fatal classification.** `Amqp::Error.recoverable?`
  class method. Per `docs/03-error-model.md` §5.

### 1.9 Observability

- **`Connection#stats`.** Bytes/frames sent and received, channel
  count, uptime, heartbeats sent/received.
- **`Channel#stats`.** Messages published, confirms received/timeout,
  unconfirmed in flight, deliveries, manual acks/nacks.
- **`Subscription#stats`.** Deliveries received, buffer depth,
  buffer high-water mark.
- **stdlib `Log`.** One logger per subsystem.

### 1.10 Tests

- **Falsifier matrix.** Every `MUST` / `MUST NOT` in `docs/` MUST
  have a passing test in `spec/`.
- **Frame corpus.** `spec/fixtures/frames/` contains hex-dump-captured
  frames from real RabbitMQ and LavinMQ sessions; round-trip
  decode/encode against this corpus is a v0 acceptance gate.
- **Two-broker matrix.** Every consumer/publisher/recovery test runs
  against RabbitMQ 3.13+ AND LavinMQ 2.x. Per
  `docs/13-broker-compat-matrix.md`.

### 1.11 Documentation

- **All 21 `docs/` files.** No `TODO` or `TBD` markers in normative
  prose at v0.1.0 release.

---

## 2. v0 OUT (deferred)

### 2.1 AMQP 1.0

Entire protocol family. Architectural slot reserved per P-9 (pure
wire codec) so v1 adds a parallel codec module; v0 implementations
MUST NOT begin wiring 1.0 in.

Deferred to v1. See `docs/18-amqp-1-0-forward-plan.md` for the
architectural shape.

### 2.2 AMQP 0-8, 0-9

Dead in target brokers; explicit non-goal forever. Not deferred — not
in scope.

### 2.3 AMQP wire features not used by target brokers

- **Decimal field type (`D`).** Listed in AMQP 0-9-1 §4.2.5.5 but
  neither RabbitMQ nor LavinMQ produces or preserves it. The shard's
  codec MUST raise `Amqp::ProtocolError` on decoding a `D` field,
  not silently coerce. Listed in `docs/20-risk-register.md` as a
  known limitation.
- **Transactions (`tx.select` / `tx.commit` / `tx.rollback`).** Both
  RabbitMQ and LavinMQ implement transactions, but they are slow and
  rarely used; publisher confirms cover the same need with better
  performance. Deferred to v0.x as a clean addition if demand
  materialises.
- **`basic.recover` / `basic.recover-async`.** Niche operation
  (requeue all unacked deliveries on a channel). Deferred to v0.x.

### 2.4 Higher-level abstractions

- **Job queue framework.** Out of scope forever; this is a transport
  shard.
- **RPC client (request/reply orchestration over AMQP).** Out of
  scope forever; build on top of the consumer + publish surface.
- **Topology DSL.** Out of scope forever per P-Anti.
- **JSON-aware publish/consume overloads.** Out of scope forever per
  P-Anti.
- **Multi-host failover URI.** Deferred. v1 candidate if the syntax
  proves wantable.

### 2.5 Recovery features

- **`Recovery::Manual`.** A future mode that re-establishes the
  connection but lets the caller drive topology and consumer
  re-creation via callbacks. Sketched in `docs/12-recovery.md`;
  deferred to v0.x.
- **Per-channel recovery scope.** Recovering only some channels of a
  connection. Out of scope; v0 recovers all or nothing.

### 2.6 SASL mechanisms beyond PLAIN/EXTERNAL

AMQPLAIN, RABBIT-CR-DEMO, ANONYMOUS, OAUTH2. Deferred to v0.x; add
on demand with the process described in `docs/04-uri-and-config.md`
§4.3.

### 2.7 Broker-specific extensions

- **RabbitMQ direct-reply-to.** Useful for RPC; deferred to v0.x.
- **RabbitMQ publisher-confirms-in-transaction.** Slow, rare,
  deferred indefinitely.
- **RabbitMQ shovel / federation control.** Not AMQP; HTTP API.
  Forever out of scope.
- **LavinMQ-specific arguments** (`x-stream-*` if/when LavinMQ adds
  AMQP-stream semantics). Pass through via `Arguments`; no special
  surface.

### 2.8 Transports beyond TCP+TLS

- **WebSocket transport** (RabbitMQ Web-STOMP / Web-MQTT analogues).
  Not AMQP 0-9-1 wire-compatible. Out of scope.
- **Unix-domain socket** for local connections. Not supported by
  RabbitMQ on the standard `5672` listener path. Out of scope.

### 2.9 Telemetry

- **OpenTelemetry integration.** Out of scope in v0 per P-12. The
  shard exposes counters; vendors wrap them externally.
- **Prometheus metrics endpoint.** Same.

### 2.10 Performance shortcuts that violate principles

- **Zero-copy publish using `IO::Memory.@buffer`.** Forbidden by P-1
  (no stdlib ivar access). If the published benchmarks indicate
  meaningful perf left on the table from stdlib-only IO, the
  resolution is to upstream a Crystal stdlib improvement, not to
  reach inside an ivar.
- **Connection sharing across processes (fork after connect).** AMQP
  state is per-process; the shard MUST NOT add hooks for re-attaching
  after fork.

---

## 3. Acceptance criteria for v0.1.0

The shard ships v0.1.0 when, against both target brokers
(`docs/13-broker-compat-matrix.md`):

1. Every `MUST` in `docs/` has a green falsifier in `spec/`.
2. Every `SHOULD` in `docs/` either has a green falsifier or a
   `pending` test with a documented rationale in
   `docs/20-risk-register.md`.
3. Every PERF-N claim in `docs/14-performance-contract.md` is
   demonstrated by a script in `spec/perf/` whose output is committed
   alongside the release.
4. Every REL-N claim in `docs/15-reliability-contract.md` is
   demonstrated by a chaos test in `spec/reliability/` whose run
   transcript is committed alongside the release.
5. `shard.lock` runtime-scope is empty (P-8).
6. The wire-codec module passes the frame-corpus round-trip
   (`docs/05-wire-0-9-1/04-recorded-frames.md`).
7. Documentation has no `TODO`/`TBD` markers in normative prose.

The release notes for v0.1.0 MUST point to this section and tick
each criterion explicitly. If even one criterion is unmet, the tag
MUST be `0.1.0-rc` or a pre-release identifier, not `0.1.0`.

---

## 4. Anti-cutline (work that looks like v0 but is not)

These items are tempting to include and have been considered; the
rationale for keeping them out is recorded so future me does not
re-litigate.

- **A reusable connection pool.** Looks small; balloons quickly
  (eviction policy, health-check policy, blocking-acquire vs
  non-blocking, fair queueing). Callers compose pools.
- **A `Channel` that auto-reopens on `ChannelError`.** Looks small;
  semantically wrong — a channel close means the broker rejected an
  operation, and re-opening masks the rejection. The right pattern
  is: surface the error, let the caller decide.
- **Per-message TTL / dead-letter helpers.** TTL is set via
  `expiration` in `Properties`; DLX is set via `Arguments` on
  `queue_declare`. The plumbing is already there; sugar belongs in
  a higher-level library.
- **A "dump everything" debug logger.** The shard's `Log` is
  already at `Debug` for frame-level diagnostics; users who want
  raw bytes use `tcpdump`. Building a debug-only mode that records
  to a file invents a maintenance burden.

These four are recorded here so the conversation does not repeat.
