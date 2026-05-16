# amqp — Architecture Overview

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of this shard and reviewers checking that
> every public claim has a defined falsifier.
> **Spec format:** Structured Markdown using RFC 2119 normative keywords
> (`MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT`, `MAY`). The intent is that
> the doc set is sufficient for a correct implementation without
> consulting external AMQP references; any external citation appears
> inline at the point where it is needed.
> **Companion documents:** every other file under `docs/`.

---

## 1. Vision

`amqp` is a Crystal client for the AMQP family of message-broker
protocols. The v0 implementation targets **AMQP 0-9-1**, which is the
dialect spoken by RabbitMQ and LavinMQ, the two brokers the author
operates in production today. AMQP 1.0 — a separate protocol despite
the shared family name — is architecturally accounted for but is not
v0 scope; see §6 and `docs/18-amqp-1-0-forward-plan.md`.

The design priorities, in strict order:

1. **Correctness.** Every public guarantee is a normative claim with a
   falsifier in `docs/16-falsifier-matrix.md`. There is no "best-effort"
   in the public API surface.
2. **Idiomatic Crystal.** Native fibers and `Channel(T)` for concurrency;
   `Time::Span` for durations; `OpenSSL::SSL::Context::Client` for TLS;
   block-helpers for lifecycle; no global singletons; URI parsing via
   stdlib `URI`. Implementers MUST NOT reach into stdlib instance
   variables (e.g., `io.@writable`); the implementation MUST work
   against any Crystal version satisfying the `shard.yml` constraint
   without source-level edits.
3. **Reliability.** Heartbeats are mandatory and fiber-driven.
   Publisher confirms are first-class. Topology recovery is opt-in but
   complete when enabled. The shard MUST NOT silently swallow broker
   close-reasons; every fatal condition surfaces as an exception in
   the iterating fiber.
4. **Performance.** Performance is a normative property, not a slogan.
   `docs/14-performance-contract.md` enumerates measurable bounds
   (PERF-1..PERF-N) with falsifier benchmarks.

### 1.1 Non-goals

- **AMQP 0-8, AMQP 0-9.** Dead in all target brokers (RabbitMQ removed
  0-8 in 3.0, 2012; LavinMQ never supported them). Implementers MUST
  NOT add code paths for these dialects.
- **AMQP 1.0 in v0.** A separate protocol. Architectural slot reserved
  (`docs/18-amqp-1-0-forward-plan.md`); v0 implementations MUST shape
  the wire-codec module boundary so that adding 1.0 later is a
  side-by-side addition, not a rewrite.
- **Broker-side admin APIs** (RabbitMQ HTTP management plugin, LavinMQ
  HTTP API). Out of scope — those are not AMQP, they are HTTP/JSON.
- **Higher-level abstractions** (job queues, RPC clients, work
  schedulers). Out of scope. This shard is the transport; building
  blocks live above it.
- **Custom transports.** v0 speaks AMQP 0-9-1 over TCP, optionally
  wrapped in TLS. No WebSocket, no Unix-domain socket, no in-process
  loopback.

### 1.2 Audience

The shard targets two kinds of caller:

- **Worker authors** consuming durable work queues with manual
  acknowledgement. The block-form `Channel#consume` covers this case
  with the minimum public surface.
- **Service authors** producing messages with at-least-once guarantees
  via publisher confirms. The `Channel#publish_confirm` /
  `Channel#publish_async` API covers this case without forcing the
  caller to track delivery tags by hand.

The shard does not target operator tooling (queue inspection UIs,
shovel-equivalents) as a primary use case, but its public API is
sufficient to build them.

---

## 2. System overview

The shard is divided into five internal subsystems, listed here with
their owning specifications:

| Subsystem            | Specs                                        |
|----------------------|----------------------------------------------|
| Wire codec           | `docs/05-wire-0-9-1/*`                       |
| Connection lifecycle | `docs/06-connection-lifecycle.md`            |
| Channel lifecycle    | `docs/07-channel-lifecycle.md`               |
| Publisher path       | `docs/08-publisher-confirms.md`              |
| Consumer path        | `docs/09-consumer.md`                        |

Cross-cutting concerns each have their own document: heartbeats (`10`),
TLS (`11`), recovery (`12`), broker compatibility (`13`), performance
(`14`), reliability (`15`), errors (`03`), observability (`19`).

### 2.1 Subsystem boundary contract

Each subsystem above MUST be implementable in isolation against its
upstream and downstream interfaces. Concretely:

- **Wire codec** depends only on Crystal stdlib `IO`, `IO::ByteFormat`,
  `Bytes`, `Slice`. It exports pure functions `decode_frame(io) :
  Frame` and `encode_frame(frame, io)`, plus the field-type primitives
  for tables and properties.
- **Connection lifecycle** depends on the wire codec and the public
  `URI`/`OpenSSL` APIs. It exports the `Connection` class and owns the
  socket, the inbound frame-reader fiber, the heartbeat fiber, and the
  per-channel inbox demultiplexer.
- **Channel lifecycle** depends on Connection. It exports the
  `Channel` class. Each `Channel` instance owns its own per-channel
  state (consumer registry, confirm-tracker, prefetch) and reads from
  the inbox that Connection routes to it.
- **Publisher path** is implemented inside `Channel`. It exports
  `publish`, `publish_confirm`, `publish_async`.
- **Consumer path** is implemented inside `Channel`. It exports
  `consume` (block form), `subscribe` (object form returning a
  `Subscription`), `get`, `ack`, `nack`, `reject`, `prefetch`.

A subsystem is **falsified** if any test in
`docs/16-falsifier-matrix.md` belonging to its prefix fails. Tests are
prefixed `T-CODEC-*`, `T-CONN-*`, `T-CHAN-*`, `T-PUB-*`, `T-CONS-*`,
`T-HB-*`, `T-TLS-*`, `T-REC-*`, `T-COMPAT-*`, `T-PERF-*`, `T-REL-*`.

### 2.2 Fiber topology

The shard MUST use exactly the following fibers per `Connection`:

- One **frame-reader** fiber: blocking-reads from the socket, decodes
  frames, demultiplexes by `channel_id` into per-channel `Channel(Frame)`
  inboxes. Owns the read half of the socket.
- One **heartbeat** fiber: sleeps until the next send deadline, writes
  a heartbeat frame, repeats. Reads the last-receive timestamp shared
  via `Atomic(Int64)` (Unix nanos) to detect broker silence; on timeout
  raises `Amqp::HeartbeatTimeoutError` into all channels and tears the
  connection down.
- Zero or more **consumer-loop** fibers, one per `consume` call. Each
  reads `Frame`s from its `Subscription`'s `Channel(DeliverMessage)` and
  yields to the user block. The Crystal fiber doing the `consume` call
  IS the loop; spawning happens only when the user opts into a
  detached consumer via `subscribe(...).spawn { |msg| ... }`.

No additional fibers are permitted in v0. Writes to the socket happen
on the **caller's** fiber under a connection-level `Mutex`; this keeps
the design simple and avoids a write-fan-in fiber that would add a
hop to the publish hot path.

`docs/14-performance-contract.md` PERF-4 falsifies the multi-channel
throughput claim against this topology.

### 2.3 Concurrency contract

- `Connection` is safe to share across fibers AFTER `connect` returns.
  Channel allocation (`conn.channel`) is serialized.
- `Channel` is **not** safe to share across fibers for unrelated
  operations. A `Channel` is owned by the fiber that allocated it OR
  by a fiber explicitly handed ownership. Concurrent `publish` calls
  on the same channel from different fibers MUST raise
  `Amqp::ConcurrencyError` rather than corrupt frame ordering.
- A `Subscription` returned by `subscribe` is itself a `Channel(T)`-like
  receiver: `msg = sub.receive` is the canonical way to consume from
  multiple subscriptions in one fiber via `select`.

---

## 3. Versioning and Crystal compatibility

The shard's public API follows semver. v0.x means the API may change;
v1.0 freezes it. The wire-codec output (bytes on the wire) is governed
by the AMQP 0-9-1 specification and is not part of semver — it is
fixed by the protocol.

Minimum Crystal version is declared in `shard.yml` and tested in CI on
both the floor version and the latest stable. The implementation MUST
NOT depend on language features introduced after the floor. Specifically,
the implementation MUST NOT:

- Access stdlib instance variables directly (`io.@buffer`, `io.@writable`).
- Use stdlib methods marked `@[Deprecated]` at the floor version.
- Rely on undocumented Crystal compiler behavior (macro internals,
  `@type` introspection beyond documented surface).

Any planned dependency on a post-floor Crystal feature MUST be raised
as a falsifier in `docs/20-risk-register.md` first, with an explicit
floor-version bump, before the implementation uses it.

---

## 4. Document conventions

Every document in `docs/` follows the following structure unless its
nature dictates otherwise (e.g., the falsifier matrix is a table).

1. **Status header.** Draft version, last-edited date, audience,
   companions. The version MUST tick when the doc's normative content
   changes (typos and editing are not a tick).
2. **Numbered top-level sections** introducing the topic.
3. **RFC 2119 keywords** for every implementable claim: `MUST`,
   `MUST NOT`, `SHOULD`, `SHOULD NOT`, `MAY`. The implementer reads
   `MUST`/`MUST NOT` as binary — any deviation is a bug. `SHOULD`/`SHOULD
   NOT` are guidance with a documented escape hatch; deviations require
   a note in the implementation explaining the situation. `MAY` is
   discretionary.
4. **Falsifier IDs** at the end of any section asserting a measurable
   property: e.g., "Falsifier: T-CODEC-FIELD-001". The matching
   test must exist in `docs/16-falsifier-matrix.md` and `spec/`.
5. **Cross-doc references** as relative paths
   (`docs/05-wire-0-9-1/01-types.md`).

Documents MUST NOT contain literal byte sequences from the AMQP
specification without inline citation back to the originating clause
in this document set. The wire-codec docs (`05-wire-0-9-1/*`)
duplicate the byte layouts in full so that an implementer never needs
to read the upstream AMQP PDF.

---

## 5. Glossary

Terms used across the doc set:

- **Broker.** The peer the shard connects to. Tested brokers are
  enumerated in `docs/13-broker-compat-matrix.md`.
- **Connection.** A single TCP (optionally TLS-wrapped) link to a
  broker, multiplexing many channels.
- **Channel.** A logical, ordered byte-stream within a connection,
  identified by a `UInt16` channel-id. Channel 0 is reserved for
  connection-level methods.
- **Frame.** The transport unit of AMQP 0-9-1. Four frame types:
  method, header, body, heartbeat. See `docs/05-wire-0-9-1/00-frames.md`.
- **Method.** A typed RPC-like operation on a channel (e.g.,
  `basic.publish`, `queue.declare`). See
  `docs/05-wire-0-9-1/02-classes-methods.md`.
- **Publisher confirms.** RabbitMQ-pioneered extension where the broker
  acknowledges (`basic.ack`) or rejects (`basic.nack`) each published
  message by delivery tag. Standardised behavior on both target
  brokers. See `docs/08-publisher-confirms.md`.
- **Recovery.** The optional post-blip restoration of declared
  topology, consumers, and unconfirmed in-flight publishes. See
  `docs/12-recovery.md`.
- **Falsifier.** The smallest test whose failure refutes a specific
  normative claim in the docs. The doc set is correct only if every
  `MUST`/`MUST NOT` has a passing falsifier.

---

## 6. Forward roadmap (informative)

The roadmap below is **informative**, not normative. Schedules may
slip; the order of concerns is the part to hold steady.

- **v0** (target: complete AMQP 0-9-1 client). Specs in `docs/`,
  implementation under `src/`, falsifiers in `spec/`. v0.1.0 is the
  urgent RabbitMQ/LavinMQ integration release; stricter full-matrix
  hardening continues in v0.x.
- **v0.x** (post-MVP polish): observability surface
  (`docs/19-observability.md`), broker-specific extension surface
  (e.g., RabbitMQ direct-reply-to, publisher confirms in transaction
  mode), additional auth mechanisms beyond PLAIN.
- **v1** (AMQP 1.0). Reserves a parallel wire-codec module per
  `docs/18-amqp-1-0-forward-plan.md` so the connection lifecycle picks
  the codec at handshake time and the rest of the runtime is shared.
  Not v0 scope.
