# amqp — AMQP 1.0 Forward Plan

> **Document status:** Draft v0.1, 2026-05-14 — **informative**, not
> normative for v0.
> **Audience:** Future-me (and future-LLM) planning v1; reviewers
> checking that v0's module boundaries do not foreclose v1.
> **Companions:** `docs/00-overview.md` §6 (forward roadmap),
> `docs/17-mvp-cutline.md` §2.1.

AMQP 1.0 is a **separate protocol** from AMQP 0-9-1, despite the
shared name. The two have different framing, different addressing
semantics, different security models, and different message
structures. This document captures the architectural decisions in
v0 that are intentionally compatible with a future v1 implementation
of AMQP 1.0.

The contents of this document are NOT normative for v0. The v0
implementation MUST satisfy `docs/00`-`docs/17`; nothing in this
document adds a v0 requirement.

---

## 1. Why a forward plan

The instinct to "make v0 forward-compatible with everything" is
costly. The instinct to "ignore v1 entirely" is also costly — the
wire-codec module boundary, in particular, is set once and refactoring
it later cascades into every other subsystem.

This document picks a small number of v0 design choices that are
specifically informed by anticipated v1 needs, so the v0 → v1
transition is additive rather than a rewrite.

---

## 2. AMQP 0-9-1 vs AMQP 1.0: where they diverge

A short, informative summary of the differences relevant to module
design:

| Aspect              | AMQP 0-9-1                                  | AMQP 1.0                                                     |
|---------------------|---------------------------------------------|--------------------------------------------------------------|
| Standardisation     | OASIS AMQP WG draft, vendor-driven          | OASIS Standard (2012) + ISO/IEC 19464                        |
| Framing             | Type-tagged frames + table-based body       | Performative-typed frames + AMQP type system                 |
| Multiplexing        | Channels (`UInt16`)                         | Sessions + Links (richer, but conceptually similar)          |
| Routing model       | Broker-side (exchanges, bindings, queues)    | Peer-to-peer (links to terminus addresses)                   |
| Auth                | SASL during `connection.start-ok`           | SASL as separate layer above TCP, before AMQP                |
| Confirms            | RabbitMQ-specific `confirm.select` extension | Built-in via link flow control + dispositions               |
| Heartbeats          | `connection.tune.heartbeat`                 | Idle-timeout field in `open` performative                    |
| Address space       | Routing key strings; exchanges named        | URIs as addresses (`amqp://host/queue/foo`)                  |

A v1 implementation against the same broker (RabbitMQ via its
`rabbitmq_amqp1_0` plug-in, Apache Qpid, ActiveMQ Artemis) will share
the **transport** (TCP + TLS) and the **fiber discipline**, but not
the wire codec or the lifecycle FSM.

---

## 3. v0 design choices that anticipate v1

### 3.1 Wire codec as a pure module

P-9 (`docs/01-design-principles.md`) makes the wire codec a set of
pure functions on `IO` and `Bytes`. The 0-9-1 codec lives under
`Amqp::Wire::AmqpZeroNineOne::*` (internal). A 1.0 codec, when added,
will live under `Amqp::Wire::AmqpOne::*` — side-by-side, NOT in place.

The `Connection` class's handshake step (`docs/06-connection-lifecycle.md`
§4) picks the codec module by examining the broker's protocol-header
response. If the broker advertises a 1.0 header, v1 dispatches into
the 1.0 codec; v0 raises `Amqp::ProtocolNegotiationError`.

### 3.2 Transport-layer separation

The transport (TCP socket + optional TLS wrap) is implemented
independently of the protocol. The transport's interface is the IO
read/write surface; whether the bytes are 0-9-1 frames or 1.0
performatives is the codec's concern.

In v0, the transport is internal and is not factored into a
separate class. The intent is that v1 factors it OUT to share
between 0-9-1 and 1.0; v0's implementation MUST be structured so
this refactor is mechanical.

### 3.3 Fiber discipline

The frame-reader / heartbeat / consumer-loop fiber topology
(`docs/00-overview.md` §2.2) is protocol-agnostic. The reader fiber
in v1 will decode 1.0 frames instead of 0-9-1 frames and route to a
different demuxing structure (sessions instead of channels), but
its existence and ownership of the read half of the socket is
unchanged.

The heartbeat fiber is conceptually identical; 1.0's
`idle-time-out` semantics map cleanly to 0-9-1's heartbeat
mechanism. v1 will reuse the heartbeat-fiber pattern.

### 3.4 Public API namespace shape

The public API namespace (`docs/02-public-api.md` §1) lives directly
under `Amqp::*`. v1 will introduce:

- `Amqp::Session` (analog of `Channel`, with richer flow control).
- `Amqp::Link` (a new concept; producer and consumer roles formalised).

The v0 names `Amqp::Connection`, `Amqp::Channel`, `Amqp::Message`,
`Amqp::Properties` may need to be:

- **Kept and re-purposed.** `Connection` is unambiguous; the v1
  surface MAY reuse it.
- **Versioned via parameter.** `Amqp.connect(url, protocol: :amqp_0_9_1)`
  vs `protocol: :amqp_1_0`. The protocol is inferred from the URI
  scheme normally; the keyword override is for the rare case where
  the same scheme is reused on a port that speaks the other version.

The v0 doc set does NOT introduce a `protocol:` keyword; that is
v1-territory. The v0 implementation MUST NOT add it speculatively.

### 3.5 Error hierarchy under shared root

`Amqp::Error` (`docs/03-error-model.md` §1) is the root for both
versions. v1 will add subclasses (e.g., `Amqp::LinkError`,
`Amqp::DispositionError`) under the same root. Callers catching
`Amqp::Error` continue to catch both versions' errors.

The v0 leaf classes (`Amqp::ChannelError`, `Amqp::PublishNackError`,
etc.) are 0-9-1-specific and will NOT be raised by 1.0 code paths.
v1 documentation will mirror this for its own leaves.

### 3.6 Configuration surface

URI grammar (`docs/04-uri-and-config.md` §1) uses `amqp` / `amqps`
schemes for 0-9-1. AMQP 1.0 conventionally uses the same schemes
on a different default port (5672 vs 5671 is the same; the spec
distinguishes by handshake). The shard MAY introduce
`amqp10` / `amqp10s` schemes if disambiguation is needed; the v0
doc set does NOT reserve those names — that decision is v1's.

### 3.7 Observability surface

`ConnectionStats` / `ChannelStats` / `SubscriptionStats`
(`docs/19-observability.md`) carry fields like `frame_max` and
`channel_max` that are 0-9-1-specific. v1 will introduce
1.0-specific stats (`max-frame-size`, `max-handle-per-session`)
under either new fields with `_v1` suffixes OR a separate stats
struct returned by a v1-specific method.

The decision is deferred. The v0 fields MUST NOT be renamed for
forward-compatibility hopes; rename is a 1.0 problem.

---

## 4. What v1 will explicitly add

A list of v1-specific surface that v0 does NOT need to anticipate
beyond §3:

- `Amqp::Session` and `Amqp::Link` classes.
- Disposition states (settle, accept, reject, release, modify).
- Source and target terminus types.
- Link flow control (credit + drain mechanisms).
- Distributed transactions (AMQP 1.0 has a transaction extension).
- SASL as a pre-AMQP layer (1.0's SASL is separate from the AMQP
  handshake).
- Connection-level `properties` field with vendor-specific keys.

None of these affect v0 module boundaries beyond what §3 covers.

---

## 5. What v1 will explicitly NOT change

- The shard's name (`amqp`).
- The transport layer (TCP + TLS).
- The fiber topology (one reader, one heartbeat, N consumers).
- The exception hierarchy root (`Amqp::Error`).
- The `Amqp` module namespace.
- The stdlib-only constraint (P-8), unless a 1.0-specific feature
  genuinely requires a dependency, in which case `docs/20-risk-register.md`
  gets a new entry.

---

## 6. Timing

The author's working plan: v0.1.0 ships when `docs/17-mvp-cutline.md`
§3 acceptance criteria are met. v0.x adds polish and operational
features. v1 is started only when:

1. Operational experience with v0 has revealed any v0 design
   mistakes — those get fixed in v0.x first, NOT papered over in
   v1.
2. There is a concrete v1 use case the author wants to ship
   (currently: none).

The doc set will be extended with `docs/21-amqp-1-0-overview.md`
(and a `05-wire-1-0/` subtree) when v1 work begins. v0's doc set is
not retroactively renumbered.

---

## 7. Decision log

Decisions made in v0 with v1 in mind, captured here so they aren't
re-litigated:

| Date       | Decision                                                   | Rationale                                       |
|------------|------------------------------------------------------------|-------------------------------------------------|
| 2026-05-14 | Top-level namespace is `Amqp`, not `Amqp::Client::Zero91`  | Forward-compatible without versioning sub-ns    |
| 2026-05-14 | Wire codec lives under `Amqp::Wire::AmqpZeroNineOne::*`    | Side-by-side path open for `AmqpOne::*`         |
| 2026-05-14 | Exception root `Amqp::Error`, not `Amqp::Zero91::Error`    | Catch-once across both versions                 |
| 2026-05-14 | URI schemes `amqp`/`amqps`, no `amqp10*` reserved          | Defer; v1 may rename or version-detect          |
| 2026-05-14 | `Connection` is shared name; v1 may reuse                  | Concept is identical at transport level         |
| 2026-05-14 | `Channel` is 0-9-1-specific; v1 introduces `Session`/`Link`| 1.0's model is genuinely different              |

Future decisions append to this table with a date.
