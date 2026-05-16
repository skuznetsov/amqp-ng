# amqp — Design Principles

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers — every principle below is normative and
> shapes the public API and internals.
> **Companion:** `docs/00-overview.md` (vision and scope),
> `docs/02-public-api.md` (the API these principles produce).

---

## 1. Why a principles document

Implementation choices in a Crystal AMQP client compound: how a
`Connection` exposes `Channel`s ripples into how publishing works,
which ripples into how confirms surface, which ripples into the
exception hierarchy. Without principles fixed up-front, the API
becomes the sum of local decisions, and refactoring the shard later
breaks every caller. This document fixes the principles so that all
later documents derive consistently.

Each principle below is numbered `P-N` and is referenced from later
docs at the point of derivation.

---

## 2. Principles

### P-1. Idiomatic Crystal is the API style

The shard reads as if it had always been part of the Crystal
ecosystem.

- Module and type names follow Crystal stdlib naming: `Amqp::Connection`,
  `Amqp::Channel`, `Amqp::Properties`, `Amqp::Error`. Not `AmqpClient`,
  not `Amqp::Client::Connection`.
- Constructors are `.new` and `.connect`; convenience constructors live
  on the module (`Amqp.connect(url)`), not on a `Client` god-class.
- Block forms exist for every lifecycle-owning type: `Amqp.connect(url)
  { |conn| ... }`, `conn.with_channel { |ch| ... }`. Block forms MUST
  close their resource on both normal return and exception.
- Keyword arguments instead of positional flags. Specifically,
  `delivery_mode: 2_u8` becomes `persistent: true` in the public API;
  the wire-level encoding is an implementation detail.
- `Time::Span` for every duration, never `Int`. `timeout: 5.seconds`,
  not `timeout: 5`.
- `URI` for connection strings, parsed via stdlib `URI.parse`. The
  shard MUST NOT introduce its own URL parser.
- Stdlib `Log` (one logger per subsystem: `Log = ::Log.for("amqp.conn")`,
  `::Log.for("amqp.codec")`, etc.). No `puts`, no `STDERR.puts`.

Falsifier: T-API-IDIOM-001 — public-API smoke test enumerates the
shard's exported symbols and asserts every method signature matches
the conventions above (no positional `Int` durations, no naked
`String` where `URI` is meant, etc.).

### P-2. Fibers and `Channel(T)` are first-class

The shard is fiber-native. Every blocking operation yields cleanly to
the Crystal scheduler.

- Inbound frame demultiplexing uses one `Channel(Frame)` per AMQP
  channel-id; the frame-reader fiber routes frames to those Crystal
  channels.
- The block form of `Channel#consume` runs the user block on the
  caller's fiber. The object form `Channel#subscribe` returns a
  `Subscription` whose `#each` is a fiber-yielding iterator and whose
  internal queue is a `Channel(DeliverMessage)`.
- `select` works on subscriptions. Concretely, the public API MUST
  expose a way to multiplex N subscriptions in one fiber using only
  Crystal's `select` statement. The reference shape:

  ```crystal
  select
  when msg = sub_a.receive
    handle_a(msg)
  when msg = sub_b.receive
    handle_b(msg)
  when timeout(5.seconds)
    # idle
  end
  ```

- Async publisher confirms surface via `Channel(ConfirmOutcome)` so the
  caller can `select` on confirms alongside subscriptions.
- Heartbeats are a fiber that `sleep`s; they MUST NOT busy-loop or
  poll a flag in a tight loop. The fiber wakes via `Channel(Nil)#receive`
  when the connection is torn down, never via timed polling.

Falsifier: T-API-FIBER-001..005 enumerate these surfaces; each test
constructs a scenario forcing the relevant fiber primitive and
asserts the absence of busy loops (CPU sample) or thread-of-execution
violations (separate stress test).

### P-3. Exceptions, not return codes

Every fallible operation either succeeds normally or raises a
subclass of `Amqp::Error`. There is no `Result(T, E)`, no `nil` for
"failed" overloaded with `nil` for "no such item."

- `publish_confirm` returns `Bool`: `true` on broker ack, `false` only
  if the caller opted into a `non_raising:` flavor (a separate API),
  default raises on nack/timeout.
- `Connection#close` is idempotent; the second call returns silently.
- Channel-level errors (broker sends `channel.close` with a reply-code)
  raise `Amqp::ChannelError` into every blocked call on that channel,
  including pending `publish_confirm`s.
- Connection-level errors (heartbeat timeout, broker `connection.close`,
  socket EOF) raise `Amqp::ConnectionError` (or a subclass) into every
  blocked call on every channel of that connection.

Full exception hierarchy in `docs/03-error-model.md`.

Falsifier: T-API-EXC-001..N exercise every documented failure mode and
assert the correct subclass surfaces with the correct fields populated.

### P-4. No global singletons in the public API

The shard exposes no global state. No "default connection," no global
registry of channels, no module-level `current_logger`.

- The user holds a `Connection` reference and threads it through
  themselves (or keeps it in a constant of their choice).
- `::Log.for("amqp.*")` uses Crystal's stdlib logger, which is itself
  configured at process scope — that is a stdlib choice, not an
  `amqp`-specific singleton.
- Test helpers MAY use a module-level connection holder for
  convenience (`spec/spec_helper.cr`), but the test helpers are
  NOT part of the shard's public API.

Falsifier: T-API-NOGLOBAL-001 — grep the public namespace for module
`@@`-vars; the only allowed module-level state is `Log` instances.

### P-5. URI is the connect entrypoint

A connection is opened from a URI:

```crystal
conn = Amqp.connect("amqps://user:pass@host:5671/myvhost")
```

- Scheme `amqp` selects plain TCP, default port 5672.
- Scheme `amqps` selects TLS, default port 5671.
- Path component (after the host:port) is the vhost; an empty path
  means the default vhost `/`. The vhost is URL-decoded once.
- Userinfo (`user:pass`) sets PLAIN-SASL credentials. If userinfo is
  absent, credentials MUST be supplied via `Amqp.connect(url, user:
  ..., password: ...)`.
- Query parameters (`?heartbeat=30&channel_max=2047`) override defaults
  for that connection. The set of recognised query parameters is
  enumerated in `docs/04-uri-and-config.md`.

The shard MUST NOT accept any other connect form for plain
configuration. A user with non-URL config sources composes a URI
themselves; the shard's job is one canonical entry point.

Falsifier: T-API-URI-001..010 cover the scheme/port/vhost/userinfo/
query-parameter parsing matrix.

### P-6. Performance is observable

Performance targets in `docs/14-performance-contract.md` are roadmap
targets until a `spec/perf/` harness exists. The current v0 shard
exports a small counter surface:

- `Connection#stats` returns `Amqp::Stats`; callers read
  `Stats::Snapshot` counters for published, confirmed, returned,
  consumed, and recovery events.
- Rich per-connection/channel/subscription stats are deferred.

These are observability primitives, not part of every hot-path call.
Implementations SHOULD use `Atomic(Int64)` (or equivalent stdlib
atomic) for counters touched on the hot path, NOT a `Mutex`-guarded
counter.

Falsifier: T-PERF-STATS-001 asserts that stats fields update under
load without serializing the hot path (latency-distribution test
with and without stats access).

### P-7. Reliability is binary

Every reliability claim in `docs/15-reliability-contract.md` is
binary: it either holds or it does not. There is no probabilistic
reliability guarantee in v0. Specifically:

- A `publish_confirm` that returns `true` MUST mean the broker
  accepted the message into a queue (or routed it to zero queues, per
  `mandatory:` flag), with the same guarantees AMQP 0-9-1 publisher
  confirms provide.
- A `publish_confirm` that raises MUST NOT leave the caller in doubt
  about commit status. Exception types and their meanings are
  enumerated in `docs/08-publisher-confirms.md`.
- Heartbeat semantics are not probabilistic. The receive-deadline is
  `2 * negotiated_interval` from the last frame received; on expiry
  the connection MUST be considered dead.
- Topology recovery, when enabled, MUST re-declare every entity that
  was declared via THIS connection's API. The shard MUST NOT
  re-declare entities that existed before this connection (no
  passive-then-active retry that would change broker state).

Falsifier: T-REL-* tests are listed in
`docs/15-reliability-contract.md`. Each is binary.

### P-8. Stdlib only in v0

v0 has zero runtime dependencies outside Crystal stdlib. This is a
hard rule:

- No `crystal-amqp-helper`-style dependencies, no JSON libraries
  (AMQP is binary), no pooling libraries (callers compose pooling).
- Test-time dependencies are permitted in `development_dependencies`.

Reason: the shard exists in part because a vendored dependency broke
on stdlib drift. The fewer transitive things to keep current, the
fewer surprises. If a future feature genuinely requires a dependency,
it MUST be motivated in `docs/20-risk-register.md` with a name,
maintainer activity check, and a fallback plan.

Falsifier: T-API-DEPS-001 — `shard.lock` MUST list only
development-scope entries; runtime-scope MUST be empty.

### P-9. The wire codec is pure

The wire codec is a set of pure functions over `IO` and `Bytes`. It
holds no state, owns no fiber, allocates no socket.

- `Amqp::Codec.decode_frame(io : IO) : Amqp::Frame` reads bytes,
  returns a Frame, raises `Amqp::ProtocolError` on malformed input.
- `Amqp::Codec.encode_frame(frame, io : IO) : Nil` writes bytes.
- All field-type encoders (`encode_short_string`, `encode_table`,
  `encode_long_long_uint`, etc.) are similarly pure.

Reason: pure codec is unit-testable without a broker, mock-free.
The recorded-frames corpus
(`docs/05-wire-0-9-1/04-recorded-frames.md`) is a long catalog of
"encode(decode(bytes)) == bytes" round-trips.

Falsifier: T-CODEC-PURE-001 — the codec module MUST NOT spawn fibers,
open sockets, or hold any module-level non-`Log` state.

### P-10. Recovery is opt-in and complete

There is no "partial recovery" mode in v0. Either the connection's
`recovery:` argument is `Recovery::None` (the default; reconnect is
the caller's problem) or `Recovery::Full` (the shard re-establishes
the connection, re-opens channels, re-declares topology, re-installs
consumers, and re-publishes in-flight unconfirmed messages).

A planned third mode `Recovery::Manual` is sketched in
`docs/12-recovery.md` for future use; it is NOT v0 scope.

Reason: partial recovery is a long tail of subtle bugs. Either the
caller owns reconnect logic, or the shard does all of it.

Falsifier: T-REC-TRIGGER-001..N, T-REC-ORDER-001..006, and
T-REC-REPLAY-001 enumerate the survivability cases for
`Recovery::Full`; T-REC-NONE-001 asserts that with `Recovery::None` a
broker close surfaces an exception to every blocked call within 500 ms.

### P-11. TLS is not optional optional

When the URI scheme is `amqps`, TLS is mandatory. The shard MUST
refuse a connection attempt that downgrades to plain text mid-handshake
(unlikely in AMQP — TLS is wrapper, not negotiated — but the rule
guards against future broker bugs).

- TLS context is constructed by the caller and passed via `tls:`
  argument; the shard provides a `Amqp.tls_context_default` helper
  that returns a `OpenSSL::SSL::Context::Client` with TLS 1.2+
  and stdlib's default cipher suite.
- SNI is mandatory: the URI's host MUST be passed as `OpenSSL::SSL::Socket`'s
  `hostname` parameter so virtual-hosted brokers route correctly.
- Cipher policy and certificate-verification policy are the caller's
  responsibility through the `OpenSSL::SSL::Context::Client` they pass;
  the shard does not silently weaken either.

Falsifier: T-TLS-SCHEME-001..003, T-TLS-CTX-001..003, T-TLS-SNI-001,
T-TLS-HOSTNAME-001, and T-TLS-ERR-001..006 (see `docs/11-tls.md`).

### P-12. Observability is not free, but it is cheap

The v0.1.0 operational surface is intentionally small: callers get
`Connection#stats`, which returns the reduced `Amqp::Stats` counter
object. Rich per-channel and per-subscription stats are deferred until
their fields have executable falsifiers.

- Stats fields are documented in `docs/19-observability.md`.
- The shard MUST NOT add OpenTelemetry or any specific telemetry
  vendor dependency in v0. It exposes counters; integration is the
  caller's choice.

Falsifier: T-OBS-STATS-001..N — for each documented v0.1.0 counter, a
test asserts that it updates without reading logs.

---

## 3. Anti-principles

For clarity, the shard explicitly avoids these patterns even though
they appear in similar libraries:

- **No "easy mode" auto-reconnect by default.** Recovery is opt-in
  (`P-10`). A surprise auto-recovery has masked production bugs in
  other clients.
- **No mutex around `IO` for sharing the connection across producers.**
  Sharing happens at the `Channel` level, not the socket level (`P-2`).
  The connection-level write-mutex exists, but it serializes frame
  writes, not user-level operations.
- **No DSL for declaring topology.** `ch.exchange_declare(...)` is the
  API. A "declarative topology spec object" can be built on top of the
  shard; it does not belong inside it.
- **No JSON-aware overloads.** Bodies are `Bytes` / `String`. Callers
  serialize themselves. The shard is a transport.
- **No singleton "default exchange" helper.** RabbitMQ's empty-name
  exchange is reachable by literally passing `""` as the exchange
  name; a helper would obscure that this is the AMQP default and
  not a shard convenience.
