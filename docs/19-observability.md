# amqp — Observability

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of stats structs; operators integrating
> the shard with monitoring.
> **Companions:** `docs/01-design-principles.md` (P-6, P-12),
> `docs/02-public-api.md` (`#stats` on Connection/Channel/Subscription),
> `docs/14-performance-contract.md` PERF-9 (stats read overhead).

The shard exposes operational state through three stats objects:
`ConnectionStats`, `ChannelStats`, `SubscriptionStats`. This document
is the **complete** specification of every field, plus the logging
contract.

---

## 1. Stats principles

- **Cheap to read.** Per P-12 and PERF-9, reading stats MUST be a
  set of atomic loads. No locks, no allocations on the read path
  beyond the snapshot struct's stack frame.
- **Immutable snapshots.** A returned stats value is a frozen
  snapshot of the moment it was read. Internal counters keep
  ticking; the caller's snapshot does not change.
- **Atomic counters internally.** Counters touched on the hot path
  (frames sent, bytes received, etc.) MUST be `Atomic(Int64)` or
  equivalent.
- **No telemetry vendor in v0.** The shard does not depend on or
  ship integration with OpenTelemetry, Prometheus, Datadog, etc.
  Vendor wrappers belong outside the shard (per `docs/17` §2.9).

---

## 2. `Amqp::ConnectionStats`

```crystal
struct Amqp::ConnectionStats
  # Identity
  getter uri_host : String              -- URI's host (no userinfo)
  getter uri_port : UInt16
  getter vhost : String
  getter open_since : Time?             -- Time the Connection entered Open; nil after Closed

  # State
  getter state : Symbol                 -- :initial | :connecting | :negotiating
                                        --  | :open | :closing | :closed | :recovering
  getter blocked? : Bool                -- broker sent connection.blocked
  getter close_reason : Amqp::CloseReason?

  # Negotiation
  getter heartbeat : Time::Span
  getter channel_max : UInt16
  getter frame_max : UInt32

  # TLS (nil for plain TCP)
  getter tls_version : String?
  getter tls_cipher : String?
  getter peer_certificate_subject : String?

  # I/O counters (atomic; monotonic)
  getter bytes_sent : Int64
  getter bytes_received : Int64
  getter frames_sent : Int64
  getter frames_received : Int64
  getter heartbeats_sent : Int64
  getter heartbeats_received : Int64

  # Channels
  getter channels_open : Int32          -- current
  getter channels_high_water : Int32    -- ever-observed peak
  getter channels_allocated_total : Int64  -- ever-allocated (monotonic)

  # Recovery (Recovery::Full only; zeros otherwise)
  getter recoveries_completed : Int64
  getter recoveries_abandoned : Int64
  getter last_recovery_attempts : Int32
  getter last_recovery_dead_window : Time::Span?
end
```

### 2.1 Field semantics

- `state` reflects the connection's lifecycle phase at the moment of
  read. The shard's state machine (`docs/06-connection-lifecycle.md`)
  has internal transitions; the public symbol is the user-visible
  one.
- `blocked?` toggles on `connection.blocked` / `unblocked` from
  RabbitMQ/LavinMQ.
- `frames_sent` includes heartbeat frames. `bytes_sent` includes the
  frame headers. The intended reader (an operator) sees raw bytes,
  not message-level counters.
- `channels_allocated_total` is the lifetime allocation count, NOT
  current; useful for detecting churn.
- The recovery fields are zero in `Recovery::None` mode.

### 2.2 Update timing

- `bytes_sent` / `frames_sent`: updated in the write path, after
  successful socket write.
- `bytes_received` / `frames_received`: updated in the frame-reader
  fiber, after successful decode.
- `heartbeats_sent` / `heartbeats_received`: subset of the totals;
  updated for type-8 frames specifically.
- `channels_open`: incremented on `channel.open-ok`, decremented on
  channel `Closed`.
- `channels_high_water`: updated only on increment of
  `channels_open` AND when the new value exceeds the previous high.

**Falsifier:** T-OBS-CONN-001..N — one assertion per field's update
discipline.

---

## 3. `Amqp::ChannelStats`

```crystal
struct Amqp::ChannelStats
  # Identity
  getter id : UInt16
  getter open_since : Time?
  getter confirms_enabled? : Bool
  getter flow_paused? : Bool

  # State
  getter state : Symbol                 -- :initial | :opening | :open
                                        --  | :confirms | :flowing
                                        --  | :closing | :closed
  getter close_reason : Amqp::CloseReason?

  # Publishing
  getter messages_published : Int64
  getter publishes_in_confirm_mode : Int64
  getter confirms_acked : Int64
  getter confirms_nacked : Int64
  getter confirms_returned : Int64
  getter confirms_timed_out : Int64
  getter unconfirmed_in_flight : Int32

  # Consuming
  getter deliveries_received : Int64
  getter manual_acks : Int64
  getter manual_nacks : Int64
  getter manual_rejects : Int64
  getter consumers_active : Int32       -- subscriptions/consume blocks alive

  # QoS
  getter prefetch_count : UInt16
  getter prefetch_global : Bool
end
```

### 3.1 Update timing

- `messages_published`: incremented after the three-frame publish
  sequence is flushed to the socket. Counts every publish (FF,
  sync, async) regardless of confirm mode.
- `publishes_in_confirm_mode`: subset, only counted when channel was
  in `Confirms` at publish time.
- `confirms_acked` etc.: incremented when the broker's
  `basic.ack`/`basic.nack` resolves the destination, NOT when the
  caller reads from the destination.
- `confirms_timed_out`: incremented when `publish_confirm`'s timeout
  elapses (the entry stays in the tracker until a later ack arrives,
  at which point `confirms_acked` is also incremented for the same
  delivery; both counters are best read as "events observed," not
  "messages in distinct states").
- `unconfirmed_in_flight`: current size of the confirm tracker.

**Falsifier:** T-OBS-CHAN-001..N.

---

## 4. `Amqp::SubscriptionStats`

```crystal
struct Amqp::SubscriptionStats
  getter consumer_tag : String
  getter queue : String
  getter open_since : Time?
  getter closed? : Bool

  getter deliveries_received : Int64
  getter buffer_depth : Int32           -- current
  getter buffer_high_water : Int32      -- ever-observed peak
  getter buffer_capacity : Int32
end
```

`buffer_depth` is a snapshot; the actual depth at read time may have
changed by the time the caller acts on it. This is acceptable for
observability.

**Falsifier:** T-OBS-SUB-001.

---

## 5. Stats and Recovery

Recovery (`docs/12-recovery.md`) presents a subtle question: what
happens to channel/subscription stats across a successful recovery?

The shard's choice: **preserve counters across recovery**. The
user-visible `Channel` reference is stable through recovery; its
`stats.messages_published` count keeps growing through the dead
window and re-publish. The connection's `recoveries_completed`
counter is the operator's signal for "this connection has been
reanimated N times."

The channel's `open_since` is updated to the time of the LATEST
successful `channel.open-ok` (i.e., it resets across recovery). The
connection's `open_since` is updated to the time of the LATEST
successful `connection.open-ok` after recovery.

`unconfirmed_in_flight` reflects the current tracker state. During
recovery, the count may temporarily double-count if the previous
incarnation's publishes are tracked alongside their re-publishes;
this is implementation-defined.

**Falsifier:** T-OBS-RECOVERY-001 — counters preserved, `open_since`
reset.

---

## 6. Logging contract

The shard uses Crystal's stdlib `Log` (`docs/01-design-principles.md`
P-1). The implementation MUST create exactly the following loggers:

| Source name        | Purpose                                                    |
|--------------------|------------------------------------------------------------|
| `amqp.conn`        | Connection lifecycle events                                |
| `amqp.chan`        | Channel lifecycle events                                   |
| `amqp.codec`       | Wire-codec decode errors and protocol violations           |
| `amqp.heartbeat`   | Heartbeat send / receive-deadline events                   |
| `amqp.recovery`    | Recovery attempts, outcomes, callback exceptions           |
| `amqp.tls`         | TLS handshake outcome, certificate-related events          |

These are the only `Log.for` names the shard creates. The shard MUST
NOT introduce additional source names in v0 without a doc revision.

### 6.1 Severity guidance

Per logger, the shard's expected severity usage:

- `amqp.conn`:
  - `Info` on successful `Open`, `Closed` for graceful causes.
  - `Warn` on receive-deadline drift exceeding 80% of the heartbeat
    threshold (early warning).
  - `Error` on broker close with reply-code != 200 / 320, and on
    socket failures.
- `amqp.chan`:
  - `Info` on `channel.open-ok` and graceful `close-ok`.
  - `Warn` on `channel.flow(false)` (production-relevant).
  - `Error` on broker-initiated channel close.
- `amqp.codec`:
  - `Debug` for frame-level traces (off by default; enable via Log
    config when diagnosing).
  - `Error` on `ProtocolError` — protocol violations.
- `amqp.heartbeat`:
  - `Debug` on each send.
  - `Info` once on connection start announcing the negotiated value.
  - `Error` on receive-deadline expiry.
- `amqp.recovery`:
  - `Info` on each successful recovery, with the `RecoveryEvent`
    fields formatted.
  - `Warn` on each failed attempt while still retrying.
  - `Error` on surrender (`RecoveryAbandoned`).
- `amqp.tls`:
  - `Info` once on connection start announcing TLS version + cipher.
  - `Warn` on `verify=none` configuration.
  - `Error` on handshake failure.

The shard MUST NOT log message bodies. Properties may be logged at
`Debug` for tracing but never at higher severities.

### 6.2 Structured logging (informative)

The shard does not enforce a structured-logging format; it uses
stdlib `Log`'s default formatter. Callers wanting structured output
configure stdlib `Log` themselves. Each log line in `amqp.*` SHOULD
include enough context to diagnose without grepping (e.g., a
connection identifier).

**Falsifier:** T-OBS-LOG-001 — fire each event class above, capture
log output, assert the expected source name and severity.

---

## 7. The `connection_id` field

For diagnostics, each `Connection` instance has an internal
monotonically-assigned `UInt64` id. The id is visible only in:

- `ErrorContext#connection_id` (used by `Amqp::Error#to_s`).
- The shard's own log output (prefix `[conn=N]`).

It is NOT exposed as a public method on `Connection`. Callers who
want a connection-identifier surface for their own logs construct
one (e.g., from the URI + a UUID).

---

## 8. Anti-patterns

- **Polling stats every microsecond.** Stats are atomic but not free.
  Scrape at monitoring frequency (1-10 Hz typical).
- **Computing derived metrics inside the shard.** The shard exports
  raw counters; consumers compute rates, ratios, percentiles. The
  shard does not bake in a "messages-per-second" gauge because the
  scrape interval is the caller's choice.
- **Logging at `Debug` in production.** Frame-level traces are
  voluminous and reveal message metadata. Default production
  severity SHOULD be `Info` or `Warn`.
- **Treating `confirms_timed_out` as "messages lost."** They are
  "outcomes the caller's timeout expired before receiving"; the
  broker may still ack them later. The corresponding ack will also
  increment `confirms_acked`. Use both counters and the caller's
  retry policy to compute "actually lost" externally.
