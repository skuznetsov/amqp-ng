# amqp — Reliability Contract

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers; callers building at-least-once systems.
> **Companions:** `docs/01-design-principles.md` (P-7 reliability is
> binary), `docs/03-error-model.md` §8 (the "no doubt" rule),
> `docs/08-publisher-confirms.md`, `docs/12-recovery.md`,
> `docs/16-falsifier-matrix.md`.

This document lists every reliability claim the v0 shard makes. Each
claim is `REL-N`, paired with a chaos test in
`spec/reliability/T-REL-*.cr`. Per P-7, every claim is binary — the
shard meets it or it does not.

---

## 1. The reliability model

The shard's reliability guarantees are AMQP 0-9-1 publisher-confirms
guarantees, plus what the shard adds on top via recovery and clean
exception semantics. The model assumes:

- The broker conforms to AMQP 0-9-1 + publisher confirms.
- Disks are functional (no silent corruption on the broker side).
- Authentication and authorisation are static across a connection's
  lifetime.

Out of scope:

- Broker bugs that violate the protocol (the shard catches obvious
  violations as `ProtocolError`; subtler ones are not addressable).
- Network adversaries (TLS protects against passive sniffing and
  active MITM; replay attacks above the message layer are the
  caller's problem).
- Disk failures on the broker.

---

## 2. REL-1: At-least-once publish (with confirms)

**Claim.** A successful `Channel#publish_confirm` returning `true`
means the broker accepted the message and either routed it to one
or more queues or, for `mandatory: false`, accepted it for routing
(it MAY have been routed nowhere if no queue was bound and
`mandatory == false`).

A `publish_confirm` that raises `Amqp::PublishNackError` means the
broker explicitly rejected the message (e.g., queue full with
`x-overflow=reject-publish`).

A `publish_confirm` that raises `Amqp::PublishReturnedError` (with
`mandatory: true`) means the broker accepted the publish but could
not route it to any queue.

A `publish_confirm` that raises `Amqp::PublishTimeoutError` means
the broker has not responded within the caller's bound; the publish
MAY have been committed or not. The caller MUST treat this as
"unknown" (`docs/03-error-model.md` §8).

**Falsifier:** `T-REL-AT-LEAST-ONCE-001..004`.

---

## 3. REL-2: Confirm exactly-once delivery

**Claim.** The `ConfirmOutcome` value for a given delivery tag is
delivered to its destination `::Channel` **exactly once**.

Specifically:

- Each `publish_async` call returns a one-shot `::Channel(ConfirmOutcome)`
  that receives exactly one `ConfirmOutcome` value (or is closed
  without a value, in the connection-close case).
- Each `publish_confirm` call resolves to exactly one outcome
  (returned/raised) per call.

The shard MUST NOT send two outcomes for the same tag. If the broker
were to send `basic.ack` followed by `basic.nack` for the same tag
(protocol violation), the shard MUST raise
`Amqp::PublishOutOfOrderError` and tear the connection down.

**Falsifier:** `T-REL-CONFIRM-ONCE-001..003`.

---

## 4. REL-3: Connection-close error visibility

**Claim.** When a connection closes for any reason (broker, network,
heartbeat, caller, recovery-abandoned), every fiber blocked on the
shard wakes within `500 ms` of the close event with an
`Amqp::Error` subclass that identifies the cause where v0 can
classify it.

**Why 500 ms.** The shard's wake path is fiber-local channel
closures, which are immediate; the only delay is scheduler latency.
500 ms is generous.

**Falsifier:** `T-REL-WAKE-001..006` (one per origin).

---

## 5. REL-4: No silent message loss in `publish`

**Claim.** A `Channel#publish` call (fire-and-forget) either:

- Returns normally, in which case the message's frames were
  successfully written to the OS socket buffer (and from there
  delivery to the broker is in the broker/network's hands), OR
- Raises an exception synchronously, in which case the message was
  NOT written or was partially written and the connection has been
  torn down.

The shard MUST NOT swallow a write error. The shard MUST NOT
silently discard a publish under any circumstance.

**Falsifier:** `T-REL-PUB-FF-001` — disconnect the socket mid-publish,
verify either the call raises OR the bytes are at the OS buffer.

---

## 6. REL-5: Acknowledgement durability

**Claim.** A `Channel#ack` call returns only after the ack frame is
written to the OS socket buffer. The shard MUST NOT buffer acks
client-side and batch them on a timer; every `ack` produces exactly
one wire frame, synchronously.

**Why this matters.** A caller who acks immediately after processing
the message and then crashes: if the shard had buffered the ack,
the broker would redeliver, which is what we want for at-least-once
processing.

**Falsifier:** `T-REL-ACK-IMMEDIATE-001` — capture wire frames,
verify each `ack` call corresponds to exactly one `basic.ack` frame
in flight order.

---

## 7. REL-6: Topology recovery completeness

**Claim** (only meaningful for `Recovery::Full`). After a successful
recovery, every queue, exchange, and binding that was declared via
the connection's API AND not subsequently deleted is re-declared on
the fresh socket. Every consumer registered via `consume`/`subscribe`
AND not subsequently cancelled is re-installed.

**Falsifier:** `T-REL-RECOV-TOPO-001..003` — declare topology,
disconnect broker, restart broker, verify topology re-exists at
broker level (via management API).

---

## 8. REL-7: Confirm preservation across recovery

**Claim** (only meaningful for `Recovery::Full`). For every
unconfirmed in-flight publish at the moment of disconnect, the
recovery pipeline re-publishes the message and re-attaches the
caller's outcome destination. The caller's
`publish_confirm`/`publish_async` eventually resolves to the new
publish's outcome.

**Caveat — at-least-once-not-exactly-once.** The original publish
MAY have been received by the broker but its ack lost; the re-publish
delivers it again. Consumers receive both copies. This is the AMQP
at-least-once contract; the shard does not paper over it.

**Falsifier:** `T-REL-RECOV-CONFIRM-001`.

---

## 9. REL-8: Caller-close finality

**Claim.** After `Connection#close` returns, no subsequent broker
frame is processed and no fiber action is taken on behalf of this
connection. In `Recovery::Full` mode, calling `close` during
`Recovering` aborts the recovery pipeline; the connection ends in
`Closed` with caller-close semantics.

The shard MUST NOT re-open a caller-closed connection under any
circumstance.

**Falsifier:** `T-REL-CALLER-CLOSE-001..002`.

---

## 10. REL-9: Heartbeat detection bound

**Claim.** When the broker stops responding (e.g., its host
crashes), the shard detects the failure within `2 * heartbeat`
seconds of the last received frame and surfaces
`Amqp::HeartbeatTimeoutError` to all blocked fibers within an
additional 500 ms.

**Why this number.** Per `docs/10-heartbeats.md` §4, the receive
deadline is `2 * heartbeat`. The 500 ms is the fiber-wake budget.

**Falsifier:** `T-REL-HB-DETECT-001`.

---

## 11. REL-10: TLS verification enforcement

**Claim.** When `verify=peer` (default), an `amqps://` connection to
a broker whose certificate does not validate against the configured
CA, OR whose SAN/CN does not match the URI host, raises
`Amqp::TlsHandshakeError` synchronously during `Amqp.connect`.

The shard MUST NOT fall back to plaintext or skip verification on
any error.

**Falsifier:** `T-REL-TLS-001..003` (bad CA, wrong CN, expired
cert).

---

## 12. REL-11: No data race on shared `Connection`

**Claim.** `Connection` is safe for concurrent use by multiple fibers
after `connect` returns. Concurrent `conn.channel` calls from N
fibers MUST produce N distinct `Channel` objects with distinct ids,
without any race.

**Falsifier:** `T-REL-CONC-001` — spawn 100 fibers each calling
`conn.channel`; assert 100 distinct ids returned.

---

## 13. REL-12: No fiber leak across connect/close cycles

**Claim.** Running 10000 `connect`/`close` cycles on a single
process leaves the fiber count within `+/- 10` of the baseline
(scheduler + stdlib fibers may fluctuate).

**Falsifier:** `T-REL-LEAK-001`.

---

## 14. The "no doubt" rule

The shard's exceptions MUST NOT leave the caller in ambiguity about
commit status, EXCEPT for `PublishTimeoutError`, which represents
the caller's chosen timeout window. All other publish-related
exceptions classify the outcome:

- `PublishNackError` → not committed.
- `PublishReturnedError` → accepted but unroutable; if the consumer
  side expects to see this message, treat as not committed.
- `ChannelError` mid-publish → unknown (publish may or may not have
  reached the broker before the channel died).
- `ConnectionError` mid-publish → unknown.
- `PublishOutOfOrderError` → protocol violation; the connection is
  closed; treat all in-flight publishes as unknown.

The "unknown" outcomes are bounded by the caller's confirms-with-
mandatory pattern and idempotent consumers (`docs/12-recovery.md`
§6). The shard cannot eliminate at-least-once duplication; it only
ensures the ambiguity is correctly typed.

---

## 15. Anti-patterns

- **Treating `PublishTimeoutError` as "didn't send."** The publish
  MAY have been committed. Treat as "unknown."
- **Treating `publish` (fire-and-forget) on a confirms channel as
  reliable.** It's not — the caller doesn't see the outcome. Use
  `publish_confirm` or `publish_async` for at-least-once.
- **Catching `Amqp::Error` and continuing without re-checking
  `closed?`.** The exception MAY have closed the connection
  (`ConnectionError`) or just the channel (`ChannelError`). Re-check
  state before continuing.
- **Mixing `Recovery::Full` with the caller's own retry loop.**
  Already noted in `docs/12-recovery.md`. Reliability claims here
  assume one recovery owner at a time.
