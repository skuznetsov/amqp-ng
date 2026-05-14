# amqp — Channel Lifecycle

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Amqp::Channel`.
> **Companions:** `docs/02-public-api.md` §4 (the surface),
> `docs/03-error-model.md` §3, §5 (errors and recoverability),
> `docs/06-connection-lifecycle.md` (the host),
> `docs/08-publisher-confirms.md`, `docs/09-consumer.md`.

This document is the **complete** normative state machine for an
`Amqp::Channel`. Each channel is owned by exactly one connection;
the connection owns the channel-id allocation and the per-channel
inbox.

---

## 1. States

| State          | Reachable from           | Meaning                                                |
|----------------|--------------------------|--------------------------------------------------------|
| `Initial`      | (constructor by Conn)    | Object exists; `channel.open` not yet sent.            |
| `Opening`      | `Initial`                | `channel.open` sent; awaiting `channel.open-ok`.       |
| `Open`         | `Opening`                | Normal operation.                                      |
| `Confirms`     | `Open`                   | Publisher confirms enabled (also normal operation).    |
| `Flowing`      | `Open` or `Confirms`     | RabbitMQ `channel.flow` toggled flow off.              |
| `Closing`      | any of the above         | Channel close in progress.                             |
| `Closed`       | `Closing`, any error     | Terminal. `close_reason` populated.                    |

`Confirms` is a property bit on top of `Open`, not a separate state in
the strict sense; the implementation MAY model it as a flag on `Open`.
`Flowing` is similar — a flag on `Open`/`Confirms`. The state-table
above treats them distinctly only for the falsifier matrix.

### 1.1 Transition diagram

```
   ┌─────────┐
   │ Initial │
   └────┬────┘
        │ Connection#channel allocates id, sends channel.open
        ▼
   ┌─────────┐  open-ok rx     ┌──────┐  confirm.select-ok rx  ┌──────────┐
   │ Opening │────────────────►│ Open │───────────────────────►│ Confirms │
   └────┬────┘                 └──┬───┘                        └────┬─────┘
        │ open-ok timeout/error   │ channel.flow(false)             │
        ▼                         │                                 │
   ┌─────────┐                    ▼                                 │
   │  Closed │◄────────────  ┌─────────┐                            │
   └─────────┘   any close   │ Flowing │◄──── (also from Confirms)──┘
                             └────┬────┘
                                  │ channel.flow(true) → back to Open/Confirms
                                  ▼
                             (back to Open or Confirms)
```

Closing pathway omitted from the diagram; see §6.

---

## 2. Allocation

A channel is created exclusively via `Connection#channel` or
`Connection#channel(id)`. The host connection is responsible for:

1. Picking or accepting the channel-id (`docs/06-connection-lifecycle.md`
   §10).
2. Creating the per-channel inbox `::Channel(Frame)` with bounded
   capacity (implementation-defined; default 32 frames).
3. Constructing the `Channel` object referencing the connection, id,
   and inbox.
4. Sending `channel.open(reserved="")` on the wire and synchronously
   awaiting `channel.open-ok` on the inbox.
5. Returning the `Channel` in state `Open`.

If `channel.open-ok` does not arrive within `connection.heartbeat`
(or 5 seconds if heartbeat is 0), the shard MUST raise
`Amqp::ChannelError` (no specific subclass — the broker is just slow)
and de-allocate the id.

`Channel#id` MUST return the id assigned at allocation; this field is
immutable.

**Falsifier:** T-CHAN-OPEN-001 (happy path), T-CHAN-OPEN-002 (open
timeout).

---

## 3. Steady state

In `Open`, `Confirms`, or `Flowing`, the channel supports the
operations enumerated in `docs/02-public-api.md` §4.

### 3.1 Inbox draining

The frame-reader fiber on the host connection routes inbound frames
to the channel's inbox. The channel itself does NOT own a fiber by
default; user code reading from `Subscription#receive` or blocking on
`publish_confirm` drives draining.

A channel has at most one "method continuation" outstanding at a
time. When a fiber blocks on `publish_confirm`, `queue.declare-ok`,
`basic.get-ok/get-empty`, etc., it owns the inbox until its reply
arrives. The implementation MUST enforce this via a per-channel
state lock; concurrent state-changing operations MUST raise
`Amqp::ConcurrencyError`.

**Falsifier:** T-CHAN-CONCURRENCY-001 — two fibers call
`queue_declare` on the same channel simultaneously; the second
raises `ConcurrencyError`.

### 3.2 Confirms enablement

`Channel#confirm_select` sends `confirm.select(nowait=false)` and
awaits `confirm.select-ok`. The shard MUST NOT use `nowait=true`
even though AMQP allows it — the round-trip ensures the channel's
state flag is updated only after the broker has acknowledged.

The transition is irreversible: a `Confirms` channel cannot revert
to `Open`. Per AMQP 0-9-1 §1.10.2.1 the broker rejects the attempt;
the shard does not even expose a method that could request it.

**Falsifier:** T-CHAN-CONFIRMS-001.

### 3.3 Flow control

The broker MAY send `channel.flow(active=false)` to ask the client to
pause publishing. The shard MUST:

1. Update the channel state to `Flowing`.
2. Send `channel.flow-ok(active=false)` immediately.
3. Block ALL subsequent `publish*` calls on a wakeup channel until
   `channel.flow(active=true)` arrives.
4. On `channel.flow(active=true)`, send `channel.flow-ok(active=true)`,
   update state back, wake blocked publishers.

The block IS visible to callers as latency. The shard MUST update
`ChannelStats#flow_paused?` so observability is possible.

RabbitMQ 3.x has effectively deprecated `channel.flow` (it uses TCP
backpressure instead), but LavinMQ may use it; the shard MUST handle
both correctly.

**Falsifier:** T-CHAN-FLOW-001 — synthetic broker sends `flow(false)`;
verify publishes block; sends `flow(true)`; verify publishes resume.

---

## 4. Per-channel state owned by `Channel`

Each `Channel` instance owns:

- `id : UInt16`
- `state : State` (atomic; see §1)
- `confirm_tracker` (only when `Confirms`): a map of
  `delivery_tag → Outcome::Channel` plus the next-tag counter.
- `consumer_registry`: a map of `consumer_tag → Subscription` for
  active consumers on this channel.
- `prefetch`: the last value sent via `basic.qos`.
- `stats : Atomic-backed ChannelStats counters`.
- `inbox : ::Channel(Frame)` — owned by the channel but allocated by
  the connection.

The `Channel` MUST NOT hold a reference to any user-supplied object
beyond what the public API mandates (Subscriptions, callbacks). In
particular, `Channel` MUST NOT capture closures that outlive the
channel's lifetime.

---

## 5. Channel-level error: broker-initiated `channel.close`

When the broker rejects an operation, it sends `channel.close` with a
reply-code (the matrix in `docs/03-error-model.md` §3 enumerates
them), reply-text, and offending `class-id`/`method-id`.

The shard MUST:

1. Atomically transition channel state to `Closing`.
2. Send `channel.close-ok`.
3. Drain the inbox; deliveries already on it are discarded (the
   consumer is dead — the broker won't requeue them automatically,
   but it WILL re-queue any unacked deliveries when the channel
   closes, per AMQP semantics).
4. Wake every blocked fiber on this channel with the appropriate
   exception subclass (per the reply-code mapping).
5. Cancel every active Subscription on this channel; their
   `closed?` flag becomes `true` and pending `receive` calls raise
   `Amqp::SubscriptionClosed` (which itself carries the
   `Amqp::ChannelError` as `cause`).
6. Transition `Closing → Closed`; populate `close_reason`.
7. Deallocate the channel-id with the host connection (recovery may
   reuse it).

The connection is NOT closed by a channel-level error; only this
channel dies. Other channels on the same connection continue.

`Recovery::Full` re-opens the channel and re-installs its topology
and consumers (see `docs/12-recovery.md`); the user-visible
`Channel` object's id MAY change as part of recovery, which is why
the public API exposes id but does not encourage callers to depend
on its stability.

**Falsifier:** T-CHAN-BROKERCLOSE-001..N (one per reply-code in the
matrix).

---

## 6. Channel-level close: caller-initiated

`Channel#close(reply_code:, reply_text:)` MUST:

1. Atomically transition `Open` (or `Confirms` or `Flowing`) →
   `Closing`. If already non-`Open`, return silently.
2. If the host connection is `Closed`, transition directly to `Closed`
   with `close_reason.origin == Caller` and return silently.
3. Send `channel.close(reply_code, reply_text, class_id=0, method_id=0)`.
4. Wait up to `connection.heartbeat` (or 5 s if 0) for
   `channel.close-ok`.
5. Wake blocked fibers with `Amqp::ChannelClosedByCaller`.
6. Cancel Subscriptions as in §5.
7. Transition `Closing → Closed`; populate `close_reason`.
8. Deallocate the id.

The caller's `close` returns normally even on `close-ok` timeout
(the channel is dead either way).

**Falsifier:** T-CHAN-CALLERCLOSE-001.

---

## 7. Conditional close on connection death

When the host connection enters `Closed` (any pathway), every
`Channel` on it MUST transition to `Closed` synchronously as part of
the connection-close cleanup. The channel's `close_reason.origin`
inherits the connection's origin (`Broker`, `Network`, `Heartbeat`,
`Caller`).

Pending operations on these channels MUST be woken with the matching
exception subclass.

**Falsifier:** T-CHAN-CONNDIES-001..004.

---

## 8. Operations table

This is the normative authority for which AMQP method each public
method emits and what reply it awaits. The "Awaits" column is the
reply the calling fiber blocks on; "None" means the call returns
after the outgoing frames are flushed.

| Public method                  | AMQP send                | Awaits                    |
|--------------------------------|--------------------------|---------------------------|
| `Channel#confirm_select`       | `confirm.select`         | `confirm.select-ok`       |
| `Channel#prefetch`             | `basic.qos`              | `basic.qos-ok`            |
| `Channel#publish`              | `basic.publish` + header + body[*] | None              |
| `Channel#publish_confirm`      | `basic.publish` + frames | `basic.ack`/`basic.nack` for tag |
| `Channel#publish_async`        | `basic.publish` + frames | None (outcome on Channel) |
| `Channel#consume`              | `basic.consume`          | `basic.consume-ok`        |
| `Channel#subscribe`            | `basic.consume`          | `basic.consume-ok`        |
| `Subscription#close`           | `basic.cancel`           | `basic.cancel-ok` + drain |
| `Channel#get`                  | `basic.get`              | `basic.get-ok` / `get-empty` |
| `Channel#ack`                  | `basic.ack`              | None                      |
| `Channel#nack`                 | `basic.nack`             | None                      |
| `Channel#reject`               | `basic.reject`           | None                      |
| `Channel#queue_declare`        | `queue.declare`          | `queue.declare-ok`        |
| `Channel#queue_delete`         | `queue.delete`           | `queue.delete-ok`         |
| `Channel#queue_bind`           | `queue.bind`             | `queue.bind-ok`           |
| `Channel#queue_unbind`         | `queue.unbind`           | `queue.unbind-ok`         |
| `Channel#queue_purge`          | `queue.purge`            | `queue.purge-ok`          |
| `Channel#exchange_declare`     | `exchange.declare`       | `exchange.declare-ok`     |
| `Channel#exchange_delete`      | `exchange.delete`        | `exchange.delete-ok`      |
| `Channel#exchange_bind`        | `exchange.bind`          | `exchange.bind-ok`        |
| `Channel#exchange_unbind`      | `exchange.unbind`        | `exchange.unbind-ok`      |
| `Channel#close`                | `channel.close`          | `channel.close-ok`        |

`[*]`: A `basic.publish` is a method frame followed by exactly one
header frame and zero or more body frames. The header frame carries
the body's total byte length and the properties. Body frames each
carry at most `frame_max - 8` bytes. The three-frame minimum
(method + header + body) MUST be flushed atomically — the shard MUST
hold the connection's write mutex for the entire sequence so that
publishes from other channels do not interleave.

**Falsifier:** T-CHAN-ATOMIC-PUBLISH-001 — concurrent publishes on
two channels never interleave bytes within a single message's
three-frame sequence.

---

## 9. Subscriptions on a closing channel

When the channel enters `Closing` (any cause), every active
Subscription MUST:

1. Have `closed?` set to `true`.
2. Have its internal `::Channel(DeliverMessage)` closed AFTER all
   in-flight deliveries already routed to it are drained, OR
   immediately if the channel's close was due to a broker-side
   error (in which case the in-flight deliveries are no longer
   ackable and are dropped).

Specifically:

- **Caller-initiated channel close.** Drain the subscription's inbox
  first, then close. Deliveries the user has already received but not
  yet acked are no longer ackable; the broker will requeue them when
  the channel closes.
- **Broker-initiated channel close (error).** Discard the subscription's
  inbox; deliveries on it have not been "delivered" from the broker's
  perspective until they are acked, so requeue is the broker's
  responsibility.

This asymmetry is intentional: caller closes are graceful, broker
closes are abrupt.

**Falsifier:** T-CHAN-SUB-CLOSE-001 (graceful), T-CHAN-SUB-CLOSE-002
(broker-abort).

---

## 10. Confirm tracker lifecycle

When a channel enters `Confirms`, the implementation creates a
confirm tracker. The tracker MUST:

1. Issue monotonically increasing `UInt64` delivery tags starting at 1.
   Tag 0 is reserved and MUST NOT be issued.
2. On every `publish_confirm` and `publish_async`, atomically:
   a. Increment the next-tag counter.
   b. Register the outcome destination (a one-shot `::Channel(ConfirmOutcome)`)
      under the new tag.
3. On inbound `basic.ack(delivery_tag, multiple)`:
   - If `multiple == false`, send `Ack` to the registered destination
     for `delivery_tag` exactly once and remove the entry.
   - If `multiple == true`, send `Ack` to every registered destination
     with tag `<= delivery_tag` and remove them, in monotonic order.
4. On inbound `basic.nack(delivery_tag, multiple, requeue)`:
   - Same as ack but send `Nack`.
5. On inbound `basic.return` followed by header+body: parse the
   returned properties to find the `message-id` or, lacking that,
   correlate by the most recent `publish_confirm` on this channel
   that used `mandatory: true`. Send `Returned` to that destination.
   **(See §10.1 for the correlation rule.)**

When the channel enters `Closing`, every still-pending destination
MUST receive `ConfirmOutcome` reflecting the close (origin: Caller →
no outcome, the caller's fiber sees `ChannelClosedByCaller`; origin:
Broker → likewise; the shard does NOT synthesise an `Ack` or `Nack`
on close).

The tracker MUST be implemented with a data structure supporting
range removal efficiently (e.g., sorted dictionary indexed by tag).
The hot path is single-tag ack/nack and range ack/nack; both MUST be
O(k) where k is the number of tags removed.

### 10.1 `basic.return` correlation

AMQP 0-9-1 does NOT carry the delivery-tag in `basic.return`; it
carries the reply-code, reply-text, exchange, routing-key, and the
returned message's header and body. RabbitMQ's documented behavior
on publisher-confirms-with-mandatory is to send `basic.return`
followed by `basic.ack` for the same logical message (or `basic.nack`
in some edge cases).

The shard MUST correlate by **send order**: `basic.return` applies
to the publish whose `basic.ack`/`basic.nack` is next in tag order
AND that was issued with `mandatory: true`. The implementation maps
this to:

1. Maintain a queue of `{tag, mandatory?}` per channel in publish
   order.
2. On inbound `basic.return`, scan the queue for the first
   `mandatory? == true` entry whose tag has not yet been resolved;
   record the `ReturnReason`.
3. When the matching `basic.ack` (or `basic.nack`) arrives, emit
   `ConfirmOutcome{kind: Returned, return_reason: <recorded>}` to
   the destination, instead of `Ack`/`Nack`.

This correlation is the trickiest single mechanism in the shard and
warrants direct falsifier coverage.

**Falsifier:** T-PUB-RETURN-001..004 (full coverage in
`docs/08-publisher-confirms.md`).

---

## 11. Anti-patterns

- **Sharing a `Channel` across fibers for unrelated operations.** The
  shard enforces this with `Amqp::ConcurrencyError`. Use one channel
  per producer/consumer fiber, or pass deliveries via Subscriptions.
- **Catching `Amqp::ChannelError` and continuing to use the channel.**
  After a `ChannelError` the channel is `Closed`. Acquire a fresh
  channel via `conn.channel`.
- **Calling `Channel#confirm_select` from multiple fibers concurrently.**
  The first call wins; the second sees `Confirms` already enabled
  and is a no-op, BUT the act of checking-then-deciding is itself a
  state mutation — protect with the same per-channel state lock as
  other operations.
- **Holding a `Channel` reference past `Closed`.** The shard does not
  re-animate a `Closed` channel even under recovery (recovery
  re-opens a new channel under the same `Channel` object reference,
  but the id may change). Callers MUST NOT cache the id.
