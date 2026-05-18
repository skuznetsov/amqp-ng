# amqp — Publisher Confirms

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Channel#publish*`; callers needing
> at-least-once guarantees.
> **Companions:** `docs/01-design-principles.md` (P-3 raises, P-7
> reliability is binary), `docs/02-public-api.md` §4.2 (signatures),
> `docs/03-error-model.md` §8 (the "no doubt" rule),
> `docs/07-channel-lifecycle.md` §10 (confirm tracker).

Publisher confirms are the mechanism by which RabbitMQ and LavinMQ
acknowledge each published message individually, providing at-least-
once delivery semantics without requiring channel-level transactions
(which are slow and rarely used). This document specifies the shard's
implementation in normative detail.

---

## 1. Background (informative)

AMQP 0-9-1 ships with `tx.*` transactions for atomic publishing.
RabbitMQ deemed them too slow and introduced publisher confirms in
2.4.x (2011), which became the de facto standard. Both target brokers
implement them identically per the RabbitMQ wire contract: after
`confirm.select-ok`, every `basic.publish` is assigned an internal
delivery-tag (monotonically increasing within the channel), and the
broker sends exactly one `basic.ack(tag)` or `basic.nack(tag)` per
publish — possibly batched via the `multiple` flag.

The shard exposes four publish APIs (`docs/02-public-api.md` §4.2):

- `publish` — no confirm wait; the caller does not need confirmation.
- `publish_batch` — publishes many messages under one write critical
  section and returns the registered delivery-tags in input order when
  confirm mode is enabled.
- `publish_confirm` — synchronous wait for ack/nack on the calling
  fiber.
- `publish_async` — returns immediately with a delivery-tag and a
  `::Channel(ConfirmOutcome)` for the outcome.

---

## 2. Enabling confirms

A channel is in confirm mode after `Channel#confirm_select` returns.
The call MUST emit `confirm.select(nowait=false)` and synchronously
await `confirm.select-ok`. Implementation rules:

- The first call transitions `Open → Confirms`. The transition is
  irreversible (`docs/07-channel-lifecycle.md` §3.2).
- The second and subsequent calls are no-ops and return immediately.
- The call MUST acquire the per-channel state lock so that concurrent
  confirm-mode transitions see a consistent state. Publishes after
  `confirm.select-ok` are correlated by delivery tag and do not use
  the method-continuation slot.

Calling `publish_confirm` or `publish_async` on a non-confirms channel
MUST raise `Amqp::ConfigurationError` BEFORE writing any bytes to the
socket.

**Falsifier:** T-PUB-MODE-001 (confirms not enabled → error),
T-PUB-MODE-002 (idempotent enable).

---

## 3. Publish frame sequence

Every publish (in any of the APIs) emits the following frames in order,
atomically under the connection's write mutex. `publish_batch` repeats
this frame sequence once per message while holding the same write
critical section for the whole batch:

1. **Method frame** (frame-type=1, channel=<channel-id>):
   `basic.publish(reserved=0, exchange, routing-key, mandatory, immediate)`.

2. **Content header frame** (frame-type=2, channel=<channel-id>):
   class-id=60 (basic), weight=0, body-size=<total bytes>, property-flags
   + property-list per `docs/05-wire-0-9-1/03-content-properties.md`.

3. **Zero or more content body frames** (frame-type=3, channel=<channel-id>):
   each body chunk at most `frame_max - 8` bytes (8 = frame header + the
   trailing `0xCE` end-byte). Empty bodies emit zero body frames; the
   header's `body-size` field is 0.

The shard MUST hold the connection-level write mutex for the entire
sequence. Atomic-publish falsifier: `T-CHAN-ATOMIC-PUBLISH-001`.

### 3.1 Batch publish confirm semantics

`publish_batch` does not create per-message outcome channels and does
not wait for broker confirms. In confirm mode it returns an
`Array(UInt64?)` containing the registered publish sequence for each
input message; outside confirm mode it returns an equally sized array
of `nil`.

Callers who want a batch confirmation barrier call
`wait_for_confirms` after `publish_batch`. Callers who need individual
ack/nack/return outcomes use `publish_async` for those messages.

`publish_confirm_batch` packages the common windowed pattern:
publish at most `window_size` messages with `publish_batch`, call
`wait_for_confirms`, then advance to the next window. This keeps the
same tracker semantics as `publish_batch` and `wait_for_confirms`, but
turns one round trip per message into one confirm barrier per window.
It returns `false` if any window sees a nack or timeout. The default
window is 500 messages; callers can lower it for tighter memory or
latency bounds, or raise it when broker/network behavior supports
larger outstanding confirm sets.

In `Recovery::None`, pending confirm entries retain only routing and
outcome metadata. In `Recovery::Full`, pending confirm entries also
retain replay payloads until the broker settles them, because recovery
may need to republish unconfirmed messages after reconnect.

### 3.2 Properties encoding

Properties present on the `Amqp::Properties` value object are
encoded via the property-flags bitmap (see
`docs/05-wire-0-9-1/03-content-properties.md`). Properties whose
field is `nil` MUST be omitted from the encoded property list (the
flag bit is 0); fields whose field is non-`nil` MUST have the flag
bit set AND the value encoded.

`persistence: Persistent` encodes `delivery-mode = 2`;
`persistence: Transient` encodes `delivery-mode = 1`;
`persistence: nil` omits `delivery-mode` entirely (broker treats as
transient by default).

**Falsifier:** T-PUB-PROPS-001..N (corpus-driven).

### 3.2 Body chunking

For a body of size `N` and frame-max `F`:

- Maximum body bytes per frame: `F - 8`.
- Number of body frames: `ceil(N / (F - 8))`.

The shard MUST NOT emit body frames with zero bytes EXCEPT in the
case `N == 0`, where zero body frames are emitted (per §3 above).

**Falsifier:** T-PUB-CHUNK-001..003 (small body, exact frame boundary,
many frames).

---

## 4. `publish` (fire-and-forget)

```crystal
ch.publish(message, exchange, routing_key, mandatory: false, immediate: false)
```

- The call writes the three-frame sequence under the write mutex and
  returns. No reply is awaited.
- If the channel is in `Confirms` mode, the publish IS tracked by the
  confirm tracker but the caller does not see the outcome — the
  outcome is delivered to a sink that drops it. The shard MUST NOT
  bypass the tracker for `publish` calls on a confirms channel, because
  ack/nack frames arrive interleaved with publishes from
  `publish_confirm`/`publish_async` on the same channel; failing to
  track them would corrupt the tracker's monotonic-tag invariant.
- The call MAY block briefly on the write mutex; it MUST NOT block
  on broker reply.
- Errors: any frame-write error surfaces synchronously as
  `Amqp::SocketError` or `Amqp::ConnectionError`; the connection is
  already torn down at that point.

**Falsifier:** T-PUB-FF-001 — fire 10k publishes on a confirms channel,
all tags accounted for in the tracker; T-PUB-FF-002 — fire on
non-confirms channel, no tracker interaction.

---

## 5. `publish_confirm` (synchronous)

```crystal
ch.publish_confirm(message, exchange, routing_key,
                   mandatory: false, timeout: 30.seconds) : Bool
```

### 5.1 Steady-state happy path

1. Verify `confirms_enabled?`; else `Amqp::ConfigurationError`.
2. Under the connection write mutex:
   a. Register a sync waiter in the confirm tracker under the next
      tag before the publish frames are flushed.
   b. Write the method/header/body frame sequence without interleaving.
3. Release the connection write mutex.
4. Wait on the shared confirm wakeup channel until the handler fiber
   settles the registered tag or the timeout expires.
5. On `outcome.kind == Ack`: return `true`.
6. On `outcome.kind == Nack`: raise
   `Amqp::PublishNackError`.
7. On `outcome.kind == Returned`: raise
   `Amqp::PublishReturnedError` carrying `outcome.return_reason`.
8. On timeout: abandon only the sync waiter result slot; KEEP the
   pending confirm entry in the tracker so a later ack/nack/return can
   still clean up the tag; raise `Amqp::PublishTimeoutError`.

### 5.2 Channel/connection failures during the wait

If the channel transitions to `Closing` or `Closed` while the wait is
in progress, the confirm tracker MUST wake the fiber with the channel
or connection close exception (per `docs/03` §6). The implementation
SHOULD send a sentinel through the outcome channel rather than spin
in a busy loop.

### 5.3 `mandatory: true` interaction

When `mandatory: true` is set and the message is unroutable, the
broker sends `basic.return` BEFORE the `basic.ack`. The confirm
tracker correlates them per `docs/07-channel-lifecycle.md` §10.1 and
emits `ConfirmOutcome::Returned` to the destination. The
`publish_confirm` call then raises `Amqp::PublishReturnedError`.

A `mandatory: true` message that IS routable receives only
`basic.ack` (no `basic.return`); the call returns `true`.

**Falsifier:** T-PUB-CONFIRM-001 (ack), T-PUB-CONFIRM-002 (nack),
T-PUB-CONFIRM-003 (mandatory + unroutable → returned),
T-PUB-CONFIRM-004 (mandatory + routable → ack),
T-PUB-CONFIRM-005 (timeout, broker silent),
T-PUB-CONFIRM-006 (broker-close mid-wait → ChannelError).

---

## 6. `publish_async`

```crystal
ch.publish_async(message, exchange, routing_key, mandatory: false)
  : {UInt64, ::Channel(ConfirmOutcome)}
```

### 6.1 Semantics

1. Verify `confirms_enabled?`; else `Amqp::ConfigurationError`.
2. Acquire the per-channel state lock.
3. Under the connection write mutex:
   a. Create a fresh `::Channel(ConfirmOutcome)` with capacity 1.
   b. Register the destination in the confirm tracker under the next
      tag.
   c. Write the three-frame sequence.
4. Release both locks.
5. Return `{tag, outcome_chan}`.

The returned `::Channel` is `select`-able alongside `Subscription#receive`,
`timeout`, and other channels. The shard guarantees:

- Exactly one `ConfirmOutcome` value will be sent on `outcome_chan`,
  unless the channel/connection closes before the outcome arrives, in
  which case the `::Channel` is closed without a value.
- The caller MAY call `outcome_chan.receive?` and observe `nil` to
  detect closure-without-outcome.
- The `outcome_chan` is closed by the shard after sending the value,
  so a second `receive` returns `nil`. This is the AMQP "ack arrives
  exactly once" guarantee.

### 6.2 No outcome on caller-initiated close

If the caller closes the channel or connection before the broker
acknowledges, the `outcome_chan` is closed without a value. The
caller's `publish_async` already returned; they have no fiber to
raise into. The shard MUST NOT leak the outcome destination after
close.

### 6.3 Receiver-gone case

If the caller drops the receiving end (lets it go out of scope, or
the receiving fiber dies), the shard's send into the outcome channel
MUST NOT block the confirm tracker. The tracker uses a non-blocking
send with `capacity: 1`; if a future ack arrives and the previous
outcome was never received, the channel is full and the new send
would block — except the new send is for a DIFFERENT tag (the
previous outcome was already sent), so this case does not occur.

The risk to guard: if the channel has `capacity: 1` AND the consumer
never reads it AND the channel is sent into twice (which the shard
already forbids per §6.1), the second send blocks. The shard MUST
NOT send twice; the outcome is sent at most once.

**Falsifier:** T-PUB-ASYNC-001 (ack delivered), T-PUB-ASYNC-002
(nack delivered), T-PUB-ASYNC-003 (channel close → outcome chan
closes without value), T-PUB-ASYNC-004 (caller drops receiver,
no fiber leak).

---

## 7. Multiple-flag handling

The broker MAY send `basic.ack(delivery_tag, multiple=true)`,
meaning "ack every tag from the previous ack up to and including
delivery_tag." The shard MUST resolve all destinations in that range
in monotonic tag order. The same applies to `basic.nack`.

Range resolution algorithm:

1. Let `last_acked_tag` = highest tag previously resolved (initially 0).
2. On ack/nack with `multiple=true`:
   a. For each registered tag in `(last_acked_tag, delivery_tag]`:
      send outcome to its destination, remove from tracker.
   b. Set `last_acked_tag = delivery_tag`.
3. On ack/nack with `multiple=false`:
   a. Resolve only `delivery_tag`.
   b. Do NOT advance `last_acked_tag`.

If `delivery_tag` references an unknown tag the broker is buggy or
the connection is corrupted; the shard MUST raise
`Amqp::PublishOutOfOrderError` (which causes the connection to
close, per `docs/03` §8).

**Falsifier:** T-PUB-MULTIPLE-001 (range ack), T-PUB-MULTIPLE-002
(out-of-order tag → error).

---

## 8. Interaction with recovery

When `Recovery::Full` is active and the connection recovers, the
publish path MUST:

1. During the dead window (no socket), new `publish*` calls raise
   `Amqp::RecoveryInProgress` synchronously. The shard does not queue
   new caller work during recovery in v0.
2. After re-open, the recovery pipeline re-publishes every
   unconfirmed in-flight publish from the previous incarnation, in
   monotonic order, on a fresh channel. The destinations of those
   publishes are re-attached so that the **original caller's**
   `publish_confirm` / `publish_async` outcome eventually fires.
3. The re-published messages MAY arrive twice at the broker (the
   pre-disconnect publish might have been received but its ack
   lost). Consumers MUST be idempotent; the shard surfaces this via
   `docs/15-reliability-contract.md` REL-AT-LEAST-ONCE.

**Falsifier:** T-PUB-RECOV-001 — kill broker mid-publish, verify the
caller's `publish_confirm` eventually returns `true` (or raises if
unrecoverable).

---

## 9. Properties of the implementation

Implementations MUST satisfy:

- **Monotonic tags.** The next-tag counter is incremented under the
  write mutex; tags are strictly increasing.
- **Tracker bound.** The tracker's memory grows in proportion to
  in-flight unconfirmed publishes only. Confirmed publishes MUST be
  removed promptly.
- **Hot-path complexity.** Single-tag ack/nack is O(log N) for a
  sorted-map implementation or O(1) for a hash-based one;
  range-ack is O(k) where k is the range size. The shard's choice is
  implementation-defined.
- **Concurrency.** `publish_confirm` and non-confirm `publish` do not
  use the method-continuation slot and MAY run concurrently on the
  same channel. Their observable contract is "no torn frames" plus
  one confirm outcome per delivery tag. `publish_async` keeps the
  conservative state-lock path in v0 because it returns a caller-owned
  outcome channel.

**Falsifier:** T-PUB-MONO-001 (monotonic), T-PUB-MEM-001 (tracker
shrinks under load), T-PUB-CONC-001 (no torn frames).

---

## 10. Anti-patterns

- **Calling `publish_confirm` with `timeout: 0.seconds`.** This is a
  poll for "did the broker already ack?", which the AMQP protocol
  does not support — the call always blocks at least one network
  round-trip. The shard MUST treat `0.seconds` as "synthetic timeout
  before send" and raise `Amqp::PublishTimeoutError` immediately
  WITHOUT writing frames. (Alternative interpretations like "infinite
  wait" are too surprising.) The kw arg has no default-to-infinite
  form; if the caller wants no timeout, they pass the maximum
  representable `Time::Span`.
- **Ignoring the outcome channel.** If a caller uses `publish_async`
  and never reads from the returned `::Channel`, the channel sits
  forever holding one outcome. With `capacity: 1` this leaks
  bounded memory (one ConfirmOutcome per ignored publish). The
  shard SHOULD log at `Log::Severity::Debug` when GC reclaims an
  unread outcome channel, but it MUST NOT enforce a usage policy.
- **Mixing `publish` and `publish_confirm` and assuming `publish`
  bypasses the tracker.** It doesn't (§4); the tracker tracks every
  publish on a confirms channel.
