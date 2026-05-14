# amqp — Consumer Path

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Channel#consume` / `Channel#subscribe`
> / `Channel#get` and the `Subscription` value type.
> **Companions:** `docs/02-public-api.md` §4.3..§4.6,
> `docs/03-error-model.md`, `docs/07-channel-lifecycle.md` §4..§9,
> `docs/15-reliability-contract.md`.

This document is the **complete** normative specification for the
consumer-side surface. Every public method that delivers messages
from the broker to user code is covered here.

---

## 1. Surface summary

Three consumption styles (`docs/02-public-api.md` §4):

| Style          | Method                          | Owns the fiber?               |
|----------------|---------------------------------|-------------------------------|
| Block form     | `Channel#consume(queue) { ... }`| Caller's fiber blocks         |
| Object form    | `Channel#subscribe(queue)`      | Caller chooses (Subscription) |
| Synchronous    | `Channel#get(queue)`            | Caller's fiber, single shot   |

The block form is the simplest correct path for a worker fiber. The
object form returns a `Subscription` for callers who need `select`
or multi-queue fan-in. The synchronous form is a pull-style poll.

---

## 2. Block form: `Channel#consume`

```crystal
ch.consume(queue,
           consumer_tag: "",
           auto_ack: false,
           exclusive: false,
           no_local: false,
           arguments: nil) do |msg : DeliverMessage|
  # process msg
  ch.ack(msg.delivery_tag) unless auto_ack
end
```

### 2.1 Wire steps

1. Acquire the per-channel state lock.
2. Send `basic.consume(reserved=0, queue, consumer_tag, no_local,
   no_ack=auto_ack, exclusive, no_wait=false, arguments=arguments)`.
3. Synchronously await `basic.consume-ok` carrying the assigned
   `consumer_tag`. If the caller supplied `consumer_tag == ""` the
   broker assigns one; the shard MUST capture it for use in `basic.cancel`.
4. Register the consumer in `Channel.consumer_registry` keyed by the
   resolved consumer tag, pointing to an inline buffer for inbound
   deliveries.
5. Release the per-channel state lock.

After step 5 the shard enters the **delivery loop** described in §2.2.

If step 3 raises (broker error, channel close), the shard MUST surface
the exception and NOT enter the loop.

### 2.2 Delivery loop

The block form runs on the caller's fiber. The shard MUST:

```crystal
loop do
  delivery = receive_next_delivery   # blocks on per-consumer inbox
  break if delivery.is_a?(EndSentinel)
  begin
    yield delivery
    ch.ack(delivery.delivery_tag) if auto_ack
  rescue ex
    handle_block_exception(ex, delivery)
  end
end
```

Where:

- `receive_next_delivery` waits for the next `basic.deliver` from the
  broker for this consumer tag. The frame-reader routes deliveries to
  this consumer's inbox.
- `EndSentinel` is the shard's internal token signalling consumer
  cancellation (caller-side, broker-side via `basic.cancel`, or
  channel close).
- `handle_block_exception`:
  - If `auto_ack == true`: the message has already been auto-acked by
    the broker (no-ack consumer); the shard cannot reject. Re-raise
    the exception to the caller of `consume`.
  - If `auto_ack == false`: issue `basic.reject(delivery_tag,
    requeue=true)`; re-raise to the caller.

The caller's `consume` returns normally on graceful cancellation
(broker-side or channel close) and raises the underlying error on
exception escape from the block.

### 2.3 Auto-ack semantics

`auto_ack: true` translates to AMQP's `no-ack` flag on `basic.consume`,
which means the broker considers each message acknowledged **as soon
as it is dispatched**, not after the caller has processed it. The
caller does NOT call `ack` explicitly; the shard MUST NOT either.

Consequence: with `auto_ack: true`, exceptions from the block result
in message loss from the broker's perspective. The shard MUST log at
`Log::Severity::Warning` when a block exception occurs in
`auto_ack: true` mode (the caller likely did not mean to lose the
message).

### 2.4 `exclusive: true`

Translates to AMQP's `exclusive` flag. The broker MUST refuse to
register a second consumer on the queue while this consumer is
active; refusal surfaces as `Amqp::ChannelError` with reply-code 403.

### 2.5 `no_local: true`

Translates to AMQP's `no-local` flag. RabbitMQ ignores this flag in
practice (the underlying AMQP semantics are not implemented). The
shard MUST pass it through unchanged; behavior is the broker's
problem.

### 2.6 `arguments`

Carried in the `arguments` field-table on `basic.consume`. Used for
RabbitMQ-specific features like `x-priority`, `x-cancel-on-ha-failover`,
and consumer-side message TTL flags. The shard does not interpret
these; it forwards them.

**Falsifier:** T-CONS-BLOCK-001 (happy path), T-CONS-BLOCK-002
(block exception with auto_ack false → reject), T-CONS-BLOCK-003
(exclusive collision → ChannelError), T-CONS-BLOCK-004
(arguments forwarded).

---

## 3. Object form: `Channel#subscribe`

```crystal
sub = ch.subscribe(queue, consumer_tag: "", auto_ack: false, ...,
                   buffer: 16)
```

### 3.1 Wire steps

Identical to §2.1 through step 4. Step 5 returns a `Subscription`
instead of entering the delivery loop.

The `Subscription` wraps a `::Channel(DeliverMessage)` of capacity
`buffer`. Frame-router writes deliveries into it.

### 3.2 Subscription API

Per `docs/02-public-api.md` §5:

```crystal
sub.receive          # blocks; raises Amqp::SubscriptionClosed on end
sub.receive?         # returns nil instead of raising
sub.closed?
sub.close            # sends basic.cancel, drains inbox
sub.each { |m| ... } # iterates until close
sub.spawn_loop { |m| ... }  # spawns a fiber that runs each
sub.stats
```

### 3.3 `receive` and `select` integration

`Subscription#receive` MUST be implementable as the receive side of
Crystal's `select`. The implementation strategy: `Subscription`
internally exposes a `::Channel(DeliverMessage)` via a method like
`internal_channel` (private to the shard), and `Subscription#receive`
delegates to it. Crystal's `select` looks at the wrapped channel
through that delegation.

The signature MUST work in this exact form:

```crystal
select
when msg = sub_a.receive
  handle_a(msg)
when msg = sub_b.receive
  handle_b(msg)
when timeout(5.seconds)
  idle
end
```

Per Crystal's `select` mechanics, `sub.receive` is recognised if it
returns a value that compiles in the `select` arm context. The
shard's `Subscription` MUST be a class with a `receive` method whose
shape matches `::Channel(T)#receive`.

A concrete implementation strategy is to make `Subscription` inherit
from or compose with `::Channel(DeliverMessage)`. The simplest correct
shape: `Subscription` exposes a `::Channel(DeliverMessage)` directly
(`sub.channel` is the `::Channel`, NOT the AMQP channel) and the
public `Subscription#receive` is sugar over `sub.channel.receive`.
The `channel` accessor and the inheritance choice are implementation
details; the falsifier checks only that `select when ... = sub.receive`
compiles and runs.

**Falsifier:** T-CONS-SELECT-001 — code sample from §3.3 compiles and
delivers messages.

### 3.4 Backpressure

When the subscription's inbox is full, the frame-reader fiber blocks
on `inbox.send(delivery)`. This blocks the frame-reader for ALL
channels on the connection. To mitigate, the shard MUST:

- Default `buffer` to a small value (16) — high-throughput consumers
  set this explicitly per their workload.
- Document this clearly: a slow consumer on one channel slows the
  whole connection. This is intentional — the shard does not
  silently drop deliveries (P-7).

The recommendation in `docs/13-broker-compat-matrix.md` §X is to give
each high-throughput consumer its own `Amqp::Connection`. The shard
itself does NOT enforce this.

**Falsifier:** T-CONS-BACKPRESSURE-001 — a slow subscription blocks
the frame-reader; metrics confirm; speeding up the subscription
releases it.

### 3.5 Subscription close

`Subscription#close`:

1. If already `closed?`, return silently.
2. Acquire the per-channel state lock.
3. Send `basic.cancel(consumer_tag, no_wait=false)`.
4. Await `basic.cancel-ok`.
5. Drain the inbox (the broker may have sent deliveries that arrived
   before `basic.cancel` was processed). These deliveries are still
   ackable; the caller may receive them via `receive?` until the
   inbox is empty.
6. Close the inbox channel.
7. Set `closed? = true`.
8. Unregister from `Channel.consumer_registry`.
9. Release the per-channel state lock.

After close, `receive` raises `Amqp::SubscriptionClosed` (a normal
terminator, NOT an error in the `Amqp::Error` recoverable sense).
`receive?` returns `nil`.

### 3.6 Broker-side cancel (`basic.cancel` from the broker)

The broker MAY send `basic.cancel` unsolicited (e.g., queue deleted
while consumer active, if the consumer registered with the
`x-cancel-on-ha-failover` argument). The shard MUST:

1. Send `basic.cancel-ok` if the broker did not set `no-wait`.
2. Drain pending deliveries from the inbox (as in §3.5 step 5).
3. Close the inbox.
4. Set `closed? = true`.
5. Unregister from the channel's consumer registry.
6. (No state lock needed for this path; it runs on the frame-reader
   fiber, and reading the consumer registry uses an internal mutex
   for that map's entries.)

The next `Subscription#receive` raises `Amqp::SubscriptionClosed`
with `cause = Amqp::BrokerCanceledError` (an internal subclass — NOT
listed in the public hierarchy because user code rarely needs to
distinguish; if a use case appears, it gets promoted).

**Falsifier:** T-CONS-CANCEL-001 (caller cancel), T-CONS-CANCEL-002
(broker cancel), T-CONS-CANCEL-003 (drain after cancel).

### 3.7 `spawn_loop`

```crystal
sub.spawn_loop do |msg|
  process(msg)
  ch.ack(msg.delivery_tag) unless auto_ack
end
```

Equivalent to:

```crystal
spawn do
  sub.each do |msg|
    begin
      yield msg
      ch.ack(msg.delivery_tag) unless auto_ack
    rescue ex
      ch.reject(msg.delivery_tag, requeue: true) unless auto_ack
      raise ex
    end
  end
end
```

The shard MAY use a more efficient implementation; the semantics
above are normative. The spawned fiber owns the subscription's
lifetime: when the block raises, the fiber terminates and the
subscription remains `Open` (the caller can still read from it from
another fiber); when the subscription closes, the spawned fiber
exits cleanly.

**Falsifier:** T-CONS-SPAWN-001.

### 3.8 `Subscription#stats`

```crystal
struct Amqp::SubscriptionStats
  getter deliveries_received : UInt64
  getter buffer_depth : Int32           -- current
  getter buffer_high_water : Int32      -- ever-observed peak
  getter buffer_capacity : Int32        -- the `buffer:` arg
end
```

All counters are atomic. The high-water mark MUST be updated whenever
a delivery is enqueued AND the resulting depth exceeds the previous
high-water value.

---

## 4. Synchronous get: `Channel#get`

```crystal
ch.get(queue, auto_ack: false) : GetMessage?
```

### 4.1 Wire steps

1. Acquire the per-channel state lock.
2. Send `basic.get(reserved=0, queue, no_ack=auto_ack)`.
3. Read the next inbound method frame on this channel:
   - `basic.get-ok` → read the following header + body frames,
     construct `GetMessage`, return it.
   - `basic.get-empty` → return `nil`.
   - `channel.close` → release lock, raise the appropriate
     `Amqp::ChannelError` subclass.
4. Release the state lock.

### 4.2 Race with active consumers

A channel that already has an active `basic.consume` subscription
SHOULD NOT use `basic.get` on the same queue — the deliveries
interleave in an order the application cannot easily predict.
The shard MUST NOT enforce this — both calls are legal AMQP —
but it SHOULD log at `Log::Severity::Debug` when `get` is called
while at least one consumer on the same channel is active for
the same queue. The log message helps diagnose surprises; it does
not change behavior.

### 4.3 No buffering

`get` does NOT pre-fetch. Each call results in one round-trip to
the broker. Callers that need throughput MUST use `consume`/
`subscribe`.

**Falsifier:** T-CONS-GET-001 (ok), T-CONS-GET-002 (empty),
T-CONS-GET-003 (channel error mid-get).

---

## 5. Acknowledgement methods

```crystal
ch.ack(delivery_tag, multiple: false)
ch.nack(delivery_tag, multiple: false, requeue: true)
ch.reject(delivery_tag, requeue: true)
```

### 5.1 Semantics

These methods write a single method frame and return; no reply is
awaited (AMQP defines no `*-ok` for them). The implementation MUST
NOT track outstanding tags client-side — the broker is the source
of truth.

- `ack`: positive acknowledgement.
- `nack` with `requeue: true`: negative, requeue the message.
- `nack` with `requeue: false`: negative, drop (or DLX-route per
  queue policy).
- `reject` with `requeue: true`: same as `nack(false, true)`. Legacy.
- `reject` with `requeue: false`: same as `nack(false, false)`. Legacy.

The shard exposes both `nack` and `reject` because RabbitMQ
documentation distinguishes them; the `multiple:` flag is only on
`nack` (AMQP doesn't define `multiple` on `reject`).

### 5.2 Error surface

Calling `ack`/`nack`/`reject` with an unknown delivery tag does NOT
fail synchronously. The broker responds by closing the channel with
reply-code 406 (PRECONDITION_FAILED). The shard surfaces this as
`Amqp::PrechargeError` on the NEXT blocked operation on this
channel; the failing `ack` itself has already returned.

This is the AMQP-defined behavior; the shard cannot improve on it
without tracking acks client-side, which it deliberately does not.

**Falsifier:** T-CONS-ACK-001..006.

---

## 6. Prefetch (QoS)

```crystal
ch.prefetch(count : UInt16, global : Bool = false)
```

Translates to `basic.qos(prefetch-size=0, prefetch-count=count,
global=global)`. The shard MUST NOT set `prefetch-size` (the
byte-based prefetch is not supported by RabbitMQ).

`global: true` applies the prefetch to all consumers on the channel
(RabbitMQ's interpretation, which LavinMQ matches); `global: false`
applies per-consumer.

The call awaits `basic.qos-ok`.

**Falsifier:** T-CONS-PREFETCH-001..002.

---

## 7. DeliverMessage and GetMessage construction

The shard receives a `basic.deliver` (or `basic.get-ok`) method frame
followed by exactly one header frame and zero or more body frames.
The implementation MUST:

1. Validate the header's `class-id == 60` (basic) and `weight == 0`;
   reject with `Amqp::ProtocolError` otherwise.
2. Read body frames until the cumulative byte count equals the
   header's `body-size`. Receiving fewer (channel closes mid-body)
   means the message is incomplete and the partial delivery is
   discarded; receiving more is a protocol violation.
3. Construct `DeliverMessage`/`GetMessage` with the body assembled,
   properties decoded, and the method-frame-supplied fields
   (`delivery_tag`, `redelivered`, `exchange`, `routing_key`,
   `consumer_tag` or `message_count`).
4. Route to the consumer or return synchronously.

The body assembly MUST be done into a single `Bytes` allocation
sized exactly to `body-size`. Implementations MAY pre-allocate once
the header is parsed and stream-copy from each body frame into the
target buffer (no intermediate `IO::Memory`).

**Falsifier:** T-CONS-ASSEMBLE-001..003.

---

## 8. Anti-patterns

- **`consume` inside `select`.** The block-form `consume` is a
  blocking loop. To use `select`, use `subscribe` and `sub.receive`.
- **Concurrent `consume` on the same channel.** The state lock
  prevents the *second registration*, but if you genuinely want
  N parallel consumers, allocate N channels. Within one channel
  the deliveries are serialised in arrival order anyway.
- **Acking from a different fiber than the one that received the
  delivery.** Legal but error-prone — the receiving fiber may
  re-iterate and lose track of pending tags. Prefer either
  block-form `consume` (where the structure is enforced) or careful
  passing of the `DeliverMessage` to the acking fiber.
- **`auto_ack: true` with non-trivial processing.** If the block
  exception is observable to the caller, the message is already
  acked and lost. Use `auto_ack: false` for anything that could
  fail.
- **High `buffer` on a Subscription you never read.** Buffered
  deliveries occupy memory; the broker considers them not yet
  delivered until they are acked (so they don't expire), but they
  count against the consumer's prefetch. A buffer of 1000 with
  prefetch of 1 is a configuration error: the buffer never fills.
