# amqp — Public API

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers writing the public surface of the shard;
> callers writing code against this API.
> **Companions:** `docs/01-design-principles.md` (the principles this
> API derives from), `docs/03-error-model.md` (exception hierarchy),
> `docs/04-uri-and-config.md` (the URI and option matrix this API
> consumes), `docs/06-connection-lifecycle.md` and
> `docs/07-channel-lifecycle.md` (state machines behind the calls).

This document is the **complete** v0 public API. Anything not listed
here is either internal or planned for a later milestone. The signatures
below are normative: the implementation MUST expose exactly these names,
arities, return types, and blocking semantics. Adding new public
symbols requires a doc revision and a new principle citation.

---

## 1. Namespace map

The shard installs a single top-level module `Amqp` and the following
public children. Every public type lives directly under `Amqp::*`; no
sub-namespaces (`Amqp::Client::*`, `Amqp::Codec::*`) appear in the
public API. The wire codec is internal and lives under `Amqp::Wire::*`
but is NOT part of the public surface.

```
Amqp                              -- module-level connect helpers, version constant
├── Connection                    -- one TCP/TLS connection
├── Channel                       -- one AMQP channel within a connection
├── Properties                    -- AMQP 0-9-1 basic-properties value object
├── Arguments                     -- AMQP field-table alias
├── Message                       -- bytes + properties + routing key (publish/get/deliver)
├── DeliverMessage                -- delivery received by a consumer
├── GetMessage                    -- result of synchronous get
├── Subscription                  -- object form of a consumer
├── Queue                         -- amqp-client.cr-style queue wrapper
├── Exchange                      -- amqp-client.cr-style exchange wrapper
├── ConfirmOutcome                -- async-publish outcome value
├── ReturnedMessage               -- basic.return callback payload
├── Stats                         -- connection-level counters and snapshots
├── Recovery                      -- enum: None | Full
├── Persistence                   -- alias: Properties::Persistence
├── Error                         -- root of the exception hierarchy
└── …subclasses of Error         -- see docs/03-error-model.md
```

Crystal exposes some implementation constants at top level because the
wire codec and helpers live in the same shard namespace. The supported
caller API is the list above plus the exception subclasses in
`docs/03-error-model.md`. `Amqp::Config`, `Amqp::QueueInfo`,
`Amqp::Delivery`, `Amqp::Wire::*`, and the per-method wire classes are
implementation-visible but unsupported: callers who depend on them do
so outside the v0 compatibility surface.

The shard MUST define a public constant `Amqp::VERSION : String`
returning the shard's semver string. Tooling reads this; the shard
MUST NOT also re-export the broker-protocol version (which is fixed
to `"0-9-1"`).

---

## 2. Module-level helpers `Amqp`

```crystal
module Amqp
  VERSION : String

  def self.connect(url : String | URI, *,
                   user : String? = nil,
                   password : String? = nil,
                   heartbeat : Time::Span? = nil,
                   channel_max : UInt16? = nil,
                   frame_max : UInt32? = nil,
                   connect_timeout : Time::Span = 30.seconds,
                   tls : OpenSSL::SSL::Context::Client? = nil,
                   recovery : Recovery = Recovery::None,
                   product : String = "amqp-ng",
                   information : String = ""
                  ) : Connection

  def self.connect(url : String | URI, **opts, & : Connection ->) : Nil

  def self.tls_context_default : OpenSSL::SSL::Context::Client
end
```

**Semantics.**

- The keyword arguments `user`, `password`, `heartbeat`, `channel_max`,
  `frame_max` MAY also be supplied as URI query parameters (see
  `docs/04-uri-and-config.md`); when both are present, the keyword
  argument wins. This precedence is normative.
- `connect_timeout` is a wall-clock bound on the entire handshake
  (TCP connect + TLS handshake if applicable + AMQP protocol
  negotiation through `connection.open-ok`). On exceeding the bound the
  call MUST raise `Amqp::ConnectTimeoutError`.
- `tls` MUST be `nil` for `amqp://` URIs and MAY be supplied for
  `amqps://`. `amqp://` with `tls:` MUST raise
  `Amqp::ConfigurationError` synchronously, before any socket I/O.
  `amqps://` with `tls: nil` uses `tls_context_default`.
- The block form MUST call `conn.close` on both normal block return
  and exception escape, and MUST NOT swallow the exception. The block's
  return value is discarded.
- `tls_context_default` returns a context with `TLS 1.2+`, stdlib
  default cipher suites, peer-verification enabled with the system's
  default CA store. The returned context MAY be mutated by the caller
  before being passed to `connect`.

**Falsifier:** T-API-CONNECT-001..010 cover scheme/option precedence,
the timeout, the configuration-conflict rule, and the block form's
finally semantics.

---

## 3. `Amqp::Connection`

```crystal
class Amqp::Connection
  # Lifecycle
  def close(*, reply_code : UInt16 = 200, reply_text : String = "OK") : Nil
  def closed? : Bool
  def close_reason : Exception?                 -- typed Amqp::Error where possible

  # Channels
  def channel : Channel
  def channel(id : UInt16) : Channel            -- explicit id (rare)
  def with_channel(& : Channel ->) : Nil

  # Negotiated parameters (post-handshake, read-only)
  def heartbeat : Time::Span
  def channel_max : UInt16
  def frame_max : UInt32
  def server_properties : Amqp::Arguments

  # Observability
  def stats : Amqp::Stats
  def blocked? : Bool
  def on_blocked(& : String -> Nil) : Nil
  def on_unblocked(& : -> Nil) : Nil

  # Recovery (informational; behavior in docs/12)
  def recovery_mode : Recovery
end
```

**Semantics.**

- `close` is idempotent. The first call sends `connection.close` with
  the given reply code and text, waits up to the heartbeat interval
  (or 5 seconds if heartbeat is disabled, whichever is greater) for
  `connection.close-ok`, then tears down. Subsequent calls return
  silently. Exceptions during teardown are logged but not raised.
- `closed?` becomes `true` after teardown completes for any reason
  (caller close, broker close, heartbeat timeout, socket EOF).
- `close_reason` returns `nil` while the connection is open and the
  exception that caused closure after closure. The exception is a typed
  `Amqp::Error` where the shard can classify the failure.
- `channel` allocates the next free channel id in `1..channel_max`
  inclusive (channel 0 is reserved by the protocol). Allocation MUST
  be O(1) amortised and MUST be safe to call concurrently from
  different fibers (the call serialises internally).
- `channel(id)` is for callers who require a specific channel id. On
  collision with an already-open channel the method raises
  `Amqp::ChannelLimitError`.
- `with_channel` opens a fresh channel, yields it, and closes it on
  both normal block exit and exception escape. Exception escape MUST
  preserve the original exception (no swallowing).
- `heartbeat`, `channel_max`, `frame_max`, `server_properties` are the
  values negotiated during `connection.tune` / `connection.tune-ok`.
  Reading them before `connect` returns is impossible (the call had
  not returned); reading them after `close` returns the last
  negotiated values for diagnostic use.
- `stats` returns the current v0 `Amqp::Stats` counter object; callers
  read an immutable `Stats::Snapshot` via `conn.stats.snapshot`.
- `recovery_mode` reflects the configured mode. Recovery callbacks and
  rich recovery-event values are not v0 public API.

**Concurrency.** `Connection` is safe to share across fibers after
`connect` returns. The connection-level write mutex serialises frame
writes (one writer at a time) but does NOT serialise high-level
operations on different channels.

**Falsifier:** T-CONN-* (see `docs/06-connection-lifecycle.md`).

---

## 4. `Amqp::Channel`

The channel is the entrypoint for publishing, consuming, and topology
declaration. Each `Channel` is owned by a single fiber for the
purposes of state-changing operations; concurrent state-changing
calls from different fibers raise `Amqp::ConcurrencyError`. The
exception is fire-and-forget `publish` / non-confirm `publish_batch`:
these calls do not await a broker method continuation, so they may be
serialised through the connection write mutex as long as their AMQP
method/header/body frame sequence is not interleaved. The
`Subscription` returned by `subscribe` is the way to fan deliveries out
to other fibers safely.

### 4.1 Lifecycle

```crystal
class Amqp::Channel
  def id : UInt16
  def open? : Bool
  def closed? : Bool
  def close(*, reply_code : UInt16 = 200, reply_text : String = "OK") : Nil
  def close_reason : Exception?

  def confirms_enabled? : Bool
  def confirm_select : Nil                     -- enable publisher confirms (idempotent)

  def prefetch(count : UInt16, *, global : Bool = false) : Nil
end
```

- `close` mirrors `Connection#close`: idempotent, best-effort wait for
  `channel.close-ok`, no exceptions on the second call.
- `confirm_select` is idempotent: the second call is a no-op. Once a
  channel is in confirm mode, it MUST NOT be put back into
  non-confirm mode (per AMQP 0-9-1; the broker rejects the
  transition).
- `prefetch` translates to `basic.qos`. `global: true` applies to all
  consumers on the channel (RabbitMQ-specific interpretation, which is
  the only target broker semantics in v0; LavinMQ matches it).

### 4.2 Publishing

```crystal
class Amqp::Channel
  # Fire-and-forget. No confirm. Returns when the frames are flushed
  # to the socket. Channel MUST be in non-confirm mode OR caller must
  # accept that no broker ack will be observed.
  def publish(message : Message,
              exchange : String,
              routing_key : String,
              *,
              mandatory : Bool = false,
              immediate : Bool = false
             ) : UInt64?

  # Batched publish. In non-confirm mode, writes all messages under one
  # connection write critical section without taking the channel
  # continuation lock. In confirm mode, every message is registered in
  # the confirm tracker.
  # Returns one publish sequence per message in confirm mode, otherwise
  # one nil per message.
  def publish_batch(messages : Array(Message),
                    exchange : String,
                    routing_key : String,
                    *,
                    mandatory : Bool = false,
                    immediate : Bool = false
                   ) : Array(UInt64?)

  def publish_batch(bodies : Array(Bytes),
                    exchange : String,
                    routing_key : String,
                    *,
                    properties : Properties = Properties.new,
                    mandatory : Bool = false,
                    immediate : Bool = false
                   ) : Array(UInt64?)

  # Windowed batch confirmation. Publishes messages in windows of
  # `window_size`, waits for all confirms after each window, and
  # returns false on nack/timeout. `confirm_select` MUST have been
  # called.
  def publish_confirm_batch(messages : Array(Message),
                            exchange : String,
                            routing_key : String,
                            *,
                            window_size : Int32 = 100,
                            mandatory : Bool = false,
                            immediate : Bool = false,
                            timeout : Time::Span = 30.seconds
                           ) : Bool

  def publish_confirm_batch(bodies : Array(Bytes),
                            exchange : String,
                            routing_key : String,
                            *,
                            properties : Properties = Properties.new,
                            window_size : Int32 = 100,
                            mandatory : Bool = false,
                            immediate : Bool = false,
                            timeout : Time::Span = 30.seconds
                           ) : Bool

  # amqp-client.cr compatibility aliases. New code should prefer the
  # shorter amqp-ng method names above.
  def basic_publish(body : Bytes | String, exchange : String, routing_key : String = "", ...)
    : UInt64
  def basic_publish(io : IO, bytesize : Int, exchange : String, routing_key : String = "", ...)
    : UInt64
  def basic_publish_confirm(body : Bytes | String, exchange : String, routing_key : String = "", ...)
    : Bool
  def basic_publish_confirm(io : IO, bytesize : Int, exchange : String, routing_key : String = "", ...)
    : Bool
  def basic_get(queue : String, no_ack : Bool = true) : GetMessage?
  def basic_consume(queue : String, tag : String = "", no_ack : Bool = true, ...)
    : String
  def basic_cancel(consumer_tag : String, no_wait : Bool = false) : Nil
  def basic_ack(delivery_tag : UInt64, multiple : Bool = false) : Nil
  def basic_reject(delivery_tag : UInt64, requeue : Bool = false) : Nil
  def basic_nack(delivery_tag : UInt64, requeue : Bool = false, multiple : Bool = false) : Nil
  def basic_qos(count : UInt16, global : Bool = false) : Nil
  def basic_recover(requeue : Bool = true) : Nil
  def flow(active : Bool) : Nil
  def tx_select : Nil
  def tx_commit : Nil
  def tx_rollback : Nil
  def transaction(& : -> T) : T forall T
  def on_return(& : ReturnedMessage -> Nil) : Nil
  def on_cancel(& : String -> Nil) : Nil
  def on_close(& : UInt16, String -> Nil) : Nil
  def queue : Queue
  def queue(name : String, ...) : Queue
  def exchange(name : String, type : String, ...) : Exchange
  def default_exchange : Exchange
  def direct_exchange(name : String = "amq.direct", passive : Bool = true) : Exchange
  def topic_exchange(name : String = "amq.topic", passive : Bool = true) : Exchange
  def fanout_exchange(name : String = "amq.fanout", passive : Bool = true) : Exchange
  def header_exchange(name : String = "amq.headers", passive : Bool = true) : Exchange

  # Synchronous confirm. Blocks the calling fiber until broker ack/nack
  # or `timeout` elapses. Returns true on ack, raises on nack or
  # timeout. `confirm_select` MUST have been called.
  def publish_confirm(message : Message,
                      exchange : String,
                      routing_key : String,
                      *,
                      mandatory : Bool = false,
                      timeout : Time::Span = 30.seconds
                     ) : Bool

  # Async confirm. Returns immediately with a delivery tag and a
  # Channel(ConfirmOutcome) on which the outcome will be sent exactly
  # once. `confirm_select` MUST have been called.
  def publish_async(message : Message,
                    exchange : String,
                    routing_key : String,
                    *,
                    mandatory : Bool = false
                   ) : {UInt64, ::Channel(ConfirmOutcome)}
end
```

**Notes.**

- `Message` is the in-memory representation; see §6 for the type.
  Convenience overloads accept `Bytes` bodies with explicit
  `Properties`.
- `publish` returns the registered publish sequence when the channel
  is in confirm mode, otherwise `nil`. Callers who need an outcome use
  `publish_confirm` or `publish_async`.
- `publish_batch` preserves input order and returns sequence numbers
  in that same order when confirm mode is enabled. It does not wait for
  broker outcomes; callers use `wait_for_confirms` for a batch barrier,
  or `publish_async` when they need a per-message outcome channel.
- `publish_confirm_batch` is the ergonomic windowed-confirm helper:
  it is equivalent to repeated `publish_batch` plus
  `wait_for_confirms`, with at most `window_size` newly published
  messages outstanding between barriers. It returns `false` when a
  window sees a nack or times out, and raises if confirm mode is not
  enabled.
- `immediate: true` is rejected by RabbitMQ and LavinMQ at the broker
  level (returns a `channel.close` with reply-code 540). The shard
  MUST pass the flag through unchanged so callers see the broker
  error rather than a synthetic one — the asymmetry exists because
  some test brokers may still implement it.
- `publish_confirm` raises `Amqp::PublishNackError` on broker nack,
  `Amqp::PublishReturnedError` on `mandatory: true` + unroutable
  (the basic.return path), `Amqp::PublishTimeoutError` on
  exceeding `timeout`, and any propagating
  `Amqp::ChannelError` / `Amqp::ConnectionError` for channel- or
  connection-level failures during the wait.
- `publish_async` MUST send the outcome exactly once per call. Sending
  on a `Channel` whose receiver is gone is the caller's responsibility;
  the shard uses an unbuffered receive-or-drop pattern documented in
  `docs/08-publisher-confirms.md`.

**Falsifier:** T-PUB-* (`docs/08-publisher-confirms.md`).

### 4.3 Consuming — block form

```crystal
class Amqp::Channel
  # Blocks the calling fiber on this channel's consumer for the queue.
  # Yields each DeliverMessage to the block. The block runs on the
  # CALLER's fiber, in order. Return from the block to ack (when
  # `auto_ack: true`) or leave the message for explicit ack.
  def consume(queue : String,
              *,
              consumer_tag : String = "",
              auto_ack : Bool = false,
              exclusive : Bool = false,
              no_local : Bool = false,
              arguments : Arguments = Arguments.new,
              & : DeliverMessage ->) : Nil
end
```

- The block returns when the consumer is cancelled (channel close,
  caller-side cancel via `Subscription#close`, or broker-side cancel
  `basic.cancel`).
- `auto_ack: true` issues `basic.ack` after the block returns
  normally. Exceptions from the block propagate to the caller of
  `consume` AND issue `basic.reject{requeue: true}` for the offending
  delivery. The behavior is documented in `docs/09-consumer.md`.
- `consume` MUST NOT spawn its own fiber. The caller has chosen to
  block this fiber for the consumer's lifetime; that is the entire
  point of the block form.

### 4.4 Consuming — object form

```crystal
class Amqp::Channel
  # Registers a consumer and returns a Subscription. The Subscription
  # is a Channel(T)-compatible receiver: `msg = sub.receive` and
  # `select when msg = sub.receive` both work.
  def subscribe(queue : String,
                *,
                consumer_tag : String = "",
                auto_ack : Bool = false,
                exclusive : Bool = false,
                no_local : Bool = false,
                arguments : Arguments = Arguments.new,
                buffer : Int32 = 16
               ) : Subscription
end
```

- `buffer` sets the capacity of the internal `::Channel(DeliverMessage)`
  the Subscription wraps. The shard MUST apply backpressure when the
  buffer fills: the per-channel handler blocks on the subscription
  inbox. If that channel's frame inbox also fills, pressure can
  propagate to the connection reader and then to the broker via TCP
  backpressure.
  The shard MUST NOT silently drop deliveries.

### 4.5 Synchronous get

```crystal
class Amqp::Channel
  def get(queue : String, *, auto_ack : Bool = false) : GetMessage?
end
```

- Returns `nil` when the queue is empty (`basic.get-empty`).
- Otherwise returns a `GetMessage` carrying the body, properties,
  routing key, redelivered flag, message count, and delivery tag.

### 4.6 Acknowledgement

```crystal
class Amqp::Channel
  def ack(delivery_tag : UInt64, *, multiple : Bool = false) : Nil
  def nack(delivery_tag : UInt64, *, multiple : Bool = false, requeue : Bool = true) : Nil
  def reject(delivery_tag : UInt64, *, requeue : Bool = true) : Nil
end
```

- These are unchecked: there is no broker round-trip. The shard MUST
  NOT track outstanding tags client-side; the broker is the source of
  truth. Sending an ack for an unknown tag triggers a broker-side
  channel close with reply-code 406 (PRECONDITION_FAILED) which
  surfaces as `Amqp::ChannelError` on the next blocked operation.

### 4.7 Topology

```crystal
class Amqp::Channel
  def queue_declare(name : String,
                    *,
                    passive : Bool = false,
                    durable : Bool = false,
                    exclusive : Bool = false,
                    auto_delete : Bool = false,
                    arguments : Arguments = Arguments.new
                   ) : Amqp::QueueDeclareOk

  def queue_delete(name : String,
                   *,
                   if_unused : Bool = false,
                   if_empty : Bool = false
                  ) : UInt32                    -- message_count

  def queue_bind(queue : String,
                 exchange : String,
                 routing_key : String,
                 *,
                 arguments : Arguments = Arguments.new
                ) : Nil

  def queue_unbind(queue : String,
                   exchange : String,
                   routing_key : String,
                   *,
                   arguments : Arguments = Arguments.new
                  ) : Nil

  def queue_purge(name : String) : UInt32       -- message_count

  def exchange_declare(name : String,
                       type : String,           -- "direct"/"fanout"/"topic"/"headers"/...
                       *,
                       passive : Bool = false,
                       durable : Bool = false,
                       auto_delete : Bool = false,
                       internal : Bool = false,
                       arguments : Arguments = Arguments.new
                      ) : Nil

  def exchange_delete(name : String, *, if_unused : Bool = false) : Nil

  def exchange_bind(destination : String,
                    source : String,
                    routing_key : String,
                    *,
                    arguments : Arguments = Arguments.new
                   ) : Nil

  def exchange_unbind(destination : String,
                      source : String,
                      routing_key : String,
                      *,
                      arguments : Arguments = Arguments.new
                     ) : Nil
end
```

- `passive: true` means "verify existence, do not create." On
  mismatch with the broker's existing definition (or absence) the
  broker returns `channel.close` with reply-code 404 or 406; the shard
  surfaces this as `Amqp::ChannelError`.
- The `auto-generated queue name` case (passing `""` to
  `queue_declare`) MUST be supported. The returned `QueueDeclareOk`
  exposes the server-assigned name.
- `arguments` is `Amqp::Arguments` (§6) — a typed field-table wrapper.
  Plain `Hash(String, X)` is NOT accepted; this is intentional to
  force the caller to consider the AMQP field-type their value maps
  to.

`Amqp::QueueDeclareOk`:

```crystal
struct Amqp::QueueDeclareOk
  getter name : String
  getter message_count : UInt32
  getter consumer_count : UInt32
end
```

---

## 5. `Amqp::Subscription`

```crystal
class Amqp::Subscription
  getter consumer_tag : String
  getter channel : Amqp::Channel
  getter queue : String

  # ::Channel(T) surface:
  def receive : DeliverMessage                   -- raises Amqp::SubscriptionClosed when ended
  def receive? : DeliverMessage?                 -- returns nil instead of raising
  def closed? : Bool

  # Lifecycle:
  def close : Nil                                -- sends basic.cancel, drains inbox
  def each(& : DeliverMessage ->) : Nil          -- block form that ends on close

  # Detached form (spawns one fiber that owns the consumer):
  def spawn_loop(& : DeliverMessage ->) : Nil    -- yields each delivery in a new fiber

end
```

- `Subscription#receive` MUST be usable as the receive side of a
  Crystal `select`, i.e. it MUST be implemented as a wrapper around an
  internal `::Channel(DeliverMessage)` exposed via `select` semantics.
  The implementation MUST NOT require ceremony beyond
  `select when msg = sub.receive`.
- `close` issues `basic.cancel`, sets `closed?` to true, then drains
  any buffered deliveries already on the inbox (those buffered
  deliveries MUST still be receivable until the inbox is empty, after
  which further `receive` calls raise `Amqp::SubscriptionClosed`).
- `spawn_loop` is the explicit detached-subscription helper. Its
  semantics: spawn one fiber that loops `receive`-and-yield until the
  subscription closes; exceptions from the user block terminate that
  spawned fiber. v0 does not provide an `on_terminate` callback or
  automatic reject policy for `spawn_loop`; callers that need explicit
  exception handling should spawn their own loop around `receive`.

---

## 6. Value types

### 6.1 `Amqp::Message`

```crystal
struct Amqp::Message
  getter body : Bytes
  getter properties : Properties

  def self.new(body : Bytes, properties : Properties = Properties.new) : Message
  def self.new(body : String, properties : Properties = Properties.new) : Message
  def self.new(body : IO, properties : Properties = Properties.new) : Message
end
```

- The `String` overload calls `.to_slice` (the bytes are UTF-8 if the
  string is UTF-8; the shard does not enforce a charset). The `IO`
  overload reads to EOF eagerly — streaming bodies are not v0 scope.
- `Properties` defaults are documented in §6.3.

### 6.2 `Amqp::DeliverMessage` and `Amqp::GetMessage`

```crystal
struct Amqp::DeliverMessage
  getter body : Bytes
  getter properties : Properties
  getter delivery_tag : UInt64
  getter redelivered : Bool
  getter exchange : String
  getter routing_key : String
  getter consumer_tag : String
end

struct Amqp::GetMessage
  getter body : Bytes
  getter properties : Properties
  getter delivery_tag : UInt64
  getter redelivered : Bool
  getter exchange : String
  getter routing_key : String
  getter message_count : UInt32
end
```

These are deliberately separate types because their broker-supplied
fields differ (`consumer_tag` on deliver, `message_count` on get).

### 6.3 `Amqp::Properties`

The 14 AMQP 0-9-1 basic-properties fields, all optional:

```crystal
struct Amqp::Properties
  getter content_type : String?
  getter content_encoding : String?
  getter headers : Arguments?
  getter delivery_mode : Persistence?
  getter persistence : Persistence?              -- maps to delivery_mode
  getter priority : UInt8?
  getter correlation_id : String?
  getter reply_to : String?
  getter expiration : String?                    -- AMQP-spec: shortstr, milliseconds as decimal-string
  getter message_id : String?
  getter timestamp : Time?
  getter type : String?
  getter user_id : String?
  getter app_id : String?
  getter cluster_id : String?                    -- legacy, unused on modern brokers

  def self.new(**fields) : Properties            -- keyword constructor
end
```

- `persistence: Persistence::Persistent` and
  `delivery_mode: Persistence::Persistent` both map to AMQP
  `delivery_mode = 2`; `Persistence::Transient` maps to `1`; `nil`
  omits the field (broker treats it as transient). The shard exposes
  the typed enum only, never a raw integer `delivery_mode`.
- `expiration` is a `String` because the AMQP 0-9-1 wire encoding for
  this field is a short-string of decimal milliseconds; offering a
  `Time::Span` overload that silently rounds is too easy to abuse.
  Callers wrap: `expiration: 30.seconds.total_milliseconds.to_i.to_s`.
- `headers` is an `Arguments`, not a raw `Hash` — same rationale as
  `queue_declare`'s `arguments:`.

### 6.4 `Amqp::Arguments` and `Amqp::FieldValue`

```crystal
alias Amqp::FieldValue =
  Bool | Int8 | UInt8 | Int16 | UInt16 | Int32 | UInt32 |
  Int64 | Float32 | Float64 |
  String | Bytes | Time | Nil |
  Array(Amqp::FieldValue) | Hash(String, Amqp::FieldValue)

alias Amqp::Arguments = Hash(String, FieldValue)
```

- Concrete AMQP field types map deterministically to Crystal types per
  `docs/05-wire-0-9-1/01-types.md`. The shard MUST NOT silently
  upcast: `args["x"] = 1_u8` writes a `short-short-uint`,
  `args["x"] = 1_i32` writes a `long-int`. The caller chooses
  the field type by choosing the Crystal type.
- Decimal-value (AMQP `D`) is intentionally not supported in v0;
  RabbitMQ and LavinMQ neither produce nor preserve it. If a v0 user
  receives a decimal field over the wire, the codec MUST raise
  `Amqp::ProtocolError`. (Listed as a known limitation in
  `docs/20-risk-register.md`.)

### 6.5 `Amqp::ConfirmOutcome`

```crystal
struct Amqp::ConfirmOutcome
  enum Kind
    Ack
    Nack
    Returned
  end

  getter kind : Kind
  getter delivery_tag : UInt64
  getter return_reason : Amqp::ReturnReason?     -- populated when kind == Returned
end

struct Amqp::ReturnReason
  getter reply_code : UInt16
  getter reply_text : String
  getter exchange : String
  getter routing_key : String
end
```

`publish_async` returns `{tag, ::Channel(ConfirmOutcome)}`; the
returned `::Channel` receives exactly one outcome and is then closed
(`.close` after `.send`). Multiple sends MUST NOT happen — the
exactly-once contract is the binary reliability claim in §P-7.

### 6.6 `Amqp::Recovery` and `Amqp::Persistence`

```crystal
enum Amqp::Recovery
  None
  Full
  # Manual reserved for v0.x, NOT v0
end

enum Amqp::Persistence
  Transient
  Persistent
end
```

### 6.7 `Amqp::Stats`

The current v0 implementation exposes one connection-level
`Amqp::Stats` counter object. `Stats#snapshot` returns an immutable
`Amqp::Stats::Snapshot` with publish, confirm, return, consume, and
recovery counters. The richer `ConnectionStats`, `ChannelStats`, and
`SubscriptionStats` model in `docs/19-observability.md` is deferred.

---

## 7. Blocking semantics summary

Every public method that may block is enumerated below with its
blocking conditions. This table is normative; the implementation MUST
NOT add a new blocking site to a method listed as non-blocking.

| Method                              | Blocks on                                       | Time-bound by                |
|-------------------------------------|-------------------------------------------------|------------------------------|
| `Amqp.connect`                      | TCP/TLS handshake + AMQP negotiation            | `connect_timeout`            |
| `Connection#channel`                | Channel-id allocation (fast)                    | None (O(1) amortised)        |
| `Connection#close`                  | Awaiting `connection.close-ok`                  | heartbeat or 5 s             |
| `Channel#publish`                   | Frame write into socket                         | None (caller's fiber drives) |
| `Channel#publish_confirm`           | Awaiting broker ack/nack                        | `timeout` arg                |
| `Channel#publish_async`             | Frame write into socket                         | None                         |
| `Channel#consume` (block form)      | For lifetime of consumer                        | None                         |
| `Subscription#receive`              | Until next delivery or sub.close                | None (use select+timeout)    |
| `Subscription#close`                | Awaiting `basic.cancel-ok` and drain            | heartbeat                    |
| `Channel#get`                       | Awaiting `basic.get-ok` / `basic.get-empty`     | None (fast)                  |
| `Channel#queue_declare`             | Awaiting `queue.declare-ok`                     | None (fast; bounded by HB)   |
| `Channel#queue_delete`              | Awaiting `queue.delete-ok`                      | None                         |
| `Channel#queue_bind`                | Awaiting `queue.bind-ok`                        | None                         |
| `Channel#queue_purge`               | Awaiting `queue.purge-ok`                       | None                         |
| `Channel#exchange_declare`          | Awaiting `exchange.declare-ok`                  | None                         |
| `Channel#exchange_delete`           | Awaiting `exchange.delete-ok`                   | None                         |
| `Channel#close`                     | Awaiting `channel.close-ok`                     | heartbeat or 5 s             |
| `Channel#ack` / `#nack` / `#reject` | Frame write into socket                         | None                         |
| `Channel#prefetch`                  | Awaiting `basic.qos-ok`                         | None                         |
| `Channel#confirm_select`            | Awaiting `confirm.select-ok`                    | None                         |

"None" means the method does not wait for a broker reply; it returns
as soon as the outgoing frames are flushed to the socket. The
connection-level write mutex is the only serialisation. A connection
or channel failure interrupts these calls and surfaces as
`Amqp::Error`.

**Falsifier:** T-API-BLOCK-001..N — each row above is an explicit
assertion in the test suite.

---

## 8. Stability and evolution

The signatures above are the v0 surface. The shard's semver promise:

- **0.x.** Any method in this document MAY change signature with a
  doc revision. The shard's `VERSION` constant ticks `0.x.0` for any
  change that is not a documentation typo.
- **1.0.** The surface in this document is frozen. Further additions
  MUST be additive (new methods, new parameters with safe defaults).
- **2.0** is reserved for the AMQP 1.0 surface (`docs/18-amqp-1-0-forward-plan.md`).

Implementations MUST treat any name not in this document as private,
even if the Crystal language exposes it. Callers who reach into
private types do so at their own risk; the shard does not protect
those names with `private` keyword universally because some need to
be visible to subclasses inside the shard, but they are not part of
the public API.

**Falsifier:** T-API-SURFACE-001 — a smoke test enumerates every
top-level name reachable from the `Amqp` constant and asserts that the
visible namespace remains intentional. Supported caller API is still
the documented subset above.
