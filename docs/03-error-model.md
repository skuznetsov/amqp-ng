# amqp — Error Model

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers; callers writing rescue clauses.
> **Companions:** `docs/01-design-principles.md` (P-3 raises rather
> than returns codes), `docs/02-public-api.md` (which methods raise
> which subclasses), `docs/06-connection-lifecycle.md` and
> `docs/07-channel-lifecycle.md` (where the broker-side close-codes
> originate).

This document is the **complete** v0 exception surface. Every method
in `docs/02-public-api.md` raises exclusively the subclasses listed
here (or no exception, on success). The implementation MUST NOT raise
any other class as part of its public contract; if a stdlib exception
escapes the shard (e.g., `IO::Error` from a socket), the shard MUST
wrap it in an `Amqp::Error` subclass at the boundary, preserving
`cause`.

---

## 1. Hierarchy

```
Amqp::Error                                          -- abstract root, < Exception
├── Amqp::ConfigurationError                         -- caller error, raised pre-flight
│   ├── Amqp::UriError
│   └── Amqp::TlsConfigError
├── Amqp::ConnectError                               -- handshake failed
│   ├── Amqp::ConnectTimeoutError
│   ├── Amqp::ConnectRefusedError
│   ├── Amqp::AuthenticationError                    -- access-refused / not-allowed (403)
│   ├── Amqp::VhostAccessError                       -- access-refused on vhost (403, vhost-scoped)
│   ├── Amqp::TlsHandshakeError
│   └── Amqp::ProtocolNegotiationError
├── Amqp::ConnectionError                            -- runtime, connection-scoped
│   ├── Amqp::ConnectionClosedByBroker               -- broker sent connection.close
│   ├── Amqp::ConnectionClosedByCaller               -- caller invoked Connection#close
│   ├── Amqp::HeartbeatTimeoutError
│   ├── Amqp::SocketError                            -- wraps IO::Error/EOFError/Socket::Error
│   ├── Amqp::FrameTooLargeError
│   └── Amqp::ProtocolError                          -- malformed frame or wire violation
├── Amqp::ChannelError                               -- runtime, channel-scoped
│   ├── Amqp::ChannelClosedByBroker
│   ├── Amqp::ChannelClosedByCaller
│   ├── Amqp::ChannelInUseError                      -- channel id collision
│   ├── Amqp::ChannelLimitError                      -- exceeded negotiated channel_max
│   ├── Amqp::ConcurrencyError                       -- concurrent ops on same channel
│   └── Amqp::PrechargeError                         -- precondition-failed (406)
├── Amqp::PublishError                               -- publish-time failures (in confirm mode)
│   ├── Amqp::PublishNackError                       -- broker sent basic.nack
│   ├── Amqp::PublishReturnedError                   -- mandatory + unroutable
│   ├── Amqp::PublishTimeoutError                    -- exceeded publish_confirm timeout
│   └── Amqp::PublishOutOfOrderError                 -- broker confirmed a tag we never sent
├── Amqp::SubscriptionClosed                         -- normal terminator for Subscription#receive
└── Amqp::RecoveryError                              -- recovery-pipeline failures
    ├── Amqp::RecoveryAbandoned                      -- exceeded retry budget
    └── Amqp::RecoveryTopologyError                  -- topology re-declare diverged
```

`Amqp::Error` is abstract: the shard MUST NOT instantiate it
directly. Callers catching `rescue ex : Amqp::Error` catch every
shard-originated exception.

---

## 2. Field shape

Every subclass of `Amqp::Error` exposes the following fields. The
exact constructor is private; instances are produced by the shard.

```crystal
abstract class Amqp::Error < Exception
  getter close_reason : CloseReason?      -- populated for broker-driven closures
  getter cause : Exception?               -- inherited from Exception, used heavily

  # Implementation-private: protocol context for diagnostics
  getter context : ErrorContext?
end

struct Amqp::CloseReason
  enum Origin
    Broker        -- broker sent close
    Caller        -- caller invoked close
    Network       -- socket-level failure
    Heartbeat     -- heartbeat-deadline expired
    Recovery      -- recovery pipeline gave up
  end

  getter origin : Origin
  getter reply_code : UInt16         -- AMQP reply-code; 0 if not applicable
  getter reply_text : String         -- AMQP reply-text; "" if not applicable
  getter class_id : UInt16           -- offending class-id; 0 if not applicable
  getter method_id : UInt16          -- offending method-id; 0 if not applicable
  getter ts : Time                   -- when the close was observed
end

struct Amqp::ErrorContext           -- implementation-private, but stable in #to_s
  getter connection_id : UInt64
  getter channel_id : UInt16?
  getter operation : String          -- e.g., "publish", "queue.declare"
end
```

The `to_s` of an `Amqp::Error` MUST include:

- the subclass name,
- the message,
- the `close_reason` summary if present (origin/reply_code/reply_text),
- the `context.operation` if present.

This is the line a caller sees in logs; it must be informative without
the caller having to introspect the exception.

---

## 3. Reply-code mapping (broker → exception subclass)

AMQP 0-9-1 defines a fixed set of `reply-code`s on
`connection.close` and `channel.close`. The shard MUST map them
exactly as follows:

| Reply | Name                  | On connection.close → | On channel.close →       |
|-------|-----------------------|-----------------------|--------------------------|
| 200   | reply-success         | (graceful, no raise)  | (graceful, no raise)     |
| 311   | content-too-large     | ConnectionError       | ChannelError             |
| 313   | no-consumers          | —                     | ChannelError             |
| 320   | connection-forced     | ConnectionClosedByBroker | —                     |
| 402   | invalid-path          | ConnectError (vhost)  | —                        |
| 403   | access-refused        | AuthenticationError or VhostAccessError\* | ChannelError |
| 404   | not-found             | —                     | ChannelError             |
| 405   | resource-locked       | —                     | ChannelError             |
| 406   | precondition-failed   | —                     | PrechargeError           |
| 501   | frame-error           | ProtocolError         | ProtocolError            |
| 502   | syntax-error          | ProtocolError         | ProtocolError            |
| 503   | command-invalid       | ProtocolError         | ProtocolError            |
| 504   | channel-error         | —                     | ChannelError             |
| 505   | unexpected-frame      | ProtocolError         | ProtocolError            |
| 506   | resource-error        | ConnectionError       | ChannelError             |
| 530   | not-allowed           | ConnectionClosedByBroker | ChannelError          |
| 540   | not-implemented       | ConnectionError       | ChannelError             |
| 541   | internal-error        | ConnectionError       | ChannelError             |

\* For reply-code 403 on connection level, the shard MUST distinguish
between authentication failure (during `connection.start-ok` /
`connection.tune` phase) and vhost-access failure (during
`connection.open`). The former raises `AuthenticationError`, the
latter raises `VhostAccessError`. Both are subclasses of
`ConnectError`.

For reply codes not in the table (broker extensions or new spec
additions) the shard MUST raise the generic `ConnectionError` /
`ChannelError` with the raw code in `close_reason.reply_code`.

**Falsifier:** T-ERR-MAP-001..N — for each row in the table, a test
fires the broker-side condition (where possible) or feeds a synthetic
close frame and asserts the exact exception class.

---

## 4. Broker close vs caller close

The shard distinguishes who initiated a close:

- `ConnectionClosedByCaller` / `ChannelClosedByCaller` ALWAYS have
  `close_reason.origin == Caller` and `reply_code = 200` unless the
  caller supplied a different code in `Connection#close`. These are
  raised into any operation already in flight when the caller invokes
  `close` from another fiber; they do NOT need to be rescued by the
  caller of `close` itself (that call returns normally).
- `ConnectionClosedByBroker` / `ChannelClosedByBroker` ALWAYS have
  `close_reason.origin == Broker` and carry the broker-supplied
  reply_code / reply_text / class_id / method_id.
- `SocketError` has `origin == Network`. `HeartbeatTimeoutError` has
  `origin == Heartbeat` and `reply_code = 0`.

The distinction matters for recovery (`docs/12-recovery.md`):
`Recovery::Full` reconnects on every origin **except** `Caller`. A
caller-initiated close is intentional and MUST NOT be reversed by
auto-recovery.

**Falsifier:** T-ERR-ORIGIN-001..006.

---

## 5. Recoverable vs fatal

The shard classifies every exception as recoverable or fatal **from
the connection's perspective**, not the application's:

| Recoverable (Recovery::Full reconnects)    | Fatal (Recovery::Full surrenders)              |
|--------------------------------------------|------------------------------------------------|
| `SocketError`                              | `AuthenticationError`                          |
| `HeartbeatTimeoutError`                    | `VhostAccessError`                             |
| `ConnectionClosedByBroker` (non-auth)      | `ConfigurationError` and subclasses            |
| `FrameTooLargeError`                       | `TlsHandshakeError` (certificate failure)      |
| `ProtocolError` (rare, often broker bug)   | `ProtocolNegotiationError`                     |
|                                            | `RecoveryAbandoned`                            |

Channel-level errors (`ChannelError` and subclasses) MUST be treated
as **caller-visible** but NOT cause connection reconnection. The
channel itself is dead after a `ChannelClosedByBroker`; recovery in
`Full` mode re-opens it and re-applies topology, then surfaces a
`RecoveryEvent` describing the re-establishment.

Implementations MUST expose this taxonomy through a class method:

```crystal
class Amqp::Error
  def self.recoverable?(klass : Amqp::Error.class) : Bool
end
```

so that `Recovery::Full` and any user-written recovery logic share
the same definition. The class-list above is normative.

**Falsifier:** T-ERR-RECOV-001..N — for each error class, a test
asserts `recoverable?` matches the table.

---

## 6. Exception propagation through fibers

The shard owns three kinds of fibers (per `docs/00-overview.md` §2.2):
frame-reader, heartbeat, consumer-loop. Errors raised by these fibers
MUST be visible to user code.

- **Frame-reader fiber** dies when the socket fails or a protocol
  violation is detected. Before exiting, it MUST:
  1. Set the connection state to `Closed` with the appropriate
     `CloseReason`.
  2. Close every per-channel inbox (the `::Channel(Frame)` for each
     active channel) with an exception sentinel.
  3. Wake every fiber blocked on `publish_confirm`, `queue.declare`-ok,
     `consume`, `Subscription#receive`, etc., with the matching
     `ConnectionError` subclass.
  4. Signal the heartbeat fiber to exit.

- **Heartbeat fiber** dies after writing its last frame or on
  receive-deadline expiry. On deadline expiry it MUST raise
  `HeartbeatTimeoutError` into the connection via the same wakeup
  path as the frame-reader.

- **Consumer-loop fiber** (only when spawned via `spawn_loop`) catches
  exceptions from the user block and issues
  `basic.reject{requeue: true}` for the offending delivery, then
  re-raises the exception inside the fiber (which terminates the
  fiber). The exception is observable via the subscription's
  `closed?` flag AND any registered `on_terminate` callback (see
  `docs/09-consumer.md` for that surface).

The shard MUST NOT silently swallow exceptions inside its own fibers.
Logging is acceptable BEFORE re-raising, never instead of.

**Falsifier:** T-ERR-FIBER-001..005 force each fiber's death path and
assert the appropriate user-facing exception surfaces within a bound.

---

## 7. The `cause` chain

The shard uses `Exception#cause` to preserve the underlying error
across boundary-wrapping:

- `SocketError.cause` is the original `IO::Error` / `Socket::Error` /
  `OpenSSL::SSL::Error` instance.
- `TlsHandshakeError.cause` is the original `OpenSSL::SSL::Error`.
- `RecoveryError.cause` is the original triggering exception that the
  recovery loop was attempting to recover from.

Callers SHOULD walk `cause` only for diagnostic logging; they MUST
NOT branch behaviour on the cause's class, because the cause is a
stdlib type that may evolve across Crystal versions.

---

## 8. The "no doubt" rule

Per P-7, every exception MUST leave the caller in a definite state:

- `PublishNackError`: broker explicitly rejected the message.
- `PublishReturnedError`: broker accepted but could not route
  (`mandatory: true` + no matching binding).
- `PublishTimeoutError`: broker neither acked nor nacked within the
  caller's timeout. The caller MUST treat the publish as "unknown" —
  not committed, not rejected. This is the only exception that leaves
  ambiguity, and the ambiguity is exactly the timeout the caller
  chose.
- `PublishOutOfOrderError`: protocol invariant violation; the
  connection MUST be closed by the shard immediately after surfacing
  this exception. (RabbitMQ confirms in monotonic delivery-tag order;
  violation indicates broker bug or man-in-the-middle.)

Any other exception during `publish_confirm` (channel or connection
close) MUST leave the publish in the "unknown" bucket from the
caller's perspective, because the AMQP wire protocol gives no
mid-flight close-reply mapping back to a specific delivery tag.
Callers who require committed-or-rejected-with-no-third-state SHOULD
use idempotent message ids and dedupe at the consumer.

**Falsifier:** T-ERR-COMMIT-001..006 exercise each outcome and assert
the exception class plus a regression check that no second outcome is
observable for the same delivery tag.

---

## 9. Examples (informative)

```crystal
begin
  conn = Amqp.connect("amqps://app:secret@rabbit:5671/prod")
rescue ex : Amqp::AuthenticationError
  Log.error { "bad credentials for #{ex.context.try &.operation}" }
rescue ex : Amqp::ConnectTimeoutError
  Log.error { "broker did not respond within 30s" }
rescue ex : Amqp::TlsHandshakeError
  Log.error { "TLS failed: #{ex.cause.try &.message}" }
end

begin
  ch.publish_confirm(msg, "events", "user.signup", timeout: 5.seconds)
rescue ex : Amqp::PublishNackError
  metrics.increment("publish.nack")
rescue ex : Amqp::PublishReturnedError
  metrics.increment("publish.returned")
  Log.warn { "unroutable: #{ex.close_reason.try &.reply_text}" }
rescue ex : Amqp::PublishTimeoutError
  schedule_retry(msg) if Amqp::Error.recoverable?(ex.class)
rescue ex : Amqp::ChannelError
  ch = conn.channel
  retry
end
```

---

## 10. Anti-patterns

The shard explicitly discourages:

- **Catching `Amqp::Error` everywhere.** Most callers care about
  exactly one or two classes (commonly `PublishNackError` and
  `ConnectionError`). Catching the root flattens distinctions the
  shard worked to preserve.
- **Re-raising as a different class.** Application code wrapping
  `Amqp::*` exceptions into application-specific errors SHOULD set
  `cause` to the original; the shard's `to_s` chain is the cheapest
  diagnostic and should remain reachable.
- **Branching on `reply_code` integers.** The shard already maps
  every defined code to a subclass (§3); branching on the integer
  defeats the purpose. If a caller really needs a code the shard
  didn't promote to a subclass, the raw value is in
  `close_reason.reply_code` — use sparingly.
