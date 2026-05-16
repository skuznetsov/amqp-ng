# amqp — Error Model

> **Document status:** Draft v0.2, 2026-05-15
> **Audience:** Implementers; callers writing rescue clauses.
> **Companions:** `docs/02-public-api.md`, `docs/06-connection-lifecycle.md`,
> `docs/07-channel-lifecycle.md`, `docs/08-publisher-confirms.md`,
> `docs/12-recovery.md`.

This document describes the current v0 exception surface. It is
intentionally narrower than earlier drafts: the implementation exposes
typed exceptions, but not `CloseReason`, `ErrorContext`,
`Error.recoverable?`, `RecoveryEvent`, or a `RecoveryError` subtree yet.
Those are deferred until they have executable falsifiers.

---

## 1. Hierarchy

```crystal
Amqp::Error
├── Amqp::ConfigurationError
│   ├── Amqp::UriError
│   └── Amqp::TlsConfigError
├── Amqp::ConnectError
│   ├── Amqp::ConnectTimeoutError
│   ├── Amqp::ConnectRefusedError
│   ├── Amqp::AuthenticationError
│   ├── Amqp::VhostAccessError
│   ├── Amqp::TlsHandshakeError
│   └── Amqp::ProtocolNegotiationError
├── Amqp::ConnectionError
│   ├── Amqp::ConnectionClosedByBroker
│   ├── Amqp::ConnectionClosedByCaller
│   ├── Amqp::SocketError
│   ├── Amqp::ProtocolError
│   ├── Amqp::FrameTooLargeError
│   ├── Amqp::HeartbeatTimeoutError
│   ├── Amqp::RecoveryInProgress
│   └── Amqp::RecoveryExhaustedError
├── Amqp::ChannelError
│   ├── Amqp::ChannelClosedByBroker
│   ├── Amqp::ChannelClosedByCaller
│   ├── Amqp::ChannelLimitError
│   ├── Amqp::ConcurrencyError
│   ├── Amqp::ChannelRpcTimeoutError
│   ├── Amqp::PublishNackError
│   ├── Amqp::PublishReturnedError
│   ├── Amqp::PublishTimeoutError
│   ├── Amqp::PublishOutOfOrderError
│   └── Amqp::PreconditionFailedError
└── Amqp::SubscriptionClosed
```

`Amqp::SubscriptionClosed` is an alias for `Amqp::Subscription::Closed`.
It is a normal consumer terminator, not a connection failure.

The previous names `ChannelInUseError`, `PrechargeError`,
`RecoveryAbandoned`, and `RecoveryTopologyError` are not v0 public API.
Use `ChannelLimitError`, `PreconditionFailedError`, and
`RecoveryExhaustedError` for the implemented v0 surface.

**Falsifier:** `T-API-SURFACE-001`, `T-API-EXC-001..N`.

---

## 2. Broker close fields

`ConnectionClosedByBroker` and `ChannelClosedByBroker` expose the raw
broker close data:

```crystal
getter reply_code : UInt16
getter reply_text : String
getter origin_class_id : UInt16
getter origin_method_id : UInt16
```

`PreconditionFailedError` exposes:

```crystal
getter reply_code : UInt16
getter reply_text : String
```

Publisher confirm exceptions expose the data callers need for retry or
dead-letter decisions:

```crystal
PublishNackError#delivery_tag : UInt64
PublishTimeoutError#delivery_tag : UInt64
PublishTimeoutError#timeout : Time::Span
PublishReturnedError#delivery_tag : UInt64
PublishReturnedError#reason : ReturnReason
PublishOutOfOrderError#delivery_tag : UInt64
```

The v0 public API does not expose a structured `CloseReason` object.
`Connection#close_reason` and `Channel#close_reason` return the
exception that caused closure where one is available.

---

## 3. Reply-code mapping

The implemented v0 mapping is intentionally small:

| Broker close code | Scope      | v0 exception                                            |
|-------------------|------------|---------------------------------------------------------|
| 403 during auth   | connection | `AuthenticationError`                                   |
| 403 during open   | connection | `VhostAccessError`                                      |
| any connection close frame | connection | `ConnectionClosedByBroker`                    |
| 406 on channel    | channel    | `PreconditionFailedError`                               |
| any other channel close frame | channel | `ChannelClosedByBroker`                       |

Protocol violations detected locally raise `ProtocolError`. Socket EOF
or IO failures after a connection was open raise `SocketError` or a
more specific connection error when the path can classify it.

**Falsifier:** `T-ERR-MAP-001..N`.

---

## 4. Recovery-facing errors

`Recovery::Full` currently treats caller close as fatal and network-like
session loss as recoverable. New caller operations attempted while the
connection is recovering raise `RecoveryInProgress`; if the retry budget
is exhausted the connection closes with `RecoveryExhaustedError`.

The v0 implementation does not expose `Amqp::Error.recoverable?`.
Recovery classification is internal to `Connection`.

**Falsifier:** `T-REC-DURING-001..004`, `T-REC-ABANDON-001`.

---

## 5. Publisher confirm certainty

Publisher confirm exceptions have the following user-visible meaning:

- `PublishNackError`: the broker explicitly rejected the publish.
- `PublishReturnedError`: the broker returned a mandatory unroutable
  publish. The `reason` contains reply code/text, exchange, and routing
  key.
- `PublishTimeoutError`: the caller's timeout expired before an ack,
  nack, or return was observed. The publish outcome is unknown.
- `PublishOutOfOrderError`: the broker confirmed a delivery tag the
  client does not track. This is a protocol invariant failure.

Channel or connection closure while a publish is in flight leaves that
publish in the unknown bucket from the caller's perspective.

**Falsifier:** `T-PUB-CONFIRM-001..006`, `T-PUB-MULTIPLE-001..003`.

---

## 6. Anti-patterns

- **Catching only `Amqp::Error` everywhere.** Useful at process
  boundaries, but application retry logic usually needs a narrower
  class such as `PublishTimeoutError`, `PublishReturnedError`, or
  `ConnectionError`.
- **Branching on broker integer codes when a subclass exists.** Prefer
  the typed exception first; use `reply_code` for diagnostics or
  broker-specific edge cases.
- **Treating `PublishTimeoutError` as loss.** It is ambiguity, not a
  broker rejection. Use idempotent message ids if retries must be safe.
