# amqp — Wire Codec: Classes and Methods

> **Document status:** Draft v0.1, 2026-05-14 — **normative** for the
> codec's class/method tables.
> **Companions:** `docs/05-wire-0-9-1/00-frames.md` (frame envelope),
> `docs/05-wire-0-9-1/01-types.md` (argument encoding),
> `docs/07-channel-lifecycle.md` (channel state machine that consumes
> these methods), `docs/03-error-model.md` (reply-code mapping).

This document enumerates every AMQP 0-9-1 class and method the v0
codec implements, with their `(class-id, method-id)` numeric pairs,
argument signatures, and sync/async character. It is the source of
truth that the codec's method dispatch table must match.

Methods marked **OUT (v0)** are part of the spec but explicitly not
implemented per `docs/17-mvp-cutline.md` §2. Methods marked
**REQUIRED** are mandatory for v0 — every implementer MUST handle
them.

---

## 1. Class ids

| Class id (decimal) | Class id (hex) | Spec class    | v0 status |
|-------------------|----------------|---------------|-----------|
| `10`              | `0x000A`       | `connection`  | REQUIRED  |
| `20`              | `0x0014`       | `channel`     | REQUIRED  |
| `30`              | `0x001E`       | `access`      | OUT (v0)  |
| `40`              | `0x0028`       | `exchange`    | REQUIRED  |
| `50`              | `0x0032`       | `queue`       | REQUIRED  |
| `60`              | `0x003C`       | `basic`       | REQUIRED  |
| `90`              | `0x005A`       | `tx`          | REQUIRED  |
| `85`              | `0x0055`       | `confirm`     | REQUIRED  |

Class id `30` (`access`) is deprecated in AMQP 0-9-1; brokers ignore
it. The v0 codec MUST NOT emit it and MUST treat receipt as a no-op
with a `Debug`-level log.

Class id `90` (`tx`) is the transactional class. v0 supports the
broker-native synchronous `tx.select`, `tx.commit`, and `tx.rollback`
methods as a compatibility surface. Recovery does not attempt to
preserve an in-flight transaction across reconnect.

Class id `85` (`confirm`) is RabbitMQ's extension. RabbitMQ
advertises the `publisher_confirms` capability during
`connection.start`; LavinMQ does likewise. v0 requires the broker to
advertise this capability (`docs/13-broker-compat-matrix.md` §2.1).

---

## 2. Sync vs async classification

Each method in the spec is either **synchronous** (the sender awaits
a specific reply method on the same channel) or **asynchronous**
(no reply expected). The codec's dispatch logic uses this classification:

- A sync send blocks the caller fiber on a per-channel reply slot
  until the matching `-ok` arrives, or the channel/connection errors.
- An async send returns immediately after the frame is written.

The "Type" column in each method table below uses S/A.

---

## 3. Class 10 — connection (channel 0 only)

All `connection.*` methods are sent on channel `0`.

| Method id | Hex      | Name                  | Type | Direction | Status |
|-----------|----------|-----------------------|------|-----------|--------|
| `10`      | `0x000A` | `start`               | S    | server→client | REQUIRED |
| `11`      | `0x000B` | `start-ok`            | S    | client→server | REQUIRED |
| `20`      | `0x0014` | `secure`              | S    | server→client | OUT (v0)¹ |
| `21`      | `0x0015` | `secure-ok`           | S    | client→server | OUT (v0)¹ |
| `30`      | `0x001E` | `tune`                | S    | server→client | REQUIRED |
| `31`      | `0x001F` | `tune-ok`             | S    | client→server | REQUIRED |
| `40`      | `0x0028` | `open`                | S    | client→server | REQUIRED |
| `41`      | `0x0029` | `open-ok`             | S    | server→client | REQUIRED |
| `50`      | `0x0032` | `close`               | S    | both          | REQUIRED |
| `51`      | `0x0033` | `close-ok`            | S    | both          | REQUIRED |
| `60`      | `0x003C` | `blocked`             | A    | server→client | REQUIRED² |
| `61`      | `0x003D` | `unblocked`           | A    | server→client | REQUIRED² |
| `70`      | `0x0046` | `update-secret`       | S    | server→client | OUT (v0)³ |
| `71`      | `0x0047` | `update-secret-ok`    | S    | client→server | OUT (v0)³ |

¹ `secure`/`secure-ok` are part of the SASL challenge-response loop.
PLAIN (the v0.1.0-supported mechanism,
`docs/04-uri-and-config.md` §4) completes authentication in a single
`start-ok`; neither broker invokes `secure` for these mechanisms. The
codec MUST raise `Amqp::ProtocolError` on receipt — surfacing as
"unsupported SASL flow."

² `blocked`/`unblocked` are RabbitMQ extensions advertised via the
`connection.blocked` capability. v0 decodes them as asynchronous
connection methods and exposes `Connection#blocked?`,
`Connection#on_blocked`, and `Connection#on_unblocked`.

³ `update-secret` is a RabbitMQ 3.8+ extension for credential
rotation without reconnect. Deferred to v0.x.

### 3.1 connection.start (10.10)

Server→client. Argument list:

| Field                | Type           |
|----------------------|----------------|
| `version-major`      | octet          |
| `version-minor`      | octet          |
| `server-properties`  | field-table    |
| `mechanisms`         | longstr        |
| `locales`            | longstr        |

The `mechanisms` longstr is a space-separated ASCII list (e.g.,
`"AMQPLAIN PLAIN"`). The `locales` longstr is a space-separated list
(e.g., `"en_US"`).

### 3.2 connection.start-ok (10.11)

Client→server. Argument list:

| Field                | Type           |
|----------------------|----------------|
| `client-properties`  | field-table    |
| `mechanism`          | shortstr       |
| `response`           | longstr        |
| `locale`             | shortstr       |

`client-properties` MUST include at least:

- `product` (longstr): library name, e.g., `"amqp-ng"`.
- `version` (longstr): library version string.
- `platform` (longstr): runtime, e.g., `"Crystal 1.10.1"`.
- `capabilities` (field-table): at minimum, `authentication_failure_close: true`,
  `connection.blocked: true`, `consumer_cancel_notify: true`,
  `publisher_confirms: true`.

`mechanism` is `PLAIN` for v0.1.0.

`response` for `PLAIN` is `NUL + user + NUL + password` (a single
longstr containing exactly that byte sequence). Future `EXTERNAL`
support would use an empty longstr, but it is not implemented in
v0.1.0.

`locale` is one of the locales the server advertised in `start`;
v0 sends `"en_US"`.

### 3.3 connection.tune (10.30) and tune-ok (10.31)

Both sides exchange:

| Field         | Type    |
|---------------|---------|
| `channel-max` | short   |
| `frame-max`   | long    |
| `heartbeat`   | short   |

Reconciliation rules: see `docs/06-connection-lifecycle.md` §4.3.

### 3.4 connection.open (10.40) and open-ok (10.41)

Client→server `open`:

| Field           | Type     | Notes                                |
|-----------------|----------|--------------------------------------|
| `virtual-host`  | shortstr | URI-decoded vhost                    |
| `reserved-1`    | shortstr | MUST be empty (`""`)                 |
| `reserved-2`    | bit      | MUST be `0`                          |

Server→client `open-ok` has a single `shortstr reserved-1` argument
(MUST be ignored).

### 3.5 connection.close (10.50) and close-ok (10.51)

Both directions. Argument list for `close`:

| Field         | Type     |
|---------------|----------|
| `reply-code`  | short    |
| `reply-text`  | shortstr |
| `class-id`    | short    |
| `method-id`   | short    |

`class-id` and `method-id` identify the class+method that triggered
the close, or are both `0` for unsolicited close (e.g., shutdown).

`close-ok` has no arguments.

The codec MUST respond to a server-sent `close` with `close-ok` as
the immediate next write on channel `0`, THEN close the socket.
Failure to respond before socket close is permitted (the broker is
already in close mode) but discouraged.

**Recorded:** `spec/fixtures/frames/handshake_auth_fail/s2c.bin` —
RabbitMQ sends `connection.close` with reply-code `403`
(`ACCESS_REFUSED`) after the bad-password `start-ok`. Reply-text
bytes form: `"ACCESS_REFUSED - Login was refused using authentication
mechanism PLAIN. For details see the broker logfile."`.

### 3.6 connection.blocked / unblocked (10.60 / 10.61)

Server→client async. `blocked` argument: `reason` (shortstr).
`unblocked` has no arguments.

The codec MUST accept both methods and route them through the
connection async-method handler. The handler updates `blocked?` and
dispatches registered callbacks without blocking the reader fiber.

---

## 4. Class 20 — channel

| Method id | Hex      | Name        | Type | Direction | Status |
|-----------|----------|-------------|------|-----------|--------|
| `10`      | `0x000A` | `open`      | S    | client→server | REQUIRED |
| `11`      | `0x000B` | `open-ok`   | S    | server→client | REQUIRED |
| `20`      | `0x0014` | `flow`      | S    | both          | REQUIRED |
| `21`      | `0x0015` | `flow-ok`   | S    | both          | REQUIRED |
| `40`      | `0x0028` | `close`     | S    | both          | REQUIRED |
| `41`      | `0x0029` | `close-ok`  | S    | both          | REQUIRED |

### 4.1 channel.open (20.10) and open-ok (20.11)

`open` arg: `reserved-1` (shortstr), MUST be empty.
`open-ok` arg: `reserved-1` (longstr), MUST be ignored.

### 4.2 channel.flow (20.20) and flow-ok (20.21)

`flow` arg: `active` (bit). `flow-ok` arg: `active` (bit).

Per `docs/07-channel-lifecycle.md` §6, RabbitMQ 3.8+ deprecates
`channel.flow(false)` from server side (uses `connection.blocked`
instead). LavinMQ 2.x still uses `channel.flow`. v0 supports both.

### 4.3 channel.close (20.40) and close-ok (20.41)

Identical argument layout to `connection.close`/`close-ok`, but
sent on the channel being closed (not channel 0).

---

## 5. Class 40 — exchange

| Method id | Hex      | Name             | Type | Direction | Status |
|-----------|----------|------------------|------|-----------|--------|
| `10`      | `0x000A` | `declare`        | S    | client→server | REQUIRED |
| `11`      | `0x000B` | `declare-ok`     | S    | server→client | REQUIRED |
| `20`      | `0x0014` | `delete`         | S    | client→server | REQUIRED |
| `21`      | `0x0015` | `delete-ok`      | S    | server→client | REQUIRED |
| `30`      | `0x001E` | `bind`           | S    | client→server | REQUIRED |
| `31`      | `0x001F` | `bind-ok`        | S    | server→client | REQUIRED |
| `40`      | `0x0028` | `unbind`         | S    | client→server | REQUIRED |
| `51`      | `0x0033` | `unbind-ok`      | S    | server→client | REQUIRED |

Note: `unbind-ok` has method-id `51` (`0x33`), not `41` — this is a
spec quirk because the spec authors inserted bind/unbind out of order.

### 5.1 exchange.declare (40.10)

| Field         | Type     | Notes                                     |
|---------------|----------|-------------------------------------------|
| `reserved-1`  | short    | MUST be `0`                               |
| `exchange`    | shortstr | exchange name                             |
| `type`        | shortstr | `direct`, `fanout`, `topic`, `headers`, or RabbitMQ plug-in name |
| `passive`     | bit      |                                           |
| `durable`     | bit      |                                           |
| `auto-delete` | bit      |                                           |
| `internal`    | bit      |                                           |
| `no-wait`     | bit      | If true, broker MUST NOT send `declare-ok` (v0 MUST send `false`) |
| `arguments`   | field-table |                                        |

The five bits (`passive, durable, auto-delete, internal, no-wait`)
pack into a single octet, LSB-first. v0 always sends `no-wait = 0`.

### 5.2 exchange.delete (40.20)

| Field         | Type     |
|---------------|----------|
| `reserved-1`  | short    |
| `exchange`    | shortstr |
| `if-unused`   | bit      |
| `no-wait`     | bit      |

### 5.3 exchange.bind / unbind (40.30 / 40.40)

| Field             | Type        |
|-------------------|-------------|
| `reserved-1`      | short       |
| `destination`     | shortstr    |
| `source`          | shortstr    |
| `routing-key`     | shortstr    |
| `no-wait`         | bit         |
| `arguments`       | field-table |

`exchange.bind` is a RabbitMQ extension (advertised via
`exchange_exchange_bindings` capability); LavinMQ 2.x supports it
likewise.

---

## 6. Class 50 — queue

| Method id | Hex      | Name           | Type | Direction | Status |
|-----------|----------|----------------|------|-----------|--------|
| `10`      | `0x000A` | `declare`      | S    | client→server | REQUIRED |
| `11`      | `0x000B` | `declare-ok`   | S    | server→client | REQUIRED |
| `20`      | `0x0014` | `bind`         | S    | client→server | REQUIRED |
| `21`      | `0x0015` | `bind-ok`      | S    | server→client | REQUIRED |
| `30`      | `0x001E` | `purge`        | S    | client→server | REQUIRED |
| `31`      | `0x001F` | `purge-ok`     | S    | server→client | REQUIRED |
| `40`      | `0x0028` | `delete`       | S    | client→server | REQUIRED |
| `41`      | `0x0029` | `delete-ok`    | S    | server→client | REQUIRED |
| `50`      | `0x0032` | `unbind`       | S    | client→server | REQUIRED |
| `51`      | `0x0033` | `unbind-ok`    | S    | server→client | REQUIRED |

### 6.1 queue.declare (50.10)

| Field         | Type        |
|---------------|-------------|
| `reserved-1`  | short       |
| `queue`       | shortstr    |
| `passive`     | bit         |
| `durable`     | bit         |
| `exclusive`   | bit         |
| `auto-delete` | bit         |
| `no-wait`     | bit         |
| `arguments`   | field-table |

queue.declare-ok response args:

| Field             | Type     |
|-------------------|----------|
| `queue`           | shortstr |
| `message-count`   | long     |
| `consumer-count`  | long     |

The codec MUST expose `queue` (echoed back, important when client
sent empty string for server-generated names) and the two counts in
the public Channel#queue_declare return value.

### 6.2 queue.bind (50.20)

| Field         | Type        |
|---------------|-------------|
| `reserved-1`  | short       |
| `queue`       | shortstr    |
| `exchange`    | shortstr    |
| `routing-key` | shortstr    |
| `no-wait`     | bit         |
| `arguments`   | field-table |

### 6.3 queue.unbind (50.50)

Identical to bind, except no-wait is absent in the spec layout
(the bit just isn't there). v0 supports unbind without no-wait per
spec.

### 6.4 queue.purge (50.30) and delete (50.40)

| purge field   | Type     | | delete field  | Type     |
|---------------|----------|-|---------------|----------|
| `reserved-1`  | short    | | `reserved-1`  | short    |
| `queue`       | shortstr | | `queue`       | shortstr |
| `no-wait`     | bit      | | `if-unused`   | bit      |
|               |          | | `if-empty`    | bit      |
|               |          | | `no-wait`     | bit      |

`purge-ok` returns `message-count` (long). `delete-ok` returns
`message-count` (long).

---

## 7. Class 60 — basic

| Method id | Hex      | Name              | Type | Direction | Status |
|-----------|----------|-------------------|------|-----------|--------|
| `10`      | `0x000A` | `qos`             | S    | client→server | REQUIRED |
| `11`      | `0x000B` | `qos-ok`          | S    | server→client | REQUIRED |
| `20`      | `0x0014` | `consume`         | S    | client→server | REQUIRED |
| `21`      | `0x0015` | `consume-ok`      | S    | server→client | REQUIRED |
| `30`      | `0x001E` | `cancel`          | S    | both          | REQUIRED |
| `31`      | `0x001F` | `cancel-ok`       | S    | both          | REQUIRED |
| `40`      | `0x0028` | `publish`         | A    | client→server | REQUIRED |
| `50`      | `0x0032` | `return`          | A    | server→client | REQUIRED |
| `60`      | `0x003C` | `deliver`         | A    | server→client | REQUIRED |
| `70`      | `0x0046` | `get`             | S    | client→server | REQUIRED |
| `71`      | `0x0047` | `get-ok`          | S    | server→client | REQUIRED |
| `72`      | `0x0048` | `get-empty`       | S    | server→client | REQUIRED |
| `80`      | `0x0050` | `ack`             | A    | client→server | REQUIRED |
| `90`      | `0x005A` | `reject`          | A    | client→server | REQUIRED |
| `100`     | `0x0064` | `recover-async`   | A    | client→server | OUT (v0) |
| `110`     | `0x006E` | `recover`         | S    | client→server | REQUIRED |
| `111`     | `0x006F` | `recover-ok`      | S    | server→client | REQUIRED |
| `120`     | `0x0078` | `nack`            | A    | both          | REQUIRED |

### 7.1 basic.qos (60.10)

| Field            | Type   |
|------------------|--------|
| `prefetch-size`  | long   |
| `prefetch-count` | short  |
| `global`         | bit    |

`prefetch-size` is an octet count; v0 always sends `0` (per
`docs/02-public-api.md` §4 — only count-based QoS is exposed).

### 7.2 basic.consume (60.20) and consume-ok (60.21)

`consume` args:

| Field           | Type        |
|-----------------|-------------|
| `reserved-1`    | short       |
| `queue`         | shortstr    |
| `consumer-tag`  | shortstr    |
| `no-local`      | bit         |
| `no-ack`        | bit         |
| `exclusive`     | bit         |
| `no-wait`       | bit         |
| `arguments`     | field-table |

`consume-ok` echoes back the assigned `consumer-tag` (shortstr).

### 7.3 basic.cancel (60.30) and cancel-ok (60.31)

`cancel` arg: `consumer-tag` (shortstr) + `no-wait` (bit).
`cancel-ok` arg: `consumer-tag` (shortstr).

Server-initiated `basic.cancel` is sent by the broker when the queue
underneath a consumer is deleted (RabbitMQ extension, advertised
via `consumer_cancel_notify` capability).

### 7.4 basic.publish (60.40)

| Field         | Type     |
|---------------|----------|
| `reserved-1`  | short    |
| `exchange`    | shortstr |
| `routing-key` | shortstr |
| `mandatory`   | bit      |
| `immediate`   | bit      |

`immediate` is removed in modern RabbitMQ; the codec MUST emit
`immediate=0` and treat broker-returned `NOT_IMPLEMENTED` for non-
zero `immediate` as a protocol violation surface.

The method frame is followed by a content header frame and zero or
more body frames (§9 of `00-frames.md`).

### 7.5 basic.return (60.50)

| Field         | Type     |
|---------------|----------|
| `reply-code`  | short    |
| `reply-text`  | shortstr |
| `exchange`    | shortstr |
| `routing-key` | shortstr |

Followed by content header + body. Sent by the broker for
unroutable publishes with `mandatory=1`. Correlation to the original
publish is by send-order on the channel (per `docs/07` §10.2).

**Recorded:** `spec/fixtures/frames/publish_mandatory_return/s2c.bin`.

### 7.6 basic.deliver (60.60)

| Field           | Type     |
|-----------------|----------|
| `consumer-tag`  | shortstr |
| `delivery-tag`  | longlong |
| `redelivered`   | bit      |
| `exchange`      | shortstr |
| `routing-key`   | shortstr |

Followed by content header + body. The `delivery-tag` is per-channel,
monotonically increasing.

### 7.7 basic.get / get-ok / get-empty (60.70 / 60.71 / 60.72)

`get` args: `reserved-1` (short) + `queue` (shortstr) + `no-ack`
(bit).

`get-ok` args:

| Field            | Type     |
|------------------|----------|
| `delivery-tag`   | longlong |
| `redelivered`    | bit      |
| `exchange`       | shortstr |
| `routing-key`    | shortstr |
| `message-count`  | long     |

Followed by content header + body.

`get-empty` args: `reserved-1` (shortstr), MUST be empty.

### 7.8 basic.ack / nack / reject (60.80 / 60.120 / 60.90)

`ack` args: `delivery-tag` (longlong) + `multiple` (bit).

`nack` args: `delivery-tag` (longlong) + `multiple` (bit) + `requeue`
(bit). RabbitMQ extension; published as `basic.nack` capability.

`reject` args: `delivery-tag` (longlong) + `requeue` (bit).

`ack` and `nack` are bi-directional:

- Client→server: caller acks/nacks a delivery.
- Server→client: broker acks/nacks a publish under publisher
  confirms (`confirm.select` mode).

The codec routes incoming server-side `ack`/`nack` to the channel's
confirm tracker (`docs/08-publisher-confirms.md` §4).

**Recorded ack:** `spec/fixtures/frames/publish_confirm/s2c.bin`.
**Recorded nack:** `spec/fixtures/frames/publish_nack/s2c.bin` —
contains `0x3C 0x78` (60.120) followed by `delivery-tag = 1`,
`multiple = 0`, `requeue = 0`.

---

## 8. Class 85 — confirm (RabbitMQ extension)

| Method id | Hex      | Name           | Type | Direction | Status |
|-----------|----------|----------------|------|-----------|--------|
| `10`      | `0x000A` | `select`       | S    | client→server | REQUIRED |
| `11`      | `0x000B` | `select-ok`    | S    | server→client | REQUIRED |

### 8.1 confirm.select (85.10)

Arg: `no-wait` (bit). v0 always sends `0`.

`select-ok` has no arguments.

Once selected, all publishes on the channel are tracked by the broker
with a monotonically-increasing delivery tag, and broker emits
`basic.ack` or `basic.nack` per publish (per the rules in
`docs/08-publisher-confirms.md`).

**Normative:** The codec MUST verify the broker's
`publisher_confirms: true` capability before sending `confirm.select`.
A broker that does not advertise this capability raises
`Amqp::ConfigurationError` at the `Channel#confirm_select` call site.

---

## 8.2 Class 90 — tx

| Method id | Hex      | Name          | Type | Direction | Status |
|-----------|----------|---------------|------|-----------|--------|
| `10`      | `0x000A` | `select`      | S    | client→server | REQUIRED |
| `11`      | `0x000B` | `select-ok`   | S    | server→client | REQUIRED |
| `20`      | `0x0014` | `commit`      | S    | client→server | REQUIRED |
| `21`      | `0x0015` | `commit-ok`   | S    | server→client | REQUIRED |
| `30`      | `0x001E` | `rollback`    | S    | client→server | REQUIRED |
| `31`      | `0x001F` | `rollback-ok` | S    | server→client | REQUIRED |

`tx.select`, `tx.commit`, and `tx.rollback` have no method arguments.
The public API exposes them as `Channel#tx_select`,
`Channel#tx_commit`, `Channel#tx_rollback`, and
`Channel#transaction`.

The shard does not promise to preserve an in-flight transaction across
automatic recovery. Use publisher confirms for recoverable publish
flows.

---

## 9. Reply-code → exception mapping

The codec maps `connection.close` / `channel.close` reply-codes to
the exception subclasses in `docs/03-error-model.md` §6. The
mapping table lives in the error model document; the codec MUST emit
the canonical mapping.

The codec MUST preserve `reply-text`, `class-id`, and `method-id`
when constructing the typed close exception so callers and tests can
inspect broker close details. v0 does not expose a separate public
`Amqp::CloseReason` value.

---

## 10. Dispatch tables

The codec implements two dispatch tables:

### 10.1 Decode dispatch

A two-level hash:
- First level: `class-id` (UInt16).
- Second level: `method-id` (UInt16) → decoder function.

The decoder function takes `(IO, channel_id)` and returns an
`Amqp::Wire::AmqpZeroNineOne::Method` variant (a sealed sum type
covering every supported `(class, method)`).

Unknown `(class-id, method-id)` raises `Amqp::ProtocolError`.

### 10.2 Encode dispatch

Each `Method` variant has a `#encode(io : IO, channel_id : UInt16)`
method that:

1. Writes the 1-byte frame type (`0x01`).
2. Writes channel-id.
3. Writes a placeholder length (4 bytes).
4. Writes class-id + method-id + arguments (computing length as we
   go).
5. Backfills the length via `IO::Memory` write-and-replace OR uses
   the build-to-memory-then-emit pattern from `01-types.md` §11.
6. Writes the `0xCE` frame-end.

Per `01-types.md` §11, the codec MUST NOT walk the arguments twice;
the build-to-memory pattern is mandatory.

---

## 11. Method-class invariants the codec enforces

- A `client→server` method MUST NOT be decoded on the read path;
  the codec MUST raise `Amqp::ProtocolError` if it is.
- A `server→client` method MUST NOT be emitted on the write path;
  the codec MUST raise `Amqp::ProtocolError` at the codec API
  boundary if it is.
- A method on the wrong channel (e.g., `connection.*` on a non-zero
  channel) MUST raise `Amqp::ProtocolError`.
- A method whose class+method id is OUT (v0) MUST raise
  `Amqp::ProtocolError` on the read path. On the write path, it
  cannot be constructed (the API doesn't expose it).

---

## 12. Recorded corpus references

| Method                       | Captured in                                                |
|------------------------------|------------------------------------------------------------|
| `connection.start`           | `handshake_success/s2c.bin` offset 0                       |
| `connection.start-ok`        | `handshake_success/c2s.bin` offset 8                       |
| `connection.tune` / `tune-ok`| `handshake_success/{s2c,c2s}.bin`                          |
| `connection.open` / `open-ok`| `handshake_success/{c2s,s2c}.bin`                          |
| `connection.close` (403)     | `handshake_auth_fail/s2c.bin` tail                         |
| `connection.close` (530)     | `handshake_vhost_deny/s2c.bin` tail                        |
| `channel.open` / `open-ok`   | `channel_open_close/c2s.bin`, `s2c.bin`                    |
| `channel.close` / `close-ok` | `channel_open_close/c2s.bin`, `s2c.bin`                    |
| `channel.close` from broker  | `broker_close_channel/s2c.bin`                             |
| `exchange.declare` / -ok     | `exchange_declare/{c2s,s2c}.bin`                           |
| `queue.declare` / -ok        | `queue_declare/{c2s,s2c}.bin`                              |
| `queue.declare` w/ args      | `field_table_types/c2s.bin`                                |
| `basic.qos` / qos-ok         | `basic_qos/{c2s,s2c}.bin`                                  |
| `basic.publish` (no confirm) | `publish_no_confirm/c2s.bin`                               |
| `basic.publish` w/ confirms  | `publish_confirm/c2s.bin`                                  |
| `basic.ack` (broker)         | `publish_confirm/s2c.bin`                                  |
| `basic.nack` (broker)        | `publish_nack/s2c.bin` — `0x3C 0x78`                       |
| `basic.return` + content     | `publish_mandatory_return/s2c.bin`                         |
| `basic.consume` / -ok        | `consume_ack/{c2s,s2c}.bin`                                |
| `basic.deliver` + content    | `consume_ack/s2c.bin`                                      |
| `basic.ack` (client)         | `consume_ack/c2s.bin`                                      |
| `basic.cancel` / -ok         | `consume_ack/{c2s,s2c}.bin` (tail)                         |
| `basic.get-ok` + content     | `basic_get/s2c.bin`                                        |
| `basic.get-empty`            | `basic_get/s2c.bin`                                        |
| `confirm.select` / -ok       | `publish_confirm/{c2s,s2c}.bin`                            |

See `04-recorded-frames.md` for byte-walk annotations of selected
frames.

---

## 13. Falsifier index

| Test ID                  | Claim                                                  |
|--------------------------|--------------------------------------------------------|
| T-CODEC-METHOD-001       | Every REQUIRED method round-trips encode→decode        |
| T-CODEC-METHOD-002       | Unknown class-id raises ProtocolError                  |
| T-CODEC-METHOD-003       | Unknown method-id within known class raises            |
| T-CODEC-METHOD-004       | `client→server` method on read path raises             |
| T-CODEC-METHOD-005       | `server→client` method on write path raises            |
| T-CODEC-METHOD-006       | OUT-of-v0 method on read path raises                   |
| T-CODEC-METHOD-007       | `connection.*` on non-zero channel raises              |
| T-CODEC-METHOD-008       | `basic.publish` followed by content frames in order    |
| T-CODEC-METHOD-009       | `connection.close` reply-code mapped to exception      |
| T-CODEC-METHOD-010       | `basic.nack` 60.120 decoded correctly                  |
| T-CODEC-METHOD-011       | `confirm.select` without broker capability raises      |
| T-CODEC-METHOD-012       | `queue.declare-ok` echoes message/consumer counts      |
| T-CODEC-METHOD-013       | `basic.return` reply-code preserved in exception       |
| T-CODEC-METHOD-014       | Five-bit-pack in exchange.declare bits LSB-first       |
| T-CODEC-METHOD-015       | `basic.cancel` from broker handled as Subscription end |
