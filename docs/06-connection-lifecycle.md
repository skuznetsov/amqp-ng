# amqp — Connection Lifecycle

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Amqp::Connection`.
> **Companions:** `docs/02-public-api.md` (the surface this document
> drives), `docs/03-error-model.md` (which exceptions surface where),
> `docs/04-uri-and-config.md` (how options reach this lifecycle),
> `docs/05-wire-0-9-1/02-classes-methods.md` (the connection.* methods
> on the wire), `docs/10-heartbeats.md`, `docs/11-tls.md`,
> `docs/12-recovery.md`.

This document is the **complete** normative state machine for a
`Connection` instance. The implementation MUST follow these transitions
exactly; every transition has a falsifier in `docs/16-falsifier-matrix.md`.

---

## 1. States

A `Connection` is in exactly one of the following states at any time:

| State           | Reachable from               | Meaning                                                            |
|-----------------|------------------------------|--------------------------------------------------------------------|
| `Initial`       | (constructor)                | Object exists; no socket yet.                                      |
| `Connecting`    | `Initial`                    | TCP/TLS handshake in progress.                                     |
| `Negotiating`   | `Connecting`                 | AMQP handshake in progress (start/start-ok/tune/tune-ok/open).     |
| `Open`          | `Negotiating`                | Handshake complete; channels can be opened.                        |
| `Closing`       | `Open`                       | Caller invoked `close` OR broker sent `connection.close`.          |
| `Closed`        | `Closing`, any error state   | Terminal. All resources released, `close_reason` populated.        |
| `Recovering`    | `Closed`, only if `Recovery::Full` | Recovery pipeline active; see `docs/12-recovery.md`.         |

`Recovering` may transition back to `Connecting` (a fresh attempt) and
eventually to `Open` or to `Closed` if recovery is abandoned.

The state is implementation-private; callers observe it via `closed?`
and `close_reason` (`docs/02-public-api.md` §3). The states are listed
here only to constrain the implementation.

### 1.1 Transition diagram

```
                ┌─────────┐
                │ Initial │
                └────┬────┘
                     │ Amqp.connect()
                     ▼
              ┌────────────┐  TCP/TLS fail → Closed (ConnectError subclass)
              │ Connecting │─────────────────────────────────┐
              └─────┬──────┘                                 │
                    │ TCP+TLS up                             │
                    ▼                                        │
              ┌──────────────┐  start-ok rejected →          │
              │ Negotiating  │  Closed (AuthenticationError) │
              └──────┬───────┘                               │
                     │ open-ok received                      │
                     ▼                                       │
                ┌────────┐  close/error                      │
                │  Open  │────────────────┐                  │
                └────────┘                ▼                  │
                                    ┌─────────┐              │
                                    │ Closing │              │
                                    └────┬────┘              │
                                         ▼                   ▼
                                    ┌────────┐    Recovery::None
                                    │ Closed │◄──────────────┘
                                    └───┬────┘
                                        │ Recovery::Full + recoverable cause
                                        ▼
                                  ┌─────────────┐
                                  │ Recovering  │
                                  └──────┬──────┘
                                         │ retry → Connecting
                                         │ surrender → Closed
                                         ▼
```

---

## 2. Constructor

```crystal
Amqp.connect(url, **opts) : Connection
```

The call MUST execute the steps below sequentially. Each step is a
transition described in §3..§8. On failure at any step the state goes
directly to `Closed` and the call raises the appropriate
`Amqp::Error` subclass; no partial `Connection` is returned.

1. Parse and validate URI + options (`docs/04-uri-and-config.md`).
2. (TCP) Open a TCP socket to `host:port`, bounded by remaining
   `connect_timeout`.
3. (TLS, if `amqps://`) Wrap the socket in `OpenSSL::SSL::Socket::Client`,
   complete the TLS handshake, bounded by remaining `connect_timeout`.
4. Write the AMQP protocol header.
5. Read `connection.start`, send `connection.start-ok` (auth).
6. Read `connection.tune`, send `connection.tune-ok` (negotiated
   parameters).
7. Send `connection.open(vhost)`, read `connection.open-ok`.
8. Start the frame-reader fiber and the heartbeat fiber.
9. Return the `Connection` in state `Open`.

The block form (`Amqp.connect(url) { |conn| ... }`) MUST call `close`
in an `ensure` block whose body completes before the surrounding
exception propagates.

---

## 3. TCP and TLS handshake

### 3.1 TCP

The shard MUST use stdlib `TCPSocket.new(host, port, connect_timeout)`,
where `connect_timeout` is the **remaining** budget at this step:

```
remaining = total_timeout - (Time.monotonic - start)
```

If `remaining <= 0.seconds`, the shard MUST raise
`Amqp::ConnectTimeoutError` before invoking the socket constructor.

On stdlib `Socket::ConnectError` the shard wraps as
`Amqp::ConnectRefusedError` (subclass of `ConnectError`) with `cause`
preserved. On stdlib `IO::TimeoutError` it raises
`Amqp::ConnectTimeoutError`.

**Falsifier:** T-CONN-TCP-001 (refused), T-CONN-TCP-002 (timeout
honored), T-CONN-TCP-003 (DNS failure → `ConnectRefusedError` with
cause exposing the resolver error).

### 3.2 TLS

If the scheme is `amqps`, the shard MUST wrap the socket as:

```crystal
ssl_sock = OpenSSL::SSL::Socket::Client.new(
  tcp_socket,
  context: tls_context,
  hostname: uri.host,            -- SNI; mandatory
  sync_close: true               -- closing ssl_sock closes tcp_socket
)
```

`hostname:` is the SNI server-name AND the name verified against the
certificate's SAN/CN list (when `verify == peer`).

TLS handshake errors:

- `OpenSSL::SSL::Error` with verification-related message →
  `Amqp::TlsHandshakeError` with `cause`.
- `OpenSSL::SSL::Error` due to protocol version / cipher mismatch →
  also `Amqp::TlsHandshakeError`.
- `IO::TimeoutError` during handshake → `Amqp::ConnectTimeoutError`.

Detailed TLS rules — including cipher policy, certificate-rotation
guidance, and the EXTERNAL-mechanism interaction — live in
`docs/11-tls.md`.

**Falsifier:** T-TLS-* (in 11-tls.md).

---

## 4. AMQP protocol header

After the (optionally TLS-wrapped) socket is up, the shard writes
exactly 8 bytes:

```
'A' 'M' 'Q' 'P' 0x00 0x00 0x09 0x01
```

These are: the literal four bytes of "AMQP", a reserved zero byte,
and the three-byte major/minor/revision triple `0/9/1`.

The shard MUST NOT write any other protocol header. If the broker
responds by closing the socket and sending its own protocol header
(indicating the broker requires a different version), the shard
reads up to 8 bytes, raises `Amqp::ProtocolNegotiationError` carrying
the broker's advertised major/minor/revision in `close_reason.reply_text`
formatted as `"AMQP <M>.<m>.<r>"`, and transitions to `Closed`.

Brokers in scope (RabbitMQ 3.13+, LavinMQ 2.x) always speak 0-9-1, so
this path is informational; the shard MUST nonetheless handle it
defensively for robustness against misconfigured peers.

**Falsifier:** T-CONN-HEADER-001 (correct bytes), T-CONN-HEADER-002
(wrong-version broker response → `ProtocolNegotiationError`).

---

## 5. `connection.start` and `connection.start-ok`

### 5.1 Reading `connection.start`

The broker sends `connection.start` on channel 0 immediately after
accepting the protocol header. Fields:

| Field             | Type           | Used by shard?                            |
|-------------------|----------------|-------------------------------------------|
| version-major     | octet          | Must be 0 (per AMQP 0-9-1).               |
| version-minor     | octet          | Must be 9.                                |
| server-properties | field-table    | Stored on Connection (read-only).         |
| mechanisms        | long-string    | Space-separated; checked against requested.|
| locales           | long-string    | Send back `en_US` in start-ok.            |

If `version-major != 0` or `version-minor != 9`, the shard MUST raise
`Amqp::ProtocolNegotiationError` and transition to `Closed`.

If the requested SASL mechanism (PLAIN or EXTERNAL, per
`docs/04-uri-and-config.md` §4) is NOT in the broker's `mechanisms`
list, the shard MUST raise `Amqp::AuthenticationError` synchronously
with `reply_text` reporting both lists.

### 5.2 Sending `connection.start-ok`

Fields:

| Field              | Value                                                       |
|--------------------|-------------------------------------------------------------|
| client-properties  | field-table; see §5.3                                       |
| mechanism          | "PLAIN" or "EXTERNAL"                                       |
| response           | mechanism-specific bytes (`docs/04-uri-and-config.md` §4)  |
| locale             | "en_US"                                                     |

### 5.3 `client-properties` field-table

Required keys:

| Key            | Type        | Value                                              |
|----------------|-------------|----------------------------------------------------|
| `product`      | longstr     | from option `product:`, default `"amqp.cr"`        |
| `version`      | longstr     | `Amqp::VERSION`                                    |
| `platform`     | longstr     | `"Crystal #{Crystal::VERSION}"`                    |
| `information`  | longstr     | from option `information:`, default `""`           |
| `capabilities` | field-table | see §5.4                                           |

The implementation MAY add other keys (e.g., `os`, `pid`) for diagnostic
purposes; brokers ignore unknown keys.

### 5.4 `capabilities` sub-table

The shard advertises:

| Capability                     | Type | Value |
|--------------------------------|------|-------|
| `publisher_confirms`           | bool | true  |
| `exchange_exchange_bindings`   | bool | true  |
| `basic.nack`                   | bool | true  |
| `consumer_cancel_notify`       | bool | true  |
| `connection.blocked`           | bool | true  |
| `authentication_failure_close` | bool | true  |

`authentication_failure_close = true` is what gets the broker to send
`connection.close` (rather than just closing the socket) when auth
fails, so the shard can surface a useful `AuthenticationError`. The
shard MUST advertise this capability.

**Falsifier:** T-CONN-STARTOK-001..004 — corpus-driven; the encoded
`start-ok` bytes for fixed inputs match the recorded reference frame.

---

## 6. `connection.tune` and `connection.tune-ok`

### 6.1 Reading `connection.tune`

Fields:

| Field          | Type   | Meaning                                       |
|----------------|--------|-----------------------------------------------|
| channel-max    | short  | Broker's max channel id (0 = unlimited).      |
| frame-max      | long   | Broker's max frame size in bytes (0 = unlim).|
| heartbeat      | short  | Broker's proposed heartbeat interval (s).     |

### 6.2 Reconciliation

The shard computes the final negotiated values per
`docs/04-uri-and-config.md` §5:

```
channel_max = reconcile(client_proposed, broker_value)  -- min, 0 = unlimited
frame_max   = reconcile(client_proposed, broker_value)  -- min, 0 = unlimited
heartbeat   = client_proposed != 0 ? client_proposed : broker_value
```

`frame_max` floor is 4096 (AMQP minimum). If the broker proposes less
than 4096 the shard MUST raise `Amqp::ProtocolNegotiationError`.

The final `frame_max` becomes the maximum size of any body or method
payload the shard sends; the shard MUST split message bodies larger
than `frame_max - 8` (8 = frame header + frame-end byte) into multiple
body frames.

### 6.3 Sending `connection.tune-ok`

Echo the negotiated values back to the broker. After this point no
further negotiation occurs; the shard moves to `connection.open`.

`Connection#heartbeat`, `#channel_max`, `#frame_max` MUST be set to the
negotiated values before any user-visible operation runs.

**Falsifier:** T-CONN-TUNE-001..005 (per §5 of 04-uri-and-config.md).

---

## 7. `connection.open` and `connection.open-ok`

The shard sends:

| Field        | Type      | Value                              |
|--------------|-----------|------------------------------------|
| virtual-host | shortstr  | Vhost (decoded per `04` §1.2).     |
| reserved-1   | shortstr  | Empty string.                      |
| reserved-2   | bit       | False.                             |

On `connection.open-ok` the connection enters state `Open`.

Possible failures at this step:

- Broker sends `connection.close` with reply-code 403 →
  `Amqp::VhostAccessError`.
- Broker sends `connection.close` with reply-code 530 →
  `Amqp::ConnectionClosedByBroker`.
- Socket EOF → `Amqp::SocketError`.

The shard MUST NOT enter `Open` unless `open-ok` is received.

**Falsifier:** T-CONN-OPEN-001..003.

---

## 8. Steady-state `Open`

In `Open`:

- One frame-reader fiber is running.
- One heartbeat fiber is running (even if `heartbeat == 0`, in which
  case it sleeps forever; this simplifies teardown).
- Per-channel inboxes (`::Channel(Frame)`) exist for every open
  channel. The map from `UInt16` channel-id to inbox is protected by
  an internal `Mutex` for channel allocation; reads from steady-state
  pathways are lock-free.
- A connection-level write `Mutex` serialises all socket writes
  (publisher, channel-open, heartbeat). Lock contention is bounded by
  the size of a single frame write.

The shard MUST observe the following invariants in `Open`:

1. Channel-id 0 is reserved for `connection.*` and `channel.close` (in
   limited cases the spec allows); no user channel uses channel-id 0.
2. The frame-reader fiber NEVER calls into user-supplied callbacks
   directly; it routes frames to per-channel inboxes only.
3. Heartbeat frames sent by the shard MUST be of type 8 (heartbeat),
   channel-id 0, payload empty.
4. The connection MUST NOT silently drop incoming frames. An incoming
   frame for an unknown channel-id is a protocol violation and surfaces
   as `Amqp::ProtocolError`.

**Falsifier:** T-CONN-INV-001..004.

---

## 9. `Closing` and `Closed`

### 9.1 Caller-initiated close

`Connection#close` (with optional `reply_code:`, `reply_text:`) MUST:

1. Atomically transition `Open → Closing` (CAS); if already non-`Open`,
   return silently.
2. Acquire the write mutex, send `connection.close` with the supplied
   code/text and `class_id=0, method_id=0`, release the mutex.
3. Wait up to `max(heartbeat, 5.seconds)` for `connection.close-ok`
   from the frame-reader fiber. On timeout, proceed to step 4 anyway.
4. Close the socket (TLS layer first if present, then TCP).
5. Drain per-channel inboxes by closing them with a sentinel.
6. Wake all fibers blocked on publish_confirm / queue.declare-ok /
   etc. with `Amqp::ConnectionClosedByCaller`.
7. Stop the heartbeat fiber via its wakeup channel.
8. Transition `Closing → Closed`.
9. Populate `close_reason`.

The call returns normally; the caller does NOT see
`ConnectionClosedByCaller`. Other fibers blocked on the connection
DO see it.

### 9.2 Broker-initiated close

When the frame-reader observes `connection.close` from the broker:

1. Atomically transition `Open → Closing`.
2. Acquire the write mutex, send `connection.close-ok`, release.
3. Close the socket.
4. Drain per-channel inboxes; wake fibers with the appropriate
   subclass (`Amqp::ConnectionClosedByBroker` or a more specific
   subclass per the reply-code mapping in `docs/03-error-model.md` §3).
5. Stop the heartbeat fiber.
6. Transition `Closing → Closed`; populate `close_reason`.

If `recovery_mode == Recovery::Full` AND the close is recoverable
(per `docs/03-error-model.md` §5), the recovery pipeline starts; see
`docs/12-recovery.md`.

### 9.3 Heartbeat-timeout close

When the heartbeat fiber detects the receive deadline has been
exceeded, it MUST:

1. Atomically transition `Open → Closing`.
2. Close the socket WITHOUT sending `connection.close` (the broker
   appears to be gone; sending would block).
3. Wake fibers with `Amqp::HeartbeatTimeoutError`.
4. Stop itself.
5. Transition `Closing → Closed`; populate `close_reason`.

### 9.4 Socket-level failure

When the frame-reader observes `IO::Error`, `EOFError`, or
`OpenSSL::SSL::Error` during a read:

1. Atomically transition `Open → Closing`.
2. Wake fibers with `Amqp::SocketError`, `cause` populated.
3. Close the socket (best effort).
4. Stop the heartbeat fiber.
5. Transition `Closing → Closed`; populate `close_reason`.

### 9.5 `Closed` is terminal (without recovery)

In `Recovery::None`, `Closed` is terminal. Any user-visible operation
on a closed connection MUST raise the latest `close_reason`-derived
exception. The shard MUST NOT silently no-op.

In `Recovery::Full`, the connection transitions to `Recovering` (see
`docs/12-recovery.md`); user operations attempted during `Recovering`
block until `Open` is reached again or the recovery pipeline
surrenders.

**Falsifier:** T-CONN-CLOSE-001..010 covering each closure pathway and
each `close_reason.origin` value.

---

## 10. Channel allocation

`Connection#channel` MUST:

1. Check state is `Open` (else raise the appropriate closed-state
   exception).
2. Allocate the next free channel-id in `1..channel_max` (inclusive)
   under the channel-allocation mutex. Allocation strategy: smallest
   free id. The implementation MUST keep allocation O(1) amortised
   by maintaining a free-id structure (bitmap, range tree, or
   equivalent).
3. Create the per-channel inbox (`::Channel(Frame)` with a small
   bounded capacity; the value is implementation-defined but MUST
   apply backpressure to the frame-reader rather than drop).
4. Construct a `Channel` object referencing the connection and the
   inbox.
5. Send `channel.open(reserved=""), read `channel.open-ok` synchronously
   (the very first operation on the new channel).
6. Return the `Channel` in state `Open` (see
   `docs/07-channel-lifecycle.md`).

If `channel_max` is exhausted, raise `Amqp::ChannelLimitError`.

`Connection#channel(id : UInt16)` does the same but with a caller-
supplied id; on collision raise `Amqp::ChannelInUseError`; if `id`
is outside `1..channel_max`, raise `Amqp::ConfigurationError`.

**Falsifier:** T-CONN-CHAN-001..004.

---

## 11. Server-blocked notifications

RabbitMQ implements `connection.blocked` / `connection.unblocked`
notifications (the broker tells the client "I'm at resource limit;
publishes will stall"). The shard MUST:

- Accept both methods on channel 0.
- Update an internal flag observable via `ConnectionStats#blocked?`.
- NOT slow down publishes synthetically — the broker's TCP backpressure
  is already the mechanism that stalls them. Surfacing the flag is for
  the caller's observability.

LavinMQ implements the same methods; behavior matches.

**Falsifier:** T-CONN-BLOCKED-001 — stub broker sends `blocked`,
verifies `stats.blocked?` flips; `unblocked` flips it back.

---

## 12. Fiber discipline

### 12.1 Frame-reader fiber

Pseudocode:

```crystal
spawn(name: "amqp.reader[#{conn_id}]") do
  loop do
    frame = decode_frame(socket)
    update_last_received_ns
    case frame
    when MethodFrame::ConnectionClose
      handle_broker_close(frame); break
    when MethodFrame::ChannelClose
      route(frame); next
    when HeartbeatFrame
      next                          -- already updated last_received_ns
    else
      route(frame)
    end
  end
rescue ex : IO::Error | EOFError | OpenSSL::SSL::Error
  handle_socket_failure(ex)
rescue ex : Amqp::ProtocolError
  handle_protocol_violation(ex)
end
```

The fiber MUST NOT hold locks across the blocking read.

### 12.2 Heartbeat fiber

Pseudocode (skipping when `heartbeat == 0`):

```crystal
spawn(name: "amqp.heartbeat[#{conn_id}]") do
  send_interval = heartbeat / 2
  recv_deadline = heartbeat * 2
  loop do
    select
    when stop_chan.receive
      break
    when timeout(send_interval)
      write_heartbeat_frame_if_idle    -- only if no other write happened
      check_recv_deadline_or_kill
    end
  end
end
```

`check_recv_deadline_or_kill` reads `last_received_ns` (an
`Atomic(Int64)` updated by the reader fiber) and triggers §9.3 if the
deadline is exceeded.

See `docs/10-heartbeats.md` for full rules.

**Falsifier:** T-CONN-FIBERS-001..003 — exactly two fibers per
connection (plus consumer-loop fibers), CPU sample under idle shows
no busy-looping.

---

## 13. Resource cleanup

On every path to `Closed`:

1. The socket MUST be closed (best-effort; ignore IO errors during
   close).
2. The heartbeat fiber's stop channel MUST be signalled.
3. Per-channel inboxes MUST be closed.
4. The free-id structure MUST be reset (recovery may reuse the
   connection object).
5. Counters in `ConnectionStats` MUST be frozen (atomic reads continue
   to work; writes stop).

The shard MUST NOT leak fibers. Tests assert this by counting
`Fiber.list.size` before and after a `connect`/`close` cycle (with
a fudge for stdlib's internal fibers).

**Falsifier:** T-CONN-LEAK-001 — 1000 connect/close cycles, fiber
count stable.

---

## 14. Anti-patterns

- **Calling `Amqp.connect` from inside a `select` branch.** The call
  is long-running; a `select` semantically expects fast resolution.
  Callers wanting nonblocking-style connect should `spawn` the
  connect themselves.
- **Manipulating the socket directly via `Connection.@socket`.** The
  shard does not expose the socket. P-1 forbids ivar access; P-8
  forbids a public accessor that would leak abstractions.
- **Re-using a `Closed` connection.** Even in `Recovery::Full` the
  user-visible `Connection` reference is the same object — but the
  shard internally cycles through `Closed → Recovering → Open`. The
  caller MUST NOT inspect `closed?` and conditionally re-open; that
  is recovery's job.
