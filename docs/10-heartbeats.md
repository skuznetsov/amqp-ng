# amqp — Heartbeats

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of the heartbeat fiber and the receive-
> deadline mechanism.
> **Companions:** `docs/01-design-principles.md` (P-7 reliability is
> binary), `docs/04-uri-and-config.md` §5 (negotiation),
> `docs/06-connection-lifecycle.md` §9.3 (timeout teardown),
> `docs/15-reliability-contract.md`.

Heartbeats are how an AMQP client and broker detect a peer that has
silently dropped off the network. The mechanism is mandatory in v0:
every connection has a heartbeat fiber, even when the negotiated
interval is 0 (the fiber exists but does no work).

---

## 1. Negotiation recap

The heartbeat interval is the number of **seconds** between expected
peer activity:

- Client proposes via `Amqp.connect(heartbeat: 30.seconds)` or
  `?heartbeat=30`. The value is converted to whole seconds; sub-second
  precision is NOT supported by AMQP and the shard MUST reject
  fractional `Time::Span` values silently by truncating toward 0 (so
  `0.9.seconds` becomes 0, meaning "disabled" — same as 0).
- Broker proposes via `connection.tune.heartbeat`.
- Reconciliation: per `docs/04-uri-and-config.md` §5, the client's
  `tune-ok` echoes `client_proposed` if non-zero, else
  `broker_proposed`.

The final value is exposed as `Connection#heartbeat : Time::Span`.

A value of `0.seconds` means **heartbeats are disabled**. The shard
MUST still run the heartbeat fiber, but it sleeps on its stop-channel
and does no other work.

**Falsifier:** T-HB-NEG-001..004.

---

## 2. The two deadlines

AMQP 0-9-1 defines heartbeat semantics in §4.2.7 (informative) and
§4.7 (normative). The two relevant rules:

1. **Send rule.** A peer MUST send a frame (heartbeat or otherwise)
   at least every `heartbeat` seconds. The shard MUST therefore send
   at least one frame every `heartbeat` seconds even when otherwise
   idle.
2. **Receive rule.** A peer MUST consider the connection dead if no
   frame is received for `2 * heartbeat` seconds.

The shard implements both:

- **Send cadence:** `heartbeat / 2`. Conservative — sends at twice the
  required rate to absorb scheduler jitter.
- **Receive deadline:** `heartbeat * 2`. Exactly per spec.

When negotiated `heartbeat == 0`, both rules are vacuous; the shard
sends no heartbeats and applies no receive deadline.

---

## 3. Send mechanics

The heartbeat fiber loop (skipping when `heartbeat == 0`):

```crystal
loop do
  select
  when stop_chan.receive
    break
  when timeout(heartbeat / 2)
    now = Time.monotonic
    if now - last_write_at >= heartbeat / 2
      write_heartbeat_frame
    end
    check_receive_deadline
  end
end
```

Key rules:

- **Frame format.** Frame type 8, channel-id 0, frame-payload length
  0, frame-end byte `0xCE`. Total 8 bytes on the wire. See
  `docs/05-wire-0-9-1/00-frames.md`.
- **Coalescing.** The shard MUST NOT send a heartbeat if it has
  already written a frame within the last `heartbeat / 2` seconds.
  `last_write_at` is an `Atomic(Int64)` (Unix nanos) updated by
  every successful socket write. This avoids redundant writes during
  high-traffic periods.
- **Locking.** The heartbeat send uses the connection-level write
  mutex like any other frame. Contention with publishes is bounded
  by the size of one frame.
- **Failure.** A write failure during heartbeat send triggers the
  same socket-failure path as the frame-reader fiber (§9.4 of
  `docs/06-connection-lifecycle.md`). The heartbeat fiber MUST NOT
  retry within the same iteration — the connection is dead.

**Falsifier:** T-HB-SEND-001 (cadence under idle), T-HB-SEND-002
(coalescing under load), T-HB-SEND-003 (write failure → connection
close).

---

## 4. Receive deadline mechanics

The frame-reader fiber updates `last_received_ns` (an `Atomic(Int64)`)
on EVERY successful frame read — method, header, body, OR heartbeat.
The heartbeat fiber reads `last_received_ns` each iteration:

```crystal
def check_receive_deadline
  return if heartbeat.zero?
  deadline_ns = last_received_ns.get + (heartbeat * 2).total_nanoseconds.to_i64
  now_ns = Time.monotonic.to_unix_ns        # monotonic time, comparable
  if now_ns >= deadline_ns
    raise_heartbeat_timeout
  end
end
```

The `raise_heartbeat_timeout` action:

1. Atomically transition `Open → Closing`.
2. Close the socket (without writing `connection.close` — the broker
   is presumed dead).
3. Wake all blocked fibers with `Amqp::HeartbeatTimeoutError` whose
   `close_reason.origin == Heartbeat`.
4. Stop the frame-reader fiber (it will see EOF on its next read and
   exit cleanly).
5. Stop self after the wakeups complete.

**Time semantics.** All times are `Time.monotonic`. The shard MUST
NOT use `Time.utc` / wall-clock time for deadlines: NTP step
adjustments would cause spurious timeouts or missed timeouts.

**Falsifier:** T-HB-RECV-001 (timeout fires when broker stops sending),
T-HB-RECV-002 (no false positive under sustained activity),
T-HB-RECV-003 (wall-clock jump does not affect deadline),
T-HB-RECV-004 (`HeartbeatTimeoutError.close_reason.origin == Heartbeat`).

---

## 5. Interaction with publishes and consumes

Heartbeats DO NOT pause user-visible operations. A heartbeat frame
arrives on the inbox of the frame-reader and is dropped there
(`docs/06-connection-lifecycle.md` §8 invariant 3: frame-reader does
not route heartbeat frames anywhere); the only effect is
`last_received_ns` being updated, which is what we want.

Publishing while the heartbeat fiber is in the middle of writing is
serialised by the write mutex. The longest a publish can wait on the
mutex due to heartbeat alone is one frame-write — bounded.

A consumer fiber blocked on `Subscription#receive` is unblocked
either by an inbound delivery OR by a fiber-wakeup from a connection
close (heartbeat timeout, broker close, etc.). The wakeup carries the
exception via the inbox's closing mechanism.

**Falsifier:** T-HB-NOPAUSE-001 — under sustained publish load, the
shard sends heartbeats at the configured cadence with no measurable
extra latency on publishes (statistical test, not exact equality).

---

## 6. Disabling heartbeats

Setting `heartbeat: 0.seconds` (or `?heartbeat=0`) disables both rules.
The shard MUST:

- Still start the heartbeat fiber, but the loop body is just `select`
  on the stop channel without a timeout.
- NOT enforce any receive deadline.
- NOT send any heartbeat frames.

Disabled heartbeats are appropriate only for short-lived test
connections or for callers running their own keepalive. The shard
MUST log at `Log::Severity::Info` once per connection when
heartbeats are disabled (so operators see the choice in logs).

**Falsifier:** T-HB-OFF-001 — `heartbeat=0` connection survives an
idle period far exceeding any default deadline.

---

## 7. TCP keepalive (informative)

TCP-level keepalive (SO_KEEPALIVE) is a different mechanism that the
shard does NOT enable by default. AMQP-level heartbeats are sufficient
and more meaningful (they exercise the AMQP framing layer, not just
the kernel's TCP state).

Callers who want TCP keepalive in addition to AMQP heartbeats MAY
configure it on the socket after `Amqp.connect` returns — except the
shard does not expose the socket (P-1). For v0, TCP keepalive is
deliberately out of reach; if the use case proves real, a future
`?tcp_keepalive=...` query key can be added.

---

## 8. Anti-patterns

- **Heartbeat as user-visible latency budget.** Setting
  `heartbeat: 1.second` to make timeouts faster is wrong: the receive
  deadline becomes 2 seconds, which means transient network jitter
  on a long-lived idle connection triggers spurious timeouts.
  Production defaults SHOULD be `15..60` seconds; below 10 is
  inappropriate.
- **Treating `HeartbeatTimeoutError` as a configuration bug.** It
  isn't — it is the shard correctly reporting that the broker has
  gone silent. The fix is either (a) the network/broker (operational)
  or (b) `Recovery::Full` (architectural).
- **Disabling heartbeats to "stop seeing those errors."** Errors then
  surface as `SocketError` minutes later (when the TCP stack gives
  up) instead of within `2 * heartbeat`. Heartbeats make failures
  surface promptly; that is the value.
