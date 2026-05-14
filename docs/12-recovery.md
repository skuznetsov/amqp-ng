# amqp — Recovery

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Recovery::Full`; callers deciding
> whether to opt in.
> **Companions:** `docs/01-design-principles.md` (P-10 recovery is
> opt-in and complete), `docs/03-error-model.md` §5 (recoverable vs
> fatal), `docs/06-connection-lifecycle.md`, `docs/07-channel-lifecycle.md`,
> `docs/08-publisher-confirms.md` §8.

Recovery is the most surface-touching feature in the shard. The
choice to opt in changes the semantics of every other public method.
This document is the **complete** specification.

---

## 1. Modes

```crystal
enum Amqp::Recovery
  None
  Full
  # Manual reserved for v0.x, NOT v0
end
```

### 1.1 `Recovery::None` (default)

When the connection dies (any cause), the `Connection` object enters
state `Closed` and stays there. All blocked fibers receive the
appropriate `Amqp::Error` subclass. The caller is responsible for
constructing a new `Connection` and re-establishing topology and
consumers.

This is the safe default. `Recovery::None` connections have no hidden
state across reconnects because there are no reconnects.

### 1.2 `Recovery::Full`

The shard:

- Records every successful topology declaration made via this
  connection's API (`queue_declare`, `exchange_declare`, bindings).
- Records every active consumer (queue + arguments + the
  `Subscription` instance or the block-form `consume` fiber).
- Tracks every unconfirmed in-flight publish (the confirm tracker
  from `docs/07-channel-lifecycle.md` §10).

On any recoverable failure (per `docs/03-error-model.md` §5), the
shard:

1. Transitions the connection to `Recovering`.
2. Loops, attempting reconnect with backoff.
3. On success, re-opens channels (re-using the user-visible
   `Channel` objects), re-declares topology, re-installs consumers,
   re-publishes unconfirmed messages.
4. Fires registered `on_recovery` callbacks.
5. Transitions back to `Open`.

On unrecoverable failure (or exhausting the retry budget), the shard
surrenders: `Recovering → Closed`, all blocked fibers wake with
`Amqp::RecoveryAbandoned` (cause = the underlying failure).

### 1.3 `Recovery::Manual` (deferred)

Sketched here for completeness; NOT v0 scope.

The intent: the shard reconnects the socket and re-authenticates,
but the caller drives topology and consumer re-creation via
callbacks. Useful for callers whose topology is dynamic and whose
authoritative source is something other than the shard's recording.

Deferred to v0.x.

---

## 2. Triggering conditions

`Recovery::Full` triggers reconnect on closures whose
`close_reason.origin` is one of:

- `Network` (`SocketError`)
- `Heartbeat` (`HeartbeatTimeoutError`)
- `Broker`, EXCEPT when the reply-code maps to:
  - 320 connection-forced (broker shutting down) — recoverable
  - 530 not-allowed when text indicates auth — NOT recoverable
  - 503/501/502/505 protocol-related — NOT recoverable
  - 506 resource-error — recoverable
  - 540 not-implemented — NOT recoverable
  - 541 internal-error — recoverable (broker bug; retry)

`Recovery::Full` does NOT trigger on:

- `close_reason.origin == Caller` — caller wanted the close;
  reversing it would surprise.
- Unrecoverable broker codes per `docs/03-error-model.md` §5.
- Initial connect failure in `Amqp.connect`: the shard does not
  retry the initial connect even in `Recovery::Full` mode. Recovery
  applies only after at least one `Open` has been reached.

**Falsifier:** T-REC-TRIGGER-001..N (one per origin).

---

## 3. Recording protocol

For `Recovery::Full` to work, the shard MUST record:

### 3.1 Topology

Every successful call to:

- `exchange_declare` (name, type, durable, auto_delete, internal,
  arguments)
- `queue_declare` (name, durable, exclusive, auto_delete, arguments;
  for server-named queues, record the assigned name)
- `queue_bind` (queue, exchange, routing_key, arguments)
- `exchange_bind` (destination, source, routing_key, arguments)

is appended to a per-connection topology log. Operations on the
log are:

- `record(operation)` after broker confirms the operation.
- `forget(operation)` after `queue_delete` / `exchange_delete` /
  `queue_unbind` / `exchange_unbind` succeeds.

`passive: true` declarations MUST NOT be recorded (they don't
create state, they only verify it; replaying them on a fresh
broker would mean nothing).

### 3.2 Consumers

Every successful `basic.consume` is recorded with:

- queue name (resolved if server-named)
- consumer tag (resolved)
- auto_ack, exclusive, no_local
- arguments
- the user-visible `Subscription` reference (for the object form)
  OR the user block + the `consume` call's fiber (for the block
  form)

`basic.cancel` and channel close remove the consumer from the
record.

### 3.3 Publisher confirms

The confirm tracker from `docs/07-channel-lifecycle.md` §10 already
records every unconfirmed publish, including the full message body,
properties, exchange, routing-key, and `mandatory` flag. The
recovery pipeline re-publishes these.

### 3.4 Channel state

The shard records, per channel:

- Whether `confirm_select` was called.
- Last `basic.qos` (prefetch) parameters.
- `channel.flow` state (typically inactive).

After re-open, the shard MUST re-apply these in order:
`confirm.select` → `basic.qos` → topology → consumers → in-flight
re-publish.

---

## 4. Reconnect loop

When recovery triggers:

```crystal
def recovery_loop
  attempt = 0
  backoff = initial_backoff   # e.g., 500.milliseconds
  loop do
    attempt += 1
    begin
      fresh_socket = open_socket(uri, opts)
      fresh_socket = wrap_tls(fresh_socket, opts) if amqps?
      perform_handshake(fresh_socket, opts)
      install_fresh_socket
      reapply_channel_state
      reapply_topology
      reinstall_consumers
      republish_unconfirmed
      transition_to_open
      fire_on_recovery_callbacks
      break
    rescue ex : Amqp::Error
      if Amqp::Error.recoverable?(ex.class) && attempt < max_attempts
        Log.info { "recovery attempt #{attempt} failed: #{ex.message}; backing off #{backoff}" }
        sleep backoff
        backoff = min(backoff * 2, max_backoff)
      else
        surrender(ex)
        break
      end
    end
  end
end
```

### 4.1 Backoff schedule

The shard MUST use exponential backoff with jitter:

- `initial_backoff = 500.milliseconds`
- `max_backoff = 30.seconds`
- `max_attempts = 12` (≈ 5 minutes at saturation; tunable)
- jitter: ±20% on each delay, computed via `Random::DEFAULT`.

Tunables are NOT exposed in v0 to keep the surface minimal; if a
caller needs different values they can run their own reconnect at
`Recovery::None`. v0.x may add tunables.

**Falsifier:** T-REC-BACKOFF-001 (delays match schedule under
deterministic Random).

### 4.2 Surrender

After `max_attempts` failures the shard transitions to `Closed`
with `close_reason.origin == Recovery`. All blocked fibers wake
with `Amqp::RecoveryAbandoned` (cause = the latest underlying
failure). The connection cannot be re-recovered; the caller must
construct a new `Connection`.

**Falsifier:** T-REC-ABANDON-001.

---

## 5. Re-apply order

After the fresh socket is up and AMQP-handshake-complete, the shard
applies recorded state in this exact order:

1. **For each recorded channel** (in original allocation order):
   a. Open the channel via `channel.open` / `channel.open-ok`.
      Assign the same id if available; if the broker rejects
      because of channel_max changes, allocate a fresh id and
      update `Channel#id`. Fire `on_recovery` with a note that the
      id changed (so callers who cached the id know).
   b. If `confirm_select` was previously called, send
      `confirm.select` / `confirm.select-ok`.
   c. If `basic.qos` was previously set, send `basic.qos` /
      `basic.qos-ok`.
2. **Exchange declarations** in recording order.
3. **Queue declarations** in recording order. For server-named queues
   whose name was previously assigned, declare with the recorded
   name (no longer server-named); the server SHOULD honour it.
4. **Bindings** (`queue_bind`, `exchange_bind`) in recording order.
5. **Consumer registrations** (`basic.consume`) for each recorded
   consumer.
6. **In-flight publish replay** (per channel) — see §6.

If ANY step fails with a recoverable error, the entire reconnect
attempt is rolled back (the fresh socket is closed) and the loop
retries. If a step fails with an unrecoverable error, the recovery
pipeline surrenders.

**Falsifier:** T-REC-ORDER-001..006 (each step in isolation),
T-REC-ALL-001 (full sequence end-to-end against a real broker).

---

## 6. In-flight publish replay

For each channel with `confirms_enabled?`:

1. Snapshot the confirm tracker's pending tags BEFORE any re-publish.
2. For each pending tag in monotonic order:
   a. Re-emit `basic.publish` + header + body with the original
      arguments.
   b. The fresh channel assigns a NEW delivery tag. The recovery
      code maps `old_tag → new_tag` and re-registers the destination
      under `new_tag`.
3. Clear the snapshot; subsequent ack/nack on the new tags resolves
   the original destinations.

**Important duplication note.** A publish whose original ack was lost
mid-failure may be successfully delivered both times — once before the
crash (the broker received it but its ack didn't reach us) and once
after. Consumers MUST treat redeliveries as possible. The shard
surfaces this clearly in `docs/15-reliability-contract.md` REL-AT-
LEAST-ONCE.

**Falsifier:** T-REC-REPLAY-001 — kill broker after publish but
before ack; recovery re-publishes; the destination's
`publish_confirm` eventually returns `true`.

---

## 7. User-visible behavior during recovery

While the connection is `Recovering`:

- **`publish*`** blocks on a wakeup channel that fires when state
  becomes `Open` (recovery succeeded) or `Closed` (surrender). On
  `Open` the publish proceeds normally; on `Closed` it raises
  `Amqp::RecoveryAbandoned`.
- **`Subscription#receive`** blocks similarly. Deliveries arrive
  after the fresh consumer is installed; their `redelivered` flag
  is what the broker says (typically `false` for messages first
  published after recovery, `true` for older messages requeued
  during the dead window).
- **Topology calls** (`queue_declare`, etc.) block until `Open` or
  `Closed`. They DO NOT re-execute during recovery; if the
  caller's call landed AFTER the original disconnect but BEFORE
  recovery completed, it runs once on the fresh channel.
- **`close`** on a `Recovering` connection MUST cancel recovery,
  close the (recovering) socket if any, surrender, and return.

The caller observes recovery only via:

- `on_recovery` callbacks (fire on each successful re-establishment).
- Latency spikes corresponding to the dead window.
- `ConnectionStats#recoveries_completed` counter.

**Falsifier:** T-REC-DURING-001..004.

---

## 8. `on_recovery` callbacks

```crystal
conn.on_recovery do |event : Amqp::RecoveryEvent|
  Log.info { "recovered: #{event.channels_reopened} channels, " \
             "#{event.consumers_restored} consumers, " \
             "#{event.republished} publishes" }
end
```

```crystal
struct Amqp::RecoveryEvent
  getter attempts : Int32                -- how many tries it took
  getter dead_window : Time::Span        -- from disconnect to fresh Open
  getter channels_reopened : Int32
  getter consumers_restored : Int32
  getter republished : Int32             -- in-flight publishes replayed
  getter channel_id_changes : Hash(UInt16, UInt16)   -- old → new, when applicable
end
```

Callbacks fire on the recovery fiber AFTER `Recovering → Open`. They
MUST NOT block; long-running work belongs in a spawned fiber by the
callback itself. The shard catches exceptions from callbacks and
logs at `Log::Severity::Error`; it does not propagate them (a buggy
callback should not crash recovery).

Multiple callbacks fire in registration order.

**Falsifier:** T-REC-CB-001 (fires on success), T-REC-CB-002
(exception in callback logged not propagated), T-REC-CB-003
(callback order matches registration).

---

## 9. Memory and durability constraints

The shard's recovery records consume memory bounded by:

- **Topology log.** O(N) where N is the number of declared
  entities. Each entry is small (names + a few flags + arguments
  table). For 10k queues, expect ~MB-scale.
- **Confirm tracker.** O(M) where M is in-flight unconfirmed
  publishes. Bodies are retained in memory until ack arrives. A
  caller publishing 1 GB of in-flight unconfirmed data is using
  1 GB of resident memory.

The shard MUST NOT swap these to disk. Persistence is the caller's
job; AMQP at-least-once guarantees apply only to in-broker state.

**Falsifier:** T-REC-MEM-001 — after 10 minutes of steady-state
publish/ack, the recovery records' RSS is bounded.

---

## 10. Anti-patterns

- **Treating `Recovery::Full` as a silver bullet.** It re-establishes
  the connection but it does NOT make at-least-once into
  exactly-once. Idempotent consumers are still required.
- **Caching `Channel#id` across recovery.** Per §5.1, the id may
  change. The user-visible `Channel` reference is stable; the id
  may not be.
- **Long-running work inside `on_recovery`.** Blocks the next
  recovery attempt's callback ordering. Spawn a fiber from the
  callback if work is non-trivial.
- **Enabling recovery on a connection that is not used for durable
  work.** Recovery is appropriate for long-lived worker connections
  and publishers with confirms. For short scripts that connect,
  publish, and exit, `Recovery::None` is correct.
- **Enabling recovery and ALSO running your own retry loop.** The
  two compete: recovery is reconnecting under you while your loop
  thinks the connection is dead and tries to construct a new one.
  Pick one.
