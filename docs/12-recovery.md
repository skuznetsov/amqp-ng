# amqp — Recovery

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Recovery::Full`; callers deciding
> whether to opt in.
> **Companions:** `docs/01-design-principles.md` (P-10 recovery is
> opt-in), `docs/03-error-model.md`, `docs/06-connection-lifecycle.md`,
> `docs/07-channel-lifecycle.md`,
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

On any internally recoverable session failure, the
shard:

1. Transitions the connection to `Recovering`.
2. Loops, attempting reconnect with backoff.
3. On success, re-opens channels (re-using the user-visible
   `Channel` objects), re-declares topology, re-installs consumers,
   re-publishes unconfirmed messages.
4. Transitions back to `Open`.

On unrecoverable failure (or exhausting the retry budget), the shard
surrenders: `Recovering → Closed`, all blocked fibers wake with
`Amqp::RecoveryExhaustedError` or the underlying typed failure.

### 1.3 `Recovery::Manual` (deferred)

Sketched here for completeness; NOT v0 scope.

The intent: the shard reconnects the socket and re-authenticates,
but the caller drives topology and consumer re-creation via
callbacks. Useful for callers whose topology is dynamic and whose
authoritative source is something other than the shard's recording.

Deferred to v0.x.

---

## 2. Triggering conditions

`Recovery::Full` triggers reconnect on connection loss after the
connection reached `Open`, when the implementation classifies the
failure as network-like or broker-forced and not caller-initiated.
Broker-forced means AMQP `connection.close` reply-code `320`
(`CONNECTION_FORCED`) or `541` (`INTERNAL_ERROR`); other broker close
reply-codes remain terminal unless mapped to a more specific
non-recoverable error.

`Recovery::Full` does NOT trigger on:

- caller-initiated close — caller wanted the close; reversing it
  would surprise.
- configuration, authentication, vhost, TLS, or protocol-negotiation
  failures.
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
- consumer tag (caller-supplied, or the v0 client-generated
  `amqp-ng-ctag-*` tag when the caller passed `""`)
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
      break
    rescue ex : Amqp::Error
      if internally_recoverable?(ex) && attempt < max_attempts
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

After `max_attempts` failures the shard transitions to `Closed`.
All blocked fibers wake with `Amqp::RecoveryExhaustedError` or the
latest underlying typed failure. The connection cannot be
re-recovered; the caller must construct a new `Connection`.

**Falsifier:** T-REC-ABANDON-001.

---

## 5. Re-apply order

After the fresh socket is up and AMQP-handshake-complete, the shard
applies recorded state in this exact order:

1. **For each recorded channel** (in original allocation order):
   a. Open the channel via `channel.open` / `channel.open-ok`.
      Assign the same id if available; if the broker rejects
      because of channel_max changes, recovery fails closed; v0 does
      not remap public channel ids.
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

If a server-named queue is re-created with a different broker-assigned
name, recovery MUST remap recorded bindings, consumers, and in-flight
publishes addressed through the default exchange (`exchange == ""`,
`routing_key == old_queue_name`) to the new queue name before replay.

If ANY step fails with a recoverable error, the entire reconnect
attempt is rolled back (the fresh socket is closed) and the loop
retries. If a step fails with an unrecoverable error, the recovery
pipeline surrenders.

On replay rejection by the broker, including topology declaration or
consumer re-registration rejection, v0 MUST fail closed with
`Amqp::RecoveryExhaustedError`; it MUST NOT leave a partially recovered
connection available for further caller operations.

**Falsifier:** T-REC-TOPO-FAIL-001, T-REC-CONS-FAIL-001.

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

- **New `publish*` calls** raise `Amqp::RecoveryInProgress`
  synchronously. v0 deliberately does not queue new caller work during
  recovery; callers who want retry semantics wrap the operation and
  retry after recovery completes.
- **Unconfirmed publishes already registered before the disconnect**
  are still owned by the confirm tracker and are replayed per §6.
- **`Subscription#receive`** continues to block on the subscription
  mailbox. Deliveries resume after the fresh consumer is installed;
  their `redelivered` flag is what the broker says (typically `false`
  for messages first published after recovery, `true` for older
  messages requeued during the dead window).
- **Topology calls** (`queue_declare`, etc.) raise
  `Amqp::RecoveryInProgress` synchronously. They are not queued or
  replayed unless the broker had already confirmed them before the
  disconnect and they entered the topology log.
- **`close`** on a `Recovering` connection MUST cancel recovery,
  close the (recovering) socket if any, surrender, and return.

The caller observes recovery only via:

- Latency spikes corresponding to the dead window.
- `Connection#recovery_mode`.
- `conn.stats.snapshot.recoveries_attempted`,
  `recoveries_succeeded`, and `recoveries_failed`.

**Falsifier:** T-REC-DURING-001 (new operations fail fast with
`RecoveryInProgress`), T-REC-DURING-002 (pre-existing subscription
receives after recovery), T-REC-DURING-003 (pre-disconnect
unconfirmed publish replay resolves), T-REC-DURING-004 (`close`
cancels recovery).

---

## 8. Recovery callbacks (deferred)

`Connection#on_recovery` and `Amqp::RecoveryEvent` are not v0 public
API. Callers that need lifecycle hooks should wrap `Amqp.connect` and
their own retry loop around `Recovery::None`, or poll
`conn.stats.snapshot` counters in v0.

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
- **Caching `Channel#id` across recovery.** v0 attempts to reopen the
  same id and fails closed if that cannot be done; callers should still
  treat the user-visible `Channel` reference as the stable handle.
- **Enabling recovery on a connection that is not used for durable
  work.** Recovery is appropriate for long-lived worker connections
  and publishers with confirms. For short scripts that connect,
  publish, and exit, `Recovery::None` is correct.
- **Enabling recovery and ALSO running your own retry loop.** The
  two compete: recovery is reconnecting under you while your loop
  thinks the connection is dead and tries to construct a new one.
  Pick one.
