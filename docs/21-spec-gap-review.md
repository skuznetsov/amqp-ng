# amqp — Spec Gap Review

> **Document status:** Working review, 2026-05-15
> **Scope:** `docs/08-publisher-confirms.md`, `docs/09-consumer.md`,
> `docs/12-recovery.md`, `docs/16-falsifier-matrix.md`, current `src/`
> and `spec/`.

This is a hostile review of what the current specs do not falsify yet.
It is not a replacement for `docs/16-falsifier-matrix.md`; it is the
ranked repair queue for converting the matrix into executable evidence.

## Baseline

- Latest no-broker full `crystal spec --error-trace` passes: 221 examples, 0 failures, 0 errors, 92 pending.
- Most no-broker pending examples are live AMQP checks. With a plain broker available, the remaining default pending examples are gated live checks: TLS (`AMQP_TLS_URL`), invasive backpressure (`AMQP_BACKPRESSURE_LIVE`), and Docker broker chaos (`AMQP_CHAOS_DOCKER_CONTAINER`).
- Latest focused LavinMQ publish/confirm smoke passes: 72 examples, 0 failures, 0 errors, 0 pending.
- Latest focused LavinMQ no-wait/API smoke passes: 39 examples, 0 failures, 0 errors, 0 pending.
- The earlier local full opt-in suite passed before later parity additions; rerun it before turning this local branch into a public release artifact.
- `tools/perf_publish.cr` now exists as a local publish benchmark witness, but `spec/perf/` remains a roadmap item.
- `Time.monotonic` deprecation warnings were removed.

## Quadrumvirate Synthesis

- **Cassandra:** The highest-risk pattern is `VERIFICATION_THEATER`: many
  normative `T-*` rows exist, but only a small subset is executable.
- **Daedalus:** The useful pivot is from "add more API surface" to "make
  the falsifier matrix executable by failure mode".
- **Maieutic:** The weakest assumption was that API-shape specs imply
  behavioral correctness. They do not; compile shape missed several
  state/order/close semantics.
- **Adversary:** A green broker happy-path suite can still miss confirm
  ordering bugs, recovery duplicate/loss bugs, consumer cancellation bugs,
  and config rejection bugs.

## Blockers

### B1. Recovery during-window semantics were contradictory

`docs/12-recovery.md` says `publish*`, `Subscription#receive`, and
topology calls block while the connection is `Recovering` and resume
or raise after recovery resolves. Current specs instead assert that
`open_channel` raises `RecoveryInProgress` during the recovery window,
and `Channel#ensure_open!` raises `RecoveryInProgress` for recovering
channels.

Evidence:
- `docs/12-recovery.md` §7 says `publish*` and topology calls block.
- `spec/recovery_spec.cr` asserts `open_channel` raises
  `RecoveryInProgress`.
- `src/amqp/channel.cr` gates recovering channels in `ensure_open!`.
- `src/amqp/connection.cr` gates `open_channel` while recovering.

Decision: v0 uses fail-fast semantics for new operations during
recovery. The docs were updated to match the existing code and new
falsifier coverage.

Proposed falsifiers:
- `T-REC-DURING-001`: `open_channel`, `publish`, `get`, and
  `queue_declare` raise `RecoveryInProgress` during recovery.

### B2. Recovery replay is not tested at the commit-boundary that matters

Docs require every unconfirmed in-flight publish to be retained,
re-published after reconnect, and resolved through the original
`publish_confirm`/`publish_async` destination. Current recovery specs
verify topology replay and post-recovery publishing, but not a publish
that is in-flight at disconnect.

Evidence:
- `docs/12-recovery.md` §6 defines old-tag to new-tag replay.
- `docs/08-publisher-confirms.md` §8 requires original caller outcome.
- `spec/recovery_spec.cr` publishes only after recovery completes.

Proposed falsifiers:
- `T-REC-REPLAY-001`: disconnect after publish frames before ack; original
  `publish_confirm` resolves after replay.
- `T-REC-REPLAY-002`: same for `publish_async` outcome channel.
- `T-REC-REPLAY-003`: mandatory returned publish survives replay without
  losing the return reason.

## Major Gaps

### M1. Publisher confirm negative paths are mostly unfalsified

The current confirm specs cover confirm mode, monotonic tags, batch
`wait_for_confirms`, and mandatory return. They do not exercise broker
nack, publish timeout, late ack after timeout, unknown delivery tag,
channel close while `publish_confirm` is waiting, or async close without
value.

Evidence:
- `docs/08-publisher-confirms.md` §5.1-§5.2 lists nack, timeout, and
  close behavior.
- `docs/08-publisher-confirms.md` §7 requires out-of-order tag handling.
- `docs/16-falsifier-matrix.md` lists `T-PUB-CONFIRM-001..006`,
  `T-PUB-ASYNC-001..004`, `T-PUB-MULTIPLE-001..002`.

Proposed falsifiers:
- `T-PUB-CONFIRM-002`: broker nack raises `PublishNackError`.
- `T-PUB-CONFIRM-005`: silent broker path raises `PublishTimeoutError`
  and later ack does not corrupt tracker state.
- `T-PUB-CONFIRM-006`: channel close while waiting wakes the caller.
- `T-PUB-MULTIPLE-002`: unknown/out-of-order ack raises
  `PublishOutOfOrderError` or docs are narrowed to current behavior.
- `T-PUB-ASYNC-003`: channel close closes outcome channel without value.

### M2. Consumer block-form and spawn-loop semantics are not covered

Docs specify block-form `consume(queue) { ... }`, exception reject,
auto-ack warning behavior, and `spawn_loop` ack/reject behavior. Current
live consumer specs mostly use object-style `consume(...).receive` and do
not cover the documented block form or exception paths.

Evidence:
- `docs/09-consumer.md` §2.2-§2.3 define block exception handling.
- `docs/09-consumer.md` §3.7 defines `spawn_loop` reject semantics.
- `docs/16-falsifier-matrix.md` lists `T-CONS-BLOCK-*` and
  `T-CONS-SPAWN-001`.

Proposed falsifiers:
- `T-CONS-BLOCK-001`: block-form consume delivers one message and exits on
  cancel.
- `T-CONS-BLOCK-002`: block exception with manual ack rejects/requeues.
- `T-CONS-SPAWN-001`: `spawn_loop` exception rejects/requeues.

### M3. Subscription cancellation and backpressure are weakly specified by tests

Docs require caller cancel drain, broker-side cancel handling, and
backpressure when the subscription buffer fills. Current specs now prove
cancel drain and broker cancel. An opt-in live timing harness covers
slow-subscription routing, but eventual whole-connection backpressure
under channel-inbox saturation still needs a safer measurement.

Evidence:
- `docs/09-consumer.md` §3.4-§3.6.
- `docs/16-falsifier-matrix.md` lists `T-CONS-BACKPRESSURE-001` and
  `T-CONS-CANCEL-001..003`.

Proposed falsifiers:
- `T-CONS-CANCEL-001`: caller `sub.close` allows already-buffered
  deliveries to be drained, then closes.
- `T-CONS-CANCEL-002`: queue deletion causes broker cancel and closes the
  subscription.
- `T-CONS-BACKPRESSURE-001`: unread buffer blocks its channel handler;
  unrelated channel RPCs continue until channel-inbox saturation.

### M4. URI/config negative matrix was too thin

Docs require query-key rejection, precedence rules, vhost encoding,
userinfo handling, and TLS scheme conflicts. The core URI/config
negative slice now has focused coverage; TLS handshake behavior still
needs live or synthetic broker falsifiers.

Evidence:
- `docs/16-falsifier-matrix.md` §2 and §9.
- `src/amqp/config.cr` rejects unknown query keys and parses the v0
  query surface: `heartbeat`, `channel_max`, `frame_max`,
  `connect_timeout`, `tcp_nodelay`, `buffer_size`, `recovery`,
  `product`, `information`.
- `spec/config_spec.cr` covers defaults/vhost decoding, recognized
  query keys, keyword precedence, unknown keys, strict numeric parsing,
  socket-tuning validation, recovery query validation, and plain-AMQP
  plus TLS-context rejection.

Converted falsifiers:
- `T-URI-UNKNOWN-001`: unknown query key raises `UriError` listing the key.
- Representative `T-URI-PRECEDENCE-*`: keyword values override query values.
- Representative `T-URI-VHOST-*`: slash-containing and empty vhost forms.
- Representative `T-TLS-SCHEME-*`: `amqps://` implies TLS and
  `amqp://` plus a TLS context is rejected before socket open.

Remaining falsifiers:
- Live TLS handshake and certificate failure rows remain environment-gated
  through `AMQP_TLS_URL` / `AMQP_TLS_CA_CERT` rather than default-local.

## Already Converted During This Review

- `T-CONS-SELECT-001`: `select when msg = sub.receive` now has a
  compile-time spec and `Subscription` delegates select actions to its
  internal `::Channel`.
- `Subscription` default buffer was aligned to documented capacity 16.
- URI unknown-key, representative precedence/vhost cases, numeric
  coercion, socket-tuning validation, TLS-context conflict, and
  recovery-query rejection now have focused coverage in
  `spec/config_spec.cr` / `spec/tls_spec.cr`.

## Recommended Next Order

1. Keep hardening `Recovery::Full` around partial topology replay,
   duplicate publishes, and consumer-tag conflicts; default-safe guards
   now cover the during-window behavior, caller-close finality,
   broker-forced reconnect, server-named queue remap, and pending
   confirm replay.
2. Extend consumer backpressure coverage only with bounded,
   process-isolated harnesses. The safe live harness covers the
   per-channel-first claim; full channel-inbox saturation remains too
   invasive for the default suite.
3. Add URI/config table exhaustiveness only if the public URI surface
   changes; representative unknown-key, precedence, numeric coercion,
   recovery query, and TLS conflict coverage already exists.
4. Revisit performance after a reproducible `spec/perf/` harness exists.
