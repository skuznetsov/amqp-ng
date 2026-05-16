# amqp — Observability

> **Document status:** Draft v0.2, 2026-05-15
> **Audience:** Implementers of stats/logging; operators integrating
> the shard with monitoring.
> **Companions:** `docs/02-public-api.md`, `docs/14-performance-contract.md`.

The current v0 implementation exposes one connection-level stats object:
`Amqp::Stats`. Earlier drafts described `ConnectionStats`,
`ChannelStats`, and `SubscriptionStats`; those richer per-surface
snapshots are deferred until implemented and covered by falsifiers.

---

## 1. `Amqp::Stats`

`Connection#stats` returns an `Amqp::Stats` counter object. Callers read
an immutable snapshot:

```crystal
snap = conn.stats.snapshot
```

`Amqp::Stats::Snapshot` exposes:

```crystal
getter published : Int64
getter confirmed_ack : Int64
getter confirmed_nack : Int64
getter returned : Int64
getter consumed : Int64
getter recoveries_attempted : Int64
getter recoveries_succeeded : Int64
getter recoveries_failed : Int64
```

Counters are monotonic for the lifetime of a `Connection` instance.
They are backed by atomics in the implementation.

**Falsifier:** `T-OBS-STATS-001..N`; current focused coverage lives in
`spec/stats_spec.cr`.

---

## 2. Update timing

- `published`: incremented after a publish frame sequence is flushed to
  the socket.
- `confirmed_ack`: incremented when a tracked publish settles as acked.
- `confirmed_nack`: incremented when a tracked publish settles as nacked.
- `returned`: incremented when `basic.return` is observed.
- `consumed`: incremented when a delivery is routed to a subscription.
- `recoveries_attempted`: incremented when automatic recovery starts an
  attempt.
- `recoveries_succeeded`: incremented after successful automatic
  recovery.
- `recoveries_failed`: incremented when automatic recovery gives up or a
  non-retryable recovery failure is observed.

These are event counters. They are not a complete state model; for
example, a publish timeout is not currently represented as a separate
counter.

---

## 3. Deferred stats model

The following surfaces are deferred, not v0 public API:

- `Amqp::ConnectionStats`
- `Amqp::ChannelStats`
- `Amqp::SubscriptionStats`
- TLS version/cipher/peer-certificate stats
- per-channel publish/confirm depth
- subscription buffer depth/high-water
- connection ids in public stats

When implemented, the falsifier matrix should promote the reserved
`T-OBS-CONN-*`, `T-OBS-CHAN-*`, and `T-OBS-SUB-*` rows from roadmap to
active checks.

---

## 4. Logging

The shard uses Crystal stdlib `Log`. v0 logging is intentionally
minimal and best-effort; it is not yet a stable observability contract.

The implementation must not log message bodies. Properties and routing
metadata should only appear at debug-level diagnostics when explicitly
enabled by the caller's `Log` configuration.

**Future harness:** `T-OBS-LOG-001`.

---

## 5. Anti-patterns

- **Treating v0 stats as a full telemetry model.** They are basic
  counters, not tracing or latency histograms.
- **Computing rates inside the shard.** The shard exports counters;
  callers compute rates according to their scrape interval.
- **Using debug logs in production by default.** Frame-level and
  metadata-level traces are high volume and may expose operational
  details.
