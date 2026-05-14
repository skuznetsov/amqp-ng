# amqp — Performance Contract

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers; benchmarkers; reviewers asking "is this
> shard fast enough."
> **Companions:** `docs/01-design-principles.md` (P-6 observable),
> `docs/15-reliability-contract.md` (reliability is binary; perf is
> bounded), `docs/16-falsifier-matrix.md`, `docs/19-observability.md`.

This document lists the **measurable performance claims** the v0
shard makes. Each claim is identified `PERF-N` and is paired with a
falsifier benchmark in `spec/perf/`. The claims are bounds, not
targets — the shard MUST meet them; it MAY exceed them.

Numbers are **per-connection** unless stated, on the reference rig
(see §7). Different hardware will produce different numbers; the
falsifier benchmarks scale via §7's normalisation procedure.

---

## 1. Reference rig

All numbers in this document assume:

- **Host.** AWS `c7i.2xlarge` (8 vCPU Sapphire Rapids, 16 GB RAM,
  ENA SR-IOV, 12.5 Gbps network), Ubuntu 24.04, Crystal `1.20.0-stable`
  (or floor-version equivalent).
- **Broker.** RabbitMQ 3.13.7-management on the same host, Docker
  net=host. Standard memory + disk-watermark defaults.
- **Network.** loopback for handshake-latency tests, dedicated
  c7i.2xlarge over 12.5 Gbps for throughput tests.
- **Message body.** 256 bytes ASCII unless stated.

Benchmarks live under `spec/perf/<perf-id>.cr`. Each writes its
recorded numbers to `spec/perf/results/<perf-id>.txt` as the
acceptance witness.

---

## 2. PERF-1: Handshake latency

**Claim.** The full `Amqp.connect` against a local RabbitMQ over plain
TCP completes in `< 10 ms` p99 over 1000 sequential connections.

**Why this number.** TCP connect on loopback is sub-millisecond;
AMQP handshake is two round-trips (start/start-ok, tune/tune-ok)
plus one for `connection.open/open-ok`. Three loopback RTTs +
broker handling SHOULD fit in 10 ms.

**Falsifier:** `T-PERF-HANDSHAKE-001` opens 1000 connections
sequentially against a local RabbitMQ, records p99, asserts `< 10 ms`.

**TLS variant.** PERF-1-TLS: same target with `amqps://` and a
freshly-issued cert, claim `< 50 ms` p99. TLS handshake dominates;
50 ms is generous.

---

## 3. PERF-2: Publish throughput, fire-and-forget

**Claim.** A single producer publishing 256-byte messages
fire-and-forget (`Channel#publish`, no confirm) on one channel to one
queue on a local broker sustains `> 200,000 messages/second` over
a 10-second steady-state window.

**Why this number.** The shard is fiber-driven; the bottleneck is
either the broker's ingest or the socket's serialization. On the
reference rig, RabbitMQ ingests fire-and-forget AMQP messages at
~300k/s. The shard MUST NOT add more than ~30% overhead.

**Falsifier:** `T-PERF-PUB-001`.

---

## 4. PERF-3: Publish throughput, confirms

**Claim.** A single producer using `Channel#publish_async` on a
confirms channel, draining the outcome channel concurrently in a
spawned fiber, sustains `> 50,000 messages/second` over a 10-second
window.

**Why this number.** Confirms add a round-trip in spirit but RabbitMQ
batches acks; the shard's tracker uses a sorted-map for O(log N)
single-tag resolution. ~50k/s is the conservative bound; 100k/s+ is
plausible.

**Falsifier:** `T-PERF-PUB-002`.

**Synchronous variant.** PERF-3-SYNC: `Channel#publish_confirm` from
a single fiber (waiting per-publish) sustains `> 5000 messages/second`.
The synchronous round-trip dominates.

---

## 5. PERF-4: Multi-channel throughput

**Claim.** Eight producer fibers, each on its own channel, publishing
fire-and-forget on one connection, sustain `> 800,000 messages/second`
aggregate over 10 seconds. (Linear scaling × 8 from PERF-2; in
practice the write mutex contends, so ~600k is realistic.)

**Why this matters.** Validates that the connection-level write
mutex does not serialise channels too aggressively. If this claim
fails, the design choice in `docs/00-overview.md` §2.2 (writes on
caller's fiber under mutex) is questionable.

**Falsifier:** `T-PERF-MULTICHAN-001`.

---

## 6. PERF-5: Consume throughput

**Claim.** A single consumer (block-form `consume`, `auto_ack: false`,
acking inline after each message, `prefetch: 1000`) draining a
pre-loaded queue of 1M messages on a local broker sustains
`> 100,000 messages/second` from first delivery to last ack.

**Why this number.** Consume is read-heavy (no write mutex contention
on the publish path, but ack frames go on the write mutex). Prefetch
1000 keeps the broker delivering ahead of the consumer.

**Falsifier:** `T-PERF-CONS-001`.

---

## 7. PERF-6: Memory per idle connection

**Claim.** An idle `Connection` with one open channel (no consumers,
no in-flight publishes) consumes `< 64 KB` of Crystal heap as
measured by `GC.stats.heap_size` delta.

**Why this matters.** Connections are sometimes pooled in hundreds.
64 KB × 500 = 32 MB; acceptable.

**Falsifier:** `T-PERF-MEM-001` opens N connections, measures heap
delta divided by N.

---

## 8. PERF-7: Memory per buffered delivery

**Claim.** A `Subscription` with `buffer: 100` holding 100 unread
256-byte deliveries consumes `< 64 KB` of heap (`< 640 bytes` per
delivery including the `DeliverMessage` struct, properties, and the
ring buffer slot).

**Why this matters.** Subscriptions are sometimes high-buffer
(thousands). Memory per delivery is the multiplier.

**Falsifier:** `T-PERF-MEM-002`.

---

## 9. PERF-8: GC pressure

**Claim.** Publishing 1M messages of 256 bytes fire-and-forget
results in `< 200` GC cycles (full collections), as reported by
`GC.stats.collections` delta.

**Why this matters.** A shard that allocates per-message in the hot
path is unusable. The implementation MUST reuse encode buffers;
`docs/05-wire-0-9-1/00-frames.md` will document the buffer-reuse
contract for the codec.

**Falsifier:** `T-PERF-GC-001`.

---

## 10. PERF-9: Stats read overhead

**Claim.** Reading `Connection#stats` 1M times in a tight loop on
the reference rig takes `< 1 second` wall time (i.e., per-call cost
`< 1 µs`).

**Why this matters.** Stats are observed at scrape frequency; if
each read is expensive, observability is too costly to deploy.

**Falsifier:** `T-PERF-STATS-001`.

---

## 11. PERF-10: Recovery dead window

**Claim.** With `Recovery::Full` against a local broker that is
restarted (full process kill + restart, no quorum), the median dead
window (from disconnect to first successful `publish_confirm` on a
recovered channel) is `< 2 seconds`.

**Why this number.** Initial backoff is 500 ms; the broker's restart
takes ~1 s on the reference rig; one or two retries fit in the
budget.

**Falsifier:** `T-PERF-RECOV-001` automates the broker restart.

---

## 12. How benchmarks pass / fail

Each `spec/perf/T-PERF-*.cr` runs:

1. A warm-up of `warmup_seconds` (default 2) NOT included in
   measurement.
2. A measurement window of `window_seconds` (default 10).
3. Asserts the bound from this document.
4. Writes `spec/perf/results/T-PERF-N.txt` with: the measured value,
   the bound, pass/fail, host fingerprint (CPU model, OS, Crystal
   version), and timestamp.

CI runs the perf suite on every PR. A regression of more than 10%
relative to the previous-merge baseline is a release-blocking
warning, even if the absolute bound is still met.

The CI baseline is the `main` branch's most recent successful perf
run; the comparison is automated.

---

## 13. Non-claims

The shard does NOT claim:

- **End-to-end latency.** This depends on broker version, queue
  type (classic vs quorum vs stream), disk speed, network. The
  shard contributes a small bounded share; the rest is the broker.
- **Burst handling beyond the stated steady-state.** The numbers
  above are 10-second averages. Sub-second bursts may show more or
  less; not guaranteed.
- **Linear scaling beyond 8 channels.** PERF-4 is at 8; beyond that,
  write-mutex contention dominates and the shard offers no
  guarantee.
- **Microbenchmarks of individual operations.** A 4-arg method
  call's nanosecond cost is whatever the Crystal compiler produces;
  the shard does not pin compiler behavior.
- **Cross-broker performance parity.** RabbitMQ and LavinMQ have
  different characteristics; the bounds above are RabbitMQ-anchored.
  LavinMQ falsifier results are tracked but bounds are advisory,
  not normative, for LavinMQ.

---

## 14. Anti-patterns in benchmarking

- **Measuring publish on confirms channel without draining the
  outcome channel.** Memory grows unbounded; the test crashes long
  before the window completes.
- **Co-locating broker and benchmark on a 2-core machine.** The
  broker starves the benchmark and vice versa; numbers are noise.
- **Using `Time.utc` for windows.** Wall-clock jitter contaminates
  the measurement. Use `Time.monotonic`.
- **Comparing across Crystal versions without the host fingerprint.**
  GC behavior, IO syscalls, and `IO::ByteFormat` performance all
  evolve across Crystal versions. The CI baseline is per-version.
