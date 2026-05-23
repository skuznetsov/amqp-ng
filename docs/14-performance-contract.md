# amqp — Performance Roadmap

> **Document status:** Draft v0.2, 2026-05-15
> **Audience:** Implementers; benchmarkers; reviewers asking "is this
> shard fast enough."
> **Companions:** `docs/01-design-principles.md` (P-6 observable),
> `docs/15-reliability-contract.md` (reliability is binary; perf is
> bounded), `docs/16-falsifier-matrix.md`, `docs/19-observability.md`.

This document lists **non-normative performance targets** for the v0
shard. They are engineering goals, not release guarantees, because
the repository only ships default-off `spec/perf/` carriers for part of
the roadmap, not a complete reproducible benchmark suite with normalized
CI baselines. Until that suite exists and is wired into CI, the `PERF-N`
entries below are roadmap targets only.

Numbers are **per-connection** unless stated, on the reference rig
(see §7). Different hardware will produce different numbers; the
future harness will need a normalisation procedure before any target
can become a normative contract.

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

The benchmark location is `spec/perf/<perf-id>.cr`. No current v0 claim
is VERIFIED by this document alone.

---

## 2. PERF-1: Handshake latency

**Roadmap target.** The full `Amqp.connect` against a local RabbitMQ over plain
TCP completes in `< 10 ms` p99 over 1000 sequential connections.

**Why this number.** TCP connect on loopback is sub-millisecond;
AMQP handshake is two round-trips (start/start-ok, tune/tune-ok)
plus one for `connection.open/open-ok`. Three loopback RTTs +
broker handling SHOULD fit in 10 ms.

**Executable harness:** `spec/perf/t_perf_handshake_001_spec.cr` is
default-off behind `AMQP_PERF_LIVE=1`. It opens
`AMQP_PERF_HANDSHAKE_CONNECTIONS` sequential connections, records p99,
asserts the caller-provided `AMQP_PERF_HANDSHAKE_P99_MS` bound, and
writes local JSON/TXT artifacts under `spec/perf/results/`.

**TLS variant.** PERF-1-TLS: same target with `amqps://` and a
freshly-issued cert, claim `< 50 ms` p99. TLS handshake dominates;
50 ms is generous. The same carrier measures TLS when `AMQP_TLS_URL` is
set, using `AMQP_TLS_CA_CERT` when supplied, and checks
`AMQP_PERF_TLS_HANDSHAKE_P99_MS`.

---

## 3. PERF-2: Publish throughput, fire-and-forget

**Roadmap target.** A single producer publishing 256-byte messages
fire-and-forget (`Channel#publish`, no confirm) on one channel to one
queue on a local broker sustains `> 200,000 messages/second` over
a 10-second steady-state window.

**Why this number.** The shard is fiber-driven; the bottleneck is
either the broker's ingest or the socket's serialization. The future
harness should measure shard overhead against a broker-only or
reference-client baseline on the same host.

**Executable harness:** `spec/perf/t_perf_pub_001_spec.cr` is default-off
behind `AMQP_PERF_LIVE=1`. It runs warm-up and measurement windows,
asserts the caller-provided `AMQP_PERF_PUB_001_MIN` bound, and writes
local JSON/TXT artifacts under `spec/perf/results/`.

---

## 4. PERF-3: Publish throughput, confirms

**Roadmap target.** A single producer using `Channel#publish_async` on a
confirms channel, draining the outcome channel concurrently in a
spawned fiber, sustains `> 50,000 messages/second` over a 10-second
window.

**Why this number.** Confirms add a round-trip in spirit but RabbitMQ
batches acks; the shard's tracker uses a sorted-map for O(log N)
single-tag resolution. ~50k/s is the conservative bound; 100k/s+ is
plausible.

**Executable harness:** `spec/perf/t_perf_pub_002_spec.cr` is default-off
behind `AMQP_PERF_LIVE=1`. It publishes with `Channel#publish_async`,
drains outcome channels on a concurrent fiber, asserts the caller-provided
`AMQP_PERF_PUB_002_MIN` bound, and writes local JSON/TXT artifacts under
`spec/perf/results/`.

**Synchronous variant.** PERF-3-SYNC: `Channel#publish_confirm` from
a single fiber (waiting per-publish) sustains `> 5000 messages/second`.
The synchronous round-trip dominates.

---

## 5. PERF-4: Multi-channel throughput

**Roadmap target.** Eight producer fibers, each on its own channel, publishing
fire-and-forget on one connection, sustain `> 800,000 messages/second`
aggregate over 10 seconds. (Linear scaling × 8 from PERF-2; in
practice the write mutex contends, so ~600k is realistic.)

**Why this matters.** Validates that the connection-level write
mutex does not serialise channels too aggressively. If this claim
fails, the design choice in `docs/00-overview.md` §2.2 (writes on
caller's fiber under mutex) is questionable.

**Executable harness:** `spec/perf/t_perf_multichan_001_spec.cr` is
default-off behind `AMQP_PERF_LIVE=1`. It opens one connection, starts
`AMQP_PERF_MULTICHAN_COUNT` channels and producer fibers, asserts the
caller-provided `AMQP_PERF_MULTICHAN_001_MIN` aggregate bound, and writes
local JSON/TXT artifacts under `spec/perf/results/`.

---

## 6. PERF-5: Consume throughput

**Roadmap target.** A single consumer (block-form `consume`, `auto_ack: false`,
acking inline after each message, `prefetch: 1000`) draining a
pre-loaded queue of 1M messages on a local broker sustains
`> 100,000 messages/second` from first delivery to last ack.

**Why this number.** Consume is read-heavy (no write mutex contention
on the publish path, but ack frames go on the write mutex). Prefetch
1000 keeps the broker delivering ahead of the consumer.

**Executable harness:** `spec/perf/t_perf_cons_001_spec.cr` is
default-off behind `AMQP_PERF_LIVE=1`. It preloads
`AMQP_PERF_CONS_MESSAGES` messages outside the timed section, drains
them through `Subscription#receive` with inline `ack`, asserts the
caller-provided `AMQP_PERF_CONS_001_MIN` bound, and writes local JSON/TXT
artifacts under `spec/perf/results/`.

---

## 7. PERF-6: Memory per idle connection

**Roadmap target.** An idle `Connection` with one open channel (no consumers,
no in-flight publishes) consumes `< 64 KB` of Crystal heap as
measured by `GC.stats.heap_size` delta.

**Why this matters.** Connections are sometimes pooled in hundreds.
64 KB × 500 = 32 MB; acceptable.

**Executable harness:** `spec/perf/t_perf_mem_001_002_spec.cr` is
default-off behind `AMQP_PERF_LIVE=1`. `T-PERF-MEM-001` opens
`AMQP_PERF_MEM_CONNECTIONS` live connections with one channel each,
records the `GC.stats.heap_size` delta after a collection, divides by
connection count, asserts `AMQP_PERF_MEM_001_MAX_BYTES`, and writes
local JSON/TXT artifacts under `spec/perf/results/`.

---

## 8. PERF-7: Memory per buffered delivery

**Roadmap target.** A `Subscription` with `buffer: 100` holding 100 unread
256-byte deliveries consumes `< 64 KB` of heap (`< 640 bytes` per
delivery including the `DeliverMessage` struct, properties, and the
ring buffer slot).

**Why this matters.** Subscriptions are sometimes high-buffer
(thousands). Memory per delivery is the multiplier.

**Executable harness:** `spec/perf/t_perf_mem_001_002_spec.cr` also
covers `T-PERF-MEM-002`. It opens a live subscription with
`AMQP_PERF_MEM_DELIVERIES` unread no-ack deliveries buffered locally,
records the `GC.stats.heap_size` delta after a collection, divides by
delivery count, asserts `AMQP_PERF_MEM_002_MAX_BYTES`, and writes local
JSON/TXT artifacts under `spec/perf/results/`.

---

## 9. PERF-8: GC pressure

**Roadmap target.** Publishing 1M messages of 256 bytes fire-and-forget
results in `< 200` GC cycles (full collections), as reported by
`GC.stats.collections` delta.

**Why this matters.** A shard that allocates heavily per message in
the hot path will be hard to use under sustained load. Buffer reuse
should be measured before becoming a normative codec requirement.

**Future harness:** `T-PERF-GC-001`.

---

## 10. PERF-9: Stats read overhead

**Roadmap target.** Reading `Connection#stats` 1M times in a tight loop on
the reference rig takes `< 1 second` wall time (i.e., per-call cost
`< 1 µs`).

**Why this matters.** Stats are observed at scrape frequency; if
each read is expensive, observability is too costly to deploy.

**Executable harness:** `spec/perf/t_perf_stats_001_spec.cr` is
default-off behind `AMQP_PERF_LIVE=1`. It calls `Amqp::Stats#snapshot`
`AMQP_PERF_STATS_READS` times, records the average microseconds per
call, asserts the caller-provided `AMQP_PERF_STATS_001_MAX_US` bound,
and writes local JSON/TXT artifacts under `spec/perf/results/`.

---

## 11. PERF-10: Recovery dead window

**Roadmap target.** With `Recovery::Full` against a local broker that is
restarted (full process kill + restart, no quorum), the median dead
window (from disconnect to first successful `publish_confirm` on a
recovered channel) is `< 2 seconds`.

**Why this number.** Initial backoff is 500 ms; the broker's restart
takes ~1 s on the reference rig; one or two retries fit in the
budget.

**Future harness:** `T-PERF-RECOV-001` automates the broker restart.

---

## 12. Requirements before these become normative

The checked-in manual `Perf Smoke` GitHub Actions workflow runs the
existing `tools/perf_publish.cr` harness against RabbitMQ and LavinMQ
with tiny default counts and stores JSON artifacts. It also runs
`tools/perf_smoke_assert.cr`, which checks that required JSON lanes are
present and have positive sample/median values. This is a harness
health check only: it catches broken/missing benchmark output, but its
low positive floors are not the normative `PERF-N` throughput bounds
listed above.

The repository also ships `tools/perf_threshold_assert.cr`, which reads
a saved `tools/perf_publish.cr` JSON artifact and a separate threshold
profile. The profile can name per-lane `median_min` and `sample_min`
bounds under `metrics` and `stages`. This is a usable local release
gate for a known host/broker/compiler profile, but it is still not a
replacement for `spec/perf/`: it does not create a benchmark window,
control warm-up, fingerprint the host, or compare against a checked-in
baseline by itself.

Saved benchmark artifacts include a small stable metadata block:
benchmark schema version, per-lane units, Crystal version/description,
and compile flags for `--release`, `preview_mt`, and `execution_context`.
They also include the workload shape: redacted URL, message counts, body size, stage
iterations, channel/connection counts, confirm windows, route fanout,
body sweep sizes, and consume buffer. `tools/perf_compare.cr` warns when
those fields differ between baseline and current artifacts, including
matching `metrics` or `stages` lanes whose `unit` values differ or exist
only on one side. Those warnings
do not fail the command by default, but they should block strong throughput
claims until the context difference is explained. For strict local gates,
set `AMQP_BENCH_COMPARE_STRICT=1` to reject metadata drift, missing
baseline lanes, and new current-only lanes in one switch. Set
`AMQP_BENCH_COMPARE_FAIL_METADATA=1` so metadata warnings become a
nonzero comparison result. Set `AMQP_BENCH_COMPARE_FAIL_MISSING_CURRENT=1`
when the gate should also reject current artifacts that no longer emit
lanes present in the baseline. Set `AMQP_BENCH_COMPARE_FAIL_NEW_LANE=1`
when the gate should reject new current-only lanes until a maintainer has
reviewed the benchmark schema expansion.

Before any `PERF-N` entry above becomes a release-blocking contract,
the repository needs a complete executable benchmark suite under
`spec/perf/`. Each `spec/perf/T-PERF-*.cr` should run:

1. A warm-up of `warmup_seconds` (default 2) NOT included in
   measurement.
2. A measurement window of `window_seconds` (default 10).
3. Asserts the bound promoted from this roadmap.
4. Writes `spec/perf/results/T-PERF-N.txt` with: the measured value,
   the bound, pass/fail, host fingerprint (CPU model, OS, Crystal
   version), and timestamp.

Only after this suite exists should CI run the perf suite on every PR.
At that point, a regression of more than 10% relative to the
previous-merge baseline can become a release-blocking warning, even if
the absolute bound is still met.

Once perf CI exists, the baseline should be the `main` branch's most
recent successful perf run; the comparison should be automated.

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
  the measurement. Use `Time.instant` / `Time::Instant`.
- **Comparing across Crystal versions without the host fingerprint.**
  GC behavior, IO syscalls, and `IO::ByteFormat` performance all
  evolve across Crystal versions. Any future CI baseline should be
  per-version.
