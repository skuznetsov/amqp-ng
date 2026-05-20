# amqp

AMQP 0-9-1 client for Crystal, written for RabbitMQ/LavinMQ and for
real worker-service integration. The project is no longer
pre-implementation: it has a working client under `src/`, executable
specs under `spec/`, a local benchmark harness, and a v0.1.0 release
branch.

Current status: **v0.1.0 local release branch**. It is usable as a path
dependency today. Publishing a public shard release should still re-run
the TLS live-broker, manual release-gate, and performance checks listed
below.

## Why This Exists

The existing Crystal AMQP clients either lag current Crystal internals
or expose migration friction for this codebase. This shard keeps the v0
surface small:

- AMQP 0-9-1 over TCP or `amqps://`.
- RabbitMQ 3.13+ and LavinMQ 2.x as target brokers.
- Publisher confirms, mandatory returns, typed errors, and heartbeats.
- Queue/exchange topology operations and opt-in recovery.
- Practical `amqp-client.cr` migration helpers.
- No runtime dependencies outside Crystal stdlib.

AMQP 1.0 is planned as a future side-by-side protocol, not part of
v0.1.0 runtime.

## Install

For local integration:

```yaml
dependencies:
  amqp:
    path: ../amqp-ng
```

For a Git dependency after pushing this branch:

```yaml
dependencies:
  amqp:
    github: skuznetsov/amqp-ng
    branch: main
```

Then:

```sh
shards install
```

## Quick Start

```crystal
require "amqp"

Amqp.connect("amqp://guest:guest@localhost:5672") do |conn|
  ch = conn.channel
  q = ch.queue_declare("", exclusive: true, auto_delete: true)

  ch.publish("", q.name, "hello".to_slice)

  msg = ch.get(q.name)
  puts String.new(msg.not_nil!.body)
end
```

### Durable Publish With Confirms

```crystal
Amqp.connect(ENV["AMQP_URL"]? || "amqp://guest:guest@localhost:5672") do |conn|
  ch = conn.channel
  ch.confirm_select
  ch.queue_declare("jobs.analyze", durable: true)

  props = Amqp::Properties.new(
    content_type: "application/json",
    persistence: Amqp::Persistence::Persistent,
    message_id: "job-123",
  )

  ok = ch.publish_confirm(
    Amqp::Message.new(%({"id":123}), props),
    "",
    "jobs.analyze",
    mandatory: true,
    timeout: 5.seconds,
  )

  raise "publish was not acked" unless ok
end
```

For higher-throughput producer paths, publish confirmed messages in
bounded windows instead of waiting for one broker round trip per
message:

```crystal
messages = jobs.map do |job|
  Amqp::Message.new(job.to_json, props)
end

ok = ch.publish_confirm_batch(
  messages,
  "",
  "jobs.analyze",
  window_size: 100,
  timeout: 10.seconds,
)

raise "confirm window failed" unless ok
```

### Worker Consumer

```crystal
Amqp.connect(ENV["AMQP_URL"]? || "amqp://guest:guest@localhost:5672") do |conn|
  ch = conn.channel
  ch.prefetch(1_u16)

  ch.consume("jobs.analyze", auto_ack: false) do |msg|
    begin
      # process message
      ch.ack(msg.delivery_tag)
    rescue
      ch.nack(msg.delivery_tag, requeue: true)
    end
  end
end
```

### Queue/Exchange Wrappers

```crystal
Amqp.connect("amqp://guest:guest@localhost:5672") do |conn|
  ch = conn.channel
  ex = ch.direct_exchange("amq.direct")
  q = ch.queue("jobs.analyze", durable: true)

  q.bind(ex.name, "jobs.analyze")
  ex.publish(%({"kind":"analyze"}), "jobs.analyze")
end
```

## Implemented Surface

| Area | Status |
| --- | --- |
| Connection lifecycle | `connect`, block form, close, server properties, `blocked?`, blocked/unblocked callbacks |
| Channel lifecycle | open/close, broker close mapping, `channel.flow`, close callbacks |
| Publishing | fire-and-forget, batch publish, sync confirms, windowed confirms, async confirms, mandatory returns |
| Consuming | `get`, `consume`, `subscribe`, `basic_consume(work_pool:)`, `ack`, `nack`, `reject`, `qos` |
| Topology | queue/exchange declare, bind, unbind, purge, delete, wrapper objects |
| Transactions | `tx_select`, `tx_commit`, `tx_rollback`, `transaction` helper |
| Recovery | opt-in topology, consumer, and unconfirmed-publish replay; new operations fail fast during recovery |
| TLS | `amqps://` via `OpenSSL::SSL::Context::Client`; live spec gated by env vars |
| Compatibility | `basic_*` aliases, queue/exchange wrappers, IO publish overloads, return/cancel/close callbacks |
| Observability | reduced `Connection#stats` counters plus typed exceptions |

## Broker Compatibility

| Broker | Current evidence |
| --- | --- |
| RabbitMQ 3.13.x | Default suite green locally; manual CI covers plain-AMQP, TLS, backpressure, and Docker chaos gates |
| LavinMQ 2.4.0 | Default suite green locally; manual CI covers plain-AMQP, TLS, backpressure, and Docker chaos gates |

The default suite leaves broker-destructive or environment-specific specs
pending unless you set the corresponding env vars:

```sh
AMQP_TLS_URL='amqps://guest:guest@localhost:5671/' \
AMQP_TLS_CA_CERT='.tmp/rabbitmq_tls/certs/ca_certificate.pem' \
AMQP_BACKPRESSURE_LIVE=1 \
AMQP_CHAOS_DOCKER_CONTAINER=amqp-ng-rabbit \
crystal spec --error-trace
```

## Benchmarks

`tools/perf_publish.cr` is a checked-in local witness harness. The
manual `Perf Smoke` workflow executes it on RabbitMQ/LavinMQ and
validates that required JSON lanes are present with positive medians
via `tools/perf_smoke_assert.cr`. This is still not a normative
performance contract.

`tools/perf_threshold_assert.cr` can compare a saved benchmark JSON file
against an explicit threshold profile. This is useful for local release
gates and host-specific baselines, but `docs/14-performance-contract.md`
remains a roadmap until `spec/perf/` contains reproducible benchmark
runners.

`tools/perf_compare.cr <baseline.json> <current.json>` compares two
saved benchmark artifacts lane-by-lane across `metrics` and `stages`.
It prints regressions, improvements, missing lanes, and new lanes. Set
`AMQP_BENCH_COMPARE_FAIL_REGRESSION_PCT` to make the command exit nonzero
when a matching lane regresses past a local threshold. New benchmark
artifacts include the benchmark schema version, Crystal description,
and release/threading compile flags; compare prints warnings when those
stable metadata fields differ.

Broad paired local release-compiler run:

- Crystal: `/opt/homebrew/bin/crystal 1.20.1 --release`
- Broker: local RabbitMQ 3.13.x
- Body: 256 bytes
- Shape: `AMQP_BENCH_PUBLISH_N=9000`, `AMQP_BENCH_CONFIRM_N=900`,
  `AMQP_BENCH_SAMPLES=3`

| Workload | amqp-ng | amqp-client.cr | Notes |
| --- | ---: | ---: | --- |
| single fire-and-forget publish | ~83.6k msg/s | ~83.0k msg/s | same queue |
| batch publish | ~85.1k msg/s | n/a | `publish_batch`, batch size 100 |
| 2 connections, separate queues | ~705k msg/s | ~682k msg/s | queue-sharded producers |
| 4 connections, separate queues | ~625k msg/s | ~621k msg/s | queue-sharded producers |
| sync confirm per publish | ~2.48k msg/s | ~2.52k msg/s | one round trip per publish |

Latest paired local release-compiler runs after the confirm batching and
heartbeat-stamp hot-path cleanup:

- Crystal: `/opt/homebrew/bin/crystal 1.20.1 --release`
- Brokers: local RabbitMQ 3.13.x and LavinMQ 2.4.0
- Body: 256 bytes
- Shape: `AMQP_BENCH_PUBLISH_N=15000`, `AMQP_BENCH_CONFIRM_N=1500`,
  `AMQP_BENCH_SAMPLES=5`, channels/connections `1,2,4`

| Broker | Workload | amqp-ng | amqp-client.cr | Delta |
| --- | --- | ---: | ---: | ---: |
| RabbitMQ | single fire-and-forget publish | ~82.3k msg/s | ~72.6k msg/s | +13.5% |
| RabbitMQ | same-connection 4-channel publish | ~77.0k msg/s | ~67.1k msg/s | +14.8% |
| RabbitMQ | sync confirm per publish | ~2.68k msg/s | ~2.52k msg/s | +6.4% |
| RabbitMQ | windowed confirm, size 500 | ~65.0k msg/s | n/a | n/a |
| LavinMQ | single fire-and-forget publish | ~744.6k msg/s | ~771.5k msg/s | -3.5% |
| LavinMQ | same-connection 2-channel publish | ~822.4k msg/s | ~722.8k msg/s | +13.8% |
| LavinMQ | 4 connections, separate queues | ~694.0k msg/s | ~678.7k msg/s | +2.2% |
| LavinMQ | sync confirm per publish | ~3.46k msg/s | ~3.37k msg/s | +2.7% |
| LavinMQ | batch confirm + wait | ~33.6k msg/s | n/a | n/a |

Run the local harness:

```sh
AMQP_BENCH_PUBLISH_N=9000 \
AMQP_BENCH_CONFIRM_N=900 \
AMQP_BENCH_BATCH_SIZE=100 \
AMQP_BENCH_SAMPLES=3 \
AMQP_BENCH_CHANNELS=1,2,4 \
AMQP_BENCH_CONNECTIONS=1,2,4 \
crystal run tools/perf_publish.cr --release --error-trace
```

The largest measured throughput lane is multi-connection publishing to
separate queues. Single-connection multi-channel publishing is bounded by
the intentional connection write mutex.

The harness also emits `publish_batch_bytes` for the public
`publish_batch(Array(Bytes))` overload. That lane is measured separately
from `publish_batch`, which uses prebuilt `Amqp::Message` values.
It also emits synthetic stage-attribution lanes such as
`encode_empty_publish_frames`, `parse_basic_ack_frame_generic`, and
`parse_basic_ack_frame_direct`; set `AMQP_BENCH_STAGE_N` to control
the number of synthetic stage iterations. These stage lanes are
diagnostic triggers, not live throughput claims.
For live single-publish attribution, the harness also emits
`publish_single_empty_body`, `publish_single_bytes_empty_body`,
`publish_prepared_empty_body`, `publish_single_alternating_routes`, and
`publish_single_repeat`. These lanes help separate body-size cost,
public API wrapper cost, prepared-route reuse, route-cache stability,
and lane-order noise.
Set `AMQP_BENCH_ROUTE_COUNTS` to add round-robin route-fanout lanes and
`AMQP_BENCH_BODY_SIZES` to sweep live single-publish body sizes plus
matching synthetic `encode_empty_publish_frames_body_<n>` stages.
Set `AMQP_BENCH_CONFIRM_WINDOWS` to sweep confirm-window sizes; the
tool emits `confirm_window_<n>` lanes that publish `n` messages, wait
for confirms, and repeat. These lanes show the transport benefit of a
ladder/windowed confirm style without changing application code.
After a run, `tools/perf_recommend.cr <bench.json>` prints conservative
run-local guidance, such as when batch publishing, prepared fixed-route
publishers, route sharding, or body-size reduction are the highest
leverage follow-ups.
When comparing a branch against a saved baseline, run
`tools/perf_compare.cr <baseline.json> <current.json>` first; it is a
longitudinal drift detector, not a substitute for paired broker runs.
Metadata warnings from that command mean the run is not an apples-to-apples
throughput comparison until the compiler/build-mode difference is explained.

Latest cross-broker release-compiler smoke after the bytes-batch fast path:

- Crystal: `/opt/homebrew/bin/crystal 1.20.1 --release`
- Brokers: `rabbitmq:3.13-management` on `5672`, `cloudamqp/lavinmq:2.4.0` on `5673`
- Full shape: `AMQP_BENCH_PUBLISH_N=30000`,
  `AMQP_BENCH_CONFIRM_N=3000`, `AMQP_BENCH_SAMPLES=3`,
  batch size `100`, channels/connections `1,2,4`
- Batch-focused shape: same counts with `AMQP_BENCH_SAMPLES=7`,
  channels/connections `1`

| Workload | RabbitMQ | LavinMQ | Notes |
| --- | ---: | ---: | --- |
| single fire-and-forget publish | ~79.3k msg/s | ~754.0k msg/s | batch-focused run |
| prebuilt-message batch | ~87.5k msg/s | ~736.9k msg/s | `Array(Amqp::Message)`, batch-focused run |
| bytes batch | ~85.4k msg/s | ~845.2k msg/s | public `Array(Bytes)` overload, batch-focused run |
| 4 connections, separate queues | ~795.5k msg/s | ~777.2k msg/s | queue-sharded producers, full run |
| sync confirm per publish | ~2.96k msg/s | ~4.29k msg/s | one round trip per publish, batch-focused run |
| batch confirm + wait | ~74.1k msg/s | ~53.7k msg/s | batch size 100, batch-focused run |

The bytes-batch fast path is a lower-allocation public API path, not a
proved live-throughput win over callers that already prebuild
`Amqp::Message` batches. On these local runs, socket/broker behavior
dominates the publish lanes. Batch publish also reuses a prebuilt
`basic.publish` method frame for every message in a batch, guarded by a
wire-level byte-equivalence spec.

## Verification

Current default local gate:

```sh
crystal spec --error-trace
```

Latest no-broker result: `229 examples, 0 failures, 0 errors, 92 pending`.
Most pending examples are live-broker specs that intentionally skip when
`AMQP_URL` is unreachable.
Checked-in no-broker CI runs this gate, format checks, and local tool
type-checks on pinned Crystal 1.19.2 and 1.20.2.
Checked-in plain-AMQP broker CI also runs the live spec surface against
RabbitMQ 3.13.7 and LavinMQ 2.4.0 on Crystal 1.20.2. A manual
`Perf Smoke` workflow runs the benchmark harness with tiny default
counts, validates required positive JSON lanes, and stores JSON
artifacts; it is a harness health check, not a throughput contract.

Latest focused LavinMQ publish/confirm smoke:
`72 examples, 0 failures, 0 errors, 0 pending` for
`spec/message_spec.cr`, `spec/api_surface_spec.cr`,
`spec/channel_spec.cr`, and `spec/confirms_spec.cr`.
Latest focused LavinMQ no-wait/API smoke:
`39 examples, 0 failures, 0 errors, 0 pending` for
`spec/wire/no_wait_methods_spec.cr`, `spec/api_surface_spec.cr`,
and `spec/channel_spec.cr`.

Additional gates used for this branch:

```sh
crystal tool format --check src spec tools/perf_publish.cr tools/capture_proxy.cr tools/perf_smoke_assert.cr tools/perf_thresholds.cr tools/perf_threshold_assert.cr tools/perf_recommendations.cr tools/perf_recommend.cr tools/perf_compare_report.cr tools/perf_compare.cr
crystal build tools/perf_publish.cr --no-codegen --error-trace
crystal build tools/capture_proxy.cr --no-codegen --error-trace
crystal build tools/perf_smoke_assert.cr --no-codegen --error-trace
crystal build tools/perf_threshold_assert.cr --no-codegen --error-trace
crystal build tools/perf_recommend.cr --no-codegen --error-trace
crystal build tools/perf_compare.cr --no-codegen --error-trace
git diff --check
```

With a plain AMQP broker available, the remaining default pending specs
are environment gated:

- TLS broker handshake via `AMQP_TLS_URL`.
- Subscription backpressure timing via `AMQP_BACKPRESSURE_LIVE`.
- Broker pause/restart chaos via `AMQP_CHAOS_DOCKER_CONTAINER`.

The checked-in broker CI covers the plain-AMQP live surface. Manual
`Release Gates` workflow jobs cover TLS, backpressure, and Docker chaos
gates for RabbitMQ/LavinMQ. The manual `Perf Smoke` workflow covers
benchmark harness execution plus positive JSON lane validation.
Thresholded performance profile checks are available locally through
`tools/perf_threshold_assert.cr`, but normative reproducible benchmark
runners remain opt-in/local release checks.

## URI And Config

Supported URI query keys in v0:

- `heartbeat`
- `channel_max`
- `frame_max`
- `max_body_size`
- `max_inflight_body_bytes`
- `max_subscription_mailbox_bytes`
- `connect_timeout`
- `recovery`
- `product`
- `information`
- `tcp_nodelay`
- `buffer_size`

TLS policy is supplied by the caller through
`OpenSSL::SSL::Context::Client`. SASL EXTERNAL, AMQPLAIN, OAuth, and
TLS client-certificate auth are deferred.

## Migration From `amqp-client.cr`

Implemented compatibility helpers include:

- `basic_publish`, callback publish, `basic_publish_confirm`
- `basic_get`, `basic_consume`, `basic_cancel`
- `basic_ack`, `basic_reject`, `basic_nack`, `basic_qos`,
  `basic_recover`
- `Channel#queue`, `#exchange`, `#default_exchange`, `#direct_exchange`,
  `#topic_exchange`, `#fanout_exchange`, `#header_exchange`
- `Amqp::Queue` and `Amqp::Exchange` wrappers
- `on_return`, `on_cancel`, `on_close`
- IO + explicit byte-size publish overloads

Compatibility notes:

- `no_wait` compatibility overloads on queue declare, queue bind,
  exchange bind/unbind, and basic cancel emit broker no-wait frames.
- NamedTuple `args` sugar is accepted for topology/consumer helpers
  and coerced into `Amqp::Arguments`; use explicit `Amqp::Arguments`
  when you need to make AMQP field widths visually obvious.
- WebSocket transport is a future transport project, not a v0
  Crystal-shard parity blocker.

## Design Tradeoffs

- The connection has one write mutex. This keeps publish frame groups
  atomic and simple. For high aggregate publish throughput, use multiple
  connections.
- Recovery is opt-in. During recovery, new caller operations fail fast
  with `RecoveryInProgress`; the client does not queue arbitrary new
  work behind a reconnect.
- Consumer backpressure is per-channel first. If a subscription inbox
  fills, that channel handler stalls; whole-connection pressure requires
  the channel frame inbox to fill too.
- The stdlib-only constraint is intentional. The shard avoids reaching
  into Crystal stdlib internals.

## Deferred

- AMQP 1.0 runtime. See `docs/22-amqp-1-0-sdd.md`.
- SASL EXTERNAL and other non-PLAIN auth mechanisms.
- `spec/perf/` reproducible benchmark suite.
- Checked-in CI for normative thresholded performance gates.
- Broader reliability transcript corpus.
- WebSocket transport.

## Documentation Map

- `docs/02-public-api.md` - public API contract.
- `docs/05-wire-0-9-1/` - AMQP 0-9-1 wire codec.
- `docs/07-channel-lifecycle.md` - channel state, confirms, flow,
  close semantics.
- `docs/08-publisher-confirms.md` - confirm tracker and return/nack
  behavior.
- `docs/09-consumer.md` - consume/subscribe/get and backpressure.
- `docs/12-recovery.md` - opt-in recovery contract.
- `docs/14-performance-contract.md` - non-normative performance roadmap.
- `docs/16-falsifier-matrix.md` - falsifier/test matrix.
- `TODO.md` and `LANDMARKS.md` - current working ledger and verified
  anchors.
