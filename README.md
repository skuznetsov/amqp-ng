# amqp

AMQP 0-9-1 client for Crystal, written for RabbitMQ/LavinMQ and for
the local `../job_hunter` service. The project is no longer
pre-implementation: it has a working client under `src/`, executable
specs under `spec/`, a local benchmark harness, and a v0.1.0 release
branch.

Current status: **v0.1.0 local release branch**. It is usable as a path
dependency today. Publishing a public shard release should still re-run
the opt-in broker matrix listed below.

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
    branch: codex/v0.1.0-release-parity
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
| Publishing | fire-and-forget, batch publish, sync confirms, async confirms, mandatory returns |
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
| RabbitMQ 3.13.x | Default suite green locally; opt-in TLS/backpressure/chaos gate has passed on the project-owned containers |
| LavinMQ 2.4.0 | Default and opt-in plain-AMQP backpressure/chaos gates passed in the release baseline; TLS remains deferred |

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

`tools/perf_publish.cr` is a checked-in local witness harness. It is not
yet a CI performance contract; `docs/14-performance-contract.md` remains
a roadmap until `spec/perf/` exists.

Latest paired local release-compiler run:

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

## Verification

Current default local gate:

```sh
crystal spec --error-trace
```

Latest result: `174 examples, 0 failures, 0 errors, 4 pending`.

Additional gates used for this branch:

```sh
crystal tool format --check src spec tools/perf_publish.cr
crystal build tools/perf_publish.cr --no-codegen --error-trace
git diff --check
cd ../job_hunter && crystal build src/cli.cr --no-codegen --error-trace
```

The four default pending specs are live/environment gated:

- TLS broker handshake via `AMQP_TLS_URL`.
- Subscription backpressure timing via `AMQP_BACKPRESSURE_LIVE`.
- Broker pause/restart chaos via `AMQP_CHAOS_DOCKER_CONTAINER`.

## URI And Config

Supported URI query keys in v0:

- `heartbeat`
- `channel_max`
- `frame_max`
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

Known lower-priority omissions:

- Some `no_wait` overload semantics are accepted but not always
  broker-no-wait optimized.
- NamedTuple `args` overload sugar is not implemented; use
  `Amqp::Arguments`.
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
- LavinMQ TLS gate.
- Full CI broker matrix.
- `spec/perf/` reproducible benchmark suite.
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
