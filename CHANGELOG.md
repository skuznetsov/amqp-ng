# Changelog

## 0.1.0 - 2026-05-15

First usable AMQP 0-9-1 release for local downstream-service integration.

### Included

- TCP and `amqps://` AMQP 0-9-1 connections with stdlib OpenSSL.
- Publisher confirms: sync, async, returned mandatory publishes, nack and
  range settlement guards.
- Queue/exchange topology operations, `basic.get`, `consume`,
  `subscribe`, manual ack/nack/reject, and prefetch.
- Migration helpers for the Crystal `amqp-client.cr` surface:
  `basic_*` aliases, queue/exchange wrappers, work-pool `basic_consume`,
  `on_return`, `on_cancel`, `on_close`, broker blocked/unblocked callbacks,
  `channel.flow`, `basic.recover`, transactions, and IO publish overloads.
- Opt-in topology/consumer/unconfirmed-publish recovery with fail-fast
  caller operations during recovery.
- Heartbeats, typed errors, reduced `Connection#stats`, and stdlib-only
  runtime dependencies.

### Verified Locally

- `crystal spec --error-trace` without a local broker: 211 examples,
  0 failures, 0 errors, 91 live-broker pending.
- `crystal tool format --check src spec tools/perf_publish.cr`.
- `crystal build tools/perf_publish.cr --no-codegen --error-trace`.
- `git diff --check`.
- LavinMQ 2.4.0 focused publish/confirm smoke:
  72 examples, 0 failures, 0 errors, 0 pending.
- LavinMQ 2.4.0 focused topology/API smoke:
  37 examples, 0 failures, 0 errors, 0 pending.
- Downstream service compile smoke passed locally.
- RabbitMQ 3.13.7 opt-in TLS/backpressure/chaos suite: 150 examples,
  0 failures, 0 errors, 0 pending.
- LavinMQ 2.4.0 default suite: 150 examples, 0 failures, 0 errors,
  4 live-gated pending.
- LavinMQ 2.4.0 opt-in backpressure/chaos suite: 150 examples,
  0 failures, 0 errors, 1 pending for LavinMQ TLS.

### Deferred

- SASL EXTERNAL.
- LavinMQ TLS.
- Full CI broker matrix.
- Reproducible `spec/perf/` harness and committed perf transcripts.
- Broader `spec/reliability/` transcript suite.
- AMQP 1.0 runtime; only a v1 SDD exists.
- WebSocket transport.
