# TODO

Status: active working ledger for `amqp-ng`.

## Definition of Done

- A task is not `DONE` unless its verification command is listed and passes.
- Docs claims that use `MUST`/`MUST NOT` need a falsifier row or an explicit scope cut.
- Any Grok/other-agent finding is candidate evidence only; verify locally before acting.
- Keep one logical change per commit. Do not stage unrelated dirty files.

## Current Slice: Bring API And Specs Into Alignment

- [x] Cut pragmatic v0.1.0 release for `../job_hunter`.
  - Risk tier: CAUTION, because this changes release surface and a sibling project's dependency.
  - DoD: `shard.yml` version is `0.1.0`; `Amqp::VERSION` remains `0.1.0`; release docs list current evidence and deferred surfaces.
  - DoD: `../job_hunter` depends on local `../amqp-ng` and compiles its CLI entrypoint.
  - Evidence: `shard.yml`, `src/amqp.cr`, `CHANGELOG.md`, and `docs/17-mvp-cutline.md`.
  - Evidence: `cd ../job_hunter && shards install` installs `amqp (0.1.0 at ../amqp-ng)`.
  - Evidence: `cd ../job_hunter && crystal build src/cli.cr --no-codegen --error-trace` exits 0.
  - Evidence: current default `crystal spec --error-trace` exits 0: 174 examples, 0 failures, 0 errors, 4 pending.
  - Evidence: RabbitMQ opt-in TLS/backpressure/chaos suite exits 0: 150 examples, 0 failures, 0 errors, 0 pending.
  - Evidence: LavinMQ 2.4.0 opt-in backpressure/chaos suite exits 0: 150 examples, 0 failures, 0 errors, 1 pending (TLS only).

- [x] Add AMQP 1.0 SDD planning specs without weakening the v0 cutline.
  - Risk tier: SAFE docs-only change.
  - DoD: AMQP 1.0 docs name concrete v1 falsifiers while remaining outside the v0.1.0 release gate.
  - Evidence: `docs/22-amqp-1-0-sdd.md` defines the v1 SDD slices for codec, message sections, SASL/open/begin, links, API, recovery, and broker matrix.
  - Evidence: `docs/16-falsifier-matrix.md` reserves `T-AMQP10-*` rows as v1-only, and `docs/17-mvp-cutline.md` still says v0 must not wire AMQP 1.0.

- [x] Re-run the full current verification baseline.
  - DoD: `crystal spec` exits 0.
  - DoD: `crystal tool format --check` status is documented as current pass/fail, with pre-existing failures separated from new failures.
  - Evidence: current default full `crystal spec --error-trace` exits 0: 174 examples, 0 failures, 0 errors, 4 pending.
  - Evidence: earlier local full opt-in `AMQP_BACKPRESSURE_LIVE=1 AMQP_CHAOS_DOCKER_CONTAINER=amqp-ng-rabbit AMQP_TLS_URL='amqps://guest:guest@localhost:5671/' AMQP_TLS_CA_CERT='.tmp/rabbitmq_tls/certs/ca_certificate.pem' timeout 180 crystal spec` exited 0 before later parity additions; re-run before public release.
  - Evidence: earlier LavinMQ 2.4.0 default and opt-in backpressure/chaos suites passed before later parity additions; re-run after broker-compat changes.
  - Evidence: `crystal tool format --check src spec` exits 0.

- [x] Finish API-to-doc surface audit.
  - Scope: `docs/02-public-api.md` vs `src/amqp*.cr` and `spec/api_surface_spec.cr`.
  - DoD: every public symbol in `docs/02-public-api.md` either compiles in `spec/api_surface_spec.cr` or is explicitly cut/deferred in docs.
  - Progress: `Connection#heartbeat` now exposes `Time::Span`; `Connection#recovery_mode` exists; `docs/02-public-api.md` now cuts recovery callbacks, rich per-channel/subscription stats, nullable `Arguments`, and EXTERNAL/TLS-query promises from v0.
  - Progress: `docs/03-error-model.md` and `docs/19-observability.md` now describe the implemented v0 error/stats surface and mark richer `CloseReason`/`ErrorContext`/rich stats as deferred.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/heartbeat_spec.cr spec/config_spec.cr` exits 0: 17 examples, 0 failures.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/stats_spec.cr` exits 0: 8 examples, 0 failures.
  - Evidence: `spec/api_surface_spec.cr` now guards the top-level `Amqp` namespace with a macro enumeration.

- [x] Quadrumvirate spec-gap review.
  - Scope: identify the contracts most likely to be false, under-specified, or unfalsified.
  - DoD: produce a ranked list of blockers/majors with file anchors and proposed falsifier IDs.
  - Evidence: `docs/21-spec-gap-review.md`.

- [x] Fix `Subscription#receive` select compatibility.
  - Evidence: `spec/api_surface_spec.cr` type-checks `select when msg = sub.receive`.
  - Evidence: `crystal spec spec/api_surface_spec.cr` exits 0.

- [x] Decide v0 recovery cutline.
  - Problem: `Recovery::Full` has high distributed-systems risk and weak current verification.
  - DoD: choose fail-fast vs block-during-recovery semantics, then update docs/specs/code to one behavior.
  - Decision: v0 new operations during recovery fail fast with `RecoveryInProgress`; pre-disconnect unconfirmed publishes are still replayed.
  - Evidence: `docs/12-recovery.md`, `docs/08-publisher-confirms.md`, `spec/recovery_spec.cr`.

- [x] Harden publisher confirm specs.
  - Required gaps: multiple ack/nack, unknown tag/out-of-order handling, zero-timeout behavior, timeout cleanup, fire-and-forget tracking in confirm mode, channel-close while waiting.
  - DoD: focused specs fail before implementation or are explicitly marked as future falsifiers.
  - Progress: `publish_confirm`/`publish_async` require confirm mode before write; `publish_confirm` ack, mandatory returned, zero-timeout-before-write; `publish_async` ack/returned/nack one-shot outcomes; unknown/out-of-order ack raises `PublishOutOfOrderError`; multiple=true nack range settlement, async close-without-value, late ack after timeout cleanup, and channel close while waiting are covered.
  - Evidence: `crystal spec spec/confirms_spec.cr` exits 0: 19 examples, 0 failures.

- [x] Harden consumer/subscription specs.
  - Required gaps: `select when msg = sub.receive` compile/run shape, caller cancel drain, broker cancel, backpressure/fiber-router blocking, `spawn_loop` ack/reject semantics.
  - DoD: focused specs or documented cutline for each behavior.
  - Progress: added deterministic subscription specs for select receive, close-after-buffer drain, and closed empty receive behavior; added `Amqp::SubscriptionClosed` alias.
  - Progress: added live broker specs for caller-side cancel and broker-side queue-deletion cancel; cut undocumented `spawn_loop` reject-on-exception semantics from v0 docs.
  - Evidence: `crystal spec spec/subscription_spec.cr spec/api_surface_spec.cr` exits 0: 7 examples, 0 failures.
  - Evidence: `crystal spec spec/subscription_spec.cr` exits 0: 5 examples, 0 failures.
  - Cutline: backpressure/fiber-router blocking measurement requires a live broker timing harness; tracked in backlog instead of faked as a unit spec.

- [x] Harden URI/config/TLS negative specs.
  - Required gaps: unknown query key, kwarg-vs-query precedence, scheme/TLS conflicts, vhost/userinfo reserved-character cases.
  - DoD: table-driven specs for documented accepted/rejected cases.
  - Progress: v0 URI query surface is intentionally narrow; `auth_mechanism`, TLS policy keys, and unknown keys are rejected instead of deferred silently.
  - Progress: added coverage for `%2F` vhost, explicit empty vhost override, deferred TLS/SASL query rejection, plain AMQP plus TLS context, unsupported schemes, missing hosts, and percent-encoded reserved userinfo.
  - Evidence: `spec/config_spec.cr` covers defaults/vhost forms, recognized query keys, keyword precedence, unknown query keys, strict numeric parsing, invalid recovery query values, and the negative TLS/SASL URI surface.
  - Evidence: `crystal spec spec/config_spec.cr spec/tls_spec.cr` exits 0: 14 examples, 0 failures, 1 pending.
  - Remaining live TLS handshake is not a negative-spec gap; it remains gated by `AMQP_TLS_URL`.

- [x] Fix documentation honesty drift.
  - Scope: `README.md` no longer describes the project as pre-implementation.
  - DoD: README status matches the current implementation and verification state.
  - Evidence: `README.md` describes active v0 implementation, current verified spec count, clean format status, and remaining high-risk areas.
  - Evidence: current default full `crystal spec --error-trace` exits 0: 174 examples, 0 failures, 0 errors, 4 pending.

- [x] Decide whether performance claims belong in v0 docs.
  - Problem: `docs/14-performance-contract.md` names concrete throughput targets without a shipped harness.
  - DoD: either add a runnable harness with reproducibility notes or downgrade claims to roadmap/non-normative.
  - Decision: downgrade to roadmap/non-normative until `spec/perf/` exists.
  - Evidence: `docs/14-performance-contract.md` is now "Performance Roadmap"; `docs/16-falsifier-matrix.md` marks `T-PERF-*` rows as reserved roadmap falsifiers.
  - Evidence: current default full `crystal spec --error-trace` exits 0: 174 examples, 0 failures, 0 errors, 4 pending.

## Backlog

- [x] Run a local `amqp-ng` vs `amqp-client.cr` microbench and optimize obvious hot-path copies.
  - Scope: RabbitMQ 3.13.7 on localhost, Crystal 1.20.1 release compiler, 256-byte direct-exchange-to-queue publishes.
  - Progress: publish hot path now writes `basic.publish` and empty content headers directly to the socket instead of allocating transient payload buffers; single-frame inbound deliveries reuse the frame payload instead of copying into `IO::Memory`.
  - Progress: confirm publish registration is now ordered by the connection write critical section while avoiding a confirm-mutex hold across socket write when `recovery: none`.
  - Progress: added checked-in `tools/perf_publish.cr` witness for single publish, batch publish, sync confirms, and batch confirms.
  - Progress: added `Channel#publish_batch` so callers can amortize the connection write critical section and use confirm mode with `wait_for_confirms` instead of one broker round trip per message.
  - Progress: documented `publish_batch` in the public API/confirm docs and added API-surface type checks.
  - Progress: split pending confirm replay retention so `Recovery::None` confirm entries keep routing/outcome metadata but no replay `Message`; `Recovery::Full` still retains replay payloads for reconnect replay.
  - Progress: added a low-watermark for pending confirms so `wait_for_confirms` no longer scans `Set#min` on each progress check.
  - Progress: extended `tools/perf_publish.cr` with synthetic encode-stage attribution and multi-channel publish probes controlled by `AMQP_BENCH_CHANNELS`.
  - Progress: extended `tools/perf_publish.cr` with multi-connection publish probes controlled by `AMQP_BENCH_CONNECTIONS`.
  - Progress: added separate-queue multi-connection probes and suppressed benchmark INFO logs so the tool emits clean JSON from the application.
  - Progress: added URI/keyword socket tuning for `tcp_nodelay` and `buffer_size`, with throughput-oriented defaults aligned with `amqp-client.cr` (`tcp_nodelay=false`, `buffer_size=16384`).
  - Progress: removed the per-channel continuation lock from non-confirm fire-and-forget `publish` / `publish_batch` while preserving whole publish frame-sequence serialization under the connection write mutex.
  - Evidence: `crystal spec` exits 0: 150 examples, 0 failures, 0 errors, 4 pending.
  - Evidence: latest `crystal spec --error-trace` exits 0: 156 examples, 0 failures, 0 errors, 4 pending.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/confirms_spec.cr spec/recovery_spec.cr --error-trace` exits 0: 41 examples, 0 failures.
  - Evidence: `crystal spec spec/confirms_spec.cr spec/recovery_spec.cr --error-trace` exits 0: 37 examples, 0 failures.
  - Evidence: `crystal spec spec/wire spec/channel_spec.cr spec/confirms_spec.cr --error-trace` exits 0: 64 examples, 0 failures.
  - Evidence: `crystal build tools/perf_publish.cr --no-codegen --error-trace` exits 0 with no deprecation warnings.
  - Evidence: `AMQP_BENCH_PUBLISH_N=20000 AMQP_BENCH_CONFIRM_N=3000 AMQP_BENCH_SAMPLES=5 ... amqp_ng_bench.cr --release` reports median ~65.5k msg/s fire-and-forget and ~3.07k msg/s sync confirms.
  - Evidence: `AMQP_BENCH_PUBLISH_N=20000 AMQP_BENCH_CONFIRM_N=3000 AMQP_BENCH_BATCH_SIZE=100 AMQP_BENCH_SAMPLES=3 timeout 120 crystal run tools/perf_publish.cr --release --error-trace` reports median ~62.0k msg/s single publish, ~68.5k msg/s batch publish, ~3.07k msg/s sync confirms, and ~69.7k msg/s batch confirm+wait.
  - Evidence: short attribution run `AMQP_BENCH_PUBLISH_N=12000 AMQP_BENCH_CONFIRM_N=1500 AMQP_BENCH_BATCH_SIZE=100 AMQP_BENCH_SAMPLES=3 AMQP_BENCH_CHANNELS=1,2,4 timeout 120 crystal run tools/perf_publish.cr --release --error-trace` reports ~7.3M ops/s synthetic empty-publish frame encoding, ~65.6k msg/s single-channel publish, and ~64-65k msg/s for 1/2/4 channel concurrent publish on one connection.
  - Evidence: short multi-connection run `AMQP_BENCH_PUBLISH_N=12000 AMQP_BENCH_CONFIRM_N=1200 AMQP_BENCH_BATCH_SIZE=100 AMQP_BENCH_SAMPLES=3 AMQP_BENCH_CHANNELS=1,2,4 AMQP_BENCH_CONNECTIONS=1,2,4 timeout 120 crystal run tools/perf_publish.cr --release --error-trace` reports ~7.15M ops/s synthetic encode, ~66.5k msg/s single publish, ~60-88k msg/s for 1/2/4 channel same-connection publish, and noisy ~78-143k msg/s for 1/2/4 connection shared-queue publish.
  - Evidence: short separate-queue run `AMQP_BENCH_PUBLISH_N=9000 AMQP_BENCH_CONFIRM_N=900 AMQP_BENCH_BATCH_SIZE=100 AMQP_BENCH_SAMPLES=3 AMQP_BENCH_CHANNELS=1,2,4 AMQP_BENCH_CONNECTIONS=1,2,4 timeout 120 crystal run tools/perf_publish.cr --release --error-trace` reports clean benchmark JSON, ~70.9k msg/s single publish, ~71-74k msg/s multi-channel same-connection publish, ~104-155k msg/s shared-queue multi-connection publish, and ~216-225k msg/s for 2/4 connection separate-queue publish.
  - Evidence: same harness for `amqp-client.cr` reports median ~67.6k msg/s fire-and-forget and ~3.29k msg/s sync confirms, but that client fails to compile on the local Crystal 1.20.0-dev compiler because its `amq-protocol` dependency uses stale stdlib internals.
  - Evidence: release-compiler scratch comparison for `amqp-client.cr` with `AMQP_BENCH_PUBLISH_N=9000 AMQP_BENCH_CONFIRM_N=900 AMQP_BENCH_SAMPLES=3 AMQP_BENCH_CHANNELS=1,2,4 AMQP_BENCH_CONNECTIONS=1,2,4 ... /opt/homebrew/bin/crystal run .tmp/bench/amqp_client_modes_bench.cr --release --error-trace` reports ~80.4k msg/s single publish, ~82-88k msg/s multi-channel same-connection publish, ~254-476k msg/s shared-queue multi-connection publish, ~560-658k msg/s separate-queue 2/4 connection publish, and ~2.68k msg/s sync confirms.
  - Evidence: release-compiler `amqp-ng` rerun after socket tuning defaults with `AMQP_BENCH_PUBLISH_N=9000 AMQP_BENCH_CONFIRM_N=900 AMQP_BENCH_BATCH_SIZE=100 AMQP_BENCH_SAMPLES=3 AMQP_BENCH_CHANNELS=1,2,4 AMQP_BENCH_CONNECTIONS=1,2,4 timeout 120 /opt/homebrew/bin/crystal run tools/perf_publish.cr --release --error-trace` reports ~76.9k msg/s single publish, ~78k msg/s batch publish, ~394k msg/s shared-queue 1-connection publish, ~615k/~511k msg/s separate-queue 2/4 connection publish, and ~2.89k msg/s sync confirms.
  - Evidence: contrast run with `AMQP_BENCH_URL='amqp://guest:guest@127.0.0.1:5672/?tcp_nodelay=true&buffer_size=16384' ...` drops separate-queue 2/4 connection publish back to ~211k/~206k msg/s, confirming TCP_NODELAY was a major throughput limiter in this workload.
  - Evidence: post-lock-elision `crystal spec --error-trace` exits 0: 160 examples, 0 failures, 0 errors, 4 pending; `crystal tool format --check src spec tools/perf_publish.cr`, `crystal build tools/perf_publish.cr --no-codegen --error-trace`, and `git diff --check` exit 0.
  - Evidence: post-lock-elision release-compiler `amqp-ng` rerun with the same 9000/900/3 benchmark shape reports ~83.6k msg/s single publish, ~85.1k msg/s batch publish, ~705k/~625k msg/s separate-queue 2/4 connection publish, and ~2.48k msg/s sync confirms.
  - Evidence: paired release-compiler `amqp-client.cr` scratch harness with the same 9000/900/3 benchmark shape reports ~83.0k msg/s single publish, ~682k/~621k msg/s separate-queue 2/4 connection publish, and ~2.52k msg/s sync confirms.
- [x] Close practical feature parity gaps with `amqp-client.cr`.
  - Current near-parity: connection/channel lifecycle, queue/exchange declare/bind/delete/purge/unbind, publish, publisher confirms, get, consume/subscribe, ack/nack/reject/qos, `basic.recover`, `channel.flow`, `tx.*`, `basic_*` compatibility aliases, queue/exchange wrapper objects, `on_return`/`on_cancel`/`on_close` callbacks, blocked/unblocked callbacks, TLS, strict URI config, RabbitMQ/LavinMQ smoke coverage.
  - Progress: added `basic_publish`, `basic_publish_confirm`, `basic_get`, `basic_consume`, `basic_cancel`, `basic_ack`, `basic_reject`, `basic_nack`, and `basic_qos` as thin compatibility aliases. Callback publish aliases auto-enable confirm mode and report broker ack as `true`.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/channel_spec.cr spec/confirms_spec.cr --error-trace` exits 0: 41 examples, 0 failures.
  - Progress: added `Amqp::Queue` and `Amqp::Exchange` wrapper objects plus `Channel#queue`, `#exchange`, `#default_exchange`, `#direct_exchange`, `#topic_exchange`, `#fanout_exchange`, and `#header_exchange`.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/channel_spec.cr --error-trace` exits 0: 19 examples, 0 failures.
  - Progress: added `Channel#on_return` and `Amqp::ReturnedMessage` for mandatory fire-and-forget `basic.return` callbacks without blocking the channel handler fiber.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/channel_spec.cr spec/confirms_spec.cr --error-trace` exits 0: 45 examples, 0 failures.
  - Progress: added synchronous `Channel#basic_recover` plus wire codec for `basic.recover` / `basic.recover-ok`; `basic.recover-async` remains deferred.
  - Evidence: `crystal spec spec/wire/basic_methods_spec.cr spec/api_surface_spec.cr spec/channel_spec.cr --error-trace` exits 0: 26 examples, 0 failures.
  - Progress: added `Connection#blocked?`, `Connection#on_blocked`, and `Connection#on_unblocked`; synthetic connection frames verify the flag and callback dispatch.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/connection_spec.cr --error-trace` exits 0: 14 examples, 0 failures.
  - Progress: added runtime `channel.flow` handling: inbound `channel.flow` updates an internal flow gate, replies with `channel.flow-ok`, and publish paths wait while inactive.
  - Evidence: `crystal spec spec/channel_spec.cr --error-trace` exits 0: 16 examples, 0 failures.
  - Progress: added broker-native `tx_select`, `tx_commit`, `tx_rollback`, and `transaction` helper, plus tx wire codec. Recovery does not promise to preserve an in-flight transaction across reconnect.
  - Evidence: `crystal spec spec/wire/tx_methods_spec.cr spec/api_surface_spec.cr spec/channel_spec.cr --error-trace` exits 0: 25 examples, 0 failures.
  - Progress: added `Channel#flow(active)`, `Channel#on_cancel`, and `Channel#on_close`; broker `basic.cancel` now replies with `basic.cancel-ok` unless `no-wait` is set.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/channel_spec.cr spec/wire/basic_methods_spec.cr --error-trace` exits 0: 29 examples, 0 failures.
  - Progress: added `IO` + explicit byte-size publish overloads for `Channel#basic_publish`, `Channel#basic_publish_confirm`, and `Queue#publish`/`#publish_confirm`.
  - Evidence: `crystal spec spec/api_surface_spec.cr spec/channel_spec.cr --error-trace` exits 0: 26 examples, 0 failures.
  - Work-pool audit: old `amqp-client.cr` exposes `work_pool` through `basic_consume` and `Queue#subscribe`; both are covered by current wrappers.
  - Decision: WebSocket transport is not a practical blocker for `amqp-client.cr` Crystal shard parity in v0; keep it as a future transport research item rather than delaying the local `job_hunter` release.
  - Lower-priority compatibility niceties: `no_wait` overloads and NamedTuple `args` overloads are still omitted from the v0 practical surface unless a real migration site needs them.
  - Evidence: latest full `crystal spec --error-trace` exits 0: 174 examples, 0 failures, 0 errors, 4 pending.
  - Evidence: `crystal tool format --check src spec tools/perf_publish.cr`, `crystal build tools/perf_publish.cr --no-codegen --error-trace`, and `git diff --check` exit 0.
  - Evidence: `cd ../job_hunter && crystal build src/cli.cr --no-codegen --error-trace` exits 0 after the parity additions.
  - Cutline: implement aliases/wrappers/callbacks before niche protocol features; any future WebSocket transport needs explicit docs/spec updates because v0 docs currently defer or omit it.
- [x] Add doc-link lint for `MUST`/`MUST NOT` claims to falsifier IDs.
  - Decision: baseline-gate the current legacy debt instead of pretending all existing normative prose is already linked.
  - Evidence: `spec/docs_falsifier_link_spec.cr` rejects new unlinked normative sections and validates explicit `Falsifier: T-*` references against `docs/16-falsifier-matrix.md`.
  - Evidence: `crystal spec spec/docs_falsifier_link_spec.cr --error-trace` exits 0: 2 examples, 0 failures.
- [x] Clean older architecture docs that still mention deferred v0 names (`CloseReason`, rich stats, recovery callbacks) outside the normative public API docs.
  - Evidence: `rg -n 'CloseReason|close_reason\.origin|ErrorContext|recoverable\?|RecoveryEvent|RecoveryAbandoned|RecoveryTopologyError|ChannelInUseError|PrechargeError|ConnectionStats|ChannelStats|SubscriptionStats|on_recovery|stats\.blocked|blocked\?' docs TODO.md LANDMARKS.md` now reports only explicit "not v0 public API" / deferred references plus this ledger.
  - Evidence: `docs/05-wire-0-9-1/02-classes-methods.md` no longer promises public `ConnectionStats#blocked?` or `Amqp::CloseReason`.
- [x] Run or provision a live TLS broker for the pending `AMQP_TLS_URL` handshake spec.
  - Evidence: provisioned `amqp-ng-rabbit-tls` on localhost:5671 with generated test CA under `.tmp/rabbitmq_tls`.
  - Evidence: `openssl s_client -connect localhost:5671 -servername localhost -CAfile .tmp/rabbitmq_tls/certs/ca_certificate.pem` reports `Verification: OK`.
  - Evidence: `AMQP_TLS_URL='amqps://guest:guest@localhost:5671/' AMQP_TLS_CA_CERT='.tmp/rabbitmq_tls/certs/ca_certificate.pem' crystal spec spec/tls_spec.cr` exits 0: 4 examples, 0 failures, 0 pending.
- [x] Add live broker backpressure/fiber-router measurement for slow subscription behavior.
  - Progress: added opt-in `AMQP_BACKPRESSURE_LIVE=1` harness in `spec/subscription_spec.cr`.
  - Finding: current implementation routes deliveries through a per-channel handler; docs now state that a slow subscription first stalls its channel, while whole-connection pressure requires channel-inbox saturation.
  - Evidence: the harness fills a subscription mailbox without saturating the channel frame inbox and verifies an unrelated channel RPC still completes.
  - Evidence: `AMQP_BACKPRESSURE_LIVE=1 timeout 20 crystal spec spec/subscription_spec.cr --error-trace` exits 0: 6 examples, 0 failures.
  - Cutline: full channel-inbox saturation remains too destructive for default specs and belongs in a separate chaos/perf process-isolated harness.
- [x] Add broker chaos fixtures for heartbeat death, broker restart, channel close mid-RPC, and mandatory return ordering.
  - Progress: added deterministic broker-frame fixtures for channel close mid-RPC and mandatory return ordering.
  - Evidence: `spec/channel_spec.cr` verifies passive declare of a missing queue surfaces `ChannelClosedByBroker` 404 and closes only the channel.
  - Evidence: `spec/confirms_spec.cr` verifies a returned mandatory async publish does not corrupt the following acked publish.
  - Evidence: `crystal spec spec/channel_spec.cr spec/confirms_spec.cr --error-trace` exits 0: 28 examples, 0 failures.
  - Evidence: `spec/recovery_spec.cr` verifies broker-forced `connection.close` recovery and non-forced broker close non-recovery.
  - Evidence: `AMQP_CHAOS_DOCKER_CONTAINER=amqp-ng-rabbit timeout 120 crystal spec spec/chaos_spec.cr --error-trace` exits 0: 2 examples, 0 failures, 0 pending.
- [x] Add leak checks for connect/close cycles, subscription close, async-confirm outcome channels, and recovery records.
  - Evidence: `spec/connection_spec.cr` now checks repeated connect/close cycles clear the channel registry.
  - Evidence: `spec/recovery_spec.cr` now checks recovery topology records shrink after consumer cancel, unbind, queue delete, and exchange delete.
  - Evidence: existing confirm/subscription specs cover async outcome channel close on abort, pending confirm cleanup, and buffered subscription close drain.
  - Evidence: `crystal spec spec/connection_spec.cr spec/recovery_spec.cr --error-trace` exits 0: 11 examples, 0 failures.
- [x] Normalize existing `crystal tool format --check src spec` failures.
  - Evidence: formatted the previously failing files and `crystal tool format --check src spec` exits 0.
- [x] Keep Grok ACP wrapper usable as a bounded review worker; store useful findings in `LANDMARKS.md` only after local verification.
  - Evidence: `/Users/sergey/.grok/bin/grok_review --help` and `grok_worker --help` now pass through to delegate help instead of treating `--help` as a task file.
  - Evidence: smoke task via `grok_review` over ACP stdio returned `OK amqp-ng ACP smoke`.
  - Hygiene: `.grok-acp/` and `.tmp/` are ignored so ACP logs and local harness scratch files do not enter the repo.
