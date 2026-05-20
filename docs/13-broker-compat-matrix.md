# amqp — Broker Compatibility Matrix

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers verifying behavior across brokers; users
> choosing a broker for production.
> **Companions:** `docs/00-overview.md` §1 (target brokers),
> `docs/15-reliability-contract.md`, `docs/16-falsifier-matrix.md`.

The shard's v0 scope is two brokers: **RabbitMQ 3.13+** and
**LavinMQ 2.x**. This document is the **complete** statement of what
the shard guarantees on each, and where they differ.

---

## 1. Target brokers

### 1.1 RabbitMQ 3.13+

Reference implementation of AMQP 0-9-1 with publisher confirms.
Open-source (Mozilla Public License 2.0). Written in Erlang. Mature
ecosystem, plug-in surface, extensive documentation.

The shard's behavior is validated against RabbitMQ versions 3.13.x
and 4.0.x. Older versions (3.12 and below) MAY work but are not in
the test matrix.

### 1.2 LavinMQ 2.x

Alternative AMQP 0-9-1 broker written in Crystal by the CloudAMQP
team. Lighter resource footprint, faster cold start, smaller surface
(no advanced clustering, no plug-ins). Used in production by the
shard author for some workloads.

The shard's behavior is validated locally against LavinMQ 2.4.0.
Older LavinMQ 2.x releases MAY work, but they are not verified in the
current checkout until a pinned broker CI matrix is added.

---

## 2. Feature matrix

For each feature, the table records:

- **Supported.** ✓ = full support; ✗ = not supported; ◐ = partial (see
  notes).
- **Notes.** Behavioral differences the shard handles.

### 2.1 Core protocol

| Feature                              | RabbitMQ 3.13+ | LavinMQ 2.x | Shard behavior                            |
|--------------------------------------|----------------|-------------|-------------------------------------------|
| AMQP 0-9-1 wire                      | ✓              | ✓           | Identical encode/decode                    |
| AMQP 0-9 / 0-8                       | ✗ (removed)    | ✗           | Out of scope (`docs/00-overview.md` §1.1)  |
| Heartbeats                           | ✓              | ✓           | `docs/10-heartbeats.md`                    |
| Publisher confirms                   | ✓              | ✓           | `docs/08-publisher-confirms.md`            |
| Transactions (`tx.*`)                | ✓              | ✓           | Broker-native `tx_select/commit/rollback` |
| Channel flow (`channel.flow`)        | ◐ (deprecated) | ✓           | Shard handles per `docs/07` §3.3           |
| Connection blocked notifications     | ✓              | ✓           | `blocked?`, `on_blocked`, `on_unblocked`   |
| Connection forced close (320)        | ✓              | ✓           | Recoverable per `docs/12` §2               |

### 2.2 SASL mechanisms

| Mechanism      | RabbitMQ 3.13+      | LavinMQ 2.x  | Shard          |
|----------------|---------------------|--------------|----------------|
| PLAIN          | ✓                   | ✓            | ✓ (default)    |
| EXTERNAL       | ✓ (via rabbitmq_auth_mechanism_ssl) | ✓ | ✗ (deferred) |
| AMQPLAIN       | ✓                   | ✗            | ✗ (deferred)   |
| ANONYMOUS      | ✗                   | ✗            | ✗              |
| OAUTH2         | ✓ (via plug-in)     | ✗            | ✗ (deferred)   |

RabbitMQ's EXTERNAL mechanism requires the `rabbitmq_auth_mechanism_ssl`
plug-in to be enabled. The shard does not implement SASL EXTERNAL in
v0.1.0; TLS client certificates remain transport-level policy only.

### 2.3 Exchange types

| Type                | RabbitMQ 3.13+ | LavinMQ 2.x | Shard accepts string? |
|---------------------|----------------|-------------|-----------------------|
| `direct`            | ✓              | ✓           | ✓                     |
| `fanout`            | ✓              | ✓           | ✓                     |
| `topic`             | ✓              | ✓           | ✓                     |
| `headers`           | ✓              | ✓           | ✓                     |
| `x-delayed-message` | ◐ (plug-in)    | ✗           | ✓ (broker rejects if missing) |
| `x-consistent-hash` | ◐ (plug-in)    | ✗           | ✓ (broker rejects if missing) |

The shard passes the type string through unchanged; broker enforcement
is the broker's job (`docs/02-public-api.md` §4.7).

### 2.4 Queue arguments

| Argument                  | RabbitMQ 3.13+ | LavinMQ 2.x | Notes                          |
|---------------------------|----------------|-------------|--------------------------------|
| `x-message-ttl`           | ✓              | ✓           | Same semantics                 |
| `x-expires`               | ✓              | ✓           | Same semantics                 |
| `x-max-length`            | ✓              | ✓           | Same semantics                 |
| `x-max-length-bytes`      | ✓              | ✓           | Same semantics                 |
| `x-overflow`              | ✓              | ✓           | drop-head / reject-publish     |
| `x-dead-letter-exchange`  | ✓              | ✓           | Same semantics                 |
| `x-dead-letter-routing-key`| ✓             | ✓           | Same semantics                 |
| `x-queue-type`            | ✓ (classic/quorum/stream) | ◐ (classic only) | LavinMQ doesn't have quorum/stream |
| `x-stream-*`              | ✓ (with quorum/stream queues) | ✗ | Reject as broker error if used on LavinMQ |
| `x-single-active-consumer`| ✓              | ✓           | Same semantics                 |

### 2.5 Consumer arguments

| Argument                       | RabbitMQ 3.13+ | LavinMQ 2.x | Notes                       |
|--------------------------------|----------------|-------------|-----------------------------|
| `x-priority`                   | ✓              | ✓           | Same semantics              |
| `x-cancel-on-ha-failover`      | ✓              | ✗           | Used for HA; LavinMQ doesn't have HA in v2 |
| `x-stream-offset` and friends  | ✓              | ✗           | Stream-queue only           |

### 2.6 Connection-time capabilities

The shard advertises capabilities in `client-properties` per
`docs/06-connection-lifecycle.md` §5.4. Broker responses to these:

| Capability                     | RabbitMQ 3.13+ | LavinMQ 2.x | Effect if missing                |
|--------------------------------|----------------|-------------|----------------------------------|
| `publisher_confirms`           | server: ✓      | server: ✓   | n/a — both support               |
| `exchange_exchange_bindings`   | server: ✓      | server: ✓   | n/a                              |
| `basic.nack`                   | server: ✓      | server: ✓   | n/a                              |
| `consumer_cancel_notify`       | server: ✓      | server: ✓   | n/a                              |
| `connection.blocked`           | server: ✓      | server: ✓   | n/a                              |
| `authentication_failure_close` | server: ✓      | server: ✓   | n/a                              |

The shard does NOT condition any code path on a missing server
capability — every target broker supports the set above.

---

## 3. Known behavioral differences

### 3.1 `channel.flow` semantics

RabbitMQ 3.x has effectively deprecated `channel.flow` (it never
sends it; flow control is via TCP). LavinMQ may emit `channel.flow`
under load. The shard's flow-handling logic (`docs/07` §3.3) is
exercised primarily against LavinMQ.

### 3.2 Default vhost authorisation

RabbitMQ's `guest` user is restricted by default to `localhost`
connections only. LavinMQ has no such default restriction. Connecting
as `guest/guest` over network to RabbitMQ MUST fail with
`Amqp::VhostAccessError`; over network to LavinMQ it MAY succeed.
The shard does not special-case this; the broker's behavior is the
ground truth.

### 3.3 Body-frame fragmentation

Both brokers accept messages spanning multiple body frames up to
`frame-max`. RabbitMQ's `frame-max` default is 131072 bytes; LavinMQ's
default is 4096 bytes. Callers who publish large messages SHOULD
override via `?frame_max=...` on LavinMQ; the shard otherwise
fragments aggressively.

### 3.4 Server-named queue persistence

When a queue is declared with empty name + `durable: true`, both
brokers honor durability. The assigned name is preserved on disk.
Recovery (`Recovery::Full`) re-declares with the assigned name, NOT
empty — which means a re-declaration after broker restart will
succeed only if the queue actually exists. If the broker was
restarted with empty queue store, the re-declare creates a fresh
queue with the same name (assuming no collision). This is the
expected behavior per AMQP 0-9-1.

### 3.5 Heartbeat interaction with TLS

LavinMQ's TLS implementation buffers slightly more aggressively than
RabbitMQ's. Under very tight heartbeat intervals (≤5 seconds) on
LavinMQ TLS, the shard MAY observe occasional spurious receive-
deadline near-expiries. The default 60-second heartbeat avoids this.
Documented as a known limitation; T-HB-LAVIN-TLS-001 is a regression
witness, NOT a guarantee.

### 3.6 `basic.recover-async`

RabbitMQ implements `basic.recover-async`; LavinMQ implements only
`basic.recover` (synchronous). Both are out of v0 scope.

---

## 4. Test policy

The release test policy is to run every test in `spec/` against BOTH
brokers. A test passing on one but not the other is treated as a
divergence bug; the shard MUST resolve by either:

- Conforming to common AMQP 0-9-1 behavior (most cases).
- Skipping the test on the non-conforming broker with an explicit
  `pending` marker citing this document.
- Documenting the divergence here and adjusting the test.

No test silently skips because of broker version. Every skip MUST
have a documented reason.

Current local evidence in this checkout:

- RabbitMQ 3.13.7: default suite and opt-in TLS/backpressure/chaos
  suites pass.
- LavinMQ 2.4.0: default suite passes with the same live-gated
  pending examples as RabbitMQ; opt-in backpressure and Docker
  pause/restart chaos pass. LavinMQ TLS is not configured locally.

A checked-in plain-AMQP broker CI matrix covers RabbitMQ 3.13.7 and
LavinMQ 2.4.0 on Crystal 1.20.2. TLS, backpressure timing, Docker
chaos, and performance gates remain opt-in/local release checks.

---

## 5. Broker versions tested against

Pinned image tags intended for CI:

- `rabbitmq:3.13.7-management` (CI floor).
- `rabbitmq:4.0.5-management` (CI latest stable).
- `cloudamqp/lavinmq:2.4.0` (currently locally verified).

The CI workflow MUST pin these tags rather than `latest` so the
matrix is deterministic. Upgrading the pinned versions is a
deliberate release-blocking step.

---

## 6. Out-of-matrix brokers (informative)

The shard MAY work against other AMQP 0-9-1 brokers, but they are
not in the test matrix:

- **Apache Qpid C++ broker.** Speaks AMQP 0-9-1 but with subtle
  field-table type quirks. Compatibility is best-effort; bug
  reports welcome but not release-blocking.
- **OpenAMQ.** Effectively unmaintained since 2014. Not supported.
- **Apache ActiveMQ Classic** (with AMQP support). Speaks AMQP 1.0,
  not 0-9-1. Out of v0 scope.

Callers using out-of-matrix brokers are on their own. The shard's
diagnostics (`docs/19-observability.md`) help, but no engineer
hours are reserved for chasing third-party broker bugs.
