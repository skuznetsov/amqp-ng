# amqp — Falsifier Matrix

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers; reviewers checking that every normative
> claim is testable.
> **Companions:** every other document in `docs/`.

This is the **complete index** of every falsifier test referenced
elsewhere in the doc set. Each row is the smallest test whose failure
refutes the cited normative claim. The implementation MUST provide a
spec file or perf script for every v0 row before v0.1.0 ships
(`docs/17-mvp-cutline.md` §3). Rows explicitly marked "v1 reserved"
are planning contracts for future AMQP 1.0 work and are not v0.1.0
release gates.

The matrix is organised by prefix:

| Prefix       | Subsystem                         | Spec path                                |
|--------------|-----------------------------------|------------------------------------------|
| `T-API-*`    | Public-API smoke + shape          | `spec/api/`                              |
| `T-URI-*`    | URI parsing and config            | `spec/uri/`                              |
| `T-SASL-*`   | SASL mechanisms                   | `spec/sasl/`                             |
| `T-CONN-*`   | Connection lifecycle              | `spec/conn/`                             |
| `T-CHAN-*`   | Channel lifecycle                 | `spec/chan/`                             |
| `T-PUB-*`    | Publisher path / confirms         | `spec/publisher/`                        |
| `T-CONS-*`   | Consumer path                     | `spec/consumer/`                         |
| `T-HB-*`     | Heartbeats                        | `spec/heartbeat/`                        |
| `T-TLS-*`    | TLS                               | `spec/tls/`                              |
| `T-REC-*`    | Recovery                          | `spec/recovery/`                         |
| `T-ERR-*`    | Error model                       | `spec/errors/`                           |
| `T-CODEC-*`  | Wire codec                        | `spec/codec/`                            |
| `T-OBS-*`    | Observability / logging           | `spec/observability/`                    |
| `T-PERF-*`   | Performance roadmap (reserved)    | `spec/perf/`                             |
| `T-REL-*`    | Reliability contract (chaos)      | `spec/reliability/`                      |
| `T-AMQP10-*` | AMQP 1.0 SDD (v1 reserved)        | `spec/amqp10/`                           |

Each row below has:

- **ID** — falsifier identifier.
- **Asserts** — what failure refutes.
- **Source doc** — the doc / section establishing the claim.

Where a row's ID is open-ended (`T-XYZ-NNN..N`), the implementer
chooses the count; the spec file MUST cover every case enumerated in
the source doc.

---

## 1. Public API surface

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-API-IDIOM-001          | Every public method signature is idiomatic Crystal (no positional Int durations, no naked String for URIs). | 01 §P-1                |
| T-API-FIBER-001..005     | Fiber primitives behave correctly (frame demux, select on subs, async confirms, heartbeat sleep, no busy loops). | 01 §P-2     |
| T-API-EXC-001..N         | Every documented failure mode raises the correct exception subclass.    | 01 §P-3                                |
| T-API-NOGLOBAL-001       | No module `@@` state beyond `Log` instances.                            | 01 §P-4                                |
| T-API-URI-001..010       | URI scheme/port/vhost/userinfo/query parsing matrix.                    | 01 §P-5, 04 §1, §2                     |
| T-API-DEPS-001           | `shard.yml` runtime dependencies are empty; empty-dependency lockfiles lock no shards. | 01 §P-8                   |
| T-API-CONNECT-001..010   | `Amqp.connect` keyword + URI precedence, timeout, conflict rules, block form. | 02 §2                            |
| T-API-BLOCK-001..N       | Each row of the blocking-semantics table holds.                         | 02 §7                                  |
| T-API-SURFACE-001        | Public-name enumeration matches the documented list, no more no less.   | 02 §8                                  |

---

## 2. URI and configuration

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-URI-VHOST-001..006     | Vhost encoding/decoding (default, slash-containing, empty path).        | 04 §1.2                                |
| T-URI-USERINFO-001..004  | Userinfo decoding and rejection of unencoded reserved characters.       | 04 §1.3                                |
| T-URI-HOST-001..003      | IPv4 / IPv6 / DNS host forms.                                           | 04 §1.4                                |
| T-URI-PRECEDENCE-001..N  | Each kw-arg vs query-key conflict row.                                  | 04 §2.3                                |
| T-URI-UNKNOWN-001        | Unknown query key raises `Amqp::UriError` listing the bad key.          | 04 §2.4                                |

---

## 3. SASL

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-SASL-PLAIN-001         | PLAIN response is `NUL + user + NUL + password`, accepted by broker.    | 04 §4.1                                |

---

## 4. Connection lifecycle

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-CONN-TCP-001..003      | TCP refused, timeout honored, DNS failure surfaces.                     | 06 §3.1                                |
| T-CONN-HEADER-001..002   | Protocol header correct; wrong-version broker handled.                  | 06 §4                                  |
| T-CONN-STARTOK-001..004  | `start-ok` encoded bytes match reference frame.                         | 06 §5.4                                |
| T-CONN-TUNE-001..005     | `tune-ok` reconciliation rules (clamp, floor, 0-as-unlimited).          | 06 §6.2, 04 §5                         |
| T-CONN-OPEN-001..003     | `connection.open` success, vhost 403, broker reject 530.                | 06 §7                                  |
| T-CONN-INV-001..004      | Steady-state invariants (channel 0 reserved, no callbacks from reader, etc). | 06 §8                              |
| T-CONN-CLOSE-001..010    | Each closure pathway sets `close_reason` to the typed closure exception where available. | 06 §9             |
| T-CONN-CHAN-001..004     | Channel allocation, id reuse, channel_max exhaustion.                   | 06 §10                                 |
| T-CONN-BLOCKED-001       | `connection.blocked/unblocked` updates flag and callbacks.              | 06 §11                                 |
| T-CONN-FIBERS-001..003   | Exactly the documented fibers; no busy loop.                            | 06 §12                                 |
| T-CONN-LEAK-001          | 1000 connect/close cycles, fiber count stable.                          | 06 §13                                 |

---

## 5. Channel lifecycle

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-CHAN-OPEN-001..002     | Happy path; open timeout.                                               | 07 §2                                  |
| T-CHAN-CONCURRENCY-001   | Concurrent state-change raises `ConcurrencyError`.                      | 07 §3.1                                |
| T-CHAN-CONFIRMS-001      | `confirm_select` idempotent.                                            | 07 §3.2                                |
| T-CHAN-FLOW-001          | Broker-initiated `channel.flow` blocks/unblocks publishes.               | 07 §3.3                                |
| T-CHAN-FLOW-002          | Client-initiated `Channel#flow` receives `channel.flow-ok`.              | 07 §3.3                                |
| T-CHAN-TX-001            | `tx.rollback` discards and `tx.commit` publishes transactional messages. | 02 §4.2                                |
| T-CHAN-BROKERCLOSE-001..N| One per reply-code; correct exception subclass surfaces.                | 07 §5                                  |
| T-CHAN-CALLERCLOSE-001   | Caller-initiated close, idempotent, no second exception.                | 07 §6                                  |
| T-CHAN-CONNDIES-001..004 | All channels close when connection dies.                                | 07 §7                                  |
| T-CHAN-ATOMIC-PUBLISH-001| Concurrent publishes on two channels never interleave bytes.            | 07 §8, 08 §3                           |
| T-CHAN-SUB-CLOSE-001..002| Graceful sub close drains; abrupt sub close discards.                   | 07 §9                                  |

---

## 6. Publisher confirms

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-PUB-MODE-001           | `publish_confirm` on non-confirms channel raises `ConfigurationError`.  | 08 §2                                  |
| T-PUB-MODE-002           | `confirm_select` idempotent.                                            | 08 §2                                  |
| T-PUB-PROPS-001..N       | Properties encode correctly per property-flags bitmap (corpus).         | 08 §3.1                                |
| T-PUB-CHUNK-001..003     | Body chunking at frame boundaries.                                      | 08 §3.2                                |
| T-PUB-FF-001..002        | Fire-and-forget tracker interaction on confirms vs non-confirms.        | 08 §4                                  |
| T-PUB-CONFIRM-001..006   | Sync confirm: ack/nack/returned/timeout/broker-close.                   | 08 §5                                  |
| T-PUB-ASYNC-001..004     | Async confirm: ack/nack/close-no-value/no-fiber-leak.                   | 08 §6                                  |
| T-PUB-MULTIPLE-001..002  | Range ack/nack; out-of-order tag raises `PublishOutOfOrderError`.        | 08 §7                                  |
| T-PUB-RETURN-001..004    | `basic.return` correlation to mandatory publish.                        | 07 §10.1, 08 §5.3                      |
| T-PUB-RECOV-001          | Mid-publish broker kill → recovery → outcome resolves.                  | 08 §8                                  |
| T-PUB-MONO-001           | Tracker tags strictly monotonic.                                        | 08 §9                                  |
| T-PUB-MEM-001            | Tracker memory shrinks under load.                                      | 08 §9                                  |
| T-PUB-CONC-001           | No torn frames under concurrent publish.                                | 08 §9                                  |

---

## 7. Consumer

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-CONS-BLOCK-001..004    | Block form happy path; exception with auto_ack false → reject; exclusive collision; arguments forwarded. | 09 §2 |
| T-CONS-SELECT-001        | `select when msg = sub.receive` compiles and runs.                      | 09 §3.3                                |
| T-CONS-BACKPRESSURE-001  | Slow sub fills mailbox; unrelated channel RPC still completes before channel inbox saturation. | 09 §3.4                 |
| T-CONS-CANCEL-001..003   | Caller cancel; broker cancel; drain after cancel.                       | 09 §3.5, §3.6                          |
| T-CONS-SPAWN-001         | `spawn_loop` ack/reject semantics correct.                              | 09 §3.7                                |
| T-CONS-GET-001..003      | `basic.get`: ok / empty / mid-channel-error.                            | 09 §4                                  |
| T-CONS-ACK-001..006      | Ack/nack/reject flag combinations + unknown-tag triggers 406.            | 09 §5                                  |
| T-CONS-PREFETCH-001..002 | `basic.qos` global vs per-consumer.                                     | 09 §6                                  |
| T-CONS-ASSEMBLE-001..003 | Message body assembly: validation, byte-exact, mid-frame-close discard. | 09 §7                                  |

---

## 8. Heartbeats

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-HB-NEG-001..004        | Negotiation: client-zero, broker-zero, both, sub-second truncation.      | 10 §1                                  |
| T-HB-SEND-001            | Send cadence under idle.                                                | 10 §3                                  |
| T-HB-SEND-002            | Coalescing under load (no redundant sends).                             | 10 §3                                  |
| T-HB-SEND-003            | Write failure during HB send → connection close.                        | 10 §3                                  |
| T-HB-RECV-001..004       | Receive-deadline detection; no false positives; wall-clock-jump immunity; correct origin tag. | 10 §4              |
| T-HB-NOPAUSE-001         | HB does not add measurable publish latency.                             | 10 §5                                  |
| T-HB-OFF-001             | `heartbeat=0` connection survives long idle.                            | 10 §6                                  |
| T-HB-LAVIN-TLS-001       | LavinMQ TLS heartbeat edge case (regression witness).                   | 13 §3.5                                |

---

## 9. TLS

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-TLS-SCHEME-001..003    | `amqp` + `tls:` rejected; `amqps` default TLS; TLS policy query keys rejected. | 11 §1                            |
| T-TLS-CTX-001..003       | Caller-supplied passthrough; default build; default helper.             | 11 §2                                  |
| T-TLS-SNI-001            | SNI passed to OpenSSL.                                                  | 11 §3.1                                |
| T-TLS-HOSTNAME-001       | Hostname verification rejects wrong-SAN cert.                           | 11 §3.2, 20 RISK-2                     |
| T-TLS-ERR-001..006       | Each stdlib error mapped to correct shard subclass.                     | 11 §3.3                                |
| T-TLS-ROTATE-001         | Reconnect uses a fresh default context when no caller context was supplied. | 11 §4                               |
| T-TLS-STATS-001          | `tls_version`/`tls_cipher`/`peer_certificate_subject` populated.        | 11 §6, 19 §2                           |

---

## 10. Recovery

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-REC-TRIGGER-001..N     | Recovery triggered for each origin (network, heartbeat, recoverable broker); NOT triggered for caller / unrecoverable. | 12 §2 |
| T-REC-BACKOFF-001        | Backoff schedule under deterministic Random.                            | 12 §4.1                                |
| T-REC-ABANDON-001        | After `max_attempts`, surrenders with `RecoveryExhaustedError` or latest typed failure. | 12 §4.2              |
| T-REC-ORDER-001..006     | Re-apply order: confirm.select, qos, exchanges, queues, bindings, consumers. | 12 §5                             |
| T-REC-ALL-001            | Full sequence end-to-end against a real broker restart.                 | 12 §5                                  |
| T-REC-RENAME-001         | Server-named queue recovery remaps consumers and default-exchange pending publish replay. | 12 §5                                  |
| T-REC-TOPO-FAIL-001      | Broker rejection during topology replay fails closed with `RecoveryExhaustedError`. | 12 §5                                  |
| T-REC-CONS-TAG-001       | Explicit consumer tags are re-installed and used for post-recovery deliveries. | 12 §3.2 / 12 §5                         |
| T-REC-CONS-FAIL-001      | Broker rejection during consumer replay fails closed with `RecoveryExhaustedError`. | 12 §5                                  |
| T-REC-REPLAY-001         | Kill broker mid-publish, recovery re-publishes, outcome resolves.       | 12 §6                                  |
| T-REC-DURING-001..004    | Behavior during `Recovering`: publish, receive, topology, close.        | 12 §7                                  |
| T-REC-MEM-001            | Recovery records bounded under steady-state load.                       | 12 §9                                  |
| T-REC-NONE-001           | With `Recovery::None`, broker close surfaces exception within 500 ms.   | 01 §P-10                               |

---

## 11. Errors

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-ERR-MAP-001..N         | Each row of the reply-code → subclass table.                            | 03 §3                                  |
| T-ERR-FIELDS-001..N      | Broker close and publisher exceptions expose documented fields.          | 03 §2                                  |
| T-ERR-COMMIT-001..006    | Publisher-confirm exception class correctly classifies commit state.     | 03 §5                                  |

---

## 12. Wire codec (deferred until corpus)

These rows are placeholders until `docs/05-wire-0-9-1/` is written
and the frame corpus is captured. The IDs are reserved.

| ID                       | Asserts (to be filled in 05-wire-0-9-1)                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-CODEC-PURE-001         | Codec module has no fibers, sockets, or non-Log module state.            | 01 §P-9                                |
| T-CODEC-FRAME-001..N     | Frame encode/decode round-trip for each frame type.                     | 05/00                                  |
| T-CODEC-FIELD-001..N     | Each field type encodes/decodes correctly.                              | 05/01                                  |
| T-CODEC-METHOD-001..N    | Each method's argument list encodes correctly.                          | 05/02                                  |
| T-CODEC-PROPS-001..N     | Content-properties encoding matches the AMQP spec bytewise.             | 05/03                                  |
| T-CODEC-CORPUS-001       | Decode/encode round-trip for every captured frame in `spec/fixtures/frames/`. | 05/04                            |

---

## 13. Observability

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-OBS-STATS-001..N       | `Amqp::Stats::Snapshot` counters start at zero and update on publish/confirm/consume/recovery events. | 19 §1, §2 |
| T-OBS-LOG-001            | Reserved future logging contract.                                       | 19 §4                                  |

`T-OBS-CONN-*`, `T-OBS-CHAN-*`, and `T-OBS-SUB-*` are reserved for the
deferred rich stats model.

---

## 14. Performance

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-PERF-HANDSHAKE-001     | PERF-1 p99 handshake < 10 ms; PERF-1-TLS < 50 ms.                       | 14 §2                                  |
| T-PERF-PUB-001           | PERF-2 fire-and-forget throughput.                                      | 14 §3; `spec/perf/t_perf_pub_001_spec.cr` |
| T-PERF-PUB-002           | PERF-3 async confirm throughput.                                        | 14 §4; `spec/perf/t_perf_pub_002_spec.cr` |
| T-PERF-MULTICHAN-001     | PERF-4 eight-channel aggregate throughput.                              | 14 §5                                  |
| T-PERF-CONS-001          | PERF-5 consume throughput.                                              | 14 §6                                  |
| T-PERF-MEM-001..002      | PERF-6/7 memory bounds.                                                 | 14 §7, §8                              |
| T-PERF-GC-001            | PERF-8 GC pressure bound.                                               | 14 §9                                  |
| T-PERF-STATS-001         | PERF-9 stats read < 1 µs/call.                                          | 14 §10                                 |
| T-PERF-RECOV-001         | PERF-10 recovery dead window < 2 s median.                              | 14 §11                                 |

`T-PERF-PUB-001` and `T-PERF-PUB-002` have default-off live carriers
under `spec/perf/`. The remaining `T-PERF-*` rows are reserved roadmap
falsifiers until their carriers exist. None of these rows are current
release-blocking checks.

---

## 15. Reliability (chaos)

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-REL-AT-LEAST-ONCE-001..004 | REL-1: each outcome class behaves as documented.                    | 15 §2                                  |
| T-REL-CONFIRM-ONCE-001..003  | REL-2: outcomes delivered exactly once per tag.                     | 15 §3                                  |
| T-REL-WAKE-001..006      | REL-3: fibers wake within 500 ms of close, correct origin.              | 15 §4                                  |
| T-REL-PUB-FF-001         | REL-4: no silent loss in fire-and-forget.                                | 15 §5                                  |
| T-REL-ACK-IMMEDIATE-001  | REL-5: each ack is one wire frame, in order.                            | 15 §6                                  |
| T-REL-RECOV-TOPO-001..003| REL-6: topology re-declared completely.                                 | 15 §7                                  |
| T-REL-RECOV-CONFIRM-001  | REL-7: unconfirmed in-flight replayed, outcome resolves.                | 15 §8                                  |
| T-REL-RECOV-DUP-001      | REL-7 caveat: if the original publish reached the broker before ack loss, replay can produce two deliveries. | 15 §8                                  |
| T-REL-CALLER-CLOSE-001..002 | REL-8: caller close is final.                                         | 15 §9                                  |
| T-REL-HB-DETECT-001      | REL-9: broker silence detected within `2*heartbeat + 500 ms`.             | 15 §10                                 |
| T-REL-TLS-001..003       | REL-10: TLS verification rejects bad CA / wrong CN / expired cert.       | 15 §11                                 |
| T-REL-CONC-001           | REL-11: concurrent channel allocation race-free.                        | 15 §12                                 |
| T-REL-LEAK-001           | REL-12: no fiber leak across 10k connect/close cycles.                  | 15 §13                                 |

---

## 16. AMQP 1.0 SDD (v1 reserved)

These rows define the first executable milestones for future AMQP 1.0
work. They are intentionally excluded from v0.1.0 acceptance criteria.

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-AMQP10-CUTLINE-001     | v0 builds expose no `Amqp::Session`, `Amqp::Link`, `protocol:` keyword, or AMQP 1.0 runtime path. | 22 §2                    |
| T-AMQP10-CODEC-001..006  | AMQP 1.0 primitive codec covers null, booleans, integers, symbols, strings, and binary values byte-exactly. | 22 §4       |
| T-AMQP10-PERFORMATIVE-001..011 | AMQP 1.0 performatives encode/decode byte-exactly for open, begin, attach, flow, transfer, disposition, detach, end, close, SASL init, and SASL outcome. | 22 §4 |
| T-AMQP10-MSG-001..006    | AMQP 1.0 message sections round-trip for header, delivery-annotations, message-annotations, properties, application-properties, and data body. | 22 §5 |
| T-AMQP10-HANDSHAKE-001..005 | SASL PLAIN, AMQP protocol header, open/begin, idle-timeout negotiation, and close handshake interoperate with one target broker. | 22 §6 |
| T-AMQP10-LINK-001..008   | Sender and receiver links handle attach, credit, transfer, settlement, reject, release, detach, and link error. | 22 §7         |
| T-AMQP10-API-001..006    | Public v1 API exposes connection/session/link concepts without reusing 0-9-1 channel/topology names incorrectly. | 22 §8       |
| T-AMQP10-RECOV-001..004  | v1 reconnect policy defines unsettled delivery behavior, link reattach, duplicate risk, and fail-closed partial recovery. | 22 §9     |
| T-AMQP10-BROKER-001..003 | RabbitMQ AMQP 1.0 plugin, ActiveMQ Artemis, or Qpid smoke targets are pinned and versioned before claims become normative. | 22 §10 |

---

## 17. Maintenance rule

Every PR that touches normative prose in `docs/` MUST also update
this matrix when:

- A new `MUST`/`MUST NOT` is added → add a falsifier row.
- A normative claim is removed → remove the falsifier row (and the
  spec file).
- A normative claim's behavior changes → update the row (and the
  spec file).

PRs that add normative prose without a falsifier row fail the
"doc-link" CI check (lints by grepping every `MUST`/`MUST NOT` and
verifying a `Falsifier: T-*` reference is in the same section, AND
that the referenced T-* appears in this matrix).
