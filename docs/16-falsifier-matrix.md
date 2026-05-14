# amqp — Falsifier Matrix

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers; reviewers checking that every normative
> claim is testable.
> **Companions:** every other document in `docs/`.

This is the **complete index** of every falsifier test referenced
elsewhere in the doc set. Each row is the smallest test whose failure
refutes the cited normative claim. The implementation MUST provide a
spec file or perf script for every row before v0.1.0 ships
(`docs/17-mvp-cutline.md` §3).

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
| `T-PERF-*`   | Performance contract              | `spec/perf/`                             |
| `T-REL-*`    | Reliability contract (chaos)      | `spec/reliability/`                      |

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
| T-API-DEPS-001           | `shard.lock` runtime-scope is empty.                                    | 01 §P-8                                |
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
| T-SASL-EXTERNAL-001..003 | EXTERNAL preconditions; happy path; broker reject.                      | 04 §4.2                                |

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
| T-CONN-CLOSE-001..010    | Each closure pathway and `close_reason.origin` value.                   | 06 §9                                  |
| T-CONN-CHAN-001..004     | Channel allocation, id reuse, channel_max exhaustion.                   | 06 §10                                 |
| T-CONN-BLOCKED-001       | `connection.blocked` flips `stats.blocked?`.                            | 06 §11                                 |
| T-CONN-FIBERS-001..003   | Exactly the documented fibers; no busy loop.                            | 06 §12                                 |
| T-CONN-LEAK-001          | 1000 connect/close cycles, fiber count stable.                          | 06 §13                                 |

---

## 5. Channel lifecycle

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-CHAN-OPEN-001..002     | Happy path; open timeout.                                               | 07 §2                                  |
| T-CHAN-CONCURRENCY-001   | Concurrent state-change raises `ConcurrencyError`.                      | 07 §3.1                                |
| T-CHAN-CONFIRMS-001      | `confirm_select` idempotent.                                            | 07 §3.2                                |
| T-CHAN-FLOW-001          | `channel.flow` blocks/unblocks publishes.                               | 07 §3.3                                |
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
| T-CONS-BACKPRESSURE-001  | Slow sub blocks frame-reader; speed-up releases.                        | 09 §3.4                                |
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
| T-TLS-SCHEME-001..003    | `amqp` + `tls:` rejected; `amqps` without TLS rejected; `amqps` + URI keys + ctx rejected. | 11 §1               |
| T-TLS-CTX-001..003       | Caller-supplied passthrough; URI-driven build; default helper.          | 11 §2                                  |
| T-TLS-SNI-001            | SNI passed to OpenSSL.                                                  | 11 §3.1                                |
| T-TLS-HOSTNAME-001       | Hostname verification rejects wrong-SAN cert.                           | 11 §3.2, 20 RISK-2                     |
| T-TLS-ERR-001..006       | Each stdlib error mapped to correct shard subclass.                     | 11 §3.3                                |
| T-TLS-ROTATE-001         | Cert rotation observed on reconnect (URI-driven only).                  | 11 §4                                  |
| T-TLS-EXTERNAL-001..003  | EXTERNAL preconditions, success, broker reject.                         | 11 §5                                  |
| T-TLS-STATS-001          | `tls_version`/`tls_cipher`/`peer_certificate_subject` populated.        | 11 §6, 19 §2                           |

---

## 10. Recovery

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-REC-TRIGGER-001..N     | Recovery triggered for each origin (network, heartbeat, recoverable broker); NOT triggered for caller / unrecoverable. | 12 §2 |
| T-REC-BACKOFF-001        | Backoff schedule under deterministic Random.                            | 12 §4.1                                |
| T-REC-ABANDON-001        | After `max_attempts`, surrenders to `RecoveryAbandoned`.                | 12 §4.2                                |
| T-REC-ORDER-001..006     | Re-apply order: confirm.select, qos, exchanges, queues, bindings, consumers. | 12 §5                             |
| T-REC-ALL-001            | Full sequence end-to-end against a real broker restart.                 | 12 §5                                  |
| T-REC-REPLAY-001         | Kill broker mid-publish, recovery re-publishes, outcome resolves.       | 12 §6                                  |
| T-REC-DURING-001..004    | Behavior during `Recovering`: publish, receive, topology, close.        | 12 §7                                  |
| T-REC-CB-001..003        | `on_recovery` fires; exception logged not propagated; order preserved.  | 12 §8                                  |
| T-REC-MEM-001            | Recovery records bounded under steady-state load.                       | 12 §9                                  |
| T-REC-NONE-001           | With `Recovery::None`, broker close surfaces exception within 500 ms.   | 01 §P-10                               |

---

## 11. Errors

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-ERR-MAP-001..N         | Each row of the reply-code → subclass table.                            | 03 §3                                  |
| T-ERR-ORIGIN-001..006    | `close_reason.origin` correct for each closure pathway.                 | 03 §4                                  |
| T-ERR-RECOV-001..N       | `Amqp::Error.recoverable?(class)` matches the documented table.          | 03 §5                                  |
| T-ERR-FIBER-001..005     | Each fiber's death path surfaces correct exception within bound.        | 03 §6                                  |
| T-ERR-COMMIT-001..006    | The "no doubt" rule: each exception class correctly classifies commit state. | 03 §8                            |

---

## 12. Wire codec (deferred until corpus)

These rows are placeholders until `docs/05-wire-0-9-1/` is written
and the frame corpus is captured. The IDs are reserved.

| ID                       | Asserts (to be filled in 05-wire-0-9-1)                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-CODEC-PURE-001         | Codec module has no fibers, sockets, or non-Log module state.            | 01 §P-9                                |
| T-CODEC-FRAME-001..N     | Frame encode/decode round-trip for each frame type.                     | 05/00 (TBD)                            |
| T-CODEC-FIELD-001..N     | Each field type encodes/decodes correctly.                              | 05/01 (TBD)                            |
| T-CODEC-METHOD-001..N    | Each method's argument list encodes correctly.                          | 05/02 (TBD)                            |
| T-CODEC-PROPS-001..N     | Content-properties encoding matches the AMQP spec bytewise.             | 05/03 (TBD)                            |
| T-CODEC-CORPUS-001       | Decode/encode round-trip for every captured frame in `spec/fixtures/frames/`. | 05/04 (TBD)                      |

---

## 13. Observability

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-OBS-CONN-001..N        | One assertion per `ConnectionStats` field's update timing.              | 19 §2                                  |
| T-OBS-CHAN-001..N        | One per `ChannelStats` field.                                            | 19 §3                                  |
| T-OBS-SUB-001            | `SubscriptionStats` fields update correctly.                            | 19 §4                                  |
| T-OBS-RECOVERY-001       | Counters preserved across recovery; `open_since` resets.                | 19 §5                                  |
| T-OBS-LOG-001            | Each documented log event fires with the expected source name + severity. | 19 §6                              |

---

## 14. Performance

| ID                       | Asserts                                                                 | Source doc                            |
|--------------------------|-------------------------------------------------------------------------|---------------------------------------|
| T-PERF-HANDSHAKE-001     | PERF-1 p99 handshake < 10 ms; PERF-1-TLS < 50 ms.                       | 14 §2                                  |
| T-PERF-PUB-001           | PERF-2 fire-and-forget throughput.                                      | 14 §3                                  |
| T-PERF-PUB-002           | PERF-3 async confirm throughput.                                        | 14 §4                                  |
| T-PERF-MULTICHAN-001     | PERF-4 eight-channel aggregate throughput.                              | 14 §5                                  |
| T-PERF-CONS-001          | PERF-5 consume throughput.                                              | 14 §6                                  |
| T-PERF-MEM-001..002      | PERF-6/7 memory bounds.                                                 | 14 §7, §8                              |
| T-PERF-GC-001            | PERF-8 GC pressure bound.                                               | 14 §9                                  |
| T-PERF-STATS-001         | PERF-9 stats read < 1 µs/call.                                          | 14 §10                                 |
| T-PERF-RECOV-001         | PERF-10 recovery dead window < 2 s median.                              | 14 §11                                 |

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
| T-REL-CALLER-CLOSE-001..002 | REL-8: caller close is final.                                         | 15 §9                                  |
| T-REL-HB-DETECT-001      | REL-9: broker silence detected within `2*heartbeat + 500 ms`.             | 15 §10                                 |
| T-REL-TLS-001..003       | REL-10: TLS verification rejects bad CA / wrong CN / expired cert.       | 15 §11                                 |
| T-REL-CONC-001           | REL-11: concurrent channel allocation race-free.                        | 15 §12                                 |
| T-REL-LEAK-001           | REL-12: no fiber leak across 10k connect/close cycles.                  | 15 §13                                 |

---

## 16. Maintenance rule

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
