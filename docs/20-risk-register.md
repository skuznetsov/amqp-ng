# amqp — Risk Register

> **Document status:** Draft v0.1, refreshed 2026-05-18
> **Audience:** Implementers; release reviewers.
> **Companions:** every other document in `docs/`.

This document is the **complete** v0 risk register. Each risk is
identified `RISK-N` and carries: description, severity, likelihood,
mitigation status, and the relevant doc / falsifier reference.

A "risk" here is anything that could falsify a `MUST`/`SHOULD` in
the rest of the doc set, or that could surprise a caller. The
register is intentionally honest — risks are recorded, not hidden.

---

## 1. RISK-1: Crystal stdlib drift

**Description.** Crystal stdlib evolves; identifiers, ivars, and
default behaviors change across versions. The vendored
`amq-protocol` shard reached into `IO::Memory.@writeable` and broke
when 1.20.0-dev renamed it to `@writable`. The same class of bug
COULD bite this shard if it reached into stdlib internals.

**Mitigation.** P-1 / P-9 forbid stdlib-ivar access. The current
source is written against public stdlib APIs. `T-CODEC-PURE-001` is
implemented by `spec/code_hygiene_spec.cr`: it scans shard/spec source
for direct private-ivar reach-in and scans `src/amqp/wire` for fiber,
socket, TLS, or class/module-variable state.

**Severity.** High (would break the entire shard on a Crystal upgrade).

**Likelihood (post-mitigation).** Low in local/default specs. This
still becomes stronger once checked-in CI runs the default spec suite.

**Status.** Mitigated by design + local executable guard.

---

## 2. RISK-2: `OpenSSL::SSL::Socket::Client` hostname verification

**Description.** The shard's TLS implementation assumes stdlib's
`OpenSSL::SSL::Socket::Client.new(hostname:)` performs hostname
verification by default (`docs/11-tls.md` §3.2). If a Crystal
version does NOT enable this by default, the shard's `verify=peer`
mode would be effectively reduced to "verify the cert chain but not
the name."

**Mitigation.** The current tree covers TLS scheme inference,
`tls_context` misuse on non-TLS URLs, and an opt-in live TLS success
path through `AMQP_TLS_URL` / `AMQP_TLS_CA_CERT`. A wrong-SAN
negative fixture is still required before `T-TLS-HOSTNAME-001` can be
treated as implemented.

**Severity.** High (silent security regression).

**Likelihood (post-mitigation).** Medium until the wrong-SAN
falsifier is checked in and run across supported Crystal versions.

**Status.** Open release-hardening item; live success coverage exists,
but hostname-mismatch coverage is not yet executable.

---

## 3. RISK-3: Decimal field type in field tables

**Description.** AMQP 0-9-1 §4.2.5.5 defines a decimal field type
(`D`). RabbitMQ and LavinMQ do not produce or preserve it; but a
broker bug, a misconfigured peer, or a future protocol extension
COULD send one to the shard.

**Mitigation.** The codec MUST raise `Amqp::ProtocolError` on
decoding `D` (`docs/02-public-api.md` §6.4). The shard never encodes
`D`.

**Severity.** Low (caller sees a clear error; no silent corruption).

**Likelihood.** Very low (target brokers don't produce it).

**Status.** Documented limitation. If a real-world need appears,
add encode + decode + a `Decimal` Crystal type to `FieldValue` and
bump the public API doc.

---

## 4. RISK-4: Connection write-mutex contention at high channel count

**Description.** Local benchmarks show one connection has a real
write-serialization ceiling. Beyond a small number of publishing
fibers/channels, the connection-level write mutex and socket path
become dominant; performance plateaus or regresses.

**Mitigation.** `docs/14-performance-contract.md` §5 is a roadmap
target, not a v0 release guarantee. `tools/perf_publish.cr` compares
single-channel, multi-channel, and multi-connection modes; callers who
need higher aggregate throughput should shard across connections.

**Severity.** Medium (high-throughput callers may need multiple
connections, which is normal practice).

**Likelihood.** N/A — design choice.

**Status.** Accepted by design. If a caller demonstrates a workload
that needs higher throughput AND cannot split across connections, a
future v0.x can explore a writer-fiber design (with `Channel(Frame)`
fanning into the socket).

---

## 5. RISK-5: Slow subscriber backpressure affecting other channels

**Description.** A slow `Subscription` whose buffer fills first blocks
its per-channel handler on the subscription inbox. If the channel frame
inbox also fills, pressure can propagate to the connection reader and
then affect other channels on the same connection.

**Mitigation.** Documented in `docs/09-consumer.md` §3.4. The default
`buffer: 16` is small enough that slow consumers surface quickly
(caller notices when their queue backs up). High-throughput or
potentially slow consumers can use a dedicated channel or connection.

**Severity.** Medium (operationally annoying, not a correctness
issue).

**Likelihood.** Medium (worker code with intermittent slow paths).

**Status.** Partially verified. The live harness covers the
per-channel-first claim; full channel-inbox saturation remains a
future process-isolated measurement.

---

## 6. RISK-6: `basic.return` correlation ambiguity

**Description.** AMQP 0-9-1 does NOT carry a delivery-tag in
`basic.return`. The shard correlates by send order
(`docs/07-channel-lifecycle.md` §10.1). If the broker's behavior
deviates from RabbitMQ's documented convention (return then ack
for the same publish), correlation breaks silently.

**Mitigation.** Falsifiers `T-PUB-RETURN-001..004` exercise the
documented order against both target brokers. Any divergence is a
release-blocking bug.

**Severity.** Medium (caller sees wrong `ConfirmOutcome::Returned`
attached to wrong publish; with `mandatory: true` only).

**Likelihood.** Low against target brokers.

**Status.** Mitigated by falsifiers. The risk exists for out-of-
matrix brokers (`docs/13-broker-compat-matrix.md` §6); those callers
are on their own.

---

## 7. RISK-7: Recovery double-publish in at-least-once mode

**Description.** Per `docs/12-recovery.md` §6 and REL-7, a publish
that was received by the broker but whose ack was lost during a
disconnect is re-published after recovery. Consumers see the
message twice.

**Mitigation.** This is the AMQP at-least-once contract, not a bug.
Documented in REL-7 and `docs/15-reliability-contract.md` §14.
Consumers MUST be idempotent.

**Severity.** High (callers misreading the contract will lose money
on duplicate orders, send duplicate emails, etc.).

**Likelihood.** Low per event, but non-zero across enough
disconnects.

**Status.** Documented contract. The shard cannot eliminate it
without exactly-once semantics, which AMQP 0-9-1 does not provide.

---

## 8. RISK-8: Memory growth from unread async-confirm channels

**Description.** `Channel#publish_async` returns a one-shot
`::Channel(ConfirmOutcome)` with `capacity: 1`. If the caller drops
the receiver without reading, the channel holds one
`ConfirmOutcome` indefinitely until GC. Bodies and properties are
NOT retained (only the `ConfirmOutcome` struct), but per
`publish_async`, the published `Message`'s body is retained in the
confirm tracker until ack/nack arrives.

**Mitigation.** Documented in `docs/08-publisher-confirms.md` §6.3.
Falsifier `T-PUB-ASYNC-004` verifies no fiber leak when receiver is
dropped, but memory is the user's responsibility.

**Severity.** Medium (operational).

**Likelihood.** Medium (callers misusing async API).

**Status.** Documented. The shard does NOT enforce a usage policy.

---

## 9. RISK-9: TLS context mutability after construction

**Description.** A caller-supplied `OpenSSL::SSL::Context::Client`
can be mutated after `Amqp.connect`. The shard uses the context at
handshake time; subsequent mutations don't affect the established
connection, BUT they DO affect future reconnects under
`Recovery::Full` (the recovery pipeline re-uses the same context
reference).

**Mitigation.** Documented in `docs/11-tls.md` §4.

**Severity.** Low.

**Likelihood.** Low (callers rarely mutate contexts they handed
off).

**Status.** Documented.

---

## 10. RISK-10: Heartbeat fiber and TLS write contention

**Description.** Heartbeats acquire the connection write mutex.
Under high-throughput publish, the mutex is held nearly continuously
by the publisher fiber. Heartbeats may be delayed beyond
`heartbeat / 2`, accumulating drift toward the broker's
`2 * heartbeat` receive-deadline.

**Mitigation.** The heartbeat fiber's coalescing logic
(`docs/10-heartbeats.md` §3) suppresses heartbeat sends when
publishes are already keeping the connection alive. The mechanism
is correct because the broker's receive-deadline restarts on ANY
frame, not specifically heartbeats.

**Severity.** Low (the design is sound).

**Likelihood.** N/A.

**Status.** Verified correct by design; T-HB-SEND-002 falsifies the
coalescing.

---

## 11. RISK-11: Channel-id reuse across recovery

**Description.** `Recovery::Full` re-opens channels on the fresh
socket. The shard tries to re-use the original channel-id, but the
broker may have a different `channel_max` post-restart, or may
refuse the id for other reasons. The shard reassigns and updates
`Channel#id`; callers caching the id see stale values.

**Mitigation.** Documented in `docs/12-recovery.md` §5.1. v0 recovery
does not expose channel-id remapping callbacks; callers SHOULD NOT
cache ids.

**Severity.** Low.

**Likelihood.** Low (uncommon for `channel_max` to change).

**Status.** Documented.

---

## 12. RISK-12: Unbounded confirm tracker if broker never acks

**Description.** A pathological broker that accepts publishes but
never sends `basic.ack`/`basic.nack` would cause the confirm tracker
to grow without bound (the shard would retain every published
body). `publish_confirm` with a `timeout` raises after the timeout,
but the entry is NOT removed from the tracker (so a later ack
correctly fires the outcome — which, however, no one is listening
for).

**Mitigation.** The shard MAY periodically GC tracker entries whose
outcome `::Channel` has been GC'd by the runtime. The implementation
is OPTIONAL for v0; if implemented, falsifier `T-PUB-TRACKER-GC-001`
verifies it.

**Severity.** Medium (memory growth on a malfunctioning broker).

**Likelihood.** Very low (target brokers don't malfunction this
way).

**Status.** Open. Decision deferred to v0.x based on operational
experience.

---

## 13. RISK-13: No protection against malicious peers

**Description.** The shard trusts the broker's wire stream. A
malicious or compromised broker could send a frame with a body-size
of `UInt32::MAX`, causing the shard to allocate ~4 GB. Or send
deeply-nested field-tables to OOM the decoder.

**Mitigation.** The frame reader rejects unknown frame types, bad
frame-end bytes, and payloads larger than `frame_max - 8`. The field
codec rejects unsupported decimal tags and unknown field-value tags.

The current implementation does not expose a maximum content body-size
cap and does not have an executable nested field-table depth cap. A
malicious broker remains able to advertise a large content body-size
and then stream enough frames to force memory growth.

**Severity.** High (DoS by malicious broker).

**Likelihood.** Very low against trusted brokers.

**Status.** Partially mitigated by per-frame and field-tag guards.
Message-size and nested-depth hard caps remain future work.

---

## 14. RISK-14: Crystal version split in downstream deployments

**Description.** Outside this shard, the author's primary project
uses a newer local Crystal than the older floor version used in some
CI/deployment contexts. The shard's `crystal: ">= 1.10.0"` floor is
intended to accommodate both. A future stdlib change between 1.10 and
current Crystal that breaks the shard would break one of those
environments.

**Mitigation.** Local verification currently exercises the Homebrew
release Crystal and the user's newer dev Crystal on focused gates.
The checked-in tree does not contain a multi-version CI workflow.
`T-API-DEPS-001` and `T-CODEC-PURE-001` remain the intended guards for
runtime dependency drift and stdlib ivar reach-in.

**Severity.** Medium (would block the author's own use of the
shard).

**Likelihood.** Medium-low. Local dual-compiler checks catch recent
drift, but the repository is not yet protected by checked-in
multi-version CI.

**Status.** Partially mitigated by local verification; CI-backed
coverage remains to be added.

---

## 15. RISK-15: Documentation drift from implementation

**Description.** The doc set is the source of truth for v0; the
implementation derives from it. If the implementation drifts (a PR
fixes a bug by changing behavior without updating docs), the docs
become aspirational rather than normative.

**Mitigation.** The acceptance criteria in `docs/17-mvp-cutline.md`
§3 require every `MUST` to have a passing falsifier; changes that
alter behavior should update tests and docs together. The local
`spec/docs_falsifier_link_spec.cr` guard resolves falsifier IDs
through the matrix and pins the current unlinked normative-section
debt so new unlinked sections are visible during local specs.

**Severity.** Medium (loss of contract integrity).

**Likelihood.** Medium across the lifetime of the project.

**Status.** Partially mitigated by local doc-link lint and
falsifier-first workflow. Checked-in CI enforcement is not present in
the current tree.

---

## 16. RISK-16: GPT-as-implementer fidelity

**Description.** The author's plan (`MEMORY.md`) is to hand this
doc set to GPT for v0 implementation. GPT may produce code that
passes some falsifiers but violates subtle invariants the docs
don't make sufficiently explicit (e.g., fiber-allocation rules,
exception-class details).

**Mitigation.** The doc set is intentionally exhaustive and
falsifier-driven. RFC 2119 keywords mark normative claims. The
falsifier matrix (`docs/16-falsifier-matrix.md`) is the test plan;
GPT's output is evaluated against it, not against the prose
ambiguity.

**Severity.** Medium.

**Likelihood.** Medium (subtle bugs in initial generation are
expected; iteration via the falsifier matrix should converge).

**Status.** Accepted; the falsifier-driven process is the
mitigation.

---

## 17. Risks deliberately NOT mitigated

The following risks are acknowledged and accepted in v0 as the cost
of scope discipline:

- **No connection pool.** Callers compose pools externally. Per
  `docs/17` §4.
- **No auto-reconnect on initial connect failure.** Per
  `docs/12-recovery.md` §2.
- **No exactly-once delivery.** Per REL-7 / RISK-7.
- **No backpressure isolation across channels.** Per RISK-5.
- **No multi-host failover URI.** Per `docs/17` §2.4.

These are not bugs; they are the v0 cutline. v0.x or v1 may
reconsider.

---

## 18. Risk-review process

This register is reviewed at every minor-version release. Entries
may be:

- **Promoted** to a falsifier-tracked status (mitigation in code).
- **Demoted** to "documented only" if operational experience shows
  the risk is theoretical.
- **Closed** if a v0.x change removes the risk.
- **Added** as new risks are identified.

Closed entries are NOT removed; they remain in the register with
status `Closed` for historical traceability.
