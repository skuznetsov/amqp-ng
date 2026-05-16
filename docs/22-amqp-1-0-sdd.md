# amqp - AMQP 1.0 SDD

> **Document status:** Draft v0.1, 2026-05-15 - informative for v0,
> planning contract for v1.
> **Audience:** Future AMQP 1.0 implementers and reviewers.
> **Companions:** `docs/18-amqp-1-0-forward-plan.md`,
> `docs/16-falsifier-matrix.md` §16, `docs/17-mvp-cutline.md` §2.1.

This document is the spec-driven development plan for AMQP 1.0. It is
not a v0 runtime requirement. Its purpose is to stop future work from
starting with a vague "add AMQP 1.0" branch that accidentally mutates
the stable AMQP 0-9-1 surface.

The first implementation milestone is a narrow proof-of-life:

1. codec primitives and performatives are byte-exact,
2. one broker accepts SASL PLAIN plus `open` / `begin` / `close`,
3. one sender link transfers a `data` body and receives an accepted
   disposition,
4. one receiver link consumes a `data` body and settles it accepted,
5. the public API uses `Session` and `Link`, not 0-9-1 `Channel`
   topology names.

Everything else, including transactions, management operations,
advanced settlement states, and broker-specific extensions, is outside
the first AMQP 1.0 slice.

---

## 1. Scope Boundary

AMQP 1.0 is a different protocol family from AMQP 0-9-1. The shared
implementation should be limited to TCP/TLS transport setup, logging,
error root, and the fiber ownership pattern. Wire types, handshake,
link state, recovery, and message sections are version-specific.

The v1 code must be additive: implementing this document must not
change AMQP 0-9-1 behavior, public `Channel` semantics, existing URI
parsing defaults, or recovery replay semantics.

Falsifiers: T-AMQP10-CUTLINE-001, T-AMQP10-API-001.

---

## 2. v0 Cutline Guard

Until v1 work is explicitly started, v0 builds must not expose AMQP 1.0
runtime names or switches. In particular, there must be no
`Amqp::Session`, no `Amqp::Link`, no `protocol:` connect keyword, and
no hidden handshake branch that attempts AMQP 1.0 negotiation.

The only accepted v0 artifacts are planning docs and reserved falsifier
IDs. This keeps the v0.1.0 release focused on AMQP 0-9-1.

Falsifier: T-AMQP10-CUTLINE-001.

---

## 3. Module Layout

The first v1 implementation should add modules side-by-side instead of
renaming the 0-9-1 code:

```text
src/amqp/wire/amqp_one/
  types.cr
  frame.cr
  performatives.cr
  message.cr
  sasl.cr

src/amqp/amqp_one/
  connection.cr
  session.cr
  link.cr
  sender_link.cr
  receiver_link.cr
```

`Amqp::Connection` may remain the public entry point, but the internal
runtime should dispatch to a version-specific connection engine after
transport setup. `Amqp::Channel` remains 0-9-1-specific; AMQP 1.0 gets
`Amqp::Session` and `Amqp::Link`.

Falsifiers: T-AMQP10-CUTLINE-001, T-AMQP10-API-001..006.

---

## 4. Wire Codec SDD

AMQP 1.0 codec work should be implemented before any network runtime.
The codec must provide byte-exact encode/decode coverage for the core
primitive types needed by the proof-of-life slice:

- null,
- booleans,
- unsigned and signed integers needed by performatives,
- symbols,
- strings,
- binary values.

The first performative set must include:

- `open`,
- `begin`,
- `attach`,
- `flow`,
- `transfer`,
- `disposition`,
- `detach`,
- `end`,
- `close`,
- SASL `init`,
- SASL `outcome`.

The codec must be pure: no sockets, no fibers, no global mutable state
outside log constants. Network tests may use the codec; codec tests
must not require a broker.

Falsifiers: T-AMQP10-CODEC-001..006, T-AMQP10-PERFORMATIVE-001..011.

---

## 5. Message SDD

The first message implementation should support enough AMQP 1.0 message
sections for normal broker queues:

- header,
- delivery annotations,
- message annotations,
- properties,
- application properties,
- one or more `data` body sections.

The v1 public message type may reuse the name `Amqp::Message` only if
the 0-9-1 constructor behavior remains source-compatible. If that
constraint creates ambiguity, v1 should introduce an internal
version-specific message representation and a public wrapper later.

Falsifier: T-AMQP10-MSG-001..006.

---

## 6. Connection And SASL SDD

The proof-of-life connection path is:

1. open TCP or TLS transport,
2. optionally run AMQP 1.0 SASL header and SASL PLAIN exchange,
3. write AMQP 1.0 protocol header,
4. exchange `open`,
5. exchange `begin`,
6. close with `close` / socket close.

Idle-timeout should reuse the v0 heartbeat-fiber shape, but the wire
behavior is AMQP 1.0 `idle-time-out`, not 0-9-1 heartbeat frames.

SASL EXTERNAL, ANONYMOUS, OAUTH2, and custom mechanisms are outside the
first slice unless a chosen broker requires one for basic local smoke
tests.

Falsifier: T-AMQP10-HANDSHAKE-001..005.

---

## 7. Link SDD

The first public v1 messaging surface should expose two explicit roles:

- `SenderLink` sends `transfer` frames and resolves settlement outcomes.
- `ReceiverLink` receives `transfer` frames and sends dispositions.

Credit is part of the core contract, not an optimization. A receiver
link must not deliver messages to user code without available credit,
and a sender link must surface link-flow exhaustion instead of silently
buffering unbounded messages.

The initial settlement states are:

- accepted,
- rejected,
- released.

Modified, transactional state, resumable links, and unsettled-map
durability are deferred until after the basic sender/receiver contract
is executable.

Falsifier: T-AMQP10-LINK-001..008.

---

## 8. Public API SDD

The public API should make the model difference visible:

```crystal
Amqp.connect(url, protocol: :amqp_1_0) do |conn|
  session = conn.session
  sender = session.sender("queue/orders")
  receiver = session.receiver("queue/results", credit: 32)
end
```

The `protocol:` keyword is v1-only. v0 must not accept it
speculatively. The API must not expose 0-9-1 topology names such as
`exchange_declare`, `queue_bind`, or `basic_qos` on AMQP 1.0 sessions.

If the same `amqp://` URI can identify either protocol for a broker,
the protocol selection rule must be explicit and falsified. Acceptable
options are:

- explicit `protocol: :amqp_1_0`,
- `amqp10://` / `amqp10s://` schemes,
- broker-specific probe only after a failed explicit 0-9-1 header is
  ruled out as safe.

Falsifiers: T-AMQP10-API-001..006, T-AMQP10-CUTLINE-001.

---

## 9. Recovery SDD

AMQP 1.0 recovery must not copy the 0-9-1 topology replay model. The
state to reason about is sessions, links, unsettled delivery state,
link credit, and settlement outcomes.

The first v1 recovery contract should be conservative:

- no automatic unsettled-map persistence,
- no claim of exactly-once delivery,
- fail closed if link reattach or settlement replay is ambiguous,
- document duplicate risk before enabling automatic replay.

Automatic v1 recovery should remain disabled until these states have
broker-backed falsifiers.

Falsifier: T-AMQP10-RECOV-001..004.

---

## 10. Broker Matrix SDD

No AMQP 1.0 compatibility claim should be made until at least one target
broker image and version is pinned. Candidate targets:

- RabbitMQ with the AMQP 1.0 plugin,
- Apache ActiveMQ Artemis,
- Apache Qpid Dispatch Router or broker tooling.

The first broker matrix is a smoke matrix, not a production claim:
SASL PLAIN, open/begin/close, one sender link, one receiver link, and
accepted settlement.

Falsifier: T-AMQP10-BROKER-001..003.

---

## 11. First Implementation Order

1. Add byte-exact primitive and performative codec specs.
2. Add a local fixture corpus for one SASL handshake and one transfer.
3. Extract reusable transport setup from the 0-9-1 `Connection` without
   changing public behavior.
4. Add a private AMQP 1.0 connection engine behind an explicit v1-only
   branch.
5. Add sender and receiver link proof-of-life specs against one broker.
6. Only then design richer public ergonomics.

This order is deliberately codec-first. If the codec cannot be proven
byte-exact independently, live broker success would create false
confidence.

Falsifiers: T-AMQP10-CODEC-001..006, T-AMQP10-HANDSHAKE-001..005,
T-AMQP10-LINK-001..008.
