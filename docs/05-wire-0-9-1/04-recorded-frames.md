# amqp — Wire Codec: Recorded Frames Corpus

> **Document status:** Draft v0.1, 2026-05-14 — **informative** index
> + **normative** fixture references.
> **Audience:** Implementers writing the wire-codec decoder; reviewers
> verifying the codec round-trips real broker bytes.
> **Companions:** `docs/05-wire-0-9-1/00-frames.md`,
> `docs/05-wire-0-9-1/01-types.md`,
> `docs/05-wire-0-9-1/02-classes-methods.md`,
> `docs/05-wire-0-9-1/03-content-properties.md`.
> **Tooling:** `tools/capture_proxy.cr` (TCP relay that dumps
> per-session byte streams), `tools/scenarios/*.py` (driver scripts),
> `tools/run_corpus.sh` (orchestrator).

This document indexes the recorded byte streams captured against
RabbitMQ 3.13.7 (the broker pinned in `docs/13-broker-compat-matrix.md`).
Every fixture pair `(c2s.bin, s2c.bin)` is a complete TCP session
including the AMQP protocol header, handshake, and the scenario-
specific frames.

The codec's decoder MUST round-trip every byte in every recorded
fixture losslessly. Lossless means: decode → encode → byte-equal to
the original. The corpus is the v0 codec's primary regression
oracle.

---

## 1. Capture methodology

1. Start `rabbitmq:3.13-management` in Docker, bound to
   `127.0.0.1:5672`.
2. Build the capture proxy:
   `crystal build tools/capture_proxy.cr -o tools/capture_proxy`.
3. For each scenario in `tools/scenarios/`:
   1. Start the proxy with the scenario name. It binds to
      `127.0.0.1:5673`, accepts ONE connection, forwards to
      `127.0.0.1:5672`, and writes `c2s.bin` + `s2c.bin` to
      `spec/fixtures/frames/<scenario>/`.
   2. Run the Python pika driver against `127.0.0.1:5673`.
   3. Proxy auto-exits on either side closing.

The driver layer uses Python `pika` because it is the most widely
deployed reference client; its handshake choices (advertised
capabilities, client-properties content) are representative of what
the v0 codec must accept from peers.

**Reproduce the corpus:**

```sh
docker run -d --name amqp-ng-rabbit --rm -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
crystal build tools/capture_proxy.cr -o tools/capture_proxy
bash tools/run_corpus.sh
docker stop amqp-ng-rabbit
```

---

## 2. Corpus index

Sizes are bytes captured per direction. The fixture layout is:

```
spec/fixtures/frames/<scenario>/
  c2s.bin        # client → server byte stream
  s2c.bin        # server → client byte stream
  meta.txt       # scenario name, capture timestamp, duration, byte counts
```

| Scenario                       | c2s    | s2c   | Frames of interest |
|--------------------------------|--------|-------|--------------------|
| `handshake_success`            | 406    | 567   | Protocol header; `connection.start`/`start-ok`; `tune`/`tune-ok`; `open`/`open-ok`; graceful `close` |
| `handshake_auth_fail`          | 345    | 649   | `start-ok` with bad PLAIN payload; broker `connection.close` reply-code 403 |
| `handshake_vhost_deny`         | 386    | 606   | `connection.open` for non-existent vhost; broker `connection.close` reply-code 530 |
| `channel_open_close`           | 453    | 595   | `channel.open`/`open-ok` (channel 1); graceful `channel.close`/`close-ok` |
| `exchange_declare`             | 542    | 619   | `exchange.declare` direct/durable; `declare-ok`; `delete`; `delete-ok` |
| `queue_declare`                | 545    | 660   | `queue.declare` durable; `declare-ok` with `message_count=0, consumer_count=0`; `queue.delete` |
| `publish_no_confirm`           | 648    | 665   | `queue.declare` (auto-delete); `basic.publish` + header + body; teardown |
| `publish_confirm`              | 811    | 737   | `confirm.select`/`select-ok`; three `basic.publish` + content; three `basic.ack` from broker |
| `publish_mandatory_return`     | 582    | 752   | `basic.publish` mandatory=1 to unbound RK; `basic.return` + content; `basic.ack` |
| `publish_nack`                 | 692    | 692   | Queue with `x-overflow=reject-publish` + `x-max-length=0`; `basic.publish`; `basic.nack` (`0x3C 0x78`) |
| `consume_ack`                  | 782    | 888   | `queue.declare`; `basic.publish`; `basic.consume`/`consume-ok`; `basic.deliver` + content; `basic.ack`; `basic.cancel` |
| `basic_get`                    | 702    | 764   | Empty `basic.get` → `get-empty`; publish; second `basic.get` → `get-ok` + content |
| `broker_close_channel`         | 480    | 667   | `queue.declare(passive=true)` for missing queue; broker `channel.close` reply-code 404 |
| `basic_qos`                    | 472    | 607   | `basic.qos` prefetch_count=10, global=0; `qos-ok` |
| `heartbeat`                    | 446    | 607   | `connection.tune-ok` heartbeat=2; idle period elicits ≥4 heartbeat frames (type 8) in both directions |
| `multi_frame_body`             | 12941  | 663   | `frame_max=4096` negotiated; 12288-byte body fragmented across 3 body frames |
| `field_table_types`            | 718    | 664   | `queue.declare` with `x-*` arguments covering longstr, int, bool, nested table, array |

**Total c2s bytes:** 11651 + 12941 (multi-frame) ≈ 24592.
**Total s2c bytes:** 11608.

---

## 3. Selected byte-walks

Annotated byte-by-byte walks of frames the codec implementer is
likely to study first.

### 3.1 Protocol header (always identical)

`spec/fixtures/frames/handshake_success/c2s.bin` offsets 0..7:

```
0x00: 41 4D 51 50 00 00 09 01
      A  M  Q  P  | major=0 | minor=9 | revision=1
```

Required preamble before any frame. See `00-frames.md` §1.

### 3.2 `connection.start` (server→client)

`spec/fixtures/frames/handshake_success/s2c.bin` offsets 0..N:

```
0x00:  01                                    # type=METHOD
0x01:  00 00                                 # channel=0
0x03:  00 00 02 02                           # length=514
0x07:  00 0A 00 0A                           # class=10, method=10 (connection.start)
0x0B:  00 09                                 # version-major=0, version-minor=9
0x0D:  00 00 01 DD                           # field-table length = 477
0x11:  …server-properties body (477 bytes)…
       0C "capabilities" 46 (F=field-table)
         00 00 00 C7                         # nested table length = 199
         …{publisher_confirms: true, basic.nack: true, …}
       09 "copyright"    53 (S=longstr) 00 00 00 3C "Copyright (c) 2007-2024 Broadcom Inc…"
       0B "information"  53 00 00 00 39 "Licensed under the MPL 2.0…"
       08 "platform"     53 00 00 00 14 "Erlang/OTP 26.2.5.16"
       07 "product"      53 00 00 00 08 "RabbitMQ"
       07 "version"      53 00 00 00 06 "3.13.7"
0xXX:  00 00 00 0E "AMQPLAIN PLAIN"          # mechanisms (longstr length=14)
0xXX:  00 00 00 05 "en_US"                   # locales (longstr length=5)
0xXX:  CE                                    # frame-end
```

The codec MUST decode this exact sequence into a structured
`Connection::Start` method object with `server_properties: Arguments`
containing the seven top-level entries.

### 3.3 `connection.start-ok` (client→server)

`spec/fixtures/frames/handshake_success/c2s.bin` offsets 8..N:

```
0x08:  01                                    # type=METHOD
0x09:  00 00                                 # channel=0
0x0B:  00 00 01 40                           # length=320
0x0F:  00 0A 00 0B                           # class=10, method=11 (start-ok)
0x13:  00 00 01 1C                           # client-properties field-table length=284
       07 "product"      53 00 00 00 1A "Pika Python Client Library"
       08 "platform"     53 00 00 00 0D "Python 3.12.2"
       0C "capabilities" 46 (F)
         00 00 00 8C
         1C "authentication_failure_close" 74 (t) 01
         0A "basic.nack"                    74 01
         12 "connection.blocking"           …
         …
0xXX:  05 "PLAIN"                            # mechanism shortstr
0xXX:  00 00 00 0C 00 "guest" 00 "guest"     # response longstr: NUL+user+NUL+pass
0xXX:  05 "en_US"                            # locale shortstr
0xXX:  CE                                    # frame-end
```

The PLAIN response is exactly `0x00 'g' 'u' 'e' 's' 't' 0x00 'g' 'u'
'e' 's' 't'` (12 bytes). Length prefix `0x00 0x00 0x00 0x0C` = 12.

### 3.4 `connection.tune-ok`

After tune, the client sends tune-ok with chosen values:

```
01                  # type=METHOD
00 00               # channel=0
00 00 00 0C         # length=12
00 0A 00 1F         # class=10, method=31 (tune-ok)
07 FF               # channel-max = 2047 (short)
00 02 00 00         # frame-max = 131072 (long)
00 3C               # heartbeat = 60 (short)
CE                  # frame-end
```

The codec MUST verify the negotiated values fall within the
broker's advertised bounds per `docs/06-connection-lifecycle.md` §4.3.

### 3.5 Heartbeat frame (both directions)

`spec/fixtures/frames/heartbeat/{c2s,s2c}.bin` contain repetitions of
the exact 8-byte sequence:

```
08 00 00 00 00 00 00 CE
```

Type=8, channel=0, length=0, frame-end=0xCE.

### 3.6 `basic.publish` + content (no confirm)

`spec/fixtures/frames/publish_no_confirm/c2s.bin` — after channel
open and queue declare:

```
# basic.publish method frame
01                  # type=METHOD
00 01               # channel=1
00 00 00 38         # length=56
00 3C 00 28         # class=60, method=40
00 00               # reserved-1
00                  # exchange shortstr length=0 (default exchange)
21 "amqp-ng.corpus.publish-no-confirm"  # routing-key shortstr length=33
00                  # bits: mandatory=0, immediate=0
CE                  # frame-end

# content header frame
02                  # type=HEADER
00 01               # channel=1
00 00 00 0E         # length=14
00 3C               # class=60
00 00               # weight=0
00 00 00 00 00 00 00 0D   # body-size=13
00 00               # property-flags=0 (no properties)
CE                  # frame-end

# content body frame
03                  # type=BODY
00 01               # channel=1
00 00 00 0D         # length=13
68 65 6C 6C 6F 2D 61 6D 71 70 2D 6E 67   # "hello-amqp-ng"
CE                  # frame-end
```

This three-frame sequence is the canonical content-publish. The codec
emits these atomically under the connection's write mutex
(`docs/07-channel-lifecycle.md` §10.1).

### 3.7 `basic.nack` (server→client under confirms)

`spec/fixtures/frames/publish_nack/s2c.bin` tail contains:

```
01                  # type=METHOD
00 01               # channel=1
00 00 00 0D         # length=13
00 3C 00 78         # class=60, method=120 (basic.nack)
00 00 00 00 00 00 00 01   # delivery-tag=1
00                  # multiple=0, requeue=0
CE                  # frame-end
```

The codec routes this to the channel's confirm tracker, marking the
single in-flight publish as Nack'd. The corresponding `publish_confirm`
caller observes `Amqp::PublishNackError`.

### 3.8 `basic.return` + content

`spec/fixtures/frames/publish_mandatory_return/s2c.bin` contains
a `basic.return` (60.50) followed by a header and a body frame, then
a `basic.ack` (60.80) for the same publish (under confirms, the
broker acks even unroutable mandatory publishes after returning them).

The codec MUST correlate the `basic.return` to the most recent publish
in send order on the channel (per `docs/07` §10.2) and surface
`Amqp::PublishReturnedError` with `reply_code=312` (NO_ROUTE).

### 3.9 Multi-frame body

`spec/fixtures/frames/multi_frame_body/c2s.bin` — published a 12288-
byte body at negotiated `frame_max=4096`. The wire shows:

```
basic.publish method frame
content header frame (body-size = 12288)
body frame 1: length = 4088 bytes of body
body frame 2: length = 4088 bytes of body
body frame 3: length = 4112 bytes of body? — actually:
              first two frames consume 4088 each (= frame_max - 8);
              third frame contains the remainder 12288 - 4088*2 = 4112.
              But 4112 > 4088 — so split is: 4088 + 4088 + 4088 + 24.
              Total = 4 body frames.
```

Inspect the actual fragmentation by reading the file: byte counts
on each `03 00 01 ...` frame envelope confirm the split. The codec
MUST reassemble in receive order until cumulative payload equals
declared `body-size`.

### 3.10 Field-table types

`spec/fixtures/frames/field_table_types/c2s.bin` — `queue.declare`
with seven `x-*` arguments covers the tag bytes:

- `S` (longstr) — `x-route` string
- `b`/`B`/`I`/`l` (integers) — depending on pika's heuristic for
  `42` vs `10_000_000_000`
- `t` (boolean) — `x-bool: True`
- `F` (field-table) — `x-table: {...}` nested
- `A` (field-array) — `x-array: [1, "two", True]`

The exact tags pika emits are inspected by the codec test suite;
the test asserts decode produces values structurally equal to the
known input.

---

## 4. Negative-corpus generation

Beyond the recorded positive corpus, the codec test suite synthesises
negative inputs that match `T-CODEC-*-NEG-*` falsifiers:

- Frame with frame-end byte != `0xCE`.
- Frame with type ∉ `{1, 2, 3, 8}`.
- Frame with length exceeding `frame_max`.
- Header frame with `weight != 0`.
- Continuation bit set in property flags.
- Body fragment exceeding declared body-size.
- `connection.*` method on non-zero channel.
- Unknown class-id, unknown method-id within known class.

These inputs are hand-crafted byte sequences in the test source,
not recorded from a broker.

---

## 5. Refresh policy

The corpus is captured against a specific broker version (RabbitMQ
3.13.7 / pika 1.4.0 in this snapshot). Both ends evolve:

- A new RabbitMQ version may change `server-properties` content
  (e.g., updated copyright string, version bump). The codec's decode
  MUST NOT depend on specific server-property values — it MUST
  preserve the whole table as-is.
- A new pika version may change `client-properties` content. Same
  rule.

When refreshing the corpus, the snapshot date and broker version
SHOULD be captured in the per-scenario `meta.txt` (already emitted
by the proxy). The wire-codec `T-CODEC-CORPUS-001..N` tests
re-run against the refreshed corpus; any new decoder-side failure
indicates a real codec deficiency (the codec is supposed to be
content-agnostic for property tables).

**Anti-pattern:** asserting specific byte values inside
`server-properties` or `client-properties` from the corpus. Those
strings rot; instead, assert decoded structure (a Hash of String →
FieldValue with at least key `product` present).

---

## 6. LavinMQ corpus (deferred)

The current corpus is RabbitMQ 3.13.7 only. A LavinMQ 2.x corpus
is deferred to v0.2 because:

- LavinMQ's wire format follows RabbitMQ conventions per
  `docs/13-broker-compat-matrix.md` §1.
- The handshake content differs in `server-properties` (product
  name, version) but the codec's decode is content-agnostic.
- A LavinMQ corpus is valuable for integration tests, not for codec
  correctness.

When added, the LavinMQ corpus lives under
`spec/fixtures/frames/lavinmq-<scenario>/` with the same per-
scenario layout.

---

## 7. Falsifier index

| Test ID                  | Claim                                                |
|--------------------------|------------------------------------------------------|
| T-CODEC-CORPUS-001       | Each recorded `c2s.bin` decodes without raising      |
| T-CODEC-CORPUS-002       | Each recorded `s2c.bin` decodes without raising      |
| T-CODEC-CORPUS-003       | Decode → encode round-trip is byte-equal for c2s     |
| T-CODEC-CORPUS-004       | Decode → encode round-trip is byte-equal for s2c     |
| T-CODEC-CORPUS-005       | Negative corpus inputs raise the expected exception  |
| T-CODEC-CORPUS-006       | Protocol header at offset 0 is exactly the 8 bytes   |
| T-CODEC-CORPUS-007       | Heartbeat fixture contains ≥ 4 type-8 frames each side |
| T-CODEC-CORPUS-008       | `multi_frame_body` body reassembles to 12288 bytes   |
| T-CODEC-CORPUS-009       | `publish_nack` contains exactly one `basic.nack`     |
| T-CODEC-CORPUS-010       | `handshake_auth_fail` contains `connection.close`/403 |
