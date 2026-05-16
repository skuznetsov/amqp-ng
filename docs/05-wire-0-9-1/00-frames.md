# amqp — Wire Codec: Frames

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of the wire codec. This document is **normative**
> for the framing layer.
> **Companions:** `docs/01-design-principles.md` P-9 (codec is pure
> functions), `docs/05-wire-0-9-1/01-types.md` (type system used inside
> frame payloads), `docs/13-broker-compat-matrix.md` (broker-specific
> framing).
> **Recorded corpus:** `spec/fixtures/frames/*` — see
> `docs/05-wire-0-9-1/04-recorded-frames.md` for the index.

This document defines the on-the-wire framing for AMQP 0-9-1 as the v0
codec consumes and emits it. The framing is shared across every method,
heartbeat, and content payload; getting it right is a prerequisite for
everything else.

The reference for this layer is the AMQP 0-9-1 specification
(amqp0-9-1.pdf), §2.3 *Frames* and §4.2 *General frame format*. This
document does not restate the spec; it documents what the v0 codec
implements and which behaviors are observed in the recorded corpus.

---

## 1. Protocol header

Before any frames flow, the client MUST send the protocol header:

```
'A' 'M' 'Q' 'P'   0x00   0x00   0x09   0x01
```

That is, 8 bytes: the literal `AMQP`, a single null octet, then the
major.minor.revision triple `0.9.1` (so `0x00 0x09 0x01`). The major
byte is `0x00` — this is correct, the spec uses `0` for major in
0-9-1 to distinguish it from 0-8 (`0x00 0x08 0x00` or `0x01 0x01 0x09`
in some early variants).

The broker either:

- Responds with a `connection.start` method frame on channel 0
  (success path), OR
- Responds with its own protocol header advertising a version it speaks
  and closes the socket. The client MUST treat any header response as
  a protocol-mismatch and raise `Amqp::ProtocolNegotiationError`
  (`docs/03-error-model.md` §3).

The client MUST NOT retry with a different header; one connection
attempt = one protocol header.

**Recorded:** `spec/fixtures/frames/handshake_success/c2s.bin` bytes
`0x00..0x07` are exactly the header.

**Normative:**

- The codec module MUST expose `Amqp::Wire::ProtocolHeader` returning
  the byte sequence above as a constant.
- The codec MUST treat a server-side response that is NOT a `0x01`
  method-frame as a protocol-header-mismatch and raise.

---

## 2. General frame format

Every frame on the wire — method, content header, body, heartbeat — has
the same envelope:

```
+--------+---------+----------+------------------+------------+
| type   | channel | length   | payload          | frame-end  |
| 1 byte | 2 bytes | 4 bytes  | length bytes     | 1 byte     |
+--------+---------+----------+------------------+------------+
```

- **type**: `UInt8` — frame type discriminator, see §3.
- **channel**: `UInt16` big-endian — channel number; `0` is the
  connection-control channel (used for `connection.*` and heartbeats).
  Application channels start at 1.
- **length**: `UInt32` big-endian — number of bytes in `payload`,
  exclusive of the frame-end byte. The total wire size of a frame is
  `8 + length`.
- **payload**: opaque bytes; interpretation depends on `type`.
- **frame-end**: `UInt8` — MUST be `0xCE`. The codec MUST verify this
  byte and raise `Amqp::ProtocolError` if it is not `0xCE`.

The frame-end byte is a sanity check; it lets the codec detect a
framing desync (e.g., wrong length read) without parsing the payload.

**Normative:**

- Frame integers are **big-endian** on the wire. No exceptions.
- The codec MUST refuse a frame whose `length` field would extend the
  frame past `frame_max` (see §5) and raise `Amqp::FrameTooLargeError`.
- The codec MUST verify `frame-end == 0xCE`. On mismatch, raise
  `Amqp::ProtocolError` and tear down the connection.

---

## 3. Frame types

AMQP 0-9-1 defines four frame types relevant to v0:

| Code | Name             | Channel usage              | Payload shape                       |
|------|------------------|----------------------------|-------------------------------------|
| `1`  | `METHOD`         | 0 or application channel   | `class-id (UInt16) + method-id (UInt16) + arguments`  |
| `2`  | `HEADER`         | application channel        | `class-id (UInt16) + weight (UInt16) + body-size (UInt64) + property-flags (UInt16) + property-list` |
| `3`  | `BODY`           | application channel        | raw application bytes               |
| `8`  | `HEARTBEAT`      | channel `0` only           | empty (length == 0)                 |

Frame type `4` (`OOB-METHOD`), `5` (`OOB-HEADER`), `6` (`OOB-BODY`),
and `7` (`TRACE`) are defined in earlier 0-x drafts and were removed
from 0-9-1. The v0 codec MUST treat receipt of any frame with type
NOT IN `{1, 2, 3, 8}` as a protocol error.

### 3.1 Channel-zero invariants

The codec MUST enforce:

- Method frames on channel `0` MUST be in class `10` (`connection.*`).
- Heartbeat frames MUST be on channel `0` with `length == 0`.
- Header and body frames MUST NOT appear on channel `0`.

Violations raise `Amqp::ProtocolError`.

### 3.2 Method-frame payload layout

```
+----------+-----------+---------------+
| class-id | method-id | arguments     |
| 2 bytes  | 2 bytes   | variable      |
+----------+-----------+---------------+
```

`class-id` and `method-id` are `UInt16` big-endian. The `arguments`
section is encoded per the class+method's field list using the
AMQP type system (see `01-types.md`); the order and types are fixed
by the spec for each method. See `02-classes-methods.md` for the
complete table of `(class-id, method-id)` values used in v0.

### 3.3 Header-frame payload layout (content header)

Specified in `03-content-properties.md`. Briefly:

```
+----------+--------+-----------+----------------+----------------+
| class-id | weight | body-size | property-flags | property-list  |
| 2 bytes  | 2 bytes| 8 bytes   | 2 bytes        | variable       |
+----------+--------+-----------+----------------+----------------+
```

- `class-id` MUST equal the class of the method whose content this
  header describes; for `basic.publish`, `basic.deliver`,
  `basic.return`, and `basic.get-ok`, this is `60`.
- `weight` MUST be `0`. The codec MUST raise `Amqp::ProtocolError`
  on `weight != 0`.
- `body-size` is the total number of body bytes that follow across
  one or more body frames; it MAY be `0`.

### 3.4 Body-frame payload layout

`payload` is the raw application bytes for this body fragment. There
is no length-prefix inside the payload — the `length` field of the
outer frame is the body-fragment length. The codec MUST concatenate
fragments in receive order until the cumulative size equals the
`body-size` declared in the preceding header frame.

The codec MUST NOT deliver a `Message` to the caller until all
fragments are received and the cumulative size matches.

### 3.5 Heartbeat-frame payload layout

The payload is empty (length is `0`). The frame is exactly:

```
0x08  0x00 0x00  0x00 0x00 0x00 0x00  0xCE
```

That is, type=8, channel=0, length=0, frame-end=0xCE. 8 bytes total.

The codec MUST emit and accept exactly this byte sequence for
heartbeats; non-zero length is a protocol error.

**Recorded:** `spec/fixtures/frames/heartbeat/c2s.bin` and `s2c.bin`
contain multiple repetitions of this exact 8-byte sequence.

---

## 4. Frame sequencing rules

The codec MUST enforce these sequencing rules across the read path:

1. **Header follows publish/deliver/return/get-ok.** After a method
   frame with a content-bearing method (`basic.publish`,
   `basic.deliver`, `basic.return`, `basic.get-ok`), the NEXT frame
   on the SAME channel MUST be a header frame with matching class-id.
   Any other frame is a protocol violation.

2. **Body frames follow header.** After a header frame with non-zero
   `body-size`, one or more body frames on the SAME channel MUST
   follow until the cumulative size equals `body-size`. No method
   frame on the same channel may appear between header and complete
   body delivery.

3. **Cross-channel interleave is allowed.** Body frames on channel
   `A` MAY be interleaved with method/header/body frames on channel
   `B`. The codec MUST track per-channel content-assembly state.

4. **Heartbeats may interleave anywhere.** Channel-0 heartbeats MAY
   appear at any point in the wire stream, including in the middle
   of a multi-frame content sequence on another channel.

The codec maintains, per non-zero channel, a small content-assembly
state machine: `Idle → AwaitingHeader → AwaitingBody → Idle`. A new
method frame is only accepted in `Idle`; mid-content method frames
raise `Amqp::ProtocolError`.

---

## 5. `frame_max` and fragmentation

`frame_max` is negotiated in `connection.tune` / `tune-ok` (see
`docs/06-connection-lifecycle.md` §4.3). It bounds the total wire
size of any frame INCLUDING the 8-byte envelope. Therefore:

- The maximum **body** bytes per body frame is `frame_max - 8`.
- For a body of `B` bytes, the codec emits `ceil(B / (frame_max - 8))`
  body frames.

**Special case `frame_max == 0`.** The spec defines `0` as "no limit."
The v0 codec MUST treat the negotiated value:

- If both client preference AND broker advertise are `0`, the codec
  uses `131_072` (128 KiB) as a sane operational ceiling.
- Otherwise the negotiated minimum applies, with a floor of `4_096`
  per `connection.tune` semantics.

Body fragments are not aligned in any particular way; the codec MAY
emit the last fragment shorter than `frame_max - 8`.

**Recorded:** `spec/fixtures/frames/multi_frame_body/c2s.bin` —
publish of a 12288-byte body at negotiated `frame_max = 4096` produces
3 body frames of 4088 bytes each plus framing overhead.

---

## 6. Frame writes are atomic per channel-locked publish

Per `docs/07-channel-lifecycle.md` §10, a `basic.publish` (or the
equivalent on `basic.deliver` from the broker side) is logically one
"content publish" but takes 1 + 1 + N frames (method + header + N
body). The codec MUST emit these frames consecutively on the wire,
without interleaving frames from other channels' publishes, OR the
broker rejects the channel with reply-code `505` (UNEXPECTED_FRAME).

The implementation contract:

- The connection holds a single write-side mutex.
- A publish acquires the mutex, writes method+header+body in one
  contiguous burst, releases the mutex.
- Heartbeat frames MAY be coalesced with the publish only by waiting
  for the mutex like any other writer.

This is the only intentional serialisation point in v0; see
`docs/14-performance-contract.md` §5 and `tools/perf_publish.cr` for the
current multi-channel throughput roadmap and local witness harness.

---

## 7. Reading: frame-by-frame decode loop

The frame reader fiber (`docs/06-connection-lifecycle.md` §6) runs
an infinite loop:

```
loop do
  type     = read_u8
  channel  = read_u16_be
  length   = read_u32_be
  reject_if length > effective_frame_max - 8
  payload  = read_exactly(length)
  end_byte = read_u8
  raise ProtocolError unless end_byte == FRAME_END  # 0xCE
  decode_and_dispatch(type, channel, payload)
end
```

`read_exactly` is the fiber-suspending socket read with a fixed buffer
size. Every short read SHOULD reuse the buffer; the codec MUST NOT
allocate a fresh buffer per byte.

`decode_and_dispatch` is dispatched to per-type decoders that walk
the payload using the type-system primitives in `01-types.md`.

---

## 8. Writing: pre-encoded payloads

Outbound frames are constructed by:

1. Build the payload bytes in a fixed-size or growable buffer.
2. Write the 7-byte envelope prefix (type, channel, length).
3. Write the payload.
4. Write the `0xCE` frame-end.

The codec MUST NOT compute the payload length by walking the buffer
after the fact; the encoder for each method writes into a temporary
buffer and emits its size. This keeps the wire-emit path
allocation-bounded.

For body frames, the source buffer is `Bytes` from the caller; the
codec MAY emit body fragments via `IO#write` slices without copying.

---

## 9. Maximum frame size

Per AMQP 0-9-1 §4.2.5 *Frame body*, the `length` field is `UInt32`
big-endian, so the absolute upper bound is `2³² - 1 ≈ 4 GiB`. In
practice:

- The default negotiation in v0 prefers `131_072` (128 KiB).
- The broker's advertised `frame_max` for RabbitMQ 3.13 defaults to
  `131_072` (128 KiB).
- LavinMQ 2.x advertises `131_072` as well.
- A `frame_max` larger than `1_048_576` (1 MiB) is rare in practice
  and v0 MAY warn but MUST accept it up to the broker's advertised
  ceiling.

**Recorded:** `spec/fixtures/frames/handshake_success/s2c.bin` —
the `connection.tune` frame from RabbitMQ 3.13.7 contains
`frame_max = 0x00020000 = 131072`.

---

## 10. Framing errors

Beyond `0xCE`-check and length-bound check, the codec MUST raise
`Amqp::ProtocolError` on:

- A frame with `type` outside `{1, 2, 3, 8}`.
- A method frame on channel 0 with `class-id != 10`.
- A header or body frame on channel 0.
- A heartbeat frame with `length != 0` or on a non-zero channel.
- A header frame with `weight != 0`.
- A body fragment that pushes the cumulative size above the declared
  `body-size`.
- A new method frame on a channel currently in `AwaitingHeader` or
  `AwaitingBody` state.

Each violation is paired with a falsifier (`T-CODEC-FRAME-*` in
`docs/16-falsifier-matrix.md`).

---

## 11. Endian and integer-width discipline

Cardinal rules:

- All integers in frame envelopes and method/header payloads are
  **big-endian**.
- The codec MUST NOT use Crystal's default `IO#read_bytes` without
  explicitly passing `IO::ByteFormat::NetworkEndian` (big-endian).
- The codec MUST NOT use `to_s.to_i` round-trips on the wire path;
  bytes go through `IO::ByteFormat` accessors.

Bit widths used:

- `UInt8`  — type, frame-end, octet args.
- `UInt16` — channel, short args, class/method ids.
- `UInt32` — length, long args.
- `UInt64` — body-size, longlong args, timestamps.

Signed variants exist for some method arguments (e.g., the AMQP spec
defines short-short-int = `Int8`). The codec MUST honour signedness
per the type system in `01-types.md`.

---

## 12. Examples (corpus byte-walk)

### 12.1 Heartbeat frame

`spec/fixtures/frames/heartbeat/c2s.bin` last bytes:

```
0x08  0x00 0x00  0x00 0x00 0x00 0x00  0xCE
```

- `0x08`: type = HEARTBEAT
- `0x00 0x00`: channel = 0
- `0x00 0x00 0x00 0x00`: length = 0
- `0xCE`: frame-end

### 12.2 Method frame (connection.start-ok)

`spec/fixtures/frames/handshake_success/c2s.bin` after the
protocol header (offset 8):

```
0x01  0x00 0x00  0x00 0x00 0x01 0x40  …payload (320 bytes)…  0xCE
```

- `0x01`: type = METHOD
- `0x00 0x00`: channel = 0
- `0x00 0x00 0x01 0x40`: length = 320
- payload begins with `0x00 0x0A 0x00 0x0B` = class 10, method 11 =
  `connection.start-ok`. Argument decoding follows `02-classes-methods.md`
  §3.2.

### 12.3 Header frame for basic.publish

`spec/fixtures/frames/publish_no_confirm/c2s.bin` (after the
`basic.publish` method frame):

```
0x02  0x00 0x01  …length…  0x00 0x3C  0x00 0x00  0x00 …body-size (8 bytes)…  0x00 0x00 (or property flags)  …property values…  0xCE
```

- `0x02`: type = HEADER
- `0x00 0x01`: channel = 1
- `0x00 0x3C`: class-id = 60 (basic)
- `0x00 0x00`: weight = 0
- next 8 bytes: body-size (e.g., `0x00 …00 0x0D` for body of 13 bytes)
- next 2 bytes: property flags (e.g., `0x00 0x00` if no properties set)

Property encoding is `03-content-properties.md`'s subject.

---

## 13. Falsifier index

| Test ID                  | Claim                                          |
|--------------------------|------------------------------------------------|
| T-CODEC-FRAME-001        | `0xCE` end-byte verified; non-`0xCE` raises    |
| T-CODEC-FRAME-002        | type ∉ {1,2,3,8} raises                        |
| T-CODEC-FRAME-003        | length > frame_max raises FrameTooLargeError  |
| T-CODEC-FRAME-004        | header `weight != 0` raises ProtocolError      |
| T-CODEC-FRAME-005        | method on channel 0 with class != 10 raises   |
| T-CODEC-FRAME-006        | heartbeat length != 0 raises                   |
| T-CODEC-FRAME-007        | body fragments concatenate to declared size    |
| T-CODEC-FRAME-008        | method frame mid-content raises ProtocolError  |
| T-CODEC-FRAME-009        | big-endian decode round-trips for all widths   |
| T-CODEC-FRAME-010        | protocol-header bytes match exactly            |
| T-CODEC-FRAME-011        | multi-frame body reassembles in order          |
| T-CODEC-FRAME-012        | publish frames emitted contiguously per chan  |

Each test feeds the codec a hand-crafted byte stream (positive or
negative) and asserts the parse outcome. The `T-CODEC-FRAME-*`
falsifiers are listed in `docs/16-falsifier-matrix.md`.
