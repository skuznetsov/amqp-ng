# amqp — Wire Codec: Content Header and Properties

> **Document status:** Draft v0.1, 2026-05-14 — **normative** for the
> content-header frame encoder/decoder.
> **Companions:** `docs/05-wire-0-9-1/00-frames.md` §3.3 (header frame
> envelope), `docs/05-wire-0-9-1/01-types.md` (scalar encodings),
> `docs/02-public-api.md` §6 (`Amqp::Properties` struct).
> **Recorded corpus:** `spec/fixtures/frames/publish_no_confirm/c2s.bin`,
> `spec/fixtures/frames/publish_confirm/c2s.bin`,
> `spec/fixtures/frames/consume_ack/s2c.bin`.

Content header frames carry message metadata for `basic.publish`,
`basic.deliver`, `basic.return`, and `basic.get-ok`. They sit between
the method frame and the body frame(s).

This document defines the header frame's payload layout, the
property-flags bitmap, and the per-property wire encoding.

---

## 1. Header frame payload

Per `00-frames.md` §3.3:

```
+----------+--------+-----------+----------------+----------------+
| class-id | weight | body-size | property-flags | property-list  |
| 2 bytes  | 2 bytes| 8 bytes   | 2 bytes (+ext) | variable       |
+----------+--------+-----------+----------------+----------------+
```

- `class-id`: `UInt16` big-endian. For all v0 content carriers
  (`basic.publish`, `basic.deliver`, `basic.return`, `basic.get-ok`),
  the class is `60` (`0x003C`).
- `weight`: `UInt16` big-endian, MUST be `0`. The codec MUST raise
  `Amqp::ProtocolError` if it is non-zero on decode.
- `body-size`: `UInt64` big-endian, total bytes across all
  following body frames; MAY be `0`.
- `property-flags`: `UInt16` big-endian bitmap (§2). MAY be followed
  by additional 16-bit extension words (`bit 0` continuation flag).
- `property-list`: variable-length sequence of property values,
  encoded in spec-defined order, present only if the corresponding
  flag bit is set.

---

## 2. Property-flags bitmap

The 16-bit flags word indicates which properties are present in the
following property-list. Bits are numbered from 15 (MSB) down to 0
(LSB) in the on-the-wire `UInt16`. The spec assigns each property to
a specific bit.

| Bit | Property            | v0 type in `Amqp::Properties`  |
|-----|---------------------|--------------------------------|
| 15  | `content-type`      | `String?`                      |
| 14  | `content-encoding`  | `String?`                      |
| 13  | `headers`           | `Amqp::Arguments?`             |
| 12  | `delivery-mode`     | `Amqp::Properties::Persistence?` (`Transient`/`Persistent`) |
| 11  | `priority`          | `UInt8?`                       |
| 10  | `correlation-id`    | `String?`                      |
|  9  | `reply-to`          | `String?`                      |
|  8  | `expiration`        | `String?` (ms as ASCII)        |
|  7  | `message-id`        | `String?`                      |
|  6  | `timestamp`         | `Time?`                        |
|  5  | `type`              | `String?`                      |
|  4  | `user-id`           | `String?`                      |
|  3  | `app-id`            | `String?`                      |
|  2  | `cluster-id`        | `String?` (deprecated; v0 SHOULD NOT set) |
|  1  | `reserved`          | always 0                       |
|  0  | continuation bit    | 1 if another 16-bit flags word follows |

In AMQP 0-9-1, the 14 documented properties all fit in one 16-bit
flags word (bits 15..2; bit 1 reserved; bit 0 continuation). The
continuation bit is therefore `0` in practice; the v0 codec MUST emit
`0` for it and MUST raise `Amqp::ProtocolError` on decode if it is
non-zero (indicates a forward-extended header the v0 codec doesn't
understand).

### 2.1 Encoding order

Properties are encoded in the SAME ORDER as the bits above (MSB
first). Only properties whose flag bit is set contribute bytes to
the property-list. A flag bit of `0` means the property is absent;
the codec MUST NOT emit any bytes for it.

---

## 3. Per-property encoding

Each property has a fixed type from §1 of `01-types.md` (the
method-argument scalar set; NOT the field-table tagged form).

| Bit | Property            | Type      | Wire encoding                          |
|-----|---------------------|-----------|----------------------------------------|
| 15  | `content-type`      | shortstr  | 1-byte length + UTF-8 bytes            |
| 14  | `content-encoding`  | shortstr  | 1-byte length + UTF-8 bytes            |
| 13  | `headers`           | field-table | 4-byte length + table body (§2 of `01-types.md`) |
| 12  | `delivery-mode`     | octet     | 1 byte: `1` = transient, `2` = persistent |
| 11  | `priority`          | octet     | 1 byte, 0..9 typical                   |
| 10  | `correlation-id`    | shortstr  |                                        |
|  9  | `reply-to`          | shortstr  |                                        |
|  8  | `expiration`        | shortstr  | ASCII decimal milliseconds             |
|  7  | `message-id`        | shortstr  |                                        |
|  6  | `timestamp`         | timestamp | 8 bytes signed Int64 seconds-since-epoch |
|  5  | `type`              | shortstr  |                                        |
|  4  | `user-id`           | shortstr  |                                        |
|  3  | `app-id`            | shortstr  |                                        |
|  2  | `cluster-id`        | shortstr  | deprecated                             |

### 3.1 `delivery-mode` ↔ `Persistence`

The wire value is a raw octet `1` or `2`. The v0 public API uses an
enum `Amqp::Properties::Persistence` with members `Transient` (1)
and `Persistent` (2) so that callers cannot mis-set it to other
values (e.g., `3`).

The codec maps:

- Encode: `Transient → 0x01`, `Persistent → 0x02`.
- Decode: `0x01 → Transient`, `0x02 → Persistent`, anything else
  raises `Amqp::ProtocolError`.

### 3.2 `expiration` as a string

AMQP 0-9-1 specifies `expiration` as a shortstr containing the
TTL in milliseconds as ASCII decimal digits (e.g., `"60000"` for
one minute). This is a spec quirk; the encoded form is human-readable
but type-fragile. The v0 API accepts a `Time::Span` and serialises it
as `(span.total_milliseconds.to_i64).to_s` shortstr.

The codec MUST NOT accept a non-ASCII-digit `expiration` from the
caller; an attempt raises `Amqp::ConfigurationError`.

### 3.3 `user-id` validation

RabbitMQ enforces that `user-id`, if set on a publish, MUST match
the connection's authenticated username. The broker rejects mismatches
with `connection.close` reply-code `406` (`PRECONDITION_FAILED`).

The v0 codec MAY pre-validate by comparing against the connection's
known username; this is a "best-effort" guard that catches the error
client-side before sending. Per `docs/02-public-api.md`, the API
exposes `Amqp::Properties#user_id=` without forcing the match — the
caller is responsible.

### 3.4 `headers` field-table

The `headers` property is a full AMQP field-table (`01-types.md`
§2-§4). It carries arbitrary `(String, FieldValue)` pairs that
broker-side header-exchanges and client-side application code can
read.

Header tables can be nested via the `F` tag; the codec recurses.

---

## 4. Implementation contract

### 4.1 Encoder

```crystal
def encode_content_header(io : IO, class_id : UInt16, body_size : UInt64, props : Amqp::Properties)
  io.write_bytes(class_id, IO::ByteFormat::NetworkEndian)
  io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)     # weight
  io.write_bytes(body_size, IO::ByteFormat::NetworkEndian)

  flags, body = build_property_section(props)
  io.write_bytes(flags, IO::ByteFormat::NetworkEndian)
  io.write(body)
end
```

`build_property_section` walks the 14 properties in MSB→LSB order,
sets the flag bit if the property is present, and appends the encoded
bytes to a memory buffer. The function returns `(flags_word, body_bytes)`.

### 4.2 Decoder

```crystal
def decode_content_header(io : IO) : {UInt16, UInt64, Amqp::Properties}
  class_id  = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
  weight    = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
  raise Amqp::ProtocolError.new("weight!=0") if weight != 0
  body_size = io.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
  flags     = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
  raise Amqp::ProtocolError.new("continuation bit set") if (flags & 0x01) != 0

  props = Amqp::Properties.new
  props.content_type     = read_shortstr(io)    if (flags & 0x8000) != 0
  props.content_encoding = read_shortstr(io)    if (flags & 0x4000) != 0
  props.headers          = read_field_table(io) if (flags & 0x2000) != 0
  props.delivery_mode    = read_persistence(io) if (flags & 0x1000) != 0
  # ... continue per §3 table
  {class_id, body_size, props}
end
```

### 4.3 Round-trip invariant

A `Properties` with no fields set encodes to a flags word of `0` and
zero property bytes; total header-payload size for an empty
`Properties` is `2 + 2 + 8 + 2 = 14` bytes.

---

## 5. Properties carried by `basic.deliver`/`get-ok`/`return`

The codec attaches the decoded `Amqp::Properties` to the resulting
delivery message:

- `Amqp::DeliverMessage` (from `basic.consume` path) — `properties:
  Amqp::Properties` field.
- `Amqp::GetMessage` (from `basic.get` path) — same.
- `Amqp::ReturnedMessage` (carried in `Amqp::PublishReturnedError`) —
  same.

The codec MUST preserve every received property, including ones the
v0 API doesn't surface beyond display (e.g., `cluster-id` is
deprecated but still decoded).

---

## 6. Body-size and body-frame reassembly

The codec reads `body-size` from the header, then reads body frames
until cumulative payload bytes equal `body-size`. If a body frame
would push the cumulative size past `body-size`, the codec MUST
raise `Amqp::ProtocolError` ("body fragment overflows declared
size").

A `body-size == 0` is legal — no body frames follow. The codec
MUST deliver the message immediately on receipt of the header in
this case.

---

## 7. Worked example: a publish with content-type and persistent

Caller code:

```crystal
ch.publish(
  "",                                     # exchange
  "amqp-ng.demo",                         # routing-key
  "hello".to_slice,                       # body
  Amqp::Properties.new(
    content_type: "text/plain",
    delivery_mode: Amqp::Properties::Persistence::Persistent,
  ),
)
```

Wire bytes (header frame payload, excluding the 8-byte envelope):

```
class-id    : 0x00 0x3C                             (60)
weight      : 0x00 0x00                             (0)
body-size   : 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x05  (5 bytes)
flags       : 0x90 0x00                             (bits 15 and 12 set)
content-type: 0x0A 't' 'e' 'x' 't' '/' 'p' 'l' 'a' 'i' 'n'   (10-byte shortstr)
delivery-mode: 0x02                                 (persistent)
```

Total header-payload size: `2 + 2 + 8 + 2 + 11 + 1 = 26 bytes`.

`0x90 0x00` in binary is `1001 0000 0000 0000`. Bit 15 (`0x8000`)
is set → content-type present. Bit 12 (`0x1000`) is set →
delivery-mode present. Bit 0 is clear → no continuation.

---

## 8. Edge cases and protocol corner cases

- **Empty body with present properties.** Legal; `body-size = 0`,
  flags word non-zero, property bytes follow.
- **Body-size in the petabyte range.** The 8-byte `UInt64` field
  permits this but `frame_max` caps the per-frame size; for very
  large bodies the codec emits many body frames. The v0 codec MAY
  enforce a per-publish body cap (configurable; defaults to
  `Int64::MAX`) to detect bugs that try to publish unbounded data.
- **Multiple property words.** The continuation bit (bit 0) is set
  when a future spec revision adds more properties. v0 MUST NOT set
  it and MUST raise on decode if set.
- **`headers` containing nested tables.** Legal and supported;
  decoder recurses via `01-types.md` §3.

---

## 9. Negative cases the codec must catch

- Header frame with `class-id != 60` on a basic-class operation —
  `Amqp::ProtocolError`.
- Header frame with `weight != 0` — `Amqp::ProtocolError`.
- Header frame with continuation bit `1` — `Amqp::ProtocolError`.
- Property byte stream shorter than the flags word indicates —
  `Amqp::ProtocolError` ("truncated property list").
- Body bytes received without a preceding header — `Amqp::ProtocolError`.
- Body bytes exceeding `body-size` — `Amqp::ProtocolError`.

Each maps to a falsifier in §10.

---

## 10. Falsifier index

| Test ID                    | Claim                                                  |
|----------------------------|--------------------------------------------------------|
| T-CODEC-CONTENT-001        | Empty Properties round-trips to 14-byte header payload |
| T-CODEC-CONTENT-002        | content-type+delivery-mode encodes to `0x90 0x00` flags |
| T-CODEC-CONTENT-003        | weight != 0 on decode raises ProtocolError             |
| T-CODEC-CONTENT-004        | continuation bit on decode raises ProtocolError        |
| T-CODEC-CONTENT-005        | body-size=0 delivers immediately, no body frames       |
| T-CODEC-CONTENT-006        | Body overflow raises ProtocolError                     |
| T-CODEC-CONTENT-007        | `delivery-mode` byte outside {1,2} raises              |
| T-CODEC-CONTENT-008        | `expiration` non-ASCII-digit raises at encode          |
| T-CODEC-CONTENT-009        | `timestamp` round-trips as signed Int64 seconds        |
| T-CODEC-CONTENT-010        | `headers` field-table nested decode                    |
| T-CODEC-CONTENT-011        | Property encoding order matches bit position           |
| T-CODEC-CONTENT-012        | Truncated property list raises ProtocolError           |
| T-CODEC-CONTENT-013        | All 14 properties round-trip lossless                  |
