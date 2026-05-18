# amqp — Wire Codec: AMQP Type System

> **Document status:** Draft v0.1, 2026-05-14 — **normative** for the
> wire type-system encoders and decoders.
> **Companions:** `docs/05-wire-0-9-1/00-frames.md` (where these
> types appear inside frame payloads), `docs/05-wire-0-9-1/02-classes-methods.md`
> (which methods use which types), `docs/13-broker-compat-matrix.md`
> (broker-specific deviations).
> **Recorded corpus:** `spec/fixtures/frames/field_table_types/` —
> queue.declare with mixed `x-*` argument types.

AMQP 0-9-1's type system is used for two purposes:

1. **Method-argument encoding** — each `connection.*`, `channel.*`,
   `basic.*`, etc. method's arguments are a flat sequence of typed
   values in a class-specific order.
2. **Field tables** — used inside method arguments wherever the spec
   says "table" (e.g., `start.server-properties`, `queue.declare.arguments`).

This document defines the wire encoding the v0 codec implements,
including the parts where RabbitMQ deviates from the published spec.

---

## 1. Type catalogue (method-argument scalars)

These types appear directly in method-argument lists. Each is a
fixed-bit-width or length-prefixed encoding, big-endian.

| Spec name        | v0 Crystal mapping      | Wire encoding                                |
|------------------|-------------------------|----------------------------------------------|
| `octet`          | `UInt8`                 | 1 byte, big-endian                           |
| `short`          | `UInt16`                | 2 bytes, big-endian                          |
| `long`           | `UInt32`                | 4 bytes, big-endian                          |
| `longlong`       | `UInt64`                | 8 bytes, big-endian                          |
| `bit`            | `Bool` (packed)         | 1 bit; packed into octets, see §1.1          |
| `shortstr`       | `String`                | 1-byte length prefix + UTF-8 bytes (≤ 255)  |
| `longstr`        | `Bytes` or `String`     | 4-byte length prefix + raw bytes             |
| `timestamp`      | `Time`                  | 8 bytes, big-endian, seconds since UNIX epoch |
| `field-table`    | `Amqp::Arguments`       | 4-byte length prefix + table body (§2)      |

The `bit` type is the only one with a non-byte-aligned encoding; the
others are simple length-prefix or fixed-width.

### 1.1 Bit packing

When a method has consecutive `bit` arguments, the codec MUST pack
them into octets in the order they appear in the spec, LSB-first
within each octet. After 8 bits, a new octet starts. A non-`bit`
argument breaks the packing — the next bit, if any, starts a new
octet.

Example: `basic.publish` has `mandatory, immediate` as a bit pair
following two `shortstr` args. The wire bytes for both bits clear:
`0x00`; mandatory only set: `0x01`; immediate only set: `0x02`; both:
`0x03`.

**Normative:** The codec MUST NOT emit a partial octet if the next
encoded arg is a `bit`; instead the partial octet stays open until
either 8 bits are accumulated OR a non-`bit` arg arrives.

### 1.2 Shortstr

- Wire form: `UInt8 length` followed by exactly `length` bytes.
- Maximum length: 255 bytes (the prefix is `UInt8`).
- Encoding: UTF-8 (per AMQP 0-9-1 §4.2.5.3).
- The codec MUST reject a `shortstr` argument whose byte length
  exceeds 255 at encode time with `Amqp::ConfigurationError` (caller's
  bug, not protocol error).

### 1.3 Longstr

- Wire form: `UInt32 length` (big-endian) followed by exactly
  `length` bytes.
- Maximum length: bounded only by `frame_max` (§5 of `00-frames.md`).
- AMQP 0-9-1 declares longstr as binary (NOT UTF-8); the codec
  uses `Bytes` at the codec level and exposes `String` at higher
  levels only where the field is documented as text.

### 1.4 Timestamp

- 8 bytes, big-endian, **signed Int64**, seconds since the UNIX
  epoch (1970-01-01T00:00:00Z).
- The codec maps to `Time.unix(value)` on decode.
- On encode from `Time`, the codec emits `time.to_unix.to_i64`.
- Sub-second precision is LOST on the wire; this is an AMQP 0-9-1
  limitation. Callers needing higher precision MUST encode via a
  field-table entry of `longstr` with their own format.

---

## 2. Field-table encoding

A field table is the AMQP analog of a dictionary: a sequence of
`(name, type-tag, value)` triples wrapped in a length prefix.

```
+--------+--------+--------+--------+
| length (4 bytes, big-endian)      |
+-----------------------------------+
| entry₁ | entry₂ | … | entryₙ      |
+-----------------------------------+
```

- `length` is the byte count of the entries section, NOT including
  the 4 length bytes themselves. A length of `0` denotes an empty
  table; the codec emits `0x00 0x00 0x00 0x00`.

Each `entryᵢ` is:

```
+-------------+--------+-------------+
| name        | tag    | value       |
| shortstr    | 1 byte | type-specific |
+-------------+--------+-------------+
```

- `name` is a `shortstr` (1-byte length + bytes); the spec restricts
  names to ASCII alphanumerics and `$#_-` but RabbitMQ tolerates
  longer ones. The codec accepts any `shortstr`-encodable string.
- `tag` is a single ASCII byte identifying the value type — see §3.
- `value` is encoded per the tag.

The codec MUST decode entries until the cumulative byte count equals
the declared `length`; extra bytes or premature exhaustion raises
`Amqp::ProtocolError`.

---

## 3. Field-table value tags

This is the source of broker incompatibility. The published AMQP
0-9-1 spec defines a smaller tag set than the brokers actually
implement. The v0 codec implements the union of:

- The AMQP 0-9-1 spec set (limited).
- The RabbitMQ extension set (the practical superset; documented in
  the RabbitMQ source under `deps/rabbit_common/src/rabbit_binary_generator.erl`).

The full v0 tag table:

| Tag (ASCII) | Spec name          | v0 Crystal type                    | Wire bytes after tag                                    |
|-------------|--------------------|------------------------------------|---------------------------------------------------------|
| `t` (0x74)  | boolean            | `Bool`                             | 1 byte, `0x00` = false, non-zero = true                 |
| `b` (0x62)  | short-short-int    | `Int8`                             | 1 byte, signed                                          |
| `B` (0x42)  | short-short-uint   | `UInt8`                            | 1 byte, unsigned                                        |
| `s` (0x73)  | short-int          | `Int16`                            | 2 bytes, signed, big-endian                             |
| `u` (0x75)  | short-uint         | `UInt16`                           | 2 bytes, unsigned, big-endian                           |
| `I` (0x49)  | long-int           | `Int32`                            | 4 bytes, signed, big-endian                             |
| `i` (0x69)  | long-uint          | `UInt32`                           | 4 bytes, unsigned, big-endian                           |
| `l` (0x6C)  | long-long-int      | `Int64`                            | 8 bytes, signed, big-endian                             |
| `f` (0x66)  | float              | `Float32`                          | 4 bytes, IEEE 754 single, big-endian                    |
| `d` (0x64)  | double             | `Float64`                          | 8 bytes, IEEE 754 double, big-endian                    |
| `D` (0x44)  | decimal-value      | **NOT SUPPORTED** in v0 (see §5)   | 1 byte scale + 4 bytes value (big-endian)               |
| `S` (0x53)  | longstr            | `Bytes` (or `String` at API level) | 4-byte length + bytes                                   |
| `A` (0x41)  | field-array        | `Array(FieldValue)`                | 4-byte length + sequence of tagged values, see §4       |
| `T` (0x54)  | timestamp          | `Time`                             | 8 bytes, big-endian, signed Int64 seconds-since-epoch   |
| `F` (0x46)  | field-table        | `Amqp::Arguments`                  | nested field-table (recurse on §2)                      |
| `V` (0x56)  | void (no-value)    | `Nil`                              | (no bytes follow)                                       |
| `x` (0x78)  | byte-array         | `Bytes`                            | 4-byte length + raw bytes (RabbitMQ extension)          |

Unknown tags MUST raise `Amqp::ProtocolError`.

Nested field-arrays and field-tables are bounded to 32 recursive
container levels. Decode overflow raises `Amqp::ProtocolError`; encode
overflow raises `Amqp::ConfigurationError`.

### 3.1 Spec vs RabbitMQ differences

The published AMQP 0-9-1 spec defines: `t, b, B, U, u, I, i, L, l, f,
d, D, s, S, A, T, F, V`. RabbitMQ uses an OVERLAPPING but NOT IDENTICAL
set, notably:

- The spec's `U` (`Int16`) and `u` (`UInt16`) are encoded as `s` and
  `u` by RabbitMQ; the codec MUST accept `s, u` from RabbitMQ and
  decode them as `Int16`/`UInt16`.
- The spec's `L` (`Int64`) is encoded as `l` by RabbitMQ; the codec
  MUST accept `l` and decode as `Int64`.
- The spec's `s` for `shortstr` inside a field-table is NOT used by
  RabbitMQ (strings inside tables use `S` longstr exclusively); the
  codec MUST NOT emit `shortstr` inside a field-table.
- RabbitMQ adds `x` (byte-array); the spec does not. The codec MUST
  accept on decode and MAY emit on encode (its use is rare).

In short: the v0 codec uses the **RabbitMQ tag set** above on encode,
and accepts both spec and RabbitMQ variants on decode.

### 3.2 LavinMQ alignment

LavinMQ 2.x follows RabbitMQ's tag conventions exactly (it explicitly
targets RabbitMQ wire compatibility). The above table applies.

---

## 4. Field-array encoding

A field-array is a heterogeneous list:

```
+-----------------------------------+
| length (4 bytes, big-endian)      |
+-----------------------------------+
| tag₁ value₁ | tag₂ value₂ | …     |
+-----------------------------------+
```

- `length` is the byte count of the elements, NOT including the
  length prefix.
- Each element is a `(tag, value)` pair using the §3 encoding.

Arrays have no `name` field for elements; entries are positional.

---

## 5. Decimal type (`D`)

The spec defines `D` as `Int8 scale + UInt32 value`, representing
the rational `value × 10^(-scale)`. v0 does NOT support `D` in either
encoding or surface API:

- The codec MUST raise `Amqp::ProtocolError` when it decodes a `D`
  field-value.
- The codec MUST NOT emit `D` on the encode path. A caller who tries
  to put a BigDecimal-shaped value into `Arguments` has no accepted
  `FieldValue` union member for it; unsupported values fail at compile
  time or at the explicit field-value encoder boundary.

This is a deliberate v0 limitation; see `docs/20-risk-register.md`
RISK-3. The vast majority of `Arguments` use cases (queue arguments,
exchange arguments, message headers) work with the supported types.

---

## 6. `FieldValue` type alias

The Crystal type for the value inside an `Amqp::Arguments` entry:

```crystal
alias Amqp::FieldValue =
  Nil |
  Bool |
  Int8 | UInt8 | Int16 | UInt16 | Int32 | UInt32 | Int64 |
  Float32 | Float64 |
  Bytes | String |
  Time |
  Array(Amqp::FieldValue) |
  Hash(String, Amqp::FieldValue)
```

The recursive case (`Array(FieldValue)`, `Hash(String, FieldValue)`)
covers nested field-arrays and field-tables.

A `Hash(String, FieldValue)` is the surface type of `Amqp::Arguments`;
declaring the alias keeps the recursive shape explicit.

The codec's encode path uses a `case` over the union to dispatch
each value to the right tag emitter. Per P-15 (`docs/01-design-principles.md`),
no `Reflection.cast`; the union is closed and exhaustive.

---

## 7. String encoding within tables

Strings inside field tables are tagged `S` (longstr) per §3. The codec
MUST NOT emit `s` (shortstr) for table strings — RabbitMQ accepts the
former and may reject the latter.

On decode, the codec accepts both `S` and `s` and exposes them as
`String` at the API level when the field semantics are documented as
text (e.g., user-id, app-id) or `Bytes` when documented as binary.

The codec MAY validate UTF-8 on decode for documented-text fields
and replace invalid sequences with U+FFFD; the policy is logged but
non-fatal.

---

## 8. Endianness, signedness, IEEE 754

- All multi-byte integers in this type system are **big-endian**.
- Signed integers use **two's complement** representation.
- `f` (float) is IEEE 754 single-precision binary32, big-endian.
- `d` (double) is IEEE 754 double-precision binary64, big-endian.

The codec MUST use Crystal's `IO::ByteFormat::NetworkEndian` for
all multi-byte writes and reads. Float emission uses
`f32.bytes(NetworkEndian)` / equivalent.

---

## 9. Empty/null semantics

- An empty `shortstr` is encoded as `0x00` (length=0, no bytes).
- An empty `longstr` is `0x00 0x00 0x00 0x00`.
- An empty `field-table` is `0x00 0x00 0x00 0x00`.
- A `nil` AMQP value uses tag `V` with no following bytes.

The codec MUST distinguish "field absent from table" (entry not
emitted at all) from "field present with nil value" (entry with
tag `V`).

---

## 10. Worked decode: a recorded field-table

`spec/fixtures/frames/handshake_success/s2c.bin` contains, inside
the `connection.start` method's `server-properties` argument, a
field-table that includes RabbitMQ's identity. Walking a few
entries (offsets relative to the table's body, NOT the file):

```
[name=capabilities] S=longstr → field-table body (nested F)
[name=cluster_name] S=longstr → "rabbit@<hostname>"
[name=copyright]    S=longstr → "Copyright (c) 2007-2024 Broadcom Inc..."
[name=information]  S=longstr → "Licensed under the MPL 2.0…"
[name=platform]     S=longstr → "Erlang/OTP 26.2.5.16"
[name=product]      S=longstr → "RabbitMQ"
[name=version]      S=longstr → "3.13.7"
```

Tags observed in the corpus: `S` (longstr), `F` (field-table), `t`
(boolean within capabilities), `I` (long-int in some entries). The
codec's decode round-trip for this exact byte sequence is part of
`T-CODEC-TYPES-009` (handshake-roundtrip).

---

## 11. Encoding ergonomics (API → wire)

The API layer (`docs/02-public-api.md`) constructs `Amqp::Arguments`
hashes; the codec walks them on encode:

```crystal
def encode_field_table(io : IO, table : Amqp::Arguments)
  body = IO::Memory.new
  table.each do |name, value|
    encode_shortstr(body, name)
    encode_field_value(body, value)
  end
  io.write_bytes(body.size.to_u32, IO::ByteFormat::NetworkEndian)
  io.write(body.to_slice)
end
```

The two-pass nature (encode body to memory, then write length+body)
is required because the length prefix must be known before the body.
The temporary `IO::Memory` is bounded by the table's size, which is
bounded by `frame_max`.

**Normative:** The codec MUST NOT walk the table twice to compute
length on the first pass and emit on the second — that risks
race-time mutation if the caller's hash is shared across fibers.
The "build to memory, write length" approach is mandatory.

---

## 12. Falsifier index

| Test ID                  | Claim                                                |
|--------------------------|------------------------------------------------------|
| T-CODEC-TYPES-001        | Big-endian round-trip for `octet/short/long/longlong` |
| T-CODEC-TYPES-002        | `shortstr` over 255 bytes raises at encode           |
| T-CODEC-TYPES-003        | `longstr` length prefix is `UInt32` big-endian        |
| T-CODEC-TYPES-004        | Bit packing LSB-first, 8 bits per octet              |
| T-CODEC-TYPES-005        | Empty table encodes as `0x00 0x00 0x00 0x00`         |
| T-CODEC-TYPES-006        | Field-table entry order is preserved                 |
| T-CODEC-TYPES-007        | Each tag round-trips for sample values               |
| T-CODEC-TYPES-008        | Unknown tag on decode raises ProtocolError           |
| T-CODEC-TYPES-009        | Handshake server-properties decodes losslessly       |
| T-CODEC-TYPES-010        | `D` (decimal) on decode is dropped or raises         |
| T-CODEC-TYPES-011        | Nested `F` table decodes recursively                 |
| T-CODEC-TYPES-012        | `V` void entry has no bytes after tag                |
| T-CODEC-TYPES-013        | `Float32`/`Float64` IEEE 754 big-endian round-trip   |
| T-CODEC-TYPES-014        | `Time` timestamp is signed Int64 seconds-since-epoch |
| T-CODEC-TYPES-015        | RabbitMQ `l` (Int64) decode accepted as `l`          |
| T-CODEC-TYPES-016        | Nested table/array depth overflow raises             |
