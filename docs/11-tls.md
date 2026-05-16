# amqp — TLS

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of the TLS wrap; callers configuring
> production-grade transport security.
> **Companions:** `docs/01-design-principles.md` (P-11 TLS is not
> optional), `docs/04-uri-and-config.md` §3, §4 (URI and SASL
> configuration), `docs/06-connection-lifecycle.md` §3.2 (where
> in the handshake TLS sits), `docs/13-broker-compat-matrix.md` (per-
> broker notes).

This document is the **complete** specification for how the shard
wraps an AMQP connection in TLS. Every requirement here is normative;
falsifiers are listed inline.

---

## 1. When TLS applies

TLS is engaged when and only when the URI scheme is `amqps`. The
shard MUST:

- Use `amqps` ⇒ TLS mandatory, default port 5671.
- Use `amqp` ⇒ TLS forbidden, default port 5672.
- Reject `amqp` + `tls:` keyword as `Amqp::ConfigurationError`
  synchronously, before opening any socket.
- Reject URI query TLS policy keys (`verify`, `cacertfile`, `certfile`,
  `keyfile`, `server_name`) as `Amqp::UriError`. v0 accepts either a
  caller-supplied `tls:` context or the default context for `amqps://`.

There is no "upgrade from plain to TLS" mode (STARTTLS-style) in AMQP
0-9-1; the shard MUST NOT implement one.

**Falsifier:** T-TLS-SCHEME-001..003.

---

## 2. Context construction

Two paths produce the `OpenSSL::SSL::Context::Client` used for the
handshake:

### 2.1 Caller-supplied context

```crystal
ctx = OpenSSL::SSL::Context::Client.new
# caller configures ctx
Amqp.connect("amqps://...", tls: ctx)
```

The shard MUST use `ctx` as-is. It MUST NOT mutate the context after
receiving it (no setting `verify_mode`, no setting cipher policy). The
caller is fully responsible for TLS policy.

### 2.2 Default context

When `tls:` is `nil` AND the scheme is `amqps`, the shard constructs
the default context:

```crystal
ctx = OpenSSL::SSL::Context::Client.new
ctx.add_options(OpenSSL::SSL::Options::NO_SSL_V2 |
                OpenSSL::SSL::Options::NO_SSL_V3 |
                OpenSSL::SSL::Options::NO_TLS_V1 |
                OpenSSL::SSL::Options::NO_TLS_V1_1)
# TLS 1.2+ only; stdlib default cipher suite.
```

The default cipher suite is whatever stdlib's
`OpenSSL::SSL::Context::Client.new` configures; the shard MUST NOT
weaken it.

### 2.3 `tls_context_default`

```crystal
ctx = Amqp.tls_context_default
```

Equivalent to §2.2. The caller may mutate the returned context. This
is provided so callers wanting the shard's default TLS baseline can
start from a known context without opening a connection.

**Falsifier:** T-TLS-CTX-001 (caller-supplied passthrough),
T-TLS-CTX-002 (default build), T-TLS-CTX-003 (default helper).

---

## 3. Handshake

After TCP connect succeeds, the shard:

```crystal
ssl_sock = OpenSSL::SSL::Socket::Client.new(
  tcp_sock,
  context: ctx,
  hostname: uri.host,        # SNI; also verified against cert SAN/CN
  sync_close: true
)
ssl_sock.read_buffering = true   # if stdlib supports; else N/A
```

Then writes the 8-byte AMQP protocol header (per
`docs/06-connection-lifecycle.md` §4) over the now-encrypted socket.

### 3.1 SNI

The `hostname:` argument is mandatory. The shard MUST always pass
`uri.host`. Some brokers behind
a TLS-terminating load balancer rely on SNI to select the right
backend or certificate; omitting SNI is a known production-incident
pattern.

**Falsifier:** T-TLS-SNI-001 (handshake against SNI-required broker
mock succeeds).

### 3.2 Hostname verification

When peer verification is enabled on the context, OpenSSL's hostname
verification (PHASE 5 of RFC 5280) MUST be enabled. Stdlib's
`OpenSSL::SSL::Socket::Client.new(..., hostname:)` implementation MUST
set the verify-hostname property; the shard does not need to set it
explicitly if the stdlib version already does so.

For the floor Crystal version, the shard MUST audit that stdlib's
`OpenSSL::SSL::Socket::Client` performs hostname verification by
default. If it does not, the shard explicitly sets it. This audit
result lives in `docs/20-risk-register.md`.

**Falsifier:** T-TLS-HOSTNAME-001 (handshake against cert with wrong
SAN raises `TlsHandshakeError`).

### 3.3 Handshake errors

| Stdlib error type                                                | Shard exception            |
|------------------------------------------------------------------|----------------------------|
| `OpenSSL::SSL::Error` with `unable to get local issuer certificate` | `TlsHandshakeError`     |
| `OpenSSL::SSL::Error` with `certificate verify failed`           | `TlsHandshakeError`        |
| `OpenSSL::SSL::Error` with `tlsv1 alert handshake failure`       | `TlsHandshakeError`        |
| `OpenSSL::SSL::Error` other                                      | `TlsHandshakeError`        |
| `IO::TimeoutError` during handshake                              | `ConnectTimeoutError`      |
| `IO::Error` / `EOFError` during handshake                        | `ConnectRefusedError`      |

All shard exceptions populate `cause` with the original stdlib
exception.

**Falsifier:** T-TLS-ERR-001..006 (synthetic failures per row).

---

## 4. Certificate rotation

A `Connection` retains the negotiated TLS session for its lifetime.
The shard does NOT implement session renegotiation; it does NOT
poll the certificate files on disk for changes. Certificate rotation
is achieved by:

1. Caller rotates the cert on disk.
2. Caller calls `Connection#close`.
3. Caller calls `Amqp.connect` again (or `Recovery::Full` reconnects,
   in which case the recovery pipeline opens a fresh socket with a
   freshly-constructed default context per §2.2, unless the caller
   supplied a context object).

This is a deliberate design choice. Re-reading cert files on a long-
lived connection has no defined moment; renegotiation is deprecated
in TLS 1.3 anyway. Callers that want zero-downtime rotation MUST
schedule reconnects.

**Falsifier:** T-TLS-ROTATE-001 — under `Recovery::Full`, a reconnect
MUST use a fresh default context when no caller context was supplied.

---

## 5. SASL interaction

v0 AMQP authentication uses PLAIN credentials even when transport is
TLS-wrapped. A caller-supplied TLS context may include a client
certificate for transport-level identity or broker-side policy, but
the shard does not implement SASL EXTERNAL in v0.

`auth_mechanism=EXTERNAL`, `certfile`, `keyfile`, and related URI keys
MUST raise `Amqp::UriError` because they are outside the v0 query
surface.

**Falsifier:** T-SASL-PLAIN-001, T-URI-UNKNOWN-001.

---

## 6. TLS-specific stats

The richer `ConnectionStats` model in `docs/19-observability.md` is
deferred. When it is implemented, it MUST include, when the connection
is TLS-wrapped:

- `tls_version` (e.g., `"TLSv1.3"`).
- `tls_cipher` (e.g., `"TLS_AES_256_GCM_SHA384"`).
- `peer_certificate_subject` (the broker's certificate subject DN).

These fields are static after handshake. They are `nil` for non-TLS
connections.

**Falsifier:** T-TLS-STATS-001.

---

## 7. Anti-patterns

- **Disabling peer verification in production.** If a caller mutates
  a TLS context to disable peer verification, that is entirely caller
  policy. The shard's URI surface does not provide a `verify=none`
  escape hatch in v0.
- **Reusing a TLS context across connect attempts when files have
  rotated.** A caller-supplied context is bound to the cert it held
  at construction. After cert rotation, build a fresh context.
- **Setting `connect_timeout` very low for `amqps`.** TLS handshake
  alone can take hundreds of milliseconds on slow networks or
  high-load brokers. The default 30 s is generous; values below
  5 s are risky for TLS connections.
- **Adding custom cipher policies inline.** If a cipher restriction
  is needed, build the context externally and pass via `tls:`.
  Pushing all that policy through URI query keys is not part of the
  v0 surface.
