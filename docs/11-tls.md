# amqp — TLS

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of the TLS wrap; callers configuring
> production-grade transport security.
> **Companions:** `docs/01-design-principles.md` (P-11 TLS is not
> optional optional), `docs/04-uri-and-config.md` §3, §4 (URI keys and
> EXTERNAL mechanism), `docs/06-connection-lifecycle.md` §3.2 (where
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
- Reject `amqps` + URI query TLS keys (`verify`, `cacertfile`, etc.)
  when `tls:` keyword is also non-nil — see `docs/04-uri-and-config.md`
  §3.

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

### 2.2 URI-driven context

When `tls:` is `nil` AND the scheme is `amqps`, the shard constructs
a context from URI query keys:

```
verify:      "peer" (default) | "none"
cacertfile:  path to PEM with trust anchors
certfile:    path to client certificate PEM (mTLS)
keyfile:     path to client private-key PEM (mTLS)
server_name: SNI override (default: URI host)
```

The construction (pseudocode):

```crystal
ctx = OpenSSL::SSL::Context::Client.new
ctx.verify_mode = case verify
                  when "peer" then OpenSSL::SSL::VerifyMode::PEER
                  when "none" then OpenSSL::SSL::VerifyMode::NONE
                  end
ctx.add_options(OpenSSL::SSL::Options::NO_SSL_V2 |
                OpenSSL::SSL::Options::NO_SSL_V3 |
                OpenSSL::SSL::Options::NO_TLS_V1 |
                OpenSSL::SSL::Options::NO_TLS_V1_1)
# TLS 1.2+ only; stdlib default cipher suite.
ctx.ca_certificates    = cacertfile if cacertfile
ctx.certificate_chain  = certfile   if certfile
ctx.private_key        = keyfile    if keyfile
```

The default cipher suite is whatever stdlib's
`OpenSSL::SSL::Context::Client.new` configures; the shard MUST NOT
weaken it. The shard MAY warn at `Log::Severity::Warn` if
`verify == "none"` is selected.

### 2.3 `tls_context_default`

```crystal
ctx = Amqp.tls_context_default
```

Equivalent to §2.2 with `verify == "peer"`, no client cert, system CA
store. The caller may mutate the returned context. This is provided so
that callers wanting the shard's "good default" without parsing URI
keys can start from a known-good context.

**Falsifier:** T-TLS-CTX-001 (caller-supplied passthrough),
T-TLS-CTX-002 (URI-driven build), T-TLS-CTX-003 (default helper).

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
`uri.host` (or the `server_name` query override). Some brokers behind
a TLS-terminating load balancer rely on SNI to select the right
backend or certificate; omitting SNI is a known production-incident
pattern.

**Falsifier:** T-TLS-SNI-001 (handshake against SNI-required broker
mock succeeds).

### 3.2 Hostname verification

When `verify_mode == PEER`, OpenSSL's hostname verification (PHASE 5
of RFC 5280) MUST be enabled — this is what makes `verify` meaningful.
Stdlib's `OpenSSL::SSL::Socket::Client.new(..., hostname:)`
implementation MUST set the verify-hostname property; the shard does
not need to set it explicitly if the stdlib version already does so.

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
   freshly-constructed TLS context per §2.2 — important: under
   recovery, the URI-driven path re-reads the cert files each
   reconnect; the caller-supplied-context path uses the same context
   object indefinitely).

This is a deliberate design choice. Re-reading cert files on a long-
lived connection has no defined moment; renegotiation is deprecated
in TLS 1.3 anyway. Callers that want zero-downtime rotation MUST
schedule reconnects.

**Falsifier:** T-TLS-ROTATE-001 — under `Recovery::Full` + URI-driven
TLS, replacing the cert on disk and forcing a reconnect MUST use the
new cert on the next handshake.

---

## 5. EXTERNAL mechanism interaction

When `?auth_mechanism=EXTERNAL` is selected, the broker authenticates
the client by its TLS certificate identity. Pre-conditions enforced
synchronously (before socket I/O):

1. Scheme MUST be `amqps`.
2. The TLS context MUST be configured with a client certificate:
   - URI-driven path: `certfile=` AND `keyfile=` MUST both be present.
   - Caller-supplied path: the context MUST have `certificate_chain`
     set. The shard inspects this; if absent, raises
     `Amqp::TlsConfigError`.

The `connection.start-ok` `response` field is empty (zero-length
long-string) for EXTERNAL. The broker reads the cert during the
TLS handshake and authorises the channel by the cert's identity.

If the broker rejects (reply-code 403), the shard raises
`Amqp::AuthenticationError`. The error message SHOULD include the
broker's `reply_text` (often something like `"PLAIN login refused:
user 'CN=foo,O=bar' - no permission to access '/'"`).

**Falsifier:** T-TLS-EXTERNAL-001..003 (preconditions enforced, happy
path, broker reject).

---

## 6. TLS-specific stats

`ConnectionStats` (`docs/19-observability.md`) MUST include, when
the connection is TLS-wrapped:

- `tls_version` (e.g., `"TLSv1.3"`).
- `tls_cipher` (e.g., `"TLS_AES_256_GCM_SHA384"`).
- `peer_certificate_subject` (the broker's certificate subject DN).

These fields are static after handshake. They are `nil` for non-TLS
connections.

**Falsifier:** T-TLS-STATS-001.

---

## 7. Anti-patterns

- **Disabling peer verification in production.** `?verify=none`
  exists for testing against self-signed brokers. Production
  deployments MUST set `verify=peer` (the default). The shard logs
  at `Log::Severity::Warn` when `verify=none` is selected, but does
  not refuse.
- **Reusing a TLS context across connect attempts when files have
  rotated.** A caller-supplied context is bound to the cert it held
  at construction. After cert rotation, build a fresh context.
- **Setting `connect_timeout` very low for `amqps`.** TLS handshake
  alone can take hundreds of milliseconds on slow networks or
  high-load brokers. The default 30 s is generous; values below
  5 s are risky for TLS connections.
- **Adding custom cipher policies inline.** If a cipher restriction
  is needed, build the context externally and pass via `tls:`.
  Pushing all that policy through URI query keys is not a
  manageable surface.
