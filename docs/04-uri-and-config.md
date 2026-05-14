# amqp — URI and Configuration

> **Document status:** Draft v0.1, 2026-05-14
> **Audience:** Implementers of `Amqp.connect`; callers writing
> connection strings.
> **Companions:** `docs/01-design-principles.md` (P-5 URI is the
> connect entrypoint), `docs/02-public-api.md` (`Amqp.connect`
> signature), `docs/06-connection-lifecycle.md` (negotiation),
> `docs/11-tls.md` (TLS specifics).

This document is the **complete** specification of how a caller
configures a connection. The shard accepts configuration through
exactly two surfaces — the URI and the `Amqp.connect` keyword
arguments — and no other.

---

## 1. URI grammar

```
amqp-uri    = scheme "://" [userinfo "@"] host [":" port] ["/" vhost] ["?" query]
scheme      = "amqp" / "amqps"
userinfo    = user [":" password]
user        = *( unreserved / pct-encoded / sub-delims )
password    = *( unreserved / pct-encoded / sub-delims / ":" )
host        = IP-literal / IPv4address / reg-name
port        = 1*DIGIT
vhost       = *( unreserved / pct-encoded / sub-delims / ":" / "@" )
query       = query-pair *( "&" query-pair )
query-pair  = query-key "=" query-value
```

The shard MUST use stdlib `URI.parse` to parse the connection string;
it MUST NOT introduce a hand-written parser. Where stdlib's behavior
diverges from this grammar, the shard's behavior follows stdlib (P-1).

### 1.1 Defaults

| Element  | Default for `amqp://` | Default for `amqps://` |
|----------|-----------------------|------------------------|
| Port     | 5672                  | 5671                   |
| Vhost    | `/`                   | `/`                    |
| User     | `guest`               | `guest`                |
| Password | `guest`               | `guest`                |

The `guest`/`guest` defaults exist for parity with RabbitMQ's
out-of-the-box install. The shard MUST NOT special-case these
credentials (no warning, no rejection); whether the broker accepts
them is broker policy.

### 1.2 Vhost encoding

The vhost in the URI path is **URL-decoded exactly once** before being
sent on the wire. The empty path `amqp://host/` (a slash with nothing
after it) means vhost `/` (a single forward slash). To address a vhost
literally named empty string the URI form is `amqp://host/%20` — no,
that addresses a space; an empty-string vhost is not reachable via
URI and callers MUST use the explicit `vhost:` parameter (see §3) for
that pathological case.

Consequences:

- `amqp://h/foo` → vhost `foo`.
- `amqp://h/foo%2Fbar` → vhost `foo/bar` (slashes inside vhost names).
- `amqp://h` (no trailing slash) → vhost `/`.
- `amqp://h/` (trailing slash, empty path) → vhost `/`.

The implementation MUST encode the resulting vhost into a
`connection.open` short-string per the wire spec
(`docs/05-wire-0-9-1/01-types.md`).

**Falsifier:** T-URI-VHOST-001..006.

### 1.3 Userinfo encoding

Username and password are URL-decoded exactly once. Characters that
URI grammar reserves (`@`, `:`, `/`, `?`, `#`, `[`, `]`) MUST be
percent-encoded in the URI; the shard rejects unencoded reserved
characters by re-raising whatever `URI.parse` reports, wrapped in
`Amqp::UriError`.

After decoding, the credentials are passed to the SASL mechanism
selected at handshake (PLAIN by default, EXTERNAL if explicitly
requested; see §4).

**Falsifier:** T-URI-USERINFO-001..004.

### 1.4 Host and port

- IPv4 literal: `amqp://192.0.2.1:5672/`.
- IPv6 literal: `amqp://[2001:db8::1]:5672/` — brackets are mandatory
  per RFC 3986; stdlib URI handles this.
- DNS name: `amqp://rabbit.internal/` — resolved via stdlib's
  blocking resolver during `connect`, subject to `connect_timeout`.
- Multi-host URIs (`amqp://h1,h2,h3:5672/`) are NOT supported in v0.
  Callers compose round-robin externally. This is intentional: a
  single URI = a single peer keeps the failure modes clean.

**Falsifier:** T-URI-HOST-001..003.

---

## 2. Query parameter matrix

The following query parameters are recognised. Anything else MUST
cause `Amqp::UriError` (the implementation rejects unknown keys to
avoid silent typos like `?heartbeats=30`).

| Key            | Type / format              | Effect                                                              | Default          |
|----------------|----------------------------|---------------------------------------------------------------------|------------------|
| `heartbeat`    | Integer seconds            | Heartbeat interval requested at `connection.tune-ok`                | `60` seconds     |
| `channel_max`  | Integer 1..65535           | Maximum channel-id requested at `connection.tune-ok`                | `2047`           |
| `frame_max`    | Integer 4096..2147483647   | Maximum frame size requested at `connection.tune-ok`                | `131072` (128 KB)|
| `connect_timeout` | Integer seconds         | Wall-clock bound on full handshake                                  | `30` seconds     |
| `auth_mechanism`| One of `PLAIN`, `EXTERNAL`| SASL mechanism                                                      | `PLAIN`          |
| `recovery`     | `none` or `full`           | Recovery mode (see `docs/12-recovery.md`)                           | `none`           |
| `verify`       | `peer` or `none`           | TLS peer-cert verification toggle (TLS only)                        | `peer`           |
| `cacertfile`   | Filesystem path            | Trust anchor PEM (TLS only)                                         | system default   |
| `certfile`     | Filesystem path            | Client certificate PEM (TLS, mTLS only)                             | none             |
| `keyfile`      | Filesystem path            | Client private-key PEM (TLS, mTLS only)                             | none             |
| `server_name`  | DNS name                   | TLS SNI override (TLS only); defaults to URI host                   | URI host         |
| `product`      | String                     | `connection.start-ok` client-properties `product` field             | `"amqp.cr"`      |
| `information`  | String                     | `connection.start-ok` client-properties `information` field         | `""`             |

### 2.1 Numeric coercion

Numeric query values MUST be base-10 ASCII integers, no signs, no
underscores, no suffixes. The shard rejects `?heartbeat=30s`,
`?heartbeat=+30`, `?heartbeat=30_000` with `Amqp::UriError`. This is
strict on purpose: silent coercion is the most common source of
"why is my heartbeat 0?" bugs.

The shard MAY accept `?heartbeat=0` to mean "disabled" (broker MAY
agree to it in `connection.tune-ok`; if not, the negotiated value
wins).

### 2.2 Boolean coercion

The `verify` parameter uses the strings `peer` / `none` rather than
`true` / `false` / `1` / `0`. The shard MUST NOT accept boolean-style
inputs for this key; the strings are explicit about what is being
verified.

### 2.3 Conflict between query and keyword arguments

When both the URI query and the `Amqp.connect` keyword argument
supply the same value, the **keyword argument wins**. The
implementation MUST log at `Log::Severity::Debug` when the override
happens, so callers debugging "why did my heartbeat change?" can find
it.

The keyword argument names map to query keys as follows:

| Keyword arg              | Query key       |
|--------------------------|-----------------|
| `user:`                  | (in userinfo)   |
| `password:`              | (in userinfo)   |
| `heartbeat:`             | `heartbeat`     |
| `channel_max:`           | `channel_max`   |
| `frame_max:`             | `frame_max`     |
| `connect_timeout:`       | `connect_timeout` |
| `recovery:`              | `recovery`      |
| `product:`               | `product`       |
| `information:`           | `information`   |

Other keyword arguments (`tls:` notably) have no query-string
equivalent; TLS context construction is too rich for the URI surface.

**Falsifier:** T-URI-PRECEDENCE-001..N — each row in the table above
is a test case.

### 2.4 Unknown keys

The shard MUST raise `Amqp::UriError` on any query key not in §2.
Rationale: a typo like `?heartbets=30` silently leaving heartbeat at
the default is the worst kind of bug — the system runs, just with
wrong tuning. Strictness here is cheap.

The error message MUST include the offending key. Implementations
SHOULD list the recognised keys in the error to aid debugging.

**Falsifier:** T-URI-UNKNOWN-001.

---

## 3. Keyword-argument-only options

These options exist on `Amqp.connect` but NOT in the URI:

- **`tls: OpenSSL::SSL::Context::Client`** — caller-supplied TLS
  context. The URI's TLS-related query keys (`verify`, `cacertfile`,
  `certfile`, `keyfile`, `server_name`) are a convenience that
  constructs a context internally when `tls:` is `nil`; when `tls:`
  is non-nil the TLS query keys MUST be unused — supplying both
  raises `Amqp::TlsConfigError` synchronously. Rationale: a user
  who passes a full context has already specified the TLS policy; the
  URI's coarse flags would silently override their choices otherwise.
- **`vhost: String`** — explicit override of the URI path. Used for
  the pathological empty-string vhost (which URIs cannot express) and
  for callers who prefer to keep the URI host-only.

The shard MUST NOT add other keyword-argument-only options without a
revision of `docs/02-public-api.md`.

---

## 4. SASL mechanism

v0 supports two SASL mechanisms:

### 4.1 PLAIN (default)

`SASL response = NUL + username + NUL + password`, both fields raw
bytes (no UTF-8 validation beyond what AMQP requires). The shard
sends this in `connection.start-ok`'s `response` field.

Failure (broker sends `connection.close` with reply-code 403 during
`start-ok` / before `tune`) surfaces as `Amqp::AuthenticationError`.

### 4.2 EXTERNAL

Selected via `?auth_mechanism=EXTERNAL`. The response field is empty;
authentication is performed by the TLS layer (the broker reads the
client certificate's subject and authenticates against it).

Pre-conditions:
- The scheme MUST be `amqps://`.
- The TLS context MUST supply a client certificate (either via
  `certfile`/`keyfile` query keys or via the caller-supplied
  `OpenSSL::SSL::Context::Client`).

If pre-conditions fail, the shard MUST raise `Amqp::TlsConfigError`
synchronously, before any socket I/O.

The broker MAY still respond with reply-code 403 if it does not
recognise the certificate's identity; that surfaces as
`Amqp::AuthenticationError`.

### 4.3 Other mechanisms

AMQP defines AMQPLAIN, RABBIT-CR-DEMO, and others. v0 does NOT
implement them. Adding a mechanism in v0.x requires:

1. A new entry in this document with the response-byte layout.
2. A falsifier T-SASL-* exercising it against a configured broker.
3. A note in `docs/13-broker-compat-matrix.md` for any broker-specific
   variation.

**Falsifier:** T-SASL-PLAIN-001, T-SASL-EXTERNAL-001..003.

---

## 5. Negotiation outcome

The values the caller requests via URI/keywords are **proposals**; the
broker's response in `connection.tune` is authoritative. The shard
MUST honor the AMQP-mandated reconciliation:

- `channel_max`: `min(proposed, broker_max)`. If broker's max is 0
  (unlimited) the proposed value wins. If proposed is 0 the broker's
  max wins. If both are 0 the shard MUST default to 2047 to bound
  internal data structures.
- `frame_max`: `min(proposed, broker_max)`, with the same 0-means-
  unlimited rule. Floor is 4096 per AMQP; the shard MUST clamp up to
  that floor.
- `heartbeat`: per AMQP, `max(client_proposed, broker_proposed)` —
  the more conservative (longer interval) is NOT the choice; the
  spec says either party may propose, the value used is the one
  written in `connection.tune-ok`. The shard's `tune-ok` writes
  `client_proposed` if non-zero, else the broker's proposed value,
  per RabbitMQ convention.

The final negotiated values are exposed via `Connection#heartbeat`,
`#channel_max`, `#frame_max` (see `docs/02-public-api.md` §3).

**Falsifier:** T-CONN-TUNE-001..005.

---

## 6. Examples

```crystal
# Local dev, plain TCP, default broker, default credentials.
Amqp.connect("amqp://localhost/")

# Production: TLS, dedicated user, prod vhost, tighter heartbeat.
Amqp.connect("amqps://prod-app:#{secret}@rabbit.prod.example/prod?heartbeat=15")

# mTLS via EXTERNAL, with files referenced from the URI.
Amqp.connect(
  "amqps://rabbit.prod.example/prod" \
  "?auth_mechanism=EXTERNAL" \
  "&certfile=/etc/secrets/client.pem" \
  "&keyfile=/etc/secrets/client.key" \
  "&cacertfile=/etc/secrets/ca.pem"
)

# mTLS via caller-supplied context (no TLS query keys allowed).
ctx = OpenSSL::SSL::Context::Client.new
ctx.certificate_chain = "/etc/secrets/client.pem"
ctx.private_key       = "/etc/secrets/client.key"
ctx.ca_certificates   = "/etc/secrets/ca.pem"
Amqp.connect("amqps://rabbit.prod.example/prod?auth_mechanism=EXTERNAL",
             tls: ctx)

# Vhost with a slash in its name.
Amqp.connect("amqp://localhost/staging%2Fweb")

# Vhost `/` (the default; explicit form).
Amqp.connect("amqp://localhost/%2F")

# Block form: connection closed on both normal exit and exception.
Amqp.connect("amqp://localhost/") do |conn|
  conn.with_channel do |ch|
    ch.publish_confirm(Amqp::Message.new("hi"), "", "test")
  end
end
```

---

## 7. Anti-patterns

- **`amqp://user:pass@host/vhost?password=other`** — supplying
  password both in userinfo and as a query key is a smell. The shard
  MUST raise `Amqp::UriError` if `password` ever appears as a query
  key, because §2 doesn't list it.
- **Mixing `tls:` with TLS query keys** — see §3. Forbidden.
- **Concatenating un-escaped vhost names into URIs.** Always encode
  via `URI.encode_path_segment` (or equivalent stdlib) before
  building a URI string.
- **Reading the URI's password back from `Connection`.** The shard
  does NOT expose credentials post-connect. The password is held
  only long enough for `connection.start-ok` and then zeroed; it is
  not retained in `Connection` state.
