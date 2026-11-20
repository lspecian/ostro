# Secrets: templating, grants, scrubbing and providers

Guests never hold secret values. A guest writes a `{{secret:NAME}}` placeholder into an HTTP
header (or, when allowed, the body). The host resolves it at send time, only towards origins the
secret's grant lists, and scrubs the value from everything that comes back. This document covers
the template grammar, grants, scrubbing and its limits, the sealed store, the providers and the
admin surface (FX-06, FX-07, FX-10).

Implementation: `crates/ostro-bridge/src/secrets/`. End-to-end proof:
`crates/ostro-server/tests/fx_canary.rs` (SC3) and `fx_providers_e2e.rs` (SC5).

> Examples in this document use obvious placeholders (`sk_test_PLACEHOLDER`). Never paste a real
> secret into a config file, a command line or a ticket.

---

## 1. Template grammar and placement (W1, D-FX4)

```
{{secret:NAME}}        NAME = [a-z0-9][a-z0-9_.\-/]{0,127}
```

- Case-sensitive, lower case only. No `..`, no empty segment, no `.` segment.
- **`__ostro/` is reserved.** Guests and the admin API can never reference or set a name under it.
- No escaping: a literal `{{secret:` in a header value is always a placeholder, and an unknown or
  malformed name fails the effect.
- A resolved value must be at least **8 bytes**. Shorter values are refused when they are set and
  when a template is resolved (too many false positives for the scrubber, and too guessable).

Where placeholders are resolved:

| Location | Resolved? |
|----------|-----------|
| header **values** | always |
| request body | only if **every** secret referenced in the body has `body = true` in its grant; the body must then be valid UTF-8 |
| URL, method, header **names** | never: the request fails `egress_denied` (reason `placeholder`) before anything is sent |

Secret names resolve inside the run's tenant: the sealed provider reads `<tenant>/<NAME>`
(`_/<NAME>` for the default tenant).

The engine journals the guest's request with the templates verbatim. Headers named in the
request's `redact` list are journaled as `[redacted]` instead. The resolved value exists only in
the bridge task, in Ostro-owned buffers that are zeroized after use, and in a `HeaderValue` marked
sensitive (hyper skips HPACK indexing and redacts it in `Debug`).

Templated requests never follow redirects and force `Accept-Encoding: identity`
([connectors.md](connectors.md) §2.5).

---

## 2. Grants and origins

```
Grant{ id:      "secret:<module>:<secret>",
       subject: "module:<module>",
       scopes:  ["origin:https://api.stripe.com", "origin:https://*.example.com:8443", "body"?] }
```

- `<module>` is the tenant-qualified deployed module name (`<tenant>/<name>`), never the module
  sha, so redeploys keep their grants.
- An origin is the RFC 6454 triple (scheme, host, port), compared with the **normalised** request
  URL. `https://*.example.com` matches exactly one extra label. A bare `*`, paths, queries and
  userinfo are refused.
- `body` allows the secret in the request body.
- Every referenced secret's grant is checked **before** any provider is asked and before anything
  is sent. No grant, an origin mismatch, or a body use without `body` → `non-retryable
  {secret_not_granted}`. An ungranted secret is never fetched.

The grant is necessary but not sufficient: the module also needs an egress policy that admits the
origin ([security-egress.md](security-egress.md) §7).

**Operator templates.** `{{secret:..}}` templates inside connection configs (SQL passwords,
object-store credentials) belong to the operator. They resolve through the provider chain without
a module grant, with the same grammar and the same 8-byte minimum.

---

## 3. Errors

| Situation | Failure |
|-----------|---------|
| invalid or reserved name, no grant, origin mismatch, body without `body` scope, value under 8 bytes | `non-retryable{secret_not_granted}` |
| absent from every provider | `non-retryable{secret_not_found}` |
| absent, and at least one provider was unavailable (Vault/K8s outage) | `retryable{secret_not_found}` |
| a provider reports invalid data (tampered sealed record, file path escape) | `non-retryable{secret_not_found}`; the chain stops (fail closed) |

---

## 4. Scrubbing and its honest limits

Before an HTTP outcome is encoded, the bridge builds an aho-corasick automaton from every value
it resolved for that request and replaces every match with `[REDACTED]` in the response body and
in **every** response header value. The patterns cover:
- the raw bytes;
- standard and URL-safe base64, padded and unpadded, at all three byte alignments (using the
  stable middle of each encoding, so a value embedded in a larger base64 blob is still caught);
- lower- and upper-case hex;
- percent-encoding;
- JSON string escaping.

Overlapping matches (two secrets sharing a substring) are merged into one `[REDACTED]`. The
completion is encoded once, after scrubbing, and the guest only ever sees it from the journal, so
live and replay bytes are identical. SC3 proves this end to end with a random canary: zero hits in
API responses, gRPC, guest output, the raw and decoded journal, TRACE-level tracing output, the
data directory and the object root, plus 9 encodings echoed back by the provider.

**Limits:**
- Values are zeroized in **Ostro-owned buffers** only. Copies inside `HeaderValue`/`Bytes`, hyper,
  h2 and rustls buffers are **not** zeroized. A process memory dump can contain them.
- Scrubbing catches the listed encodings. A provider that echoes a transformed value (hashed,
  truncated, encrypted, split across headers, or in an encoding not listed) is not caught.
- A templated request refuses compressed responses because compression would hide a value from
  the scrubber. Untemplated requests carry no secret, so there is nothing to scrub.
- The SQL and object connectors return database and object bytes as they are; their secrets are
  operator credentials that never appear in results. SQL error messages are scrubbed of the
  connection password and URL credentials.
- The OTLP export path is scanned at the Phase 8 gate (D-FX6); Phase 6 scans everything that
  OTLP would export through a TRACE-level capture layer.

---

## 5. The sealed store

Secrets set through the admin API are stored in the meta log as `SealedSecret` records, encrypted
before the meta write:

```
SealedSecret{ name:       "<tenant|_>/<name>",
              ciphertext: [ver = 1][nonce: 12 bytes][ciphertext + tag: 16 bytes],
              key_id:     "aes256gcm:<kid>" }          kid = first 8 hex chars of sha256(key)
AAD = b"ostro/secret/v1" ‖ name ‖ key_id
```

- AES-256-GCM through aws-lc-rs `RandomizedNonceKey`: every seal draws a fresh random nonce, so
  nonce reuse is impossible by construction.
- The AAD binds the record name and the key id. Moving a ciphertext to another name, or
  relabelling its key id, fails authentication.
- `plaintext-v0` is the legacy Phase 4 encoding of `__ostro/token-key` (the await-token key). It
  is still readable; `seal` never produces it.
- A scan of every file under the data directory (meta log and snapshots included) finds no secret
  value (`bridge_admin::secret_values_are_sealed_at_rest_and_never_returned`).

### 5.1 Loading the sealing key

The 32-byte key is taken from, in order:

1. `OSTRO_SEALING_KEY`: base64 of 32 bytes, in the server's environment.
2. `[secrets] sealing_key_file`: a file holding the key as base64 text or exactly 32 raw bytes.
3. **Dev profile only** (`[secrets] auto_sealing_key = true`, the default for `ostro dev`): an
   auto-generated `<data_dir>/sealing.key`, mode 0600. Its path (never the key) is logged with a
   warning.

```sh
head -c 32 /dev/urandom | base64 > /etc/ostro/sealing.key.b64   # generate once, keep it safe
chmod 0600 /etc/ostro/sealing.key.b64
```

**A server without a key refuses to store secrets.** `PUT /v1/secrets/...` answers 412
`precondition_failed` with `details.reason = "sealing_key_missing"`, and local presign keys
cannot be created either. Env, file, Vault and Kubernetes providers still work without a key.

Losing the key makes every sealed secret unreadable. Back it up separately from the data
directory. Webhook verifier secrets (`aes256gcm`) are opened with the same key ring.

### 5.2 Rotation

The key ring supports several keys: new seals use the current key and ciphertexts under older
kids still open. **Phase 6 has no operator command to load an old key next to a new one or to
reseal existing records** (`ostro secret reseal` is a follow-up). Until it exists, rotate by
configuring the new key and setting every secret again (`ostro secret set`); records sealed under
a key the node no longer has fail with `secret_not_found`.

---

## 6. Providers

Secrets resolve through an ordered provider chain. The first provider that has the name wins.

```toml
[secrets]
providers        = ["sealed", "env", "file"]     # order matters; also "vault", "k8s"
sealing_key_file = "/etc/ostro/sealing.key.b64"
env_prefix       = "OSTRO_SECRET_"
file_dir         = "/run/secrets"

[secrets.vault]
addr           = "https://vault.internal:8200"
mount          = "secret"                        # KV v2 mount
auth           = "approle"                       # token | approle | kubernetes
role_id        = "ostro"
secret_id_file = "/etc/ostro/vault-secret-id"
# token_file   = "/etc/ostro/vault-token"        # auth = "token" (or VAULT_TOKEN)
# k8s_role     = "ostro"                         # auth = "kubernetes" (service-account JWT)
cache_ttl      = "30s"

[secrets.k8s]
namespace  = "ostro"                             # default: the client config's namespace
# kubeconfig = "/etc/ostro/kubeconfig"           # default: kube's inference
cache_ttl  = "30s"
```

| Provider | Secret `NAME` is read from | Notes |
|----------|----------------------------|-------|
| `sealed` | `SealedSecret` `<tenant|_>/NAME` in meta | only when a sealing key is configured; per tenant |
| `env` | `OSTRO_SECRET_` + NAME upper-cased, with `-`, `.`, `/` → `_` (`stripe/live-key.v2` → `OSTRO_SECRET_STRIPE_LIVE_KEY_V2`) | read per call; distinct names can collide (`a-b`, `a_b`) |
| `file` | `<file_dir>/NAME` | read per call (rotated mounts apply at once); one trailing `\n` stripped; symlinks escaping `file_dir` refused; skipped while `file_dir` is unset |
| `vault` | KV v2 secret at path `NAME` under `mount`, field `value` | token / AppRole / Kubernetes auth; lazy session, re-login at 3/4 of the lease, one retry after a 403; found values cached for `cache_ttl` and zeroized on eviction |
| `k8s` | `Secret` `<secret>`, key `<key>`, from `NAME = <secret>/<key>` | in the configured namespace; raw `data` bytes; other values of the Secret zeroized after reading; cached for `cache_ttl` |

- A provider that is down (`Unavailable`) does not stop the chain. If no later provider has the
  name, the result is `retryable{secret_not_found}`.
- A provider reporting invalid data (`Invalid`) stops the chain (fail closed).
- Values are resolved per (run, attempt, name) and are never cached outside the Vault/K8s TTL
  caches. Phase 7 workers will get them through `ResolveSecret` over the same chain.

---

## 7. Admin surface

| Operation | REST | gRPC (`ostro.api.v1.Secrets`) | CLI |
|-----------|------|-------------------------------|-----|
| set a secret | `PUT /v1/secrets/{name}[?tenant=T]`, body `{"value": "..."}` or `{"value_b64": "..."}` | `PutSecret` | `ostro secret set <name> [--tenant T] [--from-file P \| --from-env VAR]` (stdin by default) |
| list secrets | `GET /v1/secrets[?tenant=T]` | `ListSecrets` | `ostro secret ls` |
| delete a secret | `DELETE /v1/secrets/{name}` | `DeleteSecret` | `ostro secret rm <name>` |
| add a grant | `PUT /v1/grants`, body `{module, secret, origins, body?}` | `PutGrant` | `ostro grant add --module M --secret S --origin O... [--body]` |
| list grants | `GET /v1/grants[?module=M]` | `ListGrants` | `ostro grant ls` |
| delete a grant | `DELETE /v1/grants/{id}` | `DeleteGrant` | `ostro grant rm <id>` |

- **Values are write-only.** No response (REST, gRPC, OpenAPI examples, CLI output) ever contains a
  value; lists return names and key ids only. The OpenAPI document marks `value`/`value_b64`
  `writeOnly`.
- The CLI never takes a value on the command line (that would be a usage error, exit 2, since argv
  is visible in the process list). It reads stdin (a TTY is refused with a hint), a file or an
  environment variable.
- Names under `__ostro/` are 400 `secret_reserved`; values under 8 bytes are 400
  `secret_too_short`. Error messages never echo the value.
- Secrets live per tenant; grants and egress policies are keyed by module.

```sh
printf '%s' 'sk_test_PLACEHOLDER' | ostro secret set stripe/live
ostro grant add --module payments --secret stripe/live --origin https://api.stripe.com
ostro egress set payments --allow https://api.stripe.com
```

### 7.1 The `__ostro/` prefix

Reserved sealed names used by Ostro itself:
- `__ostro/token-key`: the await-token signing key (Phase 4, `plaintext-v0`).
- `__ostro/presign/<connection>`: the HMAC key of a local object connection's presigned URLs
  (Phase 6), created on first use.

They are not listed by `GET /v1/secrets`, cannot be set or deleted through the API, cannot be
referenced from a template, and providers refuse the prefix.

---

## 8. Phase 8 coupling

Phase 8 webhook verifiers carry a secret reference. They resolve `aes256gcm` verifier secrets
through the node's sealed store from the same key ring (`OSTRO_SEALING_KEY`,
`[secrets] sealing_key_file`, the dev auto key), not through a second mechanism. They do not use
the template resolver: a verifier secret is an exact record name, with no env/file/Vault lookup and
no 8-byte template minimum.

---

## 9. Known limitations and follow-ups

- **Vault fields and upper-case Kubernetes keys are unreachable from templates.** The provider
  grammars accept `path#field` (Vault) and `<secret>#<key>` or upper-case keys (Kubernetes), but
  the `{{secret:NAME}}` grammar has no `#` and no upper case, and operator templates use the same
  grammar. Through templates, Vault reads only the default `value` field and Kubernetes only
  lower-case keys in the `<secret>/<key>` form. Widening the grammar is a follow-up.
- **vaultrs keeps the Vault session token in a plain `String`** inside its client settings,
  because its API takes `&str`. Ostro's own copies are `SecretString`, and the vaultrs settings
  are never formatted or logged.
- **Kubernetes config inference order:** without `kubeconfig`, kube's `Config::infer()` tries
  `KUBECONFIG` / `~/.kube/config` first and the in-cluster service account second. Set
  `kubeconfig` explicitly to pin the source.
- No Vault `renew_self`: leases are refreshed by a fresh login at 3/4 of their duration; a static
  token is re-read only after a 403.
- No operational key rotation or `ostro secret reseal` (§5.2).
