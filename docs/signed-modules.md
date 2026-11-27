# Signed modules

Ostro can require every workflow module to carry a valid signature from a trusted key before it
is stored or activated. Signatures are checked with **minisign** (primary) or **cosign** with a
key pair (keyless Sigstore signing is not supported). This guide covers enabling enforcement,
signing, trusted keys, tenant rules, attaching signatures later and troubleshooting.

Related: [tenancy-and-auth.md](tenancy-and-auth.md) (scopes), [audit.md](audit.md) (every
policy change and rejected upload is recorded).

## 1. How enforcement works

The **cluster policy** has two parts: `require_signed_modules` and a list of trusted keys. It is
stored in cluster metadata, so every node enforces the same policy, including nodes whose
configuration does not mention it.

With `require_signed_modules` on:

| Situation | Answer |
|---|---|
| upload without a signature | 403 `signature_required`; nothing is stored |
| upload whose signatures all fail | 400 `signature_invalid`; nothing is stored |
| upload with a valid signature | 201; the signature is stored with the module as evidence |
| activating a version | its stored evidence is verified again over the module bytes, with the keys trusted **now**; else 403 `signature_required` |
| a start with an explicit `version`, a `migrate-runs` target, a rerun onto another version | the same check as an activation |
| versions that were already active, runs already in flight | keep working; enforcement never stops running code |

A supplied signature that does not verify is 400 `signature_invalid` **even when signing is not
required**, so a broken signing step does not go unnoticed. Both checks run before the module is
compiled or anything is written.

Because evidence is re-verified at every activation, removing a trusted key stops the activation
of versions signed only by that key. A new key that reuses an old minisign key id does not match
old evidence either: evidence is bound to the key's fingerprint, not to its id.

## 2. Enabling enforcement

From the CLI, with a system admin token:

```sh
ostro security trust add --minisign ./ostro-release.pub --comment "release signing key"
ostro security policy set --require-signed true
ostro security policy get
```

| REST | CLI |
|---|---|
| `GET /v1/security/modules/policy` | `ostro security policy get [--tenant t]` |
| `PUT /v1/security/modules/policy` | `ostro security policy set --require-signed true\|false [--tenant t]` |
| `POST /v1/security/modules/keys` | `ostro security trust add --minisign <KEY\|FILE> \| --cosign <FILE> [--comment c] [--tenant t]` |
| `DELETE /v1/security/modules/keys/{key_id}` | `ostro security trust rm <key-id> [--tenant t]` |

`policy set` with an empty key list keeps the trusted keys; it never wipes them. Add the trusted
key first, then turn enforcement on.

### Seeding from the node configuration

A node's `[security.modules]` section seeds the cluster policy while the cluster has none:

| Key | Default | Meaning |
|---|---|---|
| `[security.modules] require_signed` | `false` | require a valid signature on every upload and activation |
| `[security.modules] minisign_keys` | `[]` | trusted minisign public keys (key text or file paths) |
| `[security.modules] cosign_keys` | `[]` | trusted cosign public keys (PEM text or file paths) |

The first node that starts with the section writes it into metadata once. After that the
metadata policy wins: if a node's section differs from it, the node logs one warning and sets
`ostro_security_policy_drift` to 1, and the metadata policy still applies. Change the policy
with `ostro security policy set` / `trust add|rm` from then on.

Below finalized compatibility level 2 ([compat.md](compat.md)), signatures are verified but not
stored, and policy or key writes answer 409 `feature_not_enabled`. New clusters start at the
newest level, so this only applies to clusters upgraded from an older release before
`ostro cluster upgrade finalize`.

## 3. Signing a module

Sign the built component (`target/ostro/<name>.wasm`), not the project directory.

**minisign:**

```sh
minisign -G -p ostro-release.pub -s ostro-release.key   # once: a key pair
ostro build
minisign -S -s ostro-release.key -m target/ostro/hello.wasm   # -> hello.wasm.minisig
ostro deploy target/ostro/hello.wasm --signature target/ostro/hello.wasm.minisig --activate
```

Both minisign signature types are accepted: the default prehashed `ED` and the legacy `Ed`
(`minisign -S -l`). The trusted comment is verified too. `rsign` (rsign2) produces the same
files.

**cosign (key pair):**

```sh
cosign generate-key-pair                                  # cosign.key, cosign.pub (P-256)
cosign sign-blob --key cosign.key --output-signature hello.wasm.sig target/ostro/hello.wasm
ostro security trust add --cosign cosign.pub
ostro deploy target/ostro/hello.wasm --signature hello.wasm.sig --activate
```

A cosign signature is accepted as the raw base64 ASN.1 DER `.sig` file, or as a Sigstore bundle
(v0.3 JSON) that carries a public-key hint. Keyless bundles (certificate and transparency log)
are refused.

`--signature` is repeatable (at most 4 files, each at most 8 KiB). Over REST, each signature is
one `Ostro-Module-Signature: <base64 of the file>` header on `PUT /v1/modules/{sha}`; over gRPC,
`UploadModuleRequest.signatures`. A module keeps at most 4 signatures, one per key (the newest
wins).

## 4. Trusted keys

`ostro security trust add` accepts a minisign public key as the `RW…` line or the two-line
`.pub` file, or a cosign P-256 public key PEM file. The key id is the minisign key id (as
`minisign` prints it, upper-case hex) or, for cosign, derived by the server from the key.
Key ids are unique: adding a second key with the same id is 409.

`ostro security policy get` lists every key with its algorithm (`minisign` or `cosign-p256`),
key id, comment and tenant (or `(cluster)`).

## 5. Tenants

A tenant can be stricter than the cluster, never looser:

- **Tenant requirement.** `ostro security policy set --tenant acme --require-signed true`
  requires signatures for tenant `acme` even when the cluster does not. `false` removes that
  tightening; it cannot turn off a cluster-wide requirement. A tenant admin may set it for its
  own tenant.
- **Tenant keys.** `ostro security trust add --tenant acme --minisign …` adds a key that counts
  only for `acme`'s uploads and activations. A tenant's effective keys are the cluster keys plus
  its own. A tenant admin manages its own tenant's keys; another tenant's key ids answer 404.
- The default tenant `_` has no tenant record: only the cluster policy applies to it.

The check uses the tenant that uploads or activates: a module that another tenant uploaded with
a signature from that tenant's own key does not satisfy a second tenant that requires signatures,
and a second tenant cannot re-upload the same bytes unsigned. `trust rm --tenant t <id>` removes
tenant `t`'s key when several tenants hold the same key id.

## 6. Attaching signatures later

A module uploaded before enforcement (or by a pipeline that signs afterwards) can get its
signatures later, without uploading it again:

```sh
curl -sS -X POST "$OSTRO_SERVER/v1/modules/<sha256>/signatures" \
  -H "Authorization: Bearer $OSTRO_TOKEN" -H 'content-type: application/json' \
  -d "{\"signatures\": [\"$(base64 -w0 hello.wasm.minisig)\"]}"
# {"sha256":"…","key_ids":["…"]}
```

This needs the `deploy` scope. Invalid signatures are 400 and nothing is attached. Then activate
the version as usual. There is no separate CLI verb; `ostro deploy --signature` of the same
`.wasm` attaches the signature too, because an upload of an existing module merges its
signatures.

Re-verification needs the module bytes. A node that does not hold the module's blob answers 503
for an activation; retry through another node.

## 7. Troubleshooting

| Answer | `details` | Meaning and fix |
|---|---|---|
| 403 `signature_required` | `sha256`, `why` (for example `the upload carries no signature`, `no trusted key is configured`, `stored signatures do not verify: …`) | the module needs a valid signature by a key trusted for this tenant. Sign it, add the key, or attach a signature |
| 400 `signature_invalid` | `sha256`, `error`, `key_ids` | a signature was supplied and none verified. `error` names the most specific failure: `no trusted key matches the signature` (trust the key, or sign with a trusted one), `signature does not verify` (the file is not a signature of these bytes: sign the exact `.wasm` you deploy), `bundle message digest does not match the module`, `keyless sigstore bundles are not supported (keyed cosign only)`, `too large: …` or `malformed: …` |
| 409 | — | `trust add` with a key id that is already trusted; or `feature_not_enabled` below compatibility level 2 (§2) |
| 503 | — | the node has no copy of the module bytes for re-verification; retry through another node |

The supplied key ids are listed in `details.key_ids` of a 400; compare them with
`ostro security policy get`. Every rejected upload and every policy or key change is in the
audit log (`module.upload` with outcome `denied:signature_required` or
`failed:invalid_argument`, `security.policy.set`, `security.key.add`, `security.key.remove`).
