# `ostro` CLI: JSON output and exit codes

Every `ostro` verb that produces a result accepts `--json`. This page is the contract
scripts and CI jobs can rely on: what `--json` prints, how it may change, and what the
exit codes mean.

The local-verb envelopes are snapshot-tested in `crates/ostro/tests/json_contract.rs`
(with paths, shas, timings and versions redacted). The REST-proxy bodies are
snapshot-tested in `crates/ostro/tests/cli_verbs.rs` and `cli_p11.rs`.

## Two JSON families

**REST-proxy verbs** (`run`, `ls`, `inspect`, `history`, `signal`, `cancel`,
`terminate`, `retry`, `result`, `token complete`, `deploy <file.wasm>`, `schedules`,
`webhooks`, `secret`, `rerun`, `dlq`, `bulk`, `limits`, ...) print the server's REST
response body verbatim, pretty-printed. The schema is the server's OpenAPI document
(`ostro openapi`, `GET /openapi.json`). An API error prints the server's error body
(`{"error": {"code", "message", ...}}`) on stdout as well.

A verb that makes two calls combines the two bodies into one object:

| Verb | Object |
| --- | --- |
| `run --wait` | `{"start": <start body>, "result": <result body>}` |
| `deploy <file.wasm> --activate` | `{"upload": <upload body>, "activated": [<workflow body>, ...]}` |

REST-proxy output has no `kind` or `version` key.

**Local verbs** (`init`, `build`, `deploy [DIR]`, `replay`, `toolchain install|list|prune`,
`doctor`) do their work in the CLI, so there is no REST body to forward. They print a
versioned envelope: a JSON object whose **first two keys** are

```json
{
  "kind": "ostro.<verb>.result",
  "version": 1,
  ...
}
```

followed by the verb's fields. `<verb>` is `init`, `build`, `deploy`, `replay`,
`toolchain` (all three subcommands; the `action` field says which) or `doctor`.

## Compatibility rules

- Fields are **additive only**: a new field may appear in any release.
- `version` is bumped only on a **breaking** change (a field removed, renamed or given a
  different type or meaning). A version bump changes the envelope of that verb only.
- Consumers **must ignore unknown keys**, and should check `kind` and `version` before
  reading the rest.
- Key order beyond `kind` and `version` is not part of the contract.
- Absolute paths are printed as given by the OS; `null` means "not applicable" or
  "not found".

## Local verb fields

### `ostro init --json` (`ostro.init.result`)

| Field | Type | Meaning |
| --- | --- | --- |
| `dir` | string | Absolute project directory |
| `lang` | string | `rust`, `python` or `ts` |
| `name` | string | Project (module) name |
| `files` | string[] | Files written, relative to `dir`, `/`-separated |
| `sdk_bundle` | string | Id of the SDK bundle materialised into `target/ostro/sdk` |

### `ostro build --json` (`ostro.build.result`)

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | string | Project (module) name |
| `lang` | string | Project language |
| `wasm` | string | Absolute path of `target/ostro/<name>.wasm` |
| `sha256` | string | Lower-case hex sha256 of the component |
| `bytes` | number | Component size |
| `workflows` | `{name, version}[]` | Workflows the component describes |
| `up_to_date` | bool | `true` when the input hash was unchanged and nothing was built |
| `manifest` | string | Absolute path of `target/ostro/manifest.json` |

### `ostro deploy [DIR] --json` (`ostro.deploy.result`)

| Field | Type | Meaning |
| --- | --- | --- |
| `build` | object | The `ostro.build.result` fields above (without `kind`/`version`) |
| `upload` | object | The REST body of the module upload (`{created, module}`) |
| `activated` | object[] | The REST body of each workflow activated by this deploy (empty with `--activate=false` or when every version was already active) |

`ostro deploy <file.wasm> --json` is a REST-proxy verb (see above), not this envelope.

### `ostro replay --json` (`ostro.replay.result`)

| Field | Type | Meaning |
| --- | --- | --- |
| `module_sha256` | string or null | sha256 of `--module` (null for `--corpus` without `--module`) |
| `corpus` | string | `--corpus` only: the corpus directory |
| `verdicts` | object[] | One row per run or corpus entry |
| `summary` | object | Counts: `ok`, `ok_frontier`, `diverged`, `skipped`, `error` |

A verdict row has `run_id`, `workflow`, `verdict` (`ok`, `ok_frontier`, `diverged`,
`skipped`, `error`), and when applicable `seq` (the first diverging journal sequence),
`detail` (a message), `appended` (events the replay added past the recorded frontier)
and, offline, `entry` (the corpus entry name). See docs/replay.md.

### `ostro toolchain <install|list|prune> --json` (`ostro.toolchain.result`)

| Field | Action | Type | Meaning |
| --- | --- | --- | --- |
| `action` | all | string | `install`, `list` or `prune` |
| `cache_root` | all | string | The toolchain cache directory |
| `toolchain_set` | all | string | Id of the pinned toolchain set |
| `platform` | install, list | string | e.g. `linux-x86_64` |
| `offline` | install | bool | `OSTRO_OFFLINE` was set |
| `tools` | install | object[] | `{tool, version, status, path}`; `status` is `installed`, `cached` or `error` (then `error` replaces `path`) |
| `tools` | list | object[] | `{tool, version, entry, installed, path}` |
| `unknown` | list | object[] | Cache entries not in the current set: `{name, path, complete}` |
| `state` | prune | bool | `--state` was given |
| `removed` | prune | string[] | Paths removed |

### `ostro doctor --json` (`ostro.doctor.result`)

| Field | Type | Meaning |
| --- | --- | --- |
| `cache_root` | string | The toolchain cache directory |
| `cache_writable` | bool | It can be created and written |
| `platform` | string | e.g. `linux-x86_64` |
| `toolchain_set` | string | Id of the pinned toolchain set |
| `tools` | object[] | As in `toolchain list` |
| `cc` | object | `{present, path}` of a C compiler |
| `docker` | object | `{present, path}` of the Docker CLI |
| `glibc` | string or null | glibc version (Linux) |
| `musl` | bool | The host libc is musl |
| `offline` | bool | `OSTRO_OFFLINE` is set |
| `mirror` | string or null | `OSTRO_TOOLCHAIN_MIRROR` |
| `warnings` | string[] | Human-readable problems; `doctor` exits 0 even with warnings |

## `target/ostro/manifest.json` (`ostro.manifest` v1)

Written by `ostro build` next to the component, read by `ostro deploy` and
`ostro dev --watch`. It follows the same compatibility rules.

```json
{
  "kind": "ostro.manifest",
  "version": 1,
  "name": "hello",
  "lang": "rust",
  "module": { "sha256": "<64 hex>", "bytes": 123456 },
  "workflows": [ { "name": "hello", "version": 1 } ],
  "toolchains": { "set_id": "<16 hex>", "tools": ["system-rustup"] },
  "sdk_bundle": "<16 hex>",
  "source_date_epoch": 0,
  "input_hash": "<64 hex>",
  "embedded_runtimes": []
}
```

## Exit codes

The same table holds for every verb (`ostro_cli::output::EXIT_CODES`; a test compares
this table with it). clap's own usage errors (unknown flag, bad value, unknown shell for
`ostro completions`) also exit 2.

| Code | Meaning |
| --- | --- |
| 0 | ok |
| 1 | API, server or build error |
| 2 | usage error |
| 3 | --wait ended in a status other than completed |
| 4 | replay divergence (ostro replay) |

`ostro replay` exits 4 when a run diverged, or when a run was skipped (the module does
not export its workflow) without `--allow-missing`; a replay error without divergence
exits 1.

## stderr

- Errors are one line: `ostro: error: <message>`. Under `--json` that is still the only
  stderr line from the CLI itself; stdout carries the JSON (for an API error, the
  server's error body).
- Build tools (cargo, componentize-py, jco) write their own progress and diagnostics to
  stderr; stdout stays the JSON alone.
- Human output (no `--json`) goes to stdout; it is not a stable format.
