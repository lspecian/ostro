---
phase: 14-multi-tenancy-security-1-0-release
plan: 25
subsystem: docs
tags: [docs, dist-03, docs-check, getting-started, tenancy, audit, signed-modules, install, kubernetes]
requires: ["14-11", "14-12", "14-13", "14-14", "14-16", "14-17", "14-18", "14-19", "14-20", "14-28", "14-30", "14-32", "14-23 (bytes/step, soak finding)", "p14-key-rotation", "p14-fixups-2/3"]
provides:
  - "scripts/docs-check.sh: marker rule + links/anchors/snippets over README.md and docs/ (default) or given paths"
  - "scripts/check-links.py: offline Markdown checker (python3 -I, stdlib only): relative links, GitHub-slug anchors, ostro:snippet byte-for-byte check, --markers rule"
  - "docs/getting-started/{rust,python,typescript}.md, docs/{determinism,tenancy-and-auth,audit,signed-modules,operations-sizing,install,kubernetes}.md"
  - "connectors.md appended §10-12 (secrets/grants/egress quick reference, tenancy notes, triggers); root README and docs/README index"
affects: ["14-24 (security-model.md / security/ referenced as code spans; turn into links once they exist)", "14-26/14-27 (full-tree docs-check: 24 marker failures in files this plan does not own; CI step)", "release (publish the minisign public key into docs/install.md)"]
tech-stack:
  added: []
  patterns: ["ostro:snippet markers point at ostro init templates, test guests and test fixtures that CI builds/tests", "config defaults as tables; TOML examples only as snippets of a tested file"]
key-files:
  created: [scripts/docs-check.sh, scripts/check-links.py, docs/getting-started/rust.md, docs/getting-started/python.md, docs/getting-started/typescript.md, docs/determinism.md, docs/tenancy-and-auth.md, docs/audit.md, docs/signed-modules.md, docs/operations-sizing.md, docs/install.md, docs/kubernetes.md]
  modified: [README.md, docs/README.md, docs/connectors.md, "p14-notes hand-offs: docs/cron.md, docs/flow-control.md, docs/migrations/{index,from-cron-and-queue-tables,from-restate,from-temporal}.md, docs/secrets.md, docs/ui.md"]
decisions:
  - "Illustrative blocks outside docs/migrations/ must carry `pseudo` on the marker line (D-P14-27); a snippet marker may also sit on js/text blocks (verified the same way)"
  - "The marker sits on the nearest non-blank line before the fence (the 14-14 format)"
  - "Links into 14-24's not-yet-landed docs (security-model.md, security/auth-matrix.md) are code spans, so docs-check stays green; the plan's grep for security-model.md in docs/README.md still holds"
  - "connectors.md §2.1 stale Rust block (`.send()?`) replaced by a snippet of guests/pay-flow (`.send().wait()?`, same idea); §7 TOML defaults block turned into a defaults table plus a snippet of the TOML parsed by crates/ostro-server/tests/config.rs"
  - "Release public key shown as a placeholder (`RW<release public key: published with the first release>`) with an HTML comment telling the release to fill it; text states the key is a password-less repository secret and the public key is published (user decision)"
metrics:
  duration: "~2h30"
  completed: 2026-11-27
---

# Phase 14 Plan 25: 1.0 docs set + docs-check Summary

The DIST-03 docs set is complete. There are per-language getting-started guides (Rust, Python,
TypeScript), plus determinism, tenancy and auth, audit, signed modules, operations and sizing,
install and Kubernetes guides, an extended connectors guide, a rewritten root README and a
`docs/README.md` index of every guide. `scripts/docs-check.sh` ties every rust, python,
typescript and toml block to a CI-built file (`ostro:snippet`, byte for byte) and checks every
relative link and anchor offline. It passes on this plan's files plus 14-14's migration guides.

**Gate incomplete: consolidated gate pending on dell-4.** Only the plan's two task verifies ran
(dell-4, lane 14-25). Both are green; see "Unverified" below.

## Commits

| Task | Commit | Files |
|---|---|---|
| 1: docs-check + getting started | 467491c | scripts/check-links.py (100755), scripts/docs-check.sh (100755), docs/getting-started/{rust,python,typescript}.md |
| 2: 1.0 docs set + README | 5b03869 | README.md, docs/README.md, docs/{determinism,connectors,tenancy-and-auth,audit,signed-modules,operations-sizing,install,kubernetes}.md + hand-off edits (cron, flow-control, migrations x4, secrets, ui) |

(Hashes before the CAS rebuild; the final hashes are in the STATE line.)

## Docs inventory

| File | Content |
|---|---|
| docs/getting-started/rust.md, python.md, typescript.md | prerequisites (only `ostro`), `ostro init`, `ostro dev --ephemeral --time-skipping`, `ostro build`, `ostro deploy`, `ostro run`/`signal`/`result` and the same over curl (bearer note for token mode), `ostro ls/inspect/history`, UI link, the template's time-skipping test, `ostro replay`, next steps |
| docs/determinism.md | the virtualised WASI surface (from `ALLOWED_WASI` and the shim), per-SDK deterministic APIs, the step/effect boundary, command-hash divergence detection and what happens to the run, `ostro replay` as a CI gate, versions, `patched` (Rust/Python/TS snippets of the SC3 replay fixtures), `migrate-runs`, per-language pitfalls |
| docs/tenancy-and-auth.md | `[auth]` modes and keys, unauthenticated routes (from `authn_layer::UNAUTHENTICATED`), tenants and qualified names, connections cluster-scoped in 1.0, the 404 rule, tenant CLI/REST and delete rules, the scope table and requirement kinds (from `authz::required`), token format, creation rules, rotate/revoke, bootstrap (env, file, generated `admin.token`, offline `ostro auth bootstrap`), quotas (delay not fail), ingress rule order, **cron fires dropped while suspended (not caught up)**, held runs, the **per-node leadership share** with a worked example, **`event_rate` burst not scaled**, fuel burst scaled, quota metrics and `metrics_tenant_labels`, errors, secrets/sealing cross-links, **UI session cookie and `Secure` behind TLS proxies** (no `X-Forwarded-Proto`) |
| docs/audit.md | the recorded actions (from `audit_hooks::classify` plus the out-of-guard ones), known gaps (tenant 404s, UI scope denials, async worker join/renew, client ip `unknown`, `request_id` null), record format, chain and `ostro audit verify`, chain-head export and what it protects, coalescing windows, reserve/commit fail-closed and the availability trade-off, a **retry table after `audit_commit_failed`** (idempotent vs check-before-retrying: token create/rotate, join tokens, upload, retry/rerun/trigger, bulk create), query/CLI, retention keys, OTLP export, backups exclude audit |
| docs/signed-modules.md | the enforcement matrix, CLI/REST, the `[security.modules]` seed and drift, signing with minisign and cosign (key pair), trusted keys, tenant tightening and tenant keys, attaching later (REST; `deploy --signature` merges), troubleshooting with the exact error strings |
| docs/operations-sizing.md | P, memory per language and the Python instance budget (sdk-resources), disk per step (14-23: ≈ 246 B/step single node; ≈ 443 B WAL per step over three replicas), **audit disk**, snapshots, bottomless, single-node and whole-cluster restore, sealing key, upgrades (new clusters start at the newest level), **file descriptors / `LimitNOFILE`** (14-23 EMFILE finding), Kafka triggers, observability, advisory capacity table |
| docs/install.md | the artefact list (from release.yml), the release public key placeholder, manual minisign/sha256 checks, `install.sh` flags and every `OSTRO_*` install variable, the macOS universal `.pkg` and the curl path, the container image (links deploy/image/README.md), Helm pointer, building from source, first-server checklist and an `OSTRO_*` environment table |
| docs/kubernetes.md | what the chart renders, install/upgrade (commands run against a cluster the user chooses), install-time-fixed values, the main values, Secrets and the generated-Secret caveat, sealing-key rotation on Kubernetes, KEDA (prometheus / metrics-api, scale to zero), the NetworkPolicy matrix and egress needs, backups and cluster restore |
| docs/connectors.md | §2.1 and §7 blocks now snippet-backed; appended §10 secrets/grants/egress quick reference, §11 tenancy notes, §12 triggers (webhooks, cron, Kafka) |
| README.md | pitch, quickstart, features, docs table, layout, build/test (incl. docs-check), licence |
| docs/README.md | guide index (start here, writing workflows, triggers, running, security, formats); the 2026-11-18 design documents kept below |

## docs-check counts (dell-4, lane 14-25, at 5b03869's tree)

| Run | Files | Blocks checked (marker rule) | Snippets verified | Relative links | Result |
|---|---|---|---|---|---|
| Task 1 verify: `docs/getting-started docs/migrations` | 7 | 24 | 23 | 210 | PASS |
| Task 2 verify: the plan's 13 paths | 17 | 30 | 29 | 430 | PASS |
| full tree (`bash scripts/docs-check.sh`, info) | 65 | 54 | 29 | 564 | links 0 failures; **markers 24 failures, all in files this plan does not own** (below) |

A negative check on dell-4 (a temp doc with a wrong snippet line, missing anchors and a missing
file, an unmarked toml block, an illustrative block without `pseudo`) reported all six failures
with file:line and exited 1.

## Blocks marked illustrative

None. Every rust/python/typescript/toml block in this plan's files is an `ostro:snippet`:
the `ostro init` templates (`crates/ostro-build/templates/{rust,python,ts}`), `guests/det-basic`,
`guests/p10-replay-v3`, `crates/ostro/tests/fixtures/replay/{py-v3,ts-v3}`, `guests/pay-flow` and
`crates/ostro-server/tests/config.rs`. The TS test snippet is a `js` block (the template test is
JavaScript), also verified. Config examples without a tested source are tables.

## p14-notes hand-offs handled

| Note | Handled in |
|---|---|
| 14-11: install.md with the public key, `OSTRO_*` env, `--system`, macOS .pkg | install.md §1-4, §8 |
| 14-12: create install.md, kubernetes.md, tenancy-and-auth.md (referenced by deploy/image/README.md) | created |
| 14-13: chart docs, LimitNOFILE (14-23) | kubernetes.md; operations-sizing.md "File descriptors" |
| 14-14: cron.md positional name | cron.md now `--name nightly` |
| 14-14: connectors.md §2.1 marker | snippet of guests/pay-flow (the old block's `.send()?` did not match the SDK) |
| 14-14: link tenancy/audit/signed-modules from docs/migrations/* | index.md, from-cron-and-queue-tables.md, from-temporal.md, from-restate.md |
| 14-14: flow-control.md §2 "per node until Phase 13", Restate "counters per node" | checked against flow/mod.rs + flow/tenant.rs: queue limits are per node in a cluster (no share); tenant quotas use the leadership share. Both texts reworded |
| 14-18: D-P14-13 table vs code, "check before retrying" | audit.md §1 from `classify`, §5 retry table |
| 14-20: dropped cron fires while suspended; burst not scaled; rejected metric + rule order | tenancy-and-auth.md §5 |
| 14-21: Secure cookie behind a TLS proxy | tenancy-and-auth.md §9 (+ ui.md seam text) |
| User decisions: password-less release key / public key published; newest level for fresh clusters; connections cluster-scoped; sealing-key rotation supported | install.md §2; signed-modules.md §2 + operations-sizing.md "Upgrades"; tenancy-and-auth.md §2 + connectors.md §11; rotation cross-links in tenancy-and-auth, install, operations-sizing, kubernetes, connectors; stale secrets.md §9 bullet "No operational key rotation" removed |
| Kafka triggers and cluster-restore cross-links | connectors.md §12, operations-sizing.md, kubernetes.md §6/§7, tenancy-and-auth.md, README |

## Deviations from Plan

### Auto-fixed / scope

1. **[Rule 3] Files outside files_modified.** The p14-notes hand-offs addressed to 14-25 need
   small edits in docs/cron.md (1 line), docs/flow-control.md (§2 bullet), docs/migrations/*
   (links, one Restate sentence), docs/secrets.md (one stale bullet removed) and docs/ui.md (the
   "Phase 14 seam" paragraph). No content beyond those.
2. **[Rule 1] connectors.md existing blocks.** §2.1's Rust block used `.send()?`, which is not the
   SDK API (`.send().wait()?`). It was replaced by a snippet of guests/pay-flow. §7's defaults
   TOML had no tested source, so it became a defaults table (same values) plus a snippet of the
   TOML that tests/config.rs parses. "Appended sections only" was otherwise kept (§10-12 new).
3. **Task 1 verify ran green only after Task 2.** The Task 1 guides link to docs written in
   Task 2. At the first run the links failed and the snippets passed. Both verifies were
   re-run together on the final tree, and both passed.
4. **14-24 links as code spans.** `docs/security-model.md` and `docs/security/auth-matrix.md` do
   not exist on main yet. They are named in code spans in docs/README.md and
   tenancy-and-auth.md. 14-24 or 14-26/27 should turn them into links once those files land.

### Plan wording vs execution

- The plan's verification names `ostro-k8s-ci --parallel` (the cluster is withdrawn). This plan
  changes only Markdown and two scripts. The plan-level gate is left to the consolidated dell-4
  gate.
- Not every scope row could be re-checked by running a server. The scope tables come from
  `authz::required`, the unauthenticated routes from `authn_layer::UNAUTHENTICATED`, the audit
  actions from `audit_hooks::classify`, and the config keys and defaults from `config.rs`.

## Unverified (gate incomplete — consolidated gate pending on dell-4)

1. No ci-local or consolidated gate run at this plan's commits (docs and scripts only; no Rust
   changed).
2. Full-tree `bash scripts/docs-check.sh` (the default `README.md docs`) **fails the marker rule
   with 24 blocks in files this plan does not own**: docs/02-prd.md:80, 03-architecture.md:512,
   :694, bottomless.md:15, :41, kafka-triggers.md:39 (illustrative without `pseudo`),
   observability.md:110, :184, operations.md:238, replay.md:136, replication.md:100,
   secrets.md:190, :244, security-egress.md:169, tls.md:15, :62, ui.md:53,
   workflow-semantics.md:108, :276, archive/pgflow/02-prd.md:83, :103, :121,
   archive/pgflow/03-architecture.md:733, :994. Links are 0 failures across the full tree.
   Owner: 14-26/14-27 (or decide to exclude `docs/archive/` and the 01-05 design docs).
3. docs-check is not yet a ci-local/CI step (proposed below, for 14-27 once the full tree is
   green).
4. Commands in the guides were checked against the CLI/REST code and the templates, not run
   end to end here. Not executed in this plan: the curl start/signal/result sequence, `ostro auth bootstrap`,
   `minisign`/`cosign sign-blob` commands, the attach-signatures curl, `helm install/upgrade`,
   `kubectl create secret`, `podman run` of the image, `sudo installer -pkg`. The getting-started
   CLI loop itself is what `scripts/clean-machine.sh` runs.
5. Operations-sizing numbers are advisory (WSL2 / loaded dell-4); the 1.0 gate benchmark numbers
   are still pending (14-23).

## Proposed CI hunks (not applied; no workflow branch created)

```bash
# scripts/ci-local.sh (14-27), once the full tree passes:
step "docs check" bash scripts/docs-check.sh
```

```yaml
# .github/workflows/ci.yml (lint job), once the full tree passes:
      - name: Docs check (snippets + links, offline)
        run: bash scripts/docs-check.sh
```

## For the orchestrator

1. **Release public key.** docs/install.md §2 has a placeholder. Fill it with line 2 of the
   real `minisign.pub` when the key is generated (the same value as the repository variable
   `OSTRO_RELEASE_MINISIGN_PUBKEY`). The `<owner>` in the install URL and the image/chart
   locations also need the real GitHub owner at publish time.
2. **Full-tree docs-check** (Unverified 2): assign the 24 marker fixes to 14-26/14-27, or decide
   that `docs/archive/` and the pre-build design docs (01-05) are excluded from the marker rule.
3. **14-23 finding "tenants cannot create schedules"** (p14-notes): docs/tenancy-and-auth.md
   lists schedule/webhook create/update/delete under the tenant-admin requirement, as
   `authz::required` says. If p14-fixups-4 changes that, update the table.
4. **Secure cookie behind proxies:** documented as behaviour (no `X-Forwarded-Proto` support).
   If 1.0 should honour a trusted proxy header, that is a code change (ui_auth.rs).

## Self-Check: PASSED

- Files: scripts/docs-check.sh and scripts/check-links.py (both 100755), the three
  getting-started guides and the seven new guides exist on the final tree. README.md and
  docs/README.md are updated.
- Commits: both task commits are on main after the CAS (hashes in the STATE line).
- Verify commands: both green on dell-4 (lane 14-25) at the final tree.
