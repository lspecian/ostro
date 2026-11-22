# Embedded UI

The `ostro` binary serves an operator UI at `/ui/` on the API listener (crate
`ostro-ui`). It is server-rendered HTML with a vendored copy of htmx for partial updates;
there is no build step, no Node toolchain and no external request. `GET /` redirects to
`/ui/` when the UI is enabled.

## Pages and actions

| Page | Path | Shows | Actions |
|---|---|---|---|
| Runs | `/ui/`, `/ui/runs` | run list with status / workflow / queue / tag filters, newest first with tags | link to bulk |
| Run | `/ui/runs/{id}` | summary, published status, lineage (rerun / restart / hook), timeline with replay markers, guest logs | cancel, terminate, retry, signal, pause / resume, rerun from here |
| Rerun | `/ui/runs/{id}/rerun` | cut form (step or seq, version) | preview (dry run with the replay-check verdict), rerun |
| DLQ | `/ui/dlq` | DLQ members with reason, hook run and filters | requeue (auto / resume / restart / rerun), dismiss |
| Bulk | `/ui/bulk`, `/ui/bulk/{job}` | job list, job progress | preview (estimate + sample), create, cancel |
| Modules | `/ui/modules` | uploaded modules (sha256, size, workflows), newest first | |
| Workflows | `/ui/workflows` | workflows, versions, active version, pause state | pause / resume |
| Queues | `/ui/queues` | ready / running counts, worker demand, flow settings, pause state | pause / resume |
| Schedules | `/ui/schedules` | cron schedules, next fire, pause state | pause / resume / trigger |
| Workers | `/ui/workers` | connected workers, slots, labels, state | drain |
| Partitions | `/ui/partitions` | per-partition role, leader, epoch, applied / log end, lag, ready / running, health (refreshed every 5 s) | |

Run timelines (`/ui/runs/{id}/timeline`), logs, the run status card, bulk progress and
partition health are htmx fragments of the same pages. Every page works without
JavaScript: actions are plain HTML forms that answer `303 See Other`, and htmx only
upgrades them to in-place swaps (`HX-Redirect` after a rerun, requeue or bulk create).

Every action calls the same control plane as REST and the CLI, so its rules and errors
are the ones in [operations.md](operations.md): for example a rerun preview shows the
replay-check verdict or the `rerun_cut_unsafe` details with the nearest safe cut.

## Security model (D-P11-15)

Every `/ui` response, errors included, carries:

```
Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: no-referrer
Cache-Control: no-store            (pages and fragments; assets: no-cache)
```

There is no inline script or style, so the Content-Security-Policy needs no `unsafe-*`
source. All output is HTML-escaped by the template engine.

**Host allowlist.** A request whose `Host` (or `:authority`) is not allowed is 403 before
any handler runs (DNS-rebinding protection). Allowed are the bound listen address, the
loopback names `localhost`, `127.0.0.1` and `[::1]` with any port, and the entries of
`[ui] allowed_hosts` (an entry with a port matches exactly, without a port any port):

```toml
[ui]
allowed_hosts = ["ostro.internal", "ostro.example.com:8443"]
```

or `OSTRO_UI_ALLOWED_HOSTS=ostro.internal,ostro.example.com:8443`. A node listening on
`0.0.0.0` needs the hostname operators use in this list.

**Mutations** (every `POST /ui/...`) must pass all of:

1. same origin: `Sec-Fetch-Site` is `same-origin` (or absent with a matching `Origin`);
   a cross-site request is 403;
2. CSRF double submit: the `ostro_csrf` cookie (random 32 bytes, `HttpOnly`,
   `SameSite=Strict`, `Path=/ui`, `Secure` over https) must equal the `X-Ostro-Csrf`
   header (htmx) or the `csrf` form field (plain forms), compared in constant time;
3. the `operate` scope (see the Phase 14 seam below).

Request bodies are capped at 256 KiB.

## Exposure rules (D-P11-17)

- `[ui] enabled` unset (both profiles): the UI is on exactly when the API listener is a
  loopback address. `ostro dev` listens on `127.0.0.1:7700`, so the UI is on.
- `ostro server` on a non-loopback listener: the UI is off unless `[ui] enabled = true`.
  Enabling it there logs a WARN naming Phase 14.
- `[ui] enabled = false` turns it off everywhere; `/` then serves the plain shell and
  `/ui/*` is 404.
- **Until Phase 14 the UI is unauthenticated.** Anyone who can reach the listener can
  operate runs through it, exactly as through `/v1`. Expose it only on loopback, behind an
  authenticating reverse proxy, or on a trusted network.

Note: the dev profile does not set `enabled = true` explicitly (11-04); the loopback rule
decides. `ostro dev --listen 0.0.0.0:7700` therefore has the UI **off** unless
`[ui] enabled = true` is set, which is stricter than "dev: always on".

## Vendored assets

`crates/ostro-ui/assets/vendor/htmx-2.0.11.min.js` is htmx 2.0.11 copied byte-exact from
the npm package (0BSD), embedded with `include_bytes!` and checked against its sha256 by
`crates/ostro-ui/tests/vendored.rs`; provenance and the upgrade procedure are in
`VENDORED.md`. `app.css` and `app.js` are first-party. Assets are served from
`/ui/assets/...` with `Cache-Control: no-cache` (revalidated on every load).

## Phase 14 seam

Authorization goes through one trait, `ostro_ui::auth::UiAuth`:
`principal(&request parts) -> Result<UiPrincipal, UiReject>`. Pages require the scope
`read`; actions, rerun, DLQ and bulk require `operate`. Phase 11 installs `AllowAll`
(every scope). Phase 14 supplies the real implementation (API token to session cookie via
`POST /ui/login`) without touching the pages.

## Testing

- `crates/ostro-ui/tests`: pages, actions, inventory, security headers, CSRF, Host
  allowlist and HTML hardening against the API fakes.
- `crates/ostro/tests/ui_e2e.rs`: the same over HTTP against the spawned binary with real
  runs, a real worker and a schedule (11-18).
- Real-browser behaviour (rendering, htmx swaps, keyboard navigation, `hx-confirm`
  dialogs, polling, CSP console) is checked by hand: see the Manual-Only list in
  `.planning/phases/11-run-operations-embedded-ui/11-VALIDATION.md`.
