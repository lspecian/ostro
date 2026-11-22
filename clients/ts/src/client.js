// The Ostro REST client (Node >= 18 globals: fetch, AbortController; node:crypto for the
// module hash). Mirrors the Rust reference client (crates/ostro-client/src/typed.rs).
// Responses are the server's JSON objects; every failure throws OstroError.

import { createHash } from "node:crypto";

export const DEFAULT_SERVER = "http://127.0.0.1:7700";

/** The server caps one result long-poll at 60 s; longer waits are a loop of polls. */
const MAX_POLL_MS = 60_000;

/** Extra time a replay request may take on top of the client timeout. */
const REPLAY_EXTRA_MS = 300_000;

const UNITS = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/**
 * `"30s"` / `"500ms"` / `"2m"` / `"72h"` / `"1d"` or a number of milliseconds -> ms.
 * @param {string|number} d
 * @returns {number}
 */
export function parseDuration(d) {
  if (typeof d === "number") {
    if (!Number.isFinite(d) || d < 0) throw new TypeError(`not a duration: ${d}`);
    return d;
  }
  const m = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$/.exec(String(d));
  if (!m) throw new TypeError(`not a duration (use e.g. "30s", "500ms", "72h"): ${d}`);
  return Number(m[1]) * UNITS[m[2]];
}

/** Lowercase hex SHA-256 (a module's id). */
export function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

/**
 * The one error type: HTTP `status` (undefined without a response), `code`, `message` and
 * `details` from the REST error body. Transport failures use code "unreachable" (no
 * connection) or "timeout"; a non-JSON error response uses code "http_<status>".
 */
export class OstroError extends Error {
  constructor(status, code, message, details = {}) {
    super(message);
    this.name = "OstroError";
    this.status = status;
    this.code = code;
    this.details = details ?? {};
  }

  toString() {
    const where = this.status === undefined ? "" : `HTTP ${this.status} `;
    return `OstroError: ${where}${this.code}: ${this.message}`;
  }

  /** `details.reason` when the server sent one (e.g. `time_skipping_disabled`). */
  get reason() {
    const r = this.details?.reason;
    return typeof r === "string" ? r : undefined;
  }
}

const enc = (s) => encodeURIComponent(String(s));

export class Client {
  /**
   * @param {{server?: string, timeoutMs?: number}} [opts]
   */
  constructor({ server = process.env.OSTRO_SERVER ?? DEFAULT_SERVER, timeoutMs = 30_000 } = {}) {
    if (!server) server = DEFAULT_SERVER;
    if (!server.includes("://")) server = `http://${server}`;
    this.server = server.replace(/\/+$/, "");
    this.timeoutMs = timeoutMs;
  }

  /**
   * One request. `route` is the [METHOD, template] (ROUTES) it implements; `segments`
   * are percent-encoded one by one (a tenant-qualified `acme/w` stays one segment).
   * @private
   */
  async _request(route, segments, { query, body, raw, contentType, extraMs = 0 } = {}) {
    const [method] = route;
    let url = `${this.server}/${segments.map(enc).join("/")}`;
    if (query) {
      const q = new URLSearchParams();
      for (const [k, v] of query) if (v !== undefined && v !== null) q.append(k, String(v));
      const s = q.toString();
      if (s) url += `?${s}`;
    }
    const headers = { accept: "application/json" };
    let payload;
    if (raw !== undefined) {
      payload = raw;
      headers["content-type"] = contentType;
    } else if (body !== undefined) {
      payload = JSON.stringify(body);
      headers["content-type"] = "application/json";
    }
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), this.timeoutMs + extraMs);
    let resp;
    let text;
    try {
      resp = await fetch(url, { method, headers, body: payload, signal: ac.signal });
      text = await resp.text();
    } catch (e) {
      if (ac.signal.aborted) {
        throw new OstroError(undefined, "timeout", `request to the Ostro server at ${this.server} timed out`);
      }
      const cause = e?.cause?.code ?? e?.cause?.message ?? e?.message;
      throw new OstroError(undefined, "unreachable", `cannot reach the Ostro server at ${this.server}: ${cause}`);
    } finally {
      clearTimeout(timer);
    }
    const status = resp.status;
    let json;
    let parsed = false;
    if (text.trim()) {
      try {
        json = JSON.parse(text);
        parsed = true;
      } catch {
        parsed = false;
      }
    }
    if (status >= 200 && status < 300) {
      if (text.trim() && !parsed) {
        throw new OstroError(status, `http_${status}`, `the server at ${this.server} answered HTTP ${status} with a non-JSON body`);
      }
      return json ?? null;
    }
    const err = parsed && json && typeof json.error === "object" && json.error !== null ? json.error : null;
    if (err) {
      const details = err.details && typeof err.details === "object" ? err.details : {};
      throw new OstroError(status, String(err.code || `http_${status}`), String(err.message ?? ""), details);
    }
    throw new OstroError(status, `http_${status}`, text.trim());
  }

  // --- modules and workflows ---------------------------------------------------------

  /** `PUT /v1/modules/{sha256}`: upload a Wasm component. */
  async uploadModule(bytes, { name, tenant } = {}) {
    const sha = sha256Hex(bytes);
    return this._request(["PUT", "/v1/modules/{sha256}"], ["v1", "modules", sha], {
      query: [["name", name], ["tenant", tenant]],
      raw: bytes,
      contentType: "application/wasm",
    });
  }

  /** `GET /v1/workflows`: every workflow with its versions. */
  async workflows() {
    const r = await this._request(["GET", "/v1/workflows"], ["v1", "workflows"]);
    return r.workflows;
  }

  /** `PUT /v1/workflows/{name}/active`. */
  async activate(workflow, version) {
    return this._request(["PUT", "/v1/workflows/{name}/active"], ["v1", "workflows", workflow, "active"], {
      body: { version },
    });
  }

  // --- runs ----------------------------------------------------------------------------

  /**
   * `POST /v1/workflows/{name}/runs`. `opts`: idempotencyKey, tags, conflictPolicy, version,
   * queue, plus `extra` (further body fields, snake_case as in the API).
   */
  async startRun(workflow, input = null, opts = {}) {
    const { idempotencyKey, tags, conflictPolicy, version, queue, extra } = opts;
    const body = { input: input ?? null };
    if (idempotencyKey != null) body.idempotency_key = idempotencyKey;
    if (conflictPolicy != null) body.conflict_policy = conflictPolicy;
    if (version != null) body.version = version;
    if (queue != null) body.queue = queue;
    if (tags && tags.length) body.tags = [...tags];
    if (extra) Object.assign(body, extra);
    return this._request(["POST", "/v1/workflows/{name}/runs"], ["v1", "workflows", workflow, "runs"], { body });
  }

  /** `GET /v1/runs/{run_id}`. */
  async getRun(runId) {
    return this._request(["GET", "/v1/runs/{run_id}"], ["v1", "runs", runId]);
  }

  /** `GET /v1/runs`: one page `{runs, next_page_token}`. */
  async listRuns({ workflow, status, queue, tags, limit, pageToken } = {}) {
    const query = [["workflow", workflow], ["status", status], ["queue", queue]];
    for (const t of tags ?? []) query.push(["tag", t]);
    query.push(["limit", limit], ["page_token", pageToken]);
    return this._request(["GET", "/v1/runs"], ["v1", "runs"], { query });
  }

  /** `GET /v1/runs/{run_id}/history`: one page `{events, next_from_seq}`. */
  async history(runId, { fromSeq, limit } = {}) {
    return this._request(["GET", "/v1/runs/{run_id}/history"], ["v1", "runs", runId, "history"], {
      query: [["from_seq", fromSeq], ["limit", limit]],
    });
  }

  /**
   * `GET /v1/runs/{run_id}/result`, long-polling up to `wait` ("30s" or ms; default no
   * wait). `completed` is false when the wait ended first.
   */
  async result(runId, { wait } = {}) {
    const total = wait === undefined ? 0 : parseDuration(wait);
    const deadline = Date.now() + total;
    let left = total;
    for (;;) {
      const poll = Math.min(left, MAX_POLL_MS);
      const r = await this._request(["GET", "/v1/runs/{run_id}/result"], ["v1", "runs", runId, "result"], {
        query: [["wait", `${Math.round(poll)}ms`]],
        extraMs: poll,
      });
      if (r.completed || left <= 0) return r;
      left = Math.max(0, deadline - Date.now());
    }
  }

  // --- lifecycle -----------------------------------------------------------------------

  /** `POST /v1/runs/{run_id}/signals/{name}`. */
  async signal(runId, name, payload = null, { requestId } = {}) {
    const body = { payload: payload ?? null };
    if (requestId != null) body.request_id = requestId;
    return this._request(["POST", "/v1/runs/{run_id}/signals/{name}"], ["v1", "runs", runId, "signals", name], {
      body,
    });
  }

  /** `POST /v1/runs/{run_id}/cancel`. */
  async cancel(runId, reason = null) {
    return this._request(["POST", "/v1/runs/{run_id}/cancel"], ["v1", "runs", runId, "cancel"], {
      body: { reason: reason ?? null },
    });
  }

  /** `POST /v1/runs/{run_id}/terminate`. */
  async terminate(runId, reason = null) {
    return this._request(["POST", "/v1/runs/{run_id}/terminate"], ["v1", "runs", runId, "terminate"], {
      body: { reason: reason ?? null },
    });
  }

  /** `POST /v1/runs/{run_id}/retry`. */
  async retry(runId) {
    return this._request(["POST", "/v1/runs/{run_id}/retry"], ["v1", "runs", runId, "retry"], { body: {} });
  }

  /** `POST /v1/tokens/complete`. */
  async completeToken(token, payload = null) {
    return this._request(["POST", "/v1/tokens/complete"], ["v1", "tokens", "complete"], {
      body: { token, payload: payload ?? null },
    });
  }

  // --- dev clock and replay ------------------------------------------------------------

  /** `GET /v1/dev/time` (409 time_skipping_disabled unless --time-skipping). */
  async devTime() {
    return this._request(["GET", "/v1/dev/time"], ["v1", "dev", "time"]);
  }

  /** `POST /v1/dev/time/skip` with exactly one of `by` ("24h" or ms) / `toMs`. */
  async devSkip({ by, toMs } = {}) {
    if ((by === undefined) === (toMs === undefined)) {
      throw new TypeError("devSkip needs exactly one of by / toMs");
    }
    const body = by !== undefined ? { by: typeof by === "string" ? by : `${Math.round(parseDuration(by))}ms` } : { to_ms: toMs };
    return this._request(["POST", "/v1/dev/time/skip"], ["v1", "dev", "time", "skip"], { body });
  }

  /** `POST /v1/modules/{sha}/replay` with `runs` (ids) or `sample` (n). */
  async replay(sha, { runs, sample, workflow, status, allowMissing = false } = {}) {
    if ((runs === undefined) === (sample === undefined)) {
      throw new TypeError("replay needs exactly one of runs / sample");
    }
    const body = runs !== undefined ? { runs: [...runs] } : { sample };
    if (sample !== undefined) {
      if (workflow != null) body.workflow = workflow;
      if (status != null) body.status = status;
    }
    if (allowMissing) body.allow_missing = true;
    return this._request(["POST", "/v1/modules/{sha}/replay"], ["v1", "modules", sha, "replay"], {
      body,
      extraMs: REPLAY_EXTRA_MS,
    });
  }
}
