// Hand-written types for ostro-client (SDK-07). Response shapes follow the server's
// OpenAPI document (`ostro openapi`); unknown fields are kept (index signatures).

/** Package version. */
export declare const VERSION: string;

/** The default server address (`ostro dev` listens here). */
export declare const DEFAULT_SERVER: string;

/** Every REST route the client calls, as [METHOD, path template]. */
export declare const ROUTES: ReadonlyArray<readonly [string, string]>;

/** A duration: `"30s"`, `"500ms"`, `"2m"`, `"72h"`, `"1d"`, or a number of milliseconds. */
export type Duration = string | number;

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

/** `"30s"` / `"72h"` / ms -> milliseconds. */
export declare function parseDuration(d: Duration): number;

/** Lowercase hex SHA-256 (a module's id). */
export declare function sha256Hex(bytes: Uint8Array): string;

/** The one error type of the client. */
export declare class OstroError extends Error {
  constructor(status: number | undefined, code: string, message: string, details?: Record<string, unknown>);
  /** HTTP status; undefined when no response arrived (unreachable, timeout). */
  readonly status: number | undefined;
  /** Error code (`not_found`, `run_terminal`, ..., `unreachable`, `timeout`, `http_<status>`). */
  readonly code: string;
  readonly details: Record<string, unknown>;
  /** `details.reason` when present (e.g. `time_skipping_disabled`). */
  readonly reason: string | undefined;
}

export interface ClientOptions {
  /** Base URL; default `process.env.OSTRO_SERVER`, else `http://127.0.0.1:7700`. */
  server?: string;
  /** Per-request timeout (a result long-poll gets its wait on top). Default 30000. */
  timeoutMs?: number;
}

export interface WorkflowVersion {
  name: string;
  version: number;
  [key: string]: unknown;
}

export interface ModuleInfo {
  sha256: string;
  size_bytes?: number;
  workflows: WorkflowVersion[];
  [key: string]: unknown;
}

export interface UploadOutcome {
  created: boolean;
  module: ModuleInfo;
  [key: string]: unknown;
}

export interface WorkflowInfo {
  name: string;
  active_version?: number | null;
  versions?: unknown[];
  record_version?: number;
  [key: string]: unknown;
}

export interface StartRunOptions {
  idempotencyKey?: string;
  tags?: string[];
  conflictPolicy?: string;
  version?: number;
  queue?: string;
  /** Further body fields of the API, snake_case (`retention`, `priority`, `delay_ms`, ...). */
  extra?: Record<string, Json>;
}

export interface StartedRun {
  run_id: string;
  created: boolean;
  dry_run?: boolean;
  [key: string]: unknown;
}

export interface RunInfo {
  run_id: string;
  workflow: string;
  version: number;
  module_sha256?: string;
  queue?: string;
  status: string;
  tags?: string[];
  result?: Json;
  failure?: Json;
  created_at_ms?: number | null;
  completed_at_ms?: number | null;
  [key: string]: unknown;
}

export interface ListRunsOptions {
  workflow?: string;
  status?: string;
  queue?: string;
  tags?: string[];
  limit?: number;
  pageToken?: string;
}

export interface RunList {
  runs: RunInfo[];
  next_page_token?: string | null;
  [key: string]: unknown;
}

export interface HistoryEvent {
  seq: number;
  kind: string;
  [key: string]: unknown;
}

export interface HistoryPage {
  events: HistoryEvent[];
  next_from_seq?: number | null;
  [key: string]: unknown;
}

export interface RunResult {
  run_id: string;
  completed: boolean;
  status?: string | null;
  result?: Json;
  failure?: Json;
  [key: string]: unknown;
}

export interface SignalOutcome {
  accepted: boolean;
  duplicate?: boolean;
  dry_run?: boolean;
  [key: string]: unknown;
}

export interface CancelOutcome {
  already_requested: boolean;
  [key: string]: unknown;
}

export interface TerminateOutcome {
  status: string;
  [key: string]: unknown;
}

export interface RetryOutcome {
  mode: string;
  run_id: string;
  [key: string]: unknown;
}

export interface TokenOutcome {
  run_id: string;
  accepted: boolean;
  [key: string]: unknown;
}

export interface DevTime {
  now_ms: number;
  offset_ms: number;
  mode: string;
  idle: boolean;
  next_due_ms?: number | null;
  [key: string]: unknown;
}

export interface DevSkip {
  now_ms: number;
  offset_ms: number;
  skipped_ms: number;
  [key: string]: unknown;
}

export interface ReplayOptions {
  runs?: string[];
  sample?: number;
  workflow?: string;
  status?: string;
  allowMissing?: boolean;
}

export interface ReplayVerdict {
  run_id: string;
  workflow?: string;
  verdict: string;
  seq?: number | null;
  detail?: string | null;
  [key: string]: unknown;
}

export interface ReplayReport {
  module_sha256: string;
  verdicts: ReplayVerdict[];
  summary: { ok: number; ok_frontier: number; diverged: number; skipped: number; error: number };
  [key: string]: unknown;
}

export declare class Client {
  constructor(opts?: ClientOptions);
  readonly server: string;
  readonly timeoutMs: number;

  uploadModule(bytes: Uint8Array, opts?: { name?: string; tenant?: string }): Promise<UploadOutcome>;
  workflows(): Promise<WorkflowInfo[]>;
  activate(workflow: string, version: number): Promise<WorkflowInfo>;

  startRun(workflow: string, input?: Json, opts?: StartRunOptions): Promise<StartedRun>;
  getRun(runId: string): Promise<RunInfo>;
  listRuns(opts?: ListRunsOptions): Promise<RunList>;
  history(runId: string, opts?: { fromSeq?: number; limit?: number }): Promise<HistoryPage>;
  result(runId: string, opts?: { wait?: Duration }): Promise<RunResult>;

  signal(runId: string, name: string, payload?: Json, opts?: { requestId?: string }): Promise<SignalOutcome>;
  cancel(runId: string, reason?: string | null): Promise<CancelOutcome>;
  terminate(runId: string, reason?: string | null): Promise<TerminateOutcome>;
  retry(runId: string): Promise<RetryOutcome>;
  completeToken(token: string, payload?: Json): Promise<TokenOutcome>;

  devTime(): Promise<DevTime>;
  devSkip(opts: { by: Duration } | { toMs: number }): Promise<DevSkip>;
  replay(sha: string, opts: ReplayOptions): Promise<ReplayReport>;
}
