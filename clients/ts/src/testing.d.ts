// Hand-written types for `ostro-client/testing` (the spawned-server test kit, SDK-06).

import type {
  Client,
  DevSkip,
  Duration,
  HistoryEvent,
  Json,
  RunResult,
  StartRunOptions,
  UploadOutcome,
} from "./index.js";

/** The frozen ready line regex source (D-P10-9, amended); the same in every kit. */
export declare const READY_LINE_PATTERN: string;
export declare const READY_LINE: RegExp;
/** Run retention given to the spawned server (`87600h`). */
export declare const RETENTION: string;

/** A missing/old binary, a server that did not start, a failed deploy or run. */
export declare class TestEnvError extends Error {
  constructor(message: string);
}

/** `binary`, else `$OSTRO_BIN`, else `ostro` on PATH. */
export declare function ostroBin(binary?: string): string;
/** `ostro build-info --json` -> its version; throws when older than the kit. */
export declare function checkVersion(binary: string): string;

export interface TestEnvOptions {
  /** The ostro binary (default `$OSTRO_BIN`, else `ostro` on PATH). */
  binary?: string;
  /** Let effects reach 127.0.0.0/8 (`[egress] allow_private`), for loopback HTTP mocks. */
  egressLoopback?: boolean;
  /** Extra server environment variables. */
  env?: Record<string, string>;
  /** `auto` (default) or `manual` (skips only through `skip`). */
  timeSkipping?: "auto" | "manual";
  /** Per-request client timeout in ms (default 30000; result waits come on top). */
  timeoutMs?: number;
}

/** A started run; `acceptedAtMs` is server (dev clock) time. */
export interface Run {
  runId: string;
  workflow: string;
  acceptedAtMs: number;
}

export type RunRef = Run | string;

/** `RunResult` plus `output` (the same value as the wire field `result`). */
export interface KitResult extends RunResult {
  output: Json;
}

export interface Timer {
  seq: number;
  startedMs: number;
  fireAtMs: number;
  fired: boolean;
  firedSeq: number | null;
  firedMs: number | null;
}

/** A throwaway `ostro dev --ephemeral --time-skipping` server. */
export declare class TestEnv {
  /** Spawn the server and wait (30 s) for its ready line. */
  static start(opts?: TestEnvOptions): Promise<TestEnv>;
  readonly pid: number;
  readonly binary: string;
  readonly addr: string;
  readonly url: string;
  readonly dataDir: string;
  readonly ephemeral: boolean;
  readonly readyMs: number;
  /** The underlying REST client. */
  readonly client: Client;
  /** The tail of the server's stderr. */
  readonly stderr: string;
  /** SIGTERM, then SIGKILL after 2 s; resolves once the server exited. Idempotent. */
  close(): Promise<void>;
  /** `ostro deploy <dir> --activate --json`: the `ostro.deploy.result` JSON. */
  buildAndDeploy(projectDir?: string): Promise<Record<string, any>>;
  deployWasm(path: string, opts?: { name?: string }): Promise<UploadOutcome>;
  deployWasmBytes(bytes: Uint8Array, name: string): Promise<UploadOutcome>;
  startRun(workflow: string, input?: Json, opts?: StartRunOptions): Promise<Run>;
  signal(run: RunRef, name: string, payload?: Json): Promise<Record<string, any>>;
  result(run: RunRef, opts?: { wait?: Duration }): Promise<KitResult>;
  skip(by: Duration): Promise<DevSkip>;
  nowMs(): Promise<number>;
  history(run: RunRef): Promise<HistoryEvent[]>;
  steps(run: RunRef): Promise<string[]>;
  timers(run: RunRef): Promise<Timer[]>;
  assertCompleted(run: RunRef, output?: Json, opts?: { wait?: Duration }): Promise<Json>;
  allowEgress(moduleOrWorkflow: string, urlPrefix: string): Promise<Record<string, any>>;
}
