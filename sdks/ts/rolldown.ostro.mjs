// Ostro bundle config, passed to `jco componentize --bundle-config` (merged into jco's
// Rolldown config). `target: "es2016"` lowers every async function/await, in user code and
// bundled node_modules alike, to generators driven by `new Promise(..)`, so the SDK's
// promise (installed as globalThis.Promise) and its synchronously drained job queue run
// them inside a sync `run` export (09-RESEARCH Pattern 2).
export default { transform: { target: "es2016" } };
