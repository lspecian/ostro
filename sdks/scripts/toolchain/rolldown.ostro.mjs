// Ostro bundle config, passed to `jco componentize --bundle-config` (merged into jco's
// Rolldown config). `target: "es2016"` lowers every async function/await, in user code and
// bundled node_modules alike, to generators driven by `new Promise(..)`, so the SDK's
// promise (installed as globalThis.Promise) and its synchronously drained job queue run
// them inside a sync `run` export (09-RESEARCH Pattern 2).
//
// `inject` replaces jco's own shims for these free globals (bound at module evaluation,
// before the SDK installs its globals; on QuickJS `console` adds a `jco:node/console` WIT
// import) with the SDK's deterministic versions (09-10). A bundle that never references
// them gets no import of "ostro".
//
// `external` marks the guest-only `ostro:runtime/*` WIT imports (src/bindings.ts) as
// external up front; jco wires them to the component's imports either way, this only
// silences Rolldown's UNRESOLVED_IMPORT warnings (09-17).
export default {
  external: [/^ostro:/],
  transform: {
    target: "es2016",
    inject: {
      console: ["ostro", "__ostroConsole"],
      setTimeout: ["ostro", "__ostroSetTimeout"],
      clearTimeout: ["ostro", "__ostroClearTimeout"],
      setInterval: ["ostro", "__ostroSetInterval"],
      clearInterval: ["ostro", "__ostroClearInterval"],
      queueMicrotask: ["ostro", "__ostroQueueMicrotask"],
    },
  },
};
