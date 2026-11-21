// Entry point for `node --test test/` (and `node --test sdks/ts/test/` from the repo root).
//
// Node 24 treats a directory argument to `--test` as a single test file and loads it with
// directory resolution, which lands here. Import every `*.test.ts` in this directory, in
// name order, so all suites run under the one command the plans and CI use.
//
// `src/bindings.ts` imports the guest-only jco specifiers `ostro:runtime/<iface>@0.1.0`,
// which Node cannot load. A resolve hook maps each to a stub module whose functions (one
// per WIT import, read from the frozen WIT) throw: host tests always install a FakeHost.
import { readFileSync, readdirSync } from "node:fs";
import { registerHooks } from "node:module";

const dir = new URL("./", import.meta.url);
const wit = readFileSync(new URL("../../../wit/ostro-runtime-0.1.0/runtime.wit", import.meta.url), "utf8");

function stubSource(iface) {
  const body = new RegExp(`^interface ${iface} \\{([\\s\\S]*?)^\\}`, "m").exec(wit);
  if (!body) throw new Error(`stub: interface ${iface} not found in the WIT`);
  const lines = [];
  for (const m of body[1].matchAll(/^\s+([a-z][a-z0-9-]*): func\(/gm)) {
    const name = m[1].replace(/-([a-z0-9])/g, (_, c) => c.toUpperCase());
    lines.push(
      `export function ${name}() { throw new Error("ostro:runtime/${iface}.${m[1]} is guest-only; install a host with setHost()"); }`,
    );
  }
  return lines.join("\n");
}

registerHooks({
  resolve(specifier, context, nextResolve) {
    const m = /^ostro:runtime\/([a-z]+)@0\.1\.0$/.exec(specifier);
    if (m) return { url: `data:text/javascript,${encodeURIComponent(stubSource(m[1]))}`, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});

const files = readdirSync(dir).filter((f) => f.endsWith(".test.ts")).sort();
if (files.length === 0) throw new Error(`no *.test.ts files in ${dir.pathname}`);
for (const f of files) await import(new URL(f, dir).href);
