// Entry point for `node --test test/` (and `node --test sdks/ts/test/` from the repo root).
//
// Node 24 treats a directory argument to `--test` as a single test file and loads it with
// directory resolution, which lands here. Import every `*.test.ts` in this directory, in
// name order, so all suites run under the one command the plans and CI use.
import { readdirSync } from "node:fs";

const dir = new URL("./", import.meta.url);
const files = readdirSync(dir).filter((f) => f.endsWith(".test.ts")).sort();
if (files.length === 0) throw new Error(`no *.test.ts files in ${dir.pathname}`);
for (const f of files) await import(new URL(f, dir).href);
