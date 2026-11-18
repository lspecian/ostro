#!/usr/bin/env python3
"""Attribute differing bytes between two wasm components to sections.

    wasm_sections.py a.wasm b.wasm

Walks the component binary (nested core modules and components) and, for the
first file, maps each offset reported by a byte-wise compare to the innermost
section containing it. Prints the first divergent offset and a count of
differing bytes per section path. Same-size inputs compare byte-for-byte;
different sizes compare section-by-section in file order (path + index).
No external tools (no wasm-tools).
"""
import sys

COMP = {0: "custom", 1: "core-module", 2: "core-instance", 3: "core-type", 4: "component",
        5: "instance", 6: "alias", 7: "type", 8: "canon", 9: "start", 10: "import", 11: "export"}
MOD = {0: "custom", 1: "type", 2: "import", 3: "function", 4: "table", 5: "memory", 6: "global",
       7: "export", 8: "start", 9: "element", 10: "code", 11: "data", 12: "datacount", 13: "tag"}


def leb(b, i):
    r = s = 0
    while True:
        x = b[i]; i += 1
        r |= (x & 0x7F) << s; s += 7
        if x < 0x80:
            return r, i


def name_of(b, i):
    n, j = leb(b, i)
    return b[j:j + n].decode("utf-8", "replace")


def walk(b, start, end, kind, path, out):
    i = start + 8  # magic + version/layer
    idx = 0
    while i < end:
        sid = b[i]; size, body = leb(b, i + 1)
        table = COMP if kind == "c" else MOD
        label = table.get(sid, f"id{sid}")
        if sid == 0:
            label += ":" + name_of(b, body)
        p = f"{path}/{idx}:{label}"
        if kind == "c" and sid in (1, 4):
            walk(b, body, body + size, "m" if sid == 1 else "c", p, out)
        else:
            out.append((i, body + size, p))
        i = body + size; idx += 1


def sections(path):
    b = open(path, "rb").read()
    out = []
    walk(b, 0, len(b), "c", "", out)
    return b, out


def main():
    a, sa = sections(sys.argv[1])
    b, sb = sections(sys.argv[2])
    diffs = {}
    first = None
    if len(sa) != len(sb):
        print(f"section count differs: {len(sa)} vs {len(sb)}")
    for (s1, e1, p1), (s2, e2, p2) in zip(sa, sb):
        x, y = a[s1:e1], b[s2:e2]
        if x == y:
            continue
        n = sum(1 for u, v in zip(x, y) if u != v) + abs(len(x) - len(y))
        diffs[p1] = (n, e1 - s1, e2 - s2)
        if first is None:
            k = next((k for k, (u, v) in enumerate(zip(x, y)) if u != v), min(len(x), len(y)))
            first = (s1 + k, p1)
    if first is None:
        print("identical section contents")
        return
    print(f"first divergence at offset {first[0]} in {first[1]}")
    for p, (n, l1, l2) in diffs.items():
        print(f"  {p}: {n} differing bytes (len {l1} vs {l2})")


if __name__ == "__main__":
    main()
