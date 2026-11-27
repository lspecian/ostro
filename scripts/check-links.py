#!/usr/bin/env python3
"""Offline documentation checker (Phase 14, D-P14-27). Standard library only; run with
`python3 -I`. No network: external links (any URL scheme) are never fetched.

    python3 -I scripts/check-links.py [--markers] [--quiet] <file.md | dir> ...

Default mode checks, for every Markdown file under the given paths (fenced blocks and
inline code are skipped when looking for links):

  * relative links `[t](path)`, images `![t](path)` and reference definitions
    `[r]: path` resolve to an existing file or directory (relative to the linking file;
    `/path` is relative to the repository root);
  * `#anchor` parts resolve to a heading (GitHub slug rules: lowercase, punctuation
    stripped, spaces to `-`, duplicates `-1`, `-2`, ...) or an explicit
    `<a id=...>` / `<a name=...>` in the target Markdown file;
  * `<!-- ostro:snippet path=P lines=A-B -->` markers: the next fenced block's content
    equals lines A..B of P (repository-relative) byte for byte.

`--markers` checks the marker rule instead: every fenced block whose info string is
rust, python, typescript, ts or toml is directly preceded (blank lines allowed) by an
`ostro:snippet` or `ostro:illustrative` marker; `ostro:illustrative` is accepted only
under docs/migrations/ or when the marker line also says `pseudo`.

Every failure is printed as `file:line: message`; the exit status is 1 on any failure.
"""

import os
import re
import sys
import urllib.parse

ROOT = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
MARKED_LANGS = {"rust", "python", "typescript", "ts", "toml"}
FENCE_RE = re.compile(r"^( {0,3})(`{3,}|~{3,})(.*)$")
SNIPPET_RE = re.compile(r"<!--\s*ostro:snippet\s+path=(\S+)\s+lines=(\d+)-(\d+)\s*-->")
ILLUSTRATIVE_RE = re.compile(r"<!--\s*ostro:illustrative\b[^>]*-->")
ANY_MARKER_RE = re.compile(r"<!--\s*ostro:(snippet|illustrative)\b")
INLINE_LINK_RE = re.compile(r"!?\[(?:[^\[\]]|\[[^\[\]]*\])*\]\(\s*(<[^>]*>|[^)\s]+)(?:\s+(?:\"[^\"]*\"|'[^']*'|\([^)]*\)))?\s*\)")
REF_DEF_RE = re.compile(r"^ {0,3}\[(?!\^)[^\]]+\]:\s*(<[^>]*>|\S+)")
CODE_SPAN_RE = re.compile(r"(`+)(.+?)\1")
HEADING_RE = re.compile(r"^ {0,3}(#{1,6})\s+(.*?)\s*#*\s*$")
EXPLICIT_ANCHOR_RE = re.compile(r"<a\s+(?:[^>]*\s)?(?:id|name)\s*=\s*[\"']([^\"']+)[\"']", re.I)
SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


class Doc:
    """A parsed Markdown file: lines, fenced blocks and prose lines."""

    def __init__(self, path):
        self.path = path
        with open(path, "rb") as f:
            raw = f.read()
        self.text = raw.decode("utf-8")
        self.lines = self.text.split("\n")
        # blocks: (open_line_idx, close_line_idx, info, content_lines)
        self.blocks = []
        self.in_block = [False] * len(self.lines)
        i = 0
        n = len(self.lines)
        while i < n:
            m = FENCE_RE.match(self.lines[i])
            if not m:
                i += 1
                continue
            fence = m.group(2)
            info = m.group(3).strip()
            if fence[0] == "`" and "`" in info:
                i += 1
                continue
            j = i + 1
            while j < n:
                c = FENCE_RE.match(self.lines[j])
                if c and c.group(2)[0] == fence[0] and len(c.group(2)) >= len(fence) and not c.group(3).strip():
                    break
                j += 1
            for k in range(i, min(j, n - 1) + 1):
                self.in_block[k] = True
            self.blocks.append((i, j, info, self.lines[i + 1:j]))
            i = j + 1
        self._anchors = None

    def anchors(self):
        if self._anchors is None:
            seen = {}
            out = set()
            for idx, line in enumerate(self.lines):
                if self.in_block[idx]:
                    continue
                for m in EXPLICIT_ANCHOR_RE.finditer(line):
                    out.add(m.group(1))
                h = HEADING_RE.match(line)
                if not h:
                    continue
                base = slug(h.group(2))
                if base in seen:
                    seen[base] += 1
                    out.add(f"{base}-{seen[base]}")
                else:
                    seen[base] = 0
                    out.add(base)
            self._anchors = out
        return self._anchors


def strip_emphasis(s):
    s = re.sub(r"\*+", "", s)
    s = re.sub(r"(?<![0-9A-Za-z])_+|_+(?![0-9A-Za-z])", "", s)
    return s


def slug(heading):
    """GitHub's heading id for the rendered text of `heading`."""
    out = []
    pos = 0
    for m in CODE_SPAN_RE.finditer(heading):
        out.append(render_inline(heading[pos:m.start()]))
        out.append(m.group(2).strip())
        pos = m.end()
    out.append(render_inline(heading[pos:]))
    text = "".join(out).strip().lower()
    text = "".join(ch for ch in text if ch.isalnum() or ch in "-_ ")
    return text.replace(" ", "-")


def render_inline(s):
    s = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", s)  # links -> text
    s = re.sub(r"<[^>]+>", "", s)  # html tags
    return strip_emphasis(s)


_docs = {}


def doc(path):
    real = os.path.realpath(path)
    if real not in _docs:
        _docs[real] = Doc(real)
    return _docs[real]


def rel(path):
    return os.path.relpath(os.path.realpath(path), ROOT)


def md_files(args):
    seen = []
    for a in args:
        p = a if os.path.isabs(a) else os.path.join(ROOT, a)
        if os.path.isdir(p):
            for dp, dns, fns in os.walk(p):
                dns.sort()
                for fn in sorted(fns):
                    if fn.endswith(".md"):
                        seen.append(os.path.join(dp, fn))
        elif os.path.isfile(p):
            seen.append(p)
        else:
            print(f"{a}: no such file or directory", file=sys.stderr)
            sys.exit(2)
    out = []
    for p in seen:
        r = os.path.realpath(p)
        if r not in out:
            out.append(r)
    return out


def prose_without_code(line):
    return CODE_SPAN_RE.sub("", line)


def marker_before(d, open_idx):
    """The marker line (index, text) directly before the block, blank lines allowed."""
    k = open_idx - 1
    while k >= 0 and not d.lines[k].strip():
        k -= 1
    if k >= 0 and ANY_MARKER_RE.search(d.lines[k]):
        return k, d.lines[k]
    return None, None


def check_markers(files):
    errors = []
    checked = 0
    for path in files:
        d = doc(path)
        r = rel(path)
        in_migrations = r.replace(os.sep, "/").startswith("docs/migrations/")
        for (o, _c, info, _content) in d.blocks:
            lang = info.split()[0].lower() if info else ""
            if lang not in MARKED_LANGS:
                continue
            checked += 1
            k, line = marker_before(d, o)
            if line is None:
                errors.append(f"{r}:{o + 1}: ```{lang} block without an ostro:snippet or ostro:illustrative marker")
                continue
            if SNIPPET_RE.search(line):
                continue
            if ILLUSTRATIVE_RE.search(line):
                if not in_migrations and "pseudo" not in line:
                    errors.append(f"{r}:{k + 1}: ostro:illustrative outside docs/migrations/ must say `pseudo`")
                continue
            errors.append(f"{r}:{k + 1}: malformed ostro marker: {line.strip()}")
    return errors, checked


def check_snippets(d, r, errors):
    verified = 0
    block_at = {}
    for b in d.blocks:
        block_at[b[0]] = b
    for idx, line in enumerate(d.lines):
        if d.in_block[idx]:
            continue
        if not ANY_MARKER_RE.search(line):
            continue
        m = SNIPPET_RE.search(line)
        if ILLUSTRATIVE_RE.search(line):
            nxt = idx + 1
            while nxt < len(d.lines) and not d.lines[nxt].strip():
                nxt += 1
            if nxt not in block_at:
                errors.append(f"{r}:{idx + 1}: ostro:illustrative marker not followed by a fenced block")
            continue
        if not m:
            errors.append(f"{r}:{idx + 1}: malformed ostro marker: {line.strip()}")
            continue
        nxt = idx + 1
        while nxt < len(d.lines) and not d.lines[nxt].strip():
            nxt += 1
        if nxt not in block_at:
            errors.append(f"{r}:{idx + 1}: ostro:snippet marker not followed by a fenced block")
            continue
        spath, a, b = m.group(1), int(m.group(2)), int(m.group(3))
        src = os.path.join(ROOT, spath)
        if os.path.isabs(spath) or not os.path.realpath(src).startswith(ROOT + os.sep):
            errors.append(f"{r}:{idx + 1}: snippet path must be repository-relative: {spath}")
            continue
        if not os.path.isfile(src):
            errors.append(f"{r}:{idx + 1}: snippet source {spath} does not exist")
            continue
        with open(src, "rb") as f:
            src_lines = f.read().decode("utf-8").split("\n")
        if src_lines and src_lines[-1] == "":
            src_lines = src_lines[:-1]
        if a < 1 or b < a or b > len(src_lines):
            errors.append(f"{r}:{idx + 1}: lines={a}-{b} out of range for {spath} ({len(src_lines)} lines)")
            continue
        want = src_lines[a - 1:b]
        got = block_at[nxt][3]
        if got != want:
            diff_at = next((i for i in range(max(len(got), len(want)))
                            if i >= len(got) or i >= len(want) or got[i] != want[i]), 0)
            errors.append(
                f"{r}:{nxt + 2 + diff_at}: snippet differs from {spath}:{a + diff_at} "
                f"(block has {len(got)} lines, source range {len(want)})")
            continue
        verified += 1
    return verified


def check_links(files):
    errors = []
    links = 0
    snippets = 0
    for path in files:
        d = doc(path)
        r = rel(path)
        snippets += check_snippets(d, r, errors)
        base_dir = os.path.dirname(path)
        for idx, line in enumerate(d.lines):
            if d.in_block[idx]:
                continue
            prose = prose_without_code(line)
            targets = [m.group(1) for m in INLINE_LINK_RE.finditer(prose)]
            rd = REF_DEF_RE.match(prose)
            if rd:
                targets.append(rd.group(1))
            for t in targets:
                if t.startswith("<") and t.endswith(">"):
                    t = t[1:-1]
                if not t or SCHEME_RE.match(t) or t.startswith("//"):
                    continue
                links += 1
                target, _, frag = t.partition("#")
                target = urllib.parse.unquote(target)
                frag = urllib.parse.unquote(frag)
                if target:
                    if target.startswith("/"):
                        tp = os.path.join(ROOT, target.lstrip("/"))
                    else:
                        tp = os.path.join(base_dir, target)
                    tp = os.path.normpath(tp)
                    if not os.path.exists(tp):
                        errors.append(f"{r}:{idx + 1}: broken link {t} ({rel(tp)} does not exist)")
                        continue
                else:
                    tp = path
                if frag and tp.endswith(".md") and os.path.isfile(tp):
                    if frag not in doc(tp).anchors():
                        errors.append(f"{r}:{idx + 1}: broken anchor {t} (no heading #{frag} in {rel(tp)})")
    return errors, links, snippets


def main(argv):
    markers = False
    quiet = False
    paths = []
    for a in argv:
        if a == "--markers":
            markers = True
        elif a == "--quiet":
            quiet = True
        elif a in ("-h", "--help"):
            print(__doc__)
            return 0
        else:
            paths.append(a)
    if not paths:
        paths = ["README.md", "docs"]
    files = md_files(paths)
    if markers:
        errors, checked = check_markers(files)
        for e in errors:
            print(e)
        if not quiet:
            print(f"markers: {len(files)} files, {checked} rust/python/typescript/toml blocks checked, {len(errors)} failures")
    else:
        errors, links, snippets = check_links(files)
        for e in errors:
            print(e)
        if not quiet:
            print(f"links: {len(files)} files, {links} relative links checked, {snippets} snippets verified, {len(errors)} failures")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
