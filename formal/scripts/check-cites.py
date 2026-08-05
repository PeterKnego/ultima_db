#!/usr/bin/env python3
"""Verify that the src/*.rs:LINE cites in formal/tla/wal/ still point at what
they claim.

formal/tla/wal/ is not generated from the Rust. Its fidelity rests on prose
claims that carry explicit `src/*.rs:LINE` cites, and those cites rot on any
edit that moves a line -- including a pure perf refactor with no semantic
change at all. check-drift.sh bounds the damage (a watched src/*.rs may not
change without *something* under formal/ changing), but its predicate has no
relation to which cites were actually re-checked: c8c1f0a broke all 99
src/wal.rs cites and satisfied the drift guard in the same commit, because it
also touched one unrelated line in RESULTS.md.

This checks the cites themselves. Every distinct anchor carries an expectation
token in a sidecar manifest (formal/tla/wal/cite-anchors.tsv); the anchor is
good when that token appears EXACTLY ONCE inside the cited line range.

The manifest is a sidecar rather than an inline `{token}` annotation because
WalCrash.tla's comment blocks are column-boxed to a fixed right margin and the
.cfg headers are line-length constrained -- inline tokens would wreck both.
There are ~193 cite occurrences but only 65 distinct anchors, so the sidecar is
also the smaller thing to maintain.

Three failure modes, all fatal, and the last two are what keep the manifest and
the prose from drifting apart:

  * an anchor whose token is missing from the cited range, or ambiguous in it
    (an `sync_all` that matches three times proves nothing);
  * a cite with no manifest row -- so a NEW cite must carry its expectation;
  * a manifest row no cite references -- so a DELETED cite cannot leave a
    rotting row behind.

Two more guard the checker's own preconditions rather than a single anchor:

  * a BARE cite whose range is also an anchor under a different src_file. Such
    a cite is correct only by document position: a later prose edit that adds
    an earlier prefix for the other file re-targets it to a different, equally
    valid anchor, and every existing check stays green. Prefixed form is the
    only form inheritance cannot mis-attribute, so bare form is refused there.
  * a manifest src_file the formal.yml push filter does not watch. The cite
    guard runs in a job that a direct-to-main push only reaches when the push
    touched a listed path; a cite into a fourth source file would otherwise
    lose push-time coverage silently.

Usage:
  formal/scripts/check-cites.py [-v]
    -v / --verbose   also list every anchor and where it is cited
"""

import bisect
import fnmatch
import os
import re
import subprocess
import sys

CORPUS_DIR = "formal/tla/wal"
MANIFEST = "formal/tla/wal/cite-anchors.tsv"
WORKFLOW = ".github/workflows/formal.yml"

# ---------------------------------------------------------------------------
# The scanner.
#
# Cites appear in three shapes, and a per-token regex finds only the first:
#
#   prefixed   `src/store.rs:1073-1078`, (src/wal.rs:653-702), src/wal.rs:637
#   frozen     1e5d2b7^ src/wal.rs:1130-1136   (a deliberately historical cite)
#   bare       (:4193, `:681-683`  -- colon form, and the comma form
#              (src/wal.rs:1130-1136, 619-639) where the second range inherits
#              the first's file
#
# The bare forms are ~20% of the corpus. They carry no file of their own, so
# they inherit the nearest PRECEDING prefix cite in the same document -- which
# is how a human reads them too. That inheritance also carries the revision:
# `e60f8ce^ src/store.rs:2361-2366, inside commit_multi_writer (:2253)` means
# :2253 at e60f8ce^, not :2253 today.
#
# The prefix regex deliberately accepts non-src files (`WalCrash.tla:45`).
# Those are not cites we check, but they MUST reset the inherited context, or
# the `:213` in "(`WalCrash.tla:45`, `:213`)" would silently inherit whatever
# src/*.rs was named further up and invent an anchor out of nothing.
# ---------------------------------------------------------------------------

PREFIX_RE = re.compile(
    r"""
    (?:(?P<rev>[0-9a-fA-F]{7,40}\^?)[ \t]+)?          # optional frozen revision
    (?P<path>[A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:rs|tla|cfg|md))
    :(?P<range>\d+(?:-\d+)?)
    """,
    re.VERBOSE,
)
# A bare colon-form cite: ':1234' / ':681-683'. The lookbehind keeps it off
# word characters and dots, so `wal.rs:653` (already matched as a prefix) and
# `https://host` do not re-enter here.
BARE_RE = re.compile(r"(?<![\w.]):(?P<range>\d+(?:-\d+)?)")
# A comma continuation, chained directly onto the cite before it and on the
# same line: ", 619-639". The trailing guard keeps prose like
# "(src/wal.rs:641, 3 states later)" from being read as a cite.
COMMA_RE = re.compile(r"[ \t]*,[ \t]*(?P<range>\d+(?:-\d+)?)(?![ \t]*[A-Za-z0-9])")


class Cite:
    __slots__ = ("file", "line", "form", "rev", "path", "range", "text")

    def __init__(self, file, line, form, rev, path, rng, text):
        self.file = file
        self.line = line
        self.form = form
        self.rev = rev
        self.path = path
        self.range = rng
        self.text = text

    @property
    def is_src(self):
        return bool(self.path) and self.path.startswith("src/") and self.path.endswith(".rs")

    @property
    def anchor(self):
        return (self.rev or "-", self.path, self.range)

    def where(self):
        return f"{self.file}:{self.line}"


def scan_text(relpath, text):
    """Yield every cite in one corpus file, in document order."""
    starts = [0]
    for i, ch in enumerate(text):
        if ch == "\n":
            starts.append(i + 1)

    def lineno(pos):
        return bisect.bisect_right(starts, pos)

    out = []
    ctx = None  # (rev, path) inherited by the bare forms
    pos, n = 0, len(text)
    while pos < n:
        mp = PREFIX_RE.search(text, pos)
        mb = BARE_RE.search(text, pos)
        cands = [m for m in (mp, mb) if m]
        if not cands:
            break
        m = min(cands, key=lambda x: x.start())
        if m is mp:
            ctx = (m.group("rev"), m.group("path"))
            form = "frozen" if m.group("rev") else "prefixed"
            out.append(Cite(relpath, lineno(m.start()), form, ctx[0], ctx[1],
                            m.group("range"), m.group(0)))
        elif ctx is None:
            # A bare cite with nothing to inherit from. Fail closed: we cannot
            # tell which file it means, so we will not silently skip it.
            out.append(Cite(relpath, lineno(m.start()), "orphan", None, None,
                            m.group("range"), m.group(0)))
        else:
            out.append(Cite(relpath, lineno(m.start()), "bare-colon", ctx[0], ctx[1],
                            m.group("range"), m.group(0)))
        end = m.end()
        while True:
            mc = COMMA_RE.match(text, end)
            if not mc or "\n" in mc.group(0):
                break
            rev, path = ctx if ctx else (None, None)
            form = "bare-comma" if path else "orphan"
            out.append(Cite(relpath, lineno(mc.start()), form, rev, path,
                            mc.group("range"), mc.group(0)))
            end = mc.end()
        pos = end
    return out


def scan_corpus(root):
    cites = []
    base = os.path.join(root, CORPUS_DIR)
    for dirpath, _dirs, names in os.walk(base):
        for name in sorted(names):
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            try:
                with open(full, encoding="utf-8") as fh:
                    text = fh.read()
            except (UnicodeDecodeError, OSError):
                continue  # binary or unreadable: carries no cites
            cites.extend(scan_text(rel, text))
    cites.sort(key=lambda c: (c.file, c.line))
    return cites


# ---------------------------------------------------------------------------
# The manifest.
# ---------------------------------------------------------------------------

def read_manifest(root):
    """-> (rows, errors). rows: {(rev, path, range): (token, lineno)}"""
    rows, errors = {}, []
    path = os.path.join(root, MANIFEST)
    if not os.path.exists(path):
        return rows, [f"{MANIFEST}: missing"]
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = re.split(r"\t+", line.rstrip())
            if len(fields) != 4:
                errors.append(
                    f"{MANIFEST}:{lineno}: expected 4 tab-separated fields "
                    f"(src_file, range, rev, token), got {len(fields)}: {line!r}"
                )
                continue
            src, rng, rev, token = fields
            if not re.fullmatch(r"\d+(-\d+)?", rng):
                errors.append(f"{MANIFEST}:{lineno}: bad range {rng!r}")
                continue
            if not token:
                errors.append(f"{MANIFEST}:{lineno}: empty expectation token")
                continue
            key = (rev, src, rng)
            if key in rows:
                errors.append(
                    f"{MANIFEST}:{lineno}: duplicate row for {rev} {src}:{rng} "
                    f"(first at line {rows[key][1]})"
                )
                continue
            rows[key] = (token, lineno)
    return rows, errors


# ---------------------------------------------------------------------------
# The workflow push filter.
#
# This checker runs in formal.yml's `drift` job, and on a push that job only
# runs when the push touched one of `on.push.paths`. The coverage is complete
# today only by coincidence: every src_file in the manifest (wal.rs, store.rs,
# persistence.rs) happens to be listed. A cite into a fourth source file would
# silently lose push-time coverage -- the workflow would not fire at all for a
# change to that file, so nothing would notice the cite going stale until the
# next PR that happens to touch formal/.
#
# Parsed with a targeted scan rather than PyYAML: the repo installs nothing,
# and python3 has no YAML in the stdlib. The scan is deliberately narrow --
# `on:` -> `push:` -> `paths:` -> a block sequence of scalars -- and every way
# of not finding exactly that is an error, so a reshaped workflow fails the
# check instead of quietly passing it.
# ---------------------------------------------------------------------------

# `on` is a YAML 1.1 boolean, so a formatter may render the key as `on`, `"on"`
# or even `true`. Accept all three rather than hard-coding the current spelling.
_ON_RE = re.compile(r"""^(?:on|["']on["']|true):\s*(?:#.*)?$""")


def _indent(line):
    return len(line) - len(line.lstrip(" "))


def _block_lines(lines, start, outer_indent):
    """Yield (lineno, line) for the block indented under lines[start]."""
    for i in range(start + 1, len(lines)):
        line = lines[i]
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if _indent(line) <= outer_indent:
            return
        yield i, line


def _find_key(lines, start, outer_indent, key):
    """-> index of `key:` directly inside the block under lines[start]."""
    inner = None
    for i, line in _block_lines(lines, start, outer_indent):
        ind = _indent(line)
        if inner is None:
            inner = ind
        if ind != inner:
            continue  # deeper: belongs to some other key in this block
        if re.match(r"^" + re.escape(key) + r":\s*(?:#.*)?$", line.strip()):
            return i, ind
    return None, None


def read_push_paths(root):
    """-> (paths, errors). Fails closed: an unparseable filter is an error."""
    full = os.path.join(root, WORKFLOW)
    if not os.path.exists(full):
        return [], [f"{WORKFLOW}: missing -- cannot verify push-path coverage"]
    with open(full, encoding="utf-8") as fh:
        lines = fh.read().splitlines()

    on_idx = None
    for i, line in enumerate(lines):
        if _ON_RE.match(line):
            on_idx = i
            break
    if on_idx is None:
        return [], [f"{WORKFLOW}: no top-level `on:` block found -- cannot verify "
                    "push-path coverage"]

    push_idx, push_ind = _find_key(lines, on_idx, _indent(lines[on_idx]), "push")
    if push_idx is None:
        return [], [f"{WORKFLOW}: `on:` has no `push:` block -- this workflow no "
                    "longer gates pushes, so the cite guard has no push-time "
                    "coverage at all"]

    paths_idx, paths_ind = _find_key(lines, push_idx, push_ind, "paths")
    if paths_idx is None:
        return [], [f"{WORKFLOW}:{push_idx + 1}: `on.push` has no `paths:` list "
                    "-- cannot verify push-path coverage"]

    paths = []
    for i, line in _block_lines(lines, paths_idx, paths_ind):
        item = line.strip()
        if not item.startswith("- "):
            return [], [f"{WORKFLOW}:{i + 1}: expected a `- <path>` item under "
                        f"`on.push.paths`, got {item!r}"]
        val = item[2:].split(" #")[0].strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        if val:
            paths.append(val)
    if not paths:
        return [], [f"{WORKFLOW}:{paths_idx + 1}: `on.push.paths` is empty -- "
                    "cannot verify push-path coverage"]
    return paths, []


def path_is_watched(src, paths):
    """Does any push-filter pattern select `src`?"""
    for p in paths:
        if p == src:
            return True
        if p.endswith("/**") and src.startswith(p[:-2]):
            return True
        if fnmatch.fnmatchcase(src, p):
            return True
    return False


# ---------------------------------------------------------------------------
# Resolving a source file, at the working tree or at a frozen revision.
# ---------------------------------------------------------------------------

_source_cache = {}


def source_lines(root, rev, path):
    """-> (lines, None) or (None, error string)."""
    key = (rev, path)
    if key in _source_cache:
        return _source_cache[key]
    if rev == "-":
        full = os.path.join(root, path)
        if not os.path.exists(full):
            res = (None, f"{path} does not exist in the working tree")
        else:
            with open(full, encoding="utf-8") as fh:
                res = (fh.read().splitlines(), None)
    else:
        proc = subprocess.run(
            ["git", "show", f"{rev}:{path}"],
            cwd=root, capture_output=True, text=True,
        )
        if proc.returncode != 0:
            res = (None, f"git show {rev}:{path} failed: {proc.stderr.strip()}")
        else:
            res = (proc.stdout.splitlines(), None)
    _source_cache[key] = res
    return res


def check_anchor(root, rev, path, rng, token):
    """-> None when the anchor holds, else a list of report lines."""
    lines, err = source_lines(root, rev, path)
    if err:
        return [f"    cannot resolve source: {err}"]
    lo = int(rng.split("-")[0])
    hi = int(rng.split("-")[-1])
    if lo < 1 or lo > hi:
        return [f"    nonsensical range {rng}"]
    if lo > len(lines):
        return [
            f"    range starts past end of file ({path} at {rev} has {len(lines)} lines)"
        ]
    truncated = hi > len(lines)
    hi = min(hi, len(lines))
    hits = []
    for i in range(lo, hi + 1):
        c = lines[i - 1].count(token)
        if c:
            hits.extend([i] * c)
    if len(hits) == 1 and not truncated:
        return None
    out = []
    if truncated:
        out.append(
            f"    range ends past end of file ({path} at {rev} has {len(lines)} lines)"
        )
    if not hits:
        out.append("    expectation token NOT FOUND in the cited range")
    elif len(hits) > 1:
        out.append(
            "    expectation token is AMBIGUOUS -- "
            f"{len(hits)} matches in the cited range, at lines "
            + ", ".join(str(h) for h in hits)
        )
    # Show what is actually there, so the fix does not need a second command.
    out.append(f"    what is at {path}:{rng}"
               + ("" if rev == "-" else f" ({rev})") + ":")
    for i in range(lo, min(hi, lo + 11) + 1):
        out.append(f"      {i}: {lines[i - 1][:110]}")
    if hi > lo + 11:
        out.append(f"      ... ({hi - lo - 11} more lines)")
    if not hits:
        # Where did it go? A nearby hit is the actionable answer.
        near = [
            i for i in range(max(1, lo - 60), min(len(lines), hi + 60) + 1)
            if token in lines[i - 1]
        ]
        if near:
            out.append("    the token IS at "
                       + ", ".join(f"{path}:{i}" for i in near[:6])
                       + " -- the cite has drifted; re-anchor it (and every"
                       + " occurrence of this anchor listed above).")
        else:
            out.append("    the token is nowhere within 60 lines either side."
                       " The code may have been rewritten -- re-read it and"
                       " update the claim, not just the number.")
    return out


# ---------------------------------------------------------------------------

def main(argv):
    verbose = any(a in ("-v", "--verbose") for a in argv[1:])
    root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    cites = scan_corpus(root)
    rows, errors = read_manifest(root)
    push_paths, push_errors = read_push_paths(root)
    errors.extend(push_errors)

    # Census, for the CI log: a scan that silently stops finding the bare forms
    # is the exact failure this whole checker exists to prevent.
    #
    # `bare-nonsrc` is its own bucket rather than part of `non-src`. It is the
    # one thing the checker SKIPS: a bare cite that inherits a non-src file
    # (`WalCrash.tla:45` ... `:213`) has no anchor to verify, and folding it in
    # beside genuine prefixed `.tla` cites made the skip invisible -- in a
    # census billed as a tripwire, the silent skip is exactly what has to show.
    census = {}
    for c in cites:
        if c.is_src or c.form == "orphan":
            kind = c.form
        elif c.form.startswith("bare"):
            kind = "bare-nonsrc"
        else:
            kind = "non-src"
        census[kind] = census.get(kind, 0) + 1

    anchors = {}
    for c in cites:
        if c.is_src:
            anchors.setdefault(c.anchor, []).append(c)

    failures = []

    orphans = [c for c in cites if c.form == "orphan"]
    for c in orphans:
        failures.append(
            (f"{c.where()}: bare cite `{c.text.strip()}` has no preceding "
             f"`file:LINE` cite to inherit a file from", []))

    # 0. A bare cite may not resolve to a range that is ALSO an anchor under a
    #    different src_file. `:720` is a valid anchor in both src/store.rs and
    #    src/wal.rs; which one a bare `:720` means is decided purely by which
    #    prefix happens to sit above it, so prose churn that introduces an
    #    earlier `src/wal.rs:` prefix re-targets it to a different, equally
    #    valid anchor -- and every other check here stays green, because both
    #    anchors hold. (Today the only thing that would catch it is incidental:
    #    the abandoned anchor would trip STALE MANIFEST ROW, and only because
    #    it happens to have exactly one citer.) Prefixed form is the only form
    #    inheritance cannot mis-attribute, so bare form is refused there.
    #
    #    Collisions are collected by range alone, ignoring rev: inheritance
    #    carries the revision along with the path, so a frozen prefix re-targets
    #    a bare cite just as effectively as a working-tree one.
    by_range = {}
    for (rev, path, rng) in list(rows) + list(anchors):
        if path and path.startswith("src/") and path.endswith(".rs"):
            by_range.setdefault(rng, {}).setdefault(path, set()).add(rev)
    for c in cites:
        if not (c.is_src and c.form.startswith("bare")):
            continue
        others = by_range.get(c.range, {})
        if len(others) < 2:
            continue
        def _at(p):
            revs = sorted(others[p])
            return f"{p}:{c.range}" + ("" if revs == ["-"] else f" ({'/'.join(revs)})")
        width = max(len(_at(p)) for p in others)

        def _describe(p):
            tok = next((rows[(r, p, c.range)][0] for r in sorted(others[p])
                        if (r, p, c.range) in rows), None)
            return f"      {_at(p):<{width}}" + (f"   {tok!r}" if tok else "")
        failures.append((
            f"AMBIGUOUS BARE CITE {c.where()} `{c.text.strip()}`",
            [f"    it inherits {c.path} from an earlier prefix cite, but the",
             f"    range {c.range} is an anchor under more than one source file:"]
            + [_describe(p) for p in sorted(others)]
            + ["    Both resolve, so nothing below would fail if this cite meant",
               "    the other one -- and which it means is decided only by which",
               "    prefix happens to sit above it in the document. Write it in",
               f"    prefixed form, `{c.path}:{c.range}`, where inheritance",
               "    cannot mis-attribute it. (Do not 'fix' this by deleting the",
               "    colliding manifest row: the collision is a fact about the",
               "    two source files, not a defect in the manifest.)"],
        ))

    # 1. Every anchor must have a manifest row, and the row must hold.
    for anchor in sorted(anchors, key=lambda a: (a[1], int(a[2].split("-")[0]), a[0])):
        rev, path, rng = anchor
        occ = anchors[anchor]
        cited_at = ", ".join(o.where() for o in occ[:6]) + (
            f" (+{len(occ) - 6} more)" if len(occ) > 6 else "")
        row = rows.get(anchor)
        if row is None:
            failures.append((
                f"UNKNOWN ANCHOR {rev} {path}:{rng}",
                [f"    cited at {cited_at}",
                 "    no row in " + MANIFEST + ". Every cite must carry an",
                 "    expectation: open the source at that range, pick a token",
                 "    that occurs there exactly once, and add a tab-separated row",
                 f"      {path} <TAB> {rng} <TAB> {rev} <TAB> <token>"],
            ))
            continue
        token, mline = row
        problem = check_anchor(root, rev, path, rng, token)
        if problem:
            failures.append((
                f"STALE CITE {rev} {path}:{rng}",
                [f"    cited at {cited_at}",
                 f"    expected ({MANIFEST}:{mline}): {token!r}"] + problem,
            ))

    # 2. Every manifest row must be referenced by a cite.
    for key in sorted(rows, key=lambda a: (a[1], int(a[2].split("-")[0]), a[0])):
        if key not in anchors:
            rev, path, rng = key
            token, mline = rows[key]
            failures.append((
                f"STALE MANIFEST ROW {rev} {path}:{rng}",
                [f"    {MANIFEST}:{mline} expects {token!r}",
                 "    but nothing under " + CORPUS_DIR + " cites that anchor any",
                 "    more. Delete the row, or restore the cite it belonged to."],
            ))

    # 3. Every source file the manifest cites must be watched by the workflow's
    #    push filter, or this checker never runs for a direct-to-main push that
    #    changes it. Coverage is currently complete by coincidence -- wal.rs,
    #    store.rs and persistence.rs are all listed for other reasons.
    cited_sources = sorted({path for (_rev, path, _rng) in rows})
    unwatched = set()
    if push_paths:
        for src in cited_sources:
            if path_is_watched(src, push_paths):
                continue
            unwatched.add(src)
            failures.append((
                f"UNWATCHED SOURCE {src}",
                [f"    {MANIFEST} pins cites into {src}, but",
                 f"    {WORKFLOW}'s on.push.paths does not select it. This",
                 "    checker runs in that workflow's `drift` job, so a push to",
                 f"    main that changes only {src} starts no job at all and the",
                 "    cites into it go stale unnoticed until some later PR happens",
                 "    to touch formal/. Add it to the push filter:",
                 f"      - '{src}'",
                 "    (keep it beside the other watched sources), and check",
                 "    `lean-scope` too if the new file belongs to the Lean side."],
            ))

    print("cite-check: scanned " + CORPUS_DIR)
    print("  cite occurrences: "
          + ", ".join(f"{k}={v}" for k, v in sorted(census.items())))
    print(f"  distinct src/*.rs anchors: {len(anchors)}"
          f"   manifest rows: {len(rows)}")
    if not push_paths:
        note = "  (push filter unreadable -- see below)"
    elif unwatched:
        note = "  (UNWATCHED: " + ", ".join(sorted(unwatched)) + ")"
    else:
        note = f"  (all watched by {WORKFLOW}'s push filter)"
    print("  cited sources: " + ", ".join(cited_sources) + note)

    if verbose:
        for anchor in sorted(anchors, key=lambda a: (a[1], int(a[2].split("-")[0]), a[0])):
            rev, path, rng = anchor
            occ = anchors[anchor]
            tok = rows.get(anchor, ("<no row>", 0))[0]
            print(f"    {rev:<9} {path:<20} {rng:<12} x{len(occ):<3} {tok}")

    # Keep the census above the failures even when stdout is a pipe (CI) and
    # stderr is not: without this they interleave in the wrong order.
    sys.stdout.flush()

    if errors or failures:
        for e in errors:
            print(f"cite-check FAILED: {e}", file=sys.stderr)
        for head, body in failures:
            print(f"cite-check FAILED: {head}", file=sys.stderr)
            for b in body:
                print(b, file=sys.stderr)
        print(file=sys.stderr)
        print(f"{len(errors) + len(failures)} problem(s). "
              "The cites in formal/tla/wal/ no longer describe the source.",
              file=sys.stderr)
        print("Re-anchor by READING the code at each range -- never by applying"
              " a uniform offset;", file=sys.stderr)
        print("that is how the 2026-08 pass shipped a frozen cite that was"
              " wrong at creation.", file=sys.stderr)
        print("See formal/tla/wal/README.md, 'Source-line cites'.", file=sys.stderr)
        return 1

    print("cite-check: all anchors verified — ok.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
