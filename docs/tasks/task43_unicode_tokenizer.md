# Task 43: Unicode-aware Fulltext Tokenizer

**Origin:** 2026-06 deep-review deferred backlog — "Fulltext tokenizer splits
only on ASCII punctuation (Unicode text under-matches)."

(Numbering: task41 is used twice on main — index-DDL conflict fix and the Elle
consistency harness; task42 is the CommitWaiter timeout. This is task43.)

## What changed

`src/fulltext.rs::tokenize` now splits on Unicode-aware non-alphanumeric
boundaries (`!char::is_alphanumeric()`) instead of `is_whitespace() ||
is_ascii_punctuation()`. Unicode punctuation/symbols (em-dash, `「」`, `、`,
`«»`, `…`, etc.) now split tokens across all scripts. `to_lowercase()` (full
Unicode case folding) is unchanged.

## Why it's safe

- `FullTextIndex` is a `CustomIndex` and is never serialized — it is rebuilt
  from source data when the app re-defines the index, so there is no on-disk
  migration. Only search behavior changes.
- Every existing ASCII separator (`_ ' = ; .` and space) is non-alphanumeric,
  so all prior tokenization is preserved: `"it's a test."` →
  `["it","s","a","test"]`, `"key=value;other"` → `["key","value","other"]`.

## Limitations (follow-ups)

- **CJK** (Han/Kana/Hangul): no spaces + ideographs are alphanumeric, so a run
  stays one token — still under-matches. Needs a CJK unigram/n-gram filter
  (separate feature).
- **NFD text**: standalone combining marks are boundaries; NFC input
  recommended. Consistent-normalization search is unaffected.

Design history: `docs/superpowers/specs/2026-07-07-unicode-tokenizer-design.md`,
`docs/superpowers/plans/2026-07-07-unicode-tokenizer.md`.
