# Unicode-aware fulltext tokenizer — design

**Date:** 2026-07-07
**Status:** approved (brainstormed with Peter)
**Origin:** 2026-06 deep-review deferred backlog — "Fulltext tokenizer splits
only on ASCII punctuation (Unicode text under-matches); fixing changes index
contents."
**Task number:** task43 (task41 taken twice — index-DDL conflict + Elle
harness; task42 is CommitWaiter timeout).

## Problem

`src/fulltext.rs::tokenize` splits on `char::is_whitespace() ||
char::is_ascii_punctuation()`. Unicode punctuation (em-dash `—`, `「」`, `、`,
`«»`, `¡¿`, `…`) is neither, so it never splits tokens. Non-English text
under-matches: `"café—bar"` indexes as one token `"café—bar"`, and a query for
`"bar"` misses it.

## Decision (with Peter, 2026-07-07)

Zero-dependency fix: tokenize on Unicode-aware **non-alphanumeric** boundaries.
CJK unigram tokenization and a `unicode-segmentation` dependency were both
considered and declined — out of scope for this change.

## Design

```rust
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
```

The split predicate flips from "is whitespace or ASCII punctuation" to "is not
alphanumeric". `char::is_alphanumeric()` is Unicode-aware (Alphabetic ∪
Numeric across all scripts), so all Unicode punctuation, symbols, and
whitespace become token boundaries. `to_lowercase()` (full Unicode case
folding) is unchanged.

### Why existing data / tests are safe

- `FullTextIndex` is a `CustomIndex` — **never serialized**. Custom indexes are
  rebuilt from source data via `define_custom_index`'s backfill on recovery,
  not checkpointed. So there is no on-disk format migration; changing the
  tokenizer only changes behavior, and the index is reconstructed with the new
  tokenizer whenever the app re-defines it.
- Every existing ASCII separator is non-alphanumeric (`_`, `'`, `=`, `;`,
  space, `.`), so all current `tokenize` tests pass unchanged:
  `"it's a test."` → `["it","s","a","test"]`;
  `"key=value;other"` → `["key","value","other"]`.

### Documented limitations (out of scope, by decision)

- **CJK** (Han / Hiragana / Katakana / Hangul): no whitespace, and the
  ideographs *are* alphanumeric, so a CJK run stays a single token — still
  under-matches. A CJK unigram/n-gram filter is a separate feature.
- **NFD decomposed text**: a standalone combining mark (e.g. U+0301) is
  non-alphanumeric, so `"cafe\u{301}"` → `["cafe"]`. Harmless when corpus and
  queries share a normalization (search stays self-consistent); NFC input is
  recommended. The pre-existing cross-normalization mismatch (NFD vs NFC of the
  same word never matched byte-for-byte) is not made worse.

## Testing

Added to the existing `mod tests` in `src/fulltext.rs`:

1. Unicode punctuation splits: `tokenize("café—bar")` → `["café","bar"]`;
   `tokenize("«Rust»,Go")` → `["rust","go"]`; `tokenize("a、b。c")` →
   `["a","b","c"]`.
2. Accented word preserved as one token (NFC): `tokenize("Café")` →
   `["café"]`.
3. Non-Latin round-trip: index a Cyrillic document, find it via a Cyrillic
   query through `on_insert` + `search`.
4. CJK run stays a single token (locks in the documented limitation):
   `tokenize("東京都")` → `["東京都"]`.
5. All existing ASCII `tokenize` tests unchanged.

## Docs / task record

Canonical feature doc: `docs/tasks/task43_unicode_tokenizer.md`. Update the
deferred-review reference to mark the tokenizer item done (with CJK noted as a
remaining follow-up).
