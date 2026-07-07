# Unicode-aware Fulltext Tokenizer (task43) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `src/fulltext.rs::tokenize` split on Unicode-aware non-alphanumeric boundaries so non-English text stops under-matching, without changing any existing ASCII behavior.

**Architecture:** One-line predicate change in `tokenize` (`is_whitespace() || is_ascii_punctuation()` → `!is_alphanumeric()`), plus tests and docs. Zero new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-07-unicode-tokenizer-design.md`

## Global Constraints

- Worktree branch: `fix/unicode-tokenizer`.
- `cargo clippy --workspace --all-targets -- -D warnings` must pass.
- All existing `fulltext` tests must pass unchanged.
- Zero new dependencies.

---

### Task 1: Unicode tokenizer + tests

**Files:**
- Modify: `src/fulltext.rs::tokenize` (~line 183)
- Test: `src/fulltext.rs` tests module

**Interfaces:**
- Produces: `tokenize` now splits on `!char::is_alphanumeric()`.

- [ ] **Step 1: Write the failing tests.** Add to `mod tests` in `src/fulltext.rs`, after the existing `tokenize_punctuation` test:

```rust
    #[test]
    fn tokenize_unicode_punctuation_splits() {
        // Em-dash, guillemets, ideographic comma/full-stop, ellipsis.
        assert_eq!(tokenize("café—bar"), vec!["café", "bar"]);
        assert_eq!(tokenize("«Rust»,Go"), vec!["rust", "go"]);
        assert_eq!(tokenize("a、b。c"), vec!["a", "b", "c"]);
        assert_eq!(tokenize("one…two"), vec!["one", "two"]);
    }

    #[test]
    fn tokenize_preserves_accented_word_nfc() {
        // Precomposed (NFC) accented letters are alphanumeric — one token.
        assert_eq!(tokenize("Café"), vec!["café"]);
        assert_eq!(tokenize("naïve GARÇON"), vec!["naïve", "garçon"]);
    }

    #[test]
    fn tokenize_non_latin_scripts() {
        // Cyrillic and Greek words are alphanumeric; split on spaces.
        assert_eq!(tokenize("Привет мир"), vec!["привет", "мир"]);
        assert_eq!(tokenize("Ελληνικά"), vec!["ελληνικά"]);
    }

    #[test]
    fn tokenize_cjk_stays_single_run() {
        // Documented limitation: CJK has no spaces and ideographs are
        // alphanumeric, so a run stays one token (still under-matches).
        assert_eq!(tokenize("東京都"), vec!["東京都"]);
    }

    #[test]
    fn cyrillic_document_round_trips_through_search() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Привет".into(),
                body: "Это тест на русском языке.".into(),
            },
        )
        .unwrap();
        idx.on_insert(
            2,
            &Article {
                title: "Hello".into(),
                body: "An English article.".into(),
            },
        )
        .unwrap();

        let results = idx.search("русском");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);
    }
```

- [ ] **Step 2: Run the new tests to verify they fail.**

Run: `cargo test --lib fulltext::tests::tokenize_unicode_punctuation_splits fulltext::tests::cyrillic_document`
Expected: `tokenize_unicode_punctuation_splits` FAILS (em-dash/guillemets/、 not split under the old predicate); `cyrillic_document_round_trips_through_search` FAILS (query `"русском"` won't match because `"языке."`-style tokens aren't affected, but the doc's `"русском"` token exists — actually verify: the failing one is the punctuation test; the Cyrillic round-trip may already pass since Cyrillic words are space-separated). Record which fail. At minimum `tokenize_unicode_punctuation_splits` and `tokenize_cjk_stays_single_run` (the latter should PASS even now — it locks the limitation) must be observed.

- [ ] **Step 3: Change the tokenizer.** In `src/fulltext.rs`, replace:

```rust
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
```

with:

```rust
/// Split `text` into lowercased tokens on Unicode-aware non-alphanumeric
/// boundaries. `char::is_alphanumeric` covers all scripts, so Unicode
/// punctuation/symbols/whitespace all split tokens (not just ASCII).
///
/// Known limitations (see `docs/tasks/task43_unicode_tokenizer.md`):
/// CJK runs have no spaces and stay a single token; NFD combining marks are
/// treated as boundaries, so NFC-normalized input is recommended.
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
```

- [ ] **Step 4: Run the full fulltext suite.**

Run: `cargo test --lib fulltext::`
Expected: all pass — the new Unicode tests plus every pre-existing `tokenize_*` and search test.

- [ ] **Step 5: Commit.**

```bash
git add src/fulltext.rs
git commit -m "feat(fulltext): Unicode-aware tokenizer (split on non-alphanumeric) (task43)"
```

---

### Task 2: Feature doc + deferred-review update + full verification

**Files:**
- Create: `docs/tasks/task43_unicode_tokenizer.md`
- Check: any `docs/` reference to the ASCII-only tokenizer limitation

**Interfaces:**
- Consumes: Task 1.

- [ ] **Step 1: Write the feature doc** — `docs/tasks/task43_unicode_tokenizer.md`:

```markdown
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
- Every existing ASCII separator (`_ ' = ; . ` space) is non-alphanumeric, so
  all prior tokenization is preserved: `"it's a test."` →
  `["it","s","a","test"]`, `"key=value;other"` → `["key","value","other"]`.

## Limitations (follow-ups)

- **CJK** (Han/Kana/Hangul): no spaces + ideographs are alphanumeric, so a run
  stays one token — still under-matches. Needs a CJK unigram/n-gram filter
  (separate feature).
- **NFD text**: standalone combining marks are boundaries; NFC input
  recommended. Consistent-normalization search is unaffected.

Design history: `docs/superpowers/specs/2026-07-07-unicode-tokenizer-design.md`,
`docs/superpowers/plans/2026-07-07-unicode-tokenizer.md`.
```

- [ ] **Step 2: Check for a doc reference to the old limitation.**

Run: `grep -rn "ASCII punctuation\|ascii_punct\|Unicode text under" docs/ CLAUDE.md`
Expected: if a reference frames the ASCII-only tokenizer as a known limitation, update it to point at task43 as fixed. If none, skip (do not invent one).

- [ ] **Step 3: Full verification.**

Run: `cargo test && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all pass, zero warnings.

- [ ] **Step 4: Commit.**

```bash
git add docs/tasks/task43_unicode_tokenizer.md
git commit -m "docs: task43 Unicode tokenizer feature doc"
```
