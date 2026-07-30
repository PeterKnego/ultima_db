# 0.2.0 Release + `Send` Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish `ultima-db` and `ultima-vector` 0.2.0 to crates.io quietly (no
announcement), and determine whether `WriteTx` can be made `Send`.

**Architecture:** Steps 1–2 of
`docs/superpowers/specs/2026-07-30-adoption-program-design.md`. The release
seals `pub enum Error` with `#[non_exhaustive]` — that variant addition is what
makes this a minor bump rather than a patch, so it is the moment to make it the
last such break. The `Send` audit is placed *before* the arbitrary-primary-keys
work so that any API change it turns up rides the same breaking release (0.3.0)
instead of forcing a second one.

**Tech Stack:** Rust (edition 2024, MSRV 1.88), cargo, `gh` CLI, crates.io.

## Global Constraints

- **MSRV 1.88, edition 2024.** Do not use features newer than 1.88.
- **Zero-warning lint gate:** `cargo clippy --all-targets --features persistence -- -D warnings` must pass.
- **Do NOT run `cargo fmt`.** The repo has repo-wide rustfmt-version drift and no CI fmt gate; running it churns dozens of unrelated files. Match the surrounding style by hand.
- **Never use `perl -pi` for edits containing non-ASCII.** A stray `\x{2014}` em-dash double-encoded 106 characters in `store.rs` once. Use the Edit tool or `python3`.
- **Workspace verification:** a root `cargo test` does NOT cover member crates. Also run `cargo test -p ultima-vector`.
- **Peter runs `cargo publish` himself** (`cargo login` first). Prepare everything up to and including dry-run/package verification, then stop and hand off.
- **Tag and GitHub release happen AFTER both crates are live on crates.io**, so a failed publish cannot strand a tag.
- **Publish order is `ultima-db` first, then `ultima-vector`** (vector's path dep carries `version = "0.2.0"`, which must already exist on crates.io).
- **This is a quiet release.** No announcement, no blog post, no social posting, no README positioning rewrite. Those belong to 0.3.0.

---

### Task 1: Seal `pub enum Error` with `#[non_exhaustive]`

**Files:**
- Modify: `src/error.rs:11-12` (the `#[derive(Debug, Error)]` / `pub enum Error` lines)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `ultima_db::Error` becomes `#[non_exhaustive]`. Downstream crates must use a wildcard arm when matching it. In-crate matches are unaffected (the attribute does not apply within the defining crate).

**Why there is no unit test for this task:** `#[non_exhaustive]` is a
compile-time contract that has no effect inside the defining crate, so no
in-crate test can observe it. The meaningful verification is that the whole
workspace still compiles — i.e. that no member crate had an exhaustive match on
`ultima_db::Error`. An audit at plan time found the only workspace matches are
in `ultima_vector/src/validate.rs:35` and `ultima_vector/src/collection.rs:666`,
both of which already have `other =>` catch-all arms and one of which matches
`ultima_vector`'s own `Error`, not this one. Step 2 confirms that audit rather
than trusting it.

- [ ] **Step 1: Confirm the workspace has no exhaustive match on `ultima_db::Error`**

Run:

```bash
cargo build --workspace --all-targets --features persistence 2>&1 | tail -20
```

Expected: builds clean. This is the "before" state — record that it passes, so
that a failure after Step 3 is unambiguously caused by the attribute.

- [ ] **Step 2: Add the attribute**

In `src/error.rs`, change:

```rust
#[derive(Debug, Error)]
pub enum Error {
```

to:

```rust
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
```

- [ ] **Step 3: Extend the enum's rustdoc to state the contract**

Immediately above the `#[derive]`, the existing doc comment ends with a line
about how a caller should react. Append this paragraph to that doc comment:

```rust
/// This enum is `#[non_exhaustive]`: new variants may be added in any release
/// without a major version bump, so downstream `match` expressions must
/// include a wildcard (`_`) arm. Matching within this crate is unaffected.
```

- [ ] **Step 4: Verify the workspace still builds and tests pass**

Run:

```bash
cargo build --workspace --all-targets --features persistence
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo clippy --all-targets --features persistence -- -D warnings
```

Expected: all pass, zero warnings. If a member crate now fails with
`non-exhaustive patterns`, add a `_ => ...` arm to that match — do not remove
the attribute.

- [ ] **Step 5: Commit**

```bash
git add src/error.rs
git commit -m "api!: seal Error with #[non_exhaustive]

task44 sealed every other public config enum and missed Error.
Adding Error::DuplicateTableOpen (#20) already broke exhaustive
downstream matches, so this release is breaking regardless; sealing
now makes it the last time."
```

---

### Task 2: Version bump + CHANGELOG

**Files:**
- Modify: `Cargo.toml:7` (`version = "0.1.1"` → `"0.2.0"`)
- Modify: `ultima_vector/Cargo.toml:3` (`version = "0.1.0"` → `"0.2.0"`)
- Modify: `ultima_vector/Cargo.toml:23` (`ultima-db = { path = "..", version = "0.1.0" }` → `version = "0.2.0"`)
- Modify: `CHANGELOG.md` (new top section, below the `# Changelog` heading and above `## 0.1.1`)

**Interfaces:**
- Consumes: Task 1's `#[non_exhaustive]` change (it is a CHANGELOG line item).
- Produces: both crate manifests at `0.2.0`; `ultima-vector`'s dependency requirement pointing at `ultima-db` 0.2.0.

**Why `ultima-vector` is republished with no source changes:** for `0.x`
versions cargo reads `^0.1.0` as `>=0.1.0, <0.2.0`. The published
`ultima-vector` 0.1.0 requires `ultima-db ^0.1.0`, which *excludes* 0.2.0 — so
without a republish, a user adding both crates would be pinned to `ultima-db`
0.1.x or hit a resolution conflict. Verify this with Step 4 rather than taking
it on faith.

- [ ] **Step 1: Bump `ultima-db`**

In `Cargo.toml`, change `version = "0.1.1"` to `version = "0.2.0"`.

- [ ] **Step 2: Bump `ultima-vector` and its dependency requirement**

In `ultima_vector/Cargo.toml`, change `version = "0.1.0"` to `version = "0.2.0"`,
and change the dependency line to:

```toml
ultima-db = { path = "..", version = "0.2.0" }
```

- [ ] **Step 3: Write the CHANGELOG entry**

Insert directly below the `# Changelog` heading in `CHANGELOG.md`:

```markdown
## 0.2.0 — 2026-07-30

**Heads-up: an on-disk format break is coming in 0.3.0.** Arbitrary primary
keys will change the WAL and checkpoint formats, and recovery will reject
files written by earlier versions. Do not build long-lived persisted data on
0.2.0 that you cannot rebuild.

### Breaking

- `Error` is now `#[non_exhaustive]`. Downstream `match` expressions over it
  must include a wildcard arm. This is the last time adding an error variant
  will be a breaking change.
- `Error::DuplicateTableOpen` added (see `open_tables2`/`open_tables3` below).

### Added (ultima-db)

- `Store::pin_version(Option<u64>) -> Result<VersionPin>` — a `Send + Sync +
  Clone` handle that keeps one snapshot alive across `gc()`, for handing a
  consistent point-in-time view to another thread. Note that pinning is not
  atomic with commit: `pin_version(Some(v))` can race auto-GC under concurrent
  committers, while `pin_version(None)` is race-free.
- `WriteTx::open_tables2` / `open_tables3` — open two or three tables in one
  call and hold their writers simultaneously, instead of one at a time.
  Returns `Error::DuplicateTableOpen` if a name is repeated.
- `fanout-t8` cargo feature — narrow B-tree fanout (T=8) for write-dominated
  deployments: roughly 1.8x the default's contended write throughput, at the
  cost of about 2x read-p99-under-load and 25% slower uncontended gets.

### Changed (ultima-db)

- B-tree nodes use inline fixed-capacity storage, making a copy-on-write node
  clone a single allocation; default fanout retuned from T=64 to T=32.
- `Store::gc()` is now O(evicted + pins) per run rather than O(retained), so a
  large `num_snapshots_retained` no longer costs per-commit time.
- `WriteTx::open_table` caches its per-table metrics handle and name, removing
  a registry lookup and an allocation from every repeat call.

### ultima-vector

- Version-only release. No source changes; republished so that its dependency
  requirement admits `ultima-db` 0.2.0.
```

- [ ] **Step 4: Verify the workspace resolves at the new versions**

Run:

```bash
cargo build --workspace --all-targets --features persistence
grep -n 'name = "ultima-db"' -A2 Cargo.lock
```

Expected: builds clean, and `Cargo.lock` shows `version = "0.2.0"` for
`ultima-db`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock ultima_vector/Cargo.toml CHANGELOG.md
git commit -m "chore(release): 0.2.0

ultima-db 0.1.1 -> 0.2.0 (Error sealed + DuplicateTableOpen make this
breaking); ultima-vector 0.1.0 -> 0.2.0, version-only, because ^0.1.0
excludes 0.2.0 for 0.x crates. CHANGELOG flags the 0.3.0 format break."
```

---

### Task 3: Package verification and publish dry-run

**Files:**
- Modify (only if a defect is found): `Cargo.toml` `include`/`exclude` lists, `ultima_vector/Cargo.toml`

**Interfaces:**
- Consumes: Task 2's bumped manifests.
- Produces: verified `.crate` artifacts for both crates, and a green
  `cargo publish --dry-run` for each. No code changes expected.

**Background:** the 0.1.0 release established an `include` whitelist per crate
because the crate root is the repo root (an unfiltered `cargo package` shipped
248 files). `examples/profile_commit.rs` and `examples/fanout_microbench.rs` are
excluded because the first imports the unpublished `ultima_bench_workloads` and
the second is an internal perf probe. Nothing in this release adds files, so the
expectation is that the existing lists still hold — these steps confirm it.

- [ ] **Step 1: List the packaged files for `ultima-db`**

Run:

```bash
cargo package --list --allow-dirty 2>&1 | tee /tmp/pkg-db.txt | wc -l
grep -E "docs/|formal/|bench-infra/|autobench/|benches/|tools/|\.github/|CLAUDE\.md" /tmp/pkg-db.txt
```

Expected: a file count in the tens (not hundreds), and the `grep` returns
nothing. If internal directories appear, fix the `include` list in `Cargo.toml`
and re-run before continuing.

- [ ] **Step 2: Confirm no packaged test or example imports a path-only dev-dep**

Run:

```bash
grep -rn "ultima_bench_workloads\|bench_workloads" tests/ examples/ | grep -v profile_commit
```

Expected: no output. `ultima-bench-workloads` is the only path-only dev-dep; if
a packaged file imports it, the published crate will not build for consumers.
Add the offending file to the `exclude` list.

- [ ] **Step 3: Dry-run publish `ultima-db`**

Run:

```bash
cargo publish --dry-run --allow-dirty
```

Expected: `Packaging`/`Verifying`/`Compiling` all succeed, exit 0. A failure
here is a release blocker — resolve it before proceeding.

- [ ] **Step 4: Dry-run publish `ultima-vector`**

Run:

```bash
cargo publish --dry-run --allow-dirty -p ultima-vector 2>&1 | tail -20
```

Expected: this will fail with an error about `ultima-db 0.2.0` not being found
on crates.io, because it is not published yet. **That specific failure is
expected and acceptable.** Any *other* failure (packaging, missing README or
LICENSE, manifest error) is a blocker. Record which failure mode occurred.

- [ ] **Step 5: Verify docs.rs feature config builds**

Run:

```bash
cargo doc --no-deps --features persistence,fulltext,metrics
cargo doc --no-deps -p ultima-vector --features persistence
```

Expected: both succeed. Build success is the gate; warnings are not.

- [ ] **Step 6: Commit any packaging fixes**

If Steps 1–5 required no changes, skip this step and note that nothing needed
fixing. Otherwise:

```bash
git add Cargo.toml ultima_vector/Cargo.toml
git commit -m "chore(release): fix packaging include/exclude for 0.2.0"
```

---

### Task 4: Publish handoff, tag, and GitHub release

**Files:**
- None modified. This task runs commands and creates a git tag.

**Interfaces:**
- Consumes: Task 3's verified artifacts.
- Produces: `ultima-db` 0.2.0 and `ultima-vector` 0.2.0 live on crates.io; an
  annotated `v0.2.0` tag; a GitHub release.

**This task requires Peter.** Do not run `cargo publish` — stop at Step 1 and
wait.

- [ ] **Step 1: Merge the release branch to `main` first**

The tag must land on a commit that is on `main`. Open a PR from the working
branch, get CI green (`ci`, `consistency`, `formal`), and merge. Note that the
`docs/adoption-program` branch also carries the spec and this plan; either
include them in the same PR (they are release-adjacent documentation) or split
them into their own PR first — do not leave them unmerged, since the CHANGELOG
references the 0.3.0 plan.

```bash
gh pr create --title "chore(release): 0.2.0" --body "Seals Error, bumps both crates to 0.2.0, CHANGELOG. See docs/superpowers/plans/2026-07-30-release-020-and-send-audit.md"
```

- [ ] **Step 2: Hand off to Peter for publish**

Tell Peter, verbatim, what to run from a clean checkout of `main`:

```bash
cargo login          # if not already authenticated
cargo publish                      # ultima-db first
cargo publish -p ultima-vector     # then ultima-vector
```

Wait for confirmation that both succeeded. Do not proceed to Step 3 until
both crates are live.

- [ ] **Step 3: Verify both crates are live**

Run:

```bash
cargo search ultima-db | head -3
cargo search ultima-vector | head -3
```

Expected: both report `0.2.0`. (Do not poll the crates.io JSON API directly —
it rate-limits and returns a data-access-policy error.)

- [ ] **Step 4: Tag and create the GitHub release**

```bash
git checkout main && git pull
git tag -a v0.2.0 -m "0.2.0 — Error sealed, pin_version, open_tables2/3, FixedVec/T=32"
git push origin v0.2.0
gh release create v0.2.0 --title "v0.2.0" --notes-file <(sed -n '/## 0.2.0/,/## 0.1.1/p' CHANGELOG.md | head -n -1)
```

- [ ] **Step 5: Confirm the release is quiet**

No announcement, no social post, no README positioning rewrite. The GitHub
release notes are the only public artifact. Confirm nothing else was published.

---

### Task 5: `Send` audit for `WriteTx` / `ReadTx`

**Files:**
- Modify: `src/store.rs:1850` and `src/store.rs:2015` (the `_not_send: PhantomData<*const ()>` fields) — only if the audit concludes the marker can go
- Modify: `src/store.rs:3832-3835` (the stale comment) — in every outcome
- Create: `tests/send_bounds.rs`
- Create: `docs/tasks/task55_send_audit.md`

**Interfaces:**
- Consumes: nothing from earlier tasks; independent of the release.
- Produces: a documented verdict, and `tests/send_bounds.rs` asserting whatever
  the true bounds are. If the verdict is "removable", `WriteTx: Send` and
  `ReadTx: Send` become part of the public API (additive — an added auto-trait
  impl is not a breaking change, so this can ship in 0.3.0).

**Known defect to fix regardless of outcome:** `src/store.rs:3833-3835` claims
the `!Send` marker is "verified by a trybuild-style negative test in
`tests/store_integration.rs`". **No such test exists.** A search of `tests/`,
`src/`, and `examples/` for `not_send`, `compile_fail`, and `trybuild` returns
only that comment, and there is no `.stderr` fixture or trybuild dev-dependency
anywhere in the repo. The marker is currently unverified and the comment is
false.

- [ ] **Step 1: Write the test that asserts today's actual bounds**

Create `tests/send_bounds.rs`:

```rust
//! Compile-time assertions about the thread-safety bounds of the public API.
//!
//! `Store` must be `Send + Sync` so clones can be shared across threads.
//! `VersionPin` must be `Send + Sync` so a pinned snapshot can be handed to
//! another thread. Whether the transaction types are `Send` is the subject of
//! `docs/tasks/task55_send_audit.md`.

use ultima_db::{Store, VersionPin};

const fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn public_types_have_expected_thread_bounds() {
    assert_send_sync::<Store>();
    assert_send_sync::<VersionPin>();
}
```

- [ ] **Step 2: Run it to confirm it passes**

Run: `cargo test --test send_bounds`
Expected: PASS. This is the baseline the audit must not regress.

- [ ] **Step 3: Establish why the marker exists**

Read `src/store.rs` around `WriteTx` (struct at ~line 2015) and `ReadTx`
(~line 1850) and answer these four questions in notes:

1. What non-`Send` data does each struct actually hold? (Expected: none —
   `Arc<Snapshot>`, `Box<dyn MergeableTable: Send + Sync>`, and
   `RefCell<Vec<WalOp>>`, and `RefCell<T: Send>` is itself `Send`.)
2. Does the commit path assume the committing thread is the thread that called
   `begin_write`? Check intent registration in `src/intents.rs` and the
   `PromoteGate` ticket handling in `commit`.
3. Does any thread-local or thread-id-keyed state exist on the write path?
   Search for `thread_local!`, `thread::current`, and `ThreadId`.
4. Would `WriteTx: Send` allow a program that is currently impossible and
   unsound — specifically, can a transaction moved to another thread violate
   an invariant that the single-threaded assumption protects?

- [ ] **Step 4: Reach one of three verdicts and record it**

Create `docs/tasks/task55_send_audit.md` following the structure of the other
task docs (context, what was audited, verdict, consequences, testing). It must
state the verdict explicitly as one of:

- **A — Removable.** No thread affinity found. Drop `_not_send` from both
  structs (or from `WriteTx` alone if `ReadTx` differs), keeping `!Sync` via the
  `RefCell`.
- **B — Removable under a restriction.** Document the restriction precisely and
  what breaks if it is violated.
- **C — Load-bearing.** Document exactly which mechanism requires thread
  affinity, with file and line references. The answer for async users is
  `spawn_blocking`, and that guidance goes in the doc.

Do not pick a verdict to make the task tidy. C is a perfectly good outcome and
is more useful documented than a marker nobody can explain.

- [ ] **Step 5: Apply the verdict to the code**

For verdict A or B, remove the `_not_send: PhantomData<*const ()>` field from
the affected struct(s) and their initializers (`src/store.rs:572`, `:729`, and
the struct definitions), then extend `tests/send_bounds.rs` with:

```rust
use ultima_db::{ReadTx, WriteTx};

const fn assert_send<T: Send>() {}

#[test]
fn transactions_are_send() {
    assert_send::<WriteTx>();
    assert_send::<ReadTx>();
}
```

Both types are plain structs with no lifetime or type parameters
(`src/store.rs:1845` and `:1944`), so they name directly. Under verdict B,
assert only the type the restriction permits and say in the test's doc comment
which one is excluded and why.

For verdict C, leave the fields alone and add a comment above each explaining
the specific mechanism, referencing `docs/tasks/task55_send_audit.md`.

- [ ] **Step 6: Fix the false comment**

In every outcome, replace the claim at `src/store.rs:3833-3835` that a
trybuild-style negative test exists. For verdict C, it becomes:

```rust
// `WriteTx` and `ReadTx` are `!Send` via `PhantomData<*const ()>` so a
// transaction stays on its creating thread. See
// `docs/tasks/task55_send_audit.md` for why this is load-bearing.
// `tests/send_bounds.rs` asserts the bounds that are guaranteed.
```

For verdicts A and B, delete the comment and let `tests/send_bounds.rs` carry
the contract.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo clippy --all-targets --features persistence -- -D warnings
```

Expected: all pass, zero warnings.

- [ ] **Step 8: Commit**

```bash
git add src/store.rs tests/send_bounds.rs docs/tasks/task55_send_audit.md
git commit -m "docs+test(store): audit the transaction Send bounds (task55)

Adds tests/send_bounds.rs, which asserts the bounds the code actually
guarantees; the comment claiming a trybuild negative test existed was
false — no such test was ever written."
```

---

## Deferred to the 0.3.0 plan

Not in scope here, recorded so it is not lost: the README positioning rewrite,
`examples/time_travel.rs`, `examples/durable_store.rs`, `docs/guide.md`, and the
`lib.rs` crate-docs pass. All of them describe an API that
`Table<R, K = u64>` is about to change, so they are written once, against the
final surface, in step 4 of the spec's sequence.
