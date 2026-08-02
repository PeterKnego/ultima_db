# Diátaxis documentation plan — ultima-db

Durable record of the documentation runs. Last full run: 2026-08-02
(all four quadrants written; plan approved by the user, reference scope
approved as its own item). Run 2 (2026-08-02, skill v1.2.0): approved
register repairs — architecture/comparison pages rewritten to explanation
register, embedded tutorial removed from prevent-write-skew, StoreConfig
field table deferred to rustdoc, unpinned SSI cost numbers removed,
docs/tasks/ links dropped from landing pages.

## Output format and location

Markdown under `docs/` (no site generator). Quadrant dirs with landing
READMEs: `docs/tutorials/`, `docs/how-to/`, `docs/reference/`,
`docs/explanation/`. `docs/README.md` is the home landing page.

## Audiences (user-confirmed 2026-08-02)

**Primary: Rust application developers** — tutorial and landing pages lead
with Store/Table/transactions. Then: distributed-systems/SMR builders,
concurrency-sensitive and async users, evaluators/benchmarkers,
vector-search users. Contributors are served by CLAUDE.md + docs/tasks/,
outside the user-facing set.

## Approved reference scope

- rustdoc/docs.rs is the canonical API-item reference; no hand-written
  per-item pages, ever (autodoc rule).
- Hand-written reference is limited to cross-cutting surfaces:
  configuration, cargo features, isolation matrices/cost/caveats,
  key encoding + on-disk/wire formats, provenance-pinned performance
  numbers.
- Rustdoc upkeep is in scope for reference work: doc-comment links must not
  point at unpublished in-repo paths (they were repointed to GitHub blob
  URLs, 2026-08-02).

## The documents (title — need — source)

### Tutorials
- `tutorials/getting-started.md` — onboarding for app developers —
  examples/basic_usage.rs, multi_store.rs, store_integration tests.

### How-to
- `how-to/set-up-durable-persistence.md` — durable setup + crash recovery —
  tests/persistence_integration.rs, tests/corruption_recovery.rs, store.rs
  rustdoc.
- `how-to/bulk-load-and-restore.md` — restore/backup/ingest, incl. the
  BulkLoadNotCheckpointed trap — examples/bulk_restore.rs, tests/bulk_load.rs.
- `how-to/replicate-with-snapshot-streams.md` — replication + SMR —
  tests/snapshot_stream.rs, task27/task12.
- `how-to/migrate-from-0-2-to-0-3.md` — format-break migration —
  CHANGELOG 0.3.0 (canonical), task56 §Migration.
- `how-to/handle-write-conflicts.md` — MultiWriter retry/rebase,
  CommitWaiter — examples/concurrent_writes.rs, task42.
- `how-to/prevent-write-skew.md` — SSI usage + verification recipes —
  former docs/isolation-levels.md how-to sections.
- `how-to/use-from-async-code.md` — spawn_blocking pattern, Send bounds —
  CHANGELOG 0.2.0 notes, tests/send_bounds.rs.
- `how-to/use-natural-primary-keys.md` — non-u64 keys, tuples, prefix scans
  — examples/string_keyed_table.rs, task56 §API.
- `how-to/query-with-indexes.md` — secondary/full-text/custom indexes —
  tests/fulltext_integration.rs, tests/custom_index_api.rs.
- `how-to/add-vector-search.md` — ultima-vector usage —
  ultima_vector/examples/, ultima_vector rustdoc.
- `how-to/choose-a-configuration.md` — goal→config decisions — former
  docs/configuration.md §7 + advice cells.

### Reference
- `reference/configuration.md` — fields/variants/legal combinations —
  former docs/configuration.md §1–§5, verified against src.
- `reference/cargo-features.md` — feature table — Cargo.toml.
- `reference/isolation-levels.md` — anomaly matrices, tracking granularity,
  cost, caveats — former docs/isolation-levels.md reference sections.
- `reference/key-encoding-and-formats.md` — PrimaryKey encodings, type ids,
  64 KiB cap, v2 formats — src/primary_key.rs, task56 §formats.
- `reference/performance.md` — provenance-pinned YCSB tables —
  docs/benchmarks/competitor-nvme-2026-07-13.md.

### Explanation
- `explanation/architecture.md` — the CoW/MVCC design and its decision
  record — git-mv of docs/ARCHITECTURE.md, stale facts fixed 2026-08-02
  (T=32/FixedVec/digest write-sets/keyed tables), KEY_TYPE_ID + write-side
  cap rationale added from task56.
- `explanation/isolation.md` — SI vs SSI, write skew — former
  docs/isolation-levels.md theory half, task21.
- `explanation/how-ultimadb-is-verified.md` — Elle, Lean/Aeneas proofs,
  crash contract — consistency-verification docs, formal/README+WRITEUP.
- `explanation/reading-our-benchmarks.md` — methodology/noise honesty —
  docs/benchmarks methodology sections, CLAUDE.md bench guidance.
- `explanation/vector-search.md` — HNSW trade-offs, SIMD dispatch,
  boundary validation — task22/25/40, ultima_vector src.
- `explanation/compared-with-lmdb-and-rocksdb.md` — positioning — git-mv +
  refresh of the stale comparison doc (11 corrections, 2026-08-02).

## Not created

- None. All four quadrants had genuine material; ultima-vector was
  confirmed in scope (how-to + explanation pages; rustdoc remains its API
  reference). Improvement remedy noted for future runs: a vector section in
  the tutorial would need a narrated end-to-end example beyond
  ultima_vector/examples/.

## Tutorial verification status

- getting-started: **verified by execution 2026-08-02.** Every stage built
  and run in a disposable cargo project; all expected outputs are captured
  real output (including the narrated transient dead_code warning). One
  normalization: the sandbox used a path dependency where the page says
  `cargo add ultima-db` — the only step not literally executed.

## Standing decisions for future runs

- `docs/tasks/` and `docs/superpowers/` are internal records (CLAUDE.md
  mandates keeping them); user-facing docs must not link into them.
  Historical records (CHANGELOG entries, task docs) are excerpted, never
  gutted.
- `docs/benchmarks/` is an immutable dated archive; `reference/performance.md`
  is the promoted current summary and must carry provenance.
- Corrections discovered 2026-08-02, already applied in the new docs but
  left uncorrected in the internal records they came from: task27 says
  `commit_version` is ignored (it is honored now); the pre-refresh
  comparison doc claimed SingleWriter was unenforced (it returns
  WriterBusy).

- Reference pages state no measured numbers without an archived
  `docs/benchmarks/` record (SSI cost numbers removed 2026-08-02 for lack
  of one; restore them only together with such a record).
- The Overview diagram in `explanation/architecture.md` names struct fields
  in ASCII-art form. Reviewed 2026-08-02 and accepted: it is an
  illustration of relationships, which the explanation sheet permits —
  the sheet forbids reference *tables*, not diagrams. Future runs should
  not flag or convert it.
