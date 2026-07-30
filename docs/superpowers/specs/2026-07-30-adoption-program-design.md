# Adoption Program — Design

**Date:** 2026-07-30
**Status:** Approved, pending implementation plan

## Context

UltimaDB is technically mature and unused. `main` is clean, CI (ci /
consistency / formal) is green, 54 features are documented in `docs/tasks/`,
and the crate has an Elle-verified isolation story and a Lean-verified B-tree
kernel. It has no users, no filed issues from outside the project, and 53
commits — including two new public APIs (`Store::pin_version`,
`WriteTx::open_tables2/3`) and the FixedVec/fanout performance change — sitting
unreleased since `v0.1.1` on 2026-07-13.

The last six weeks were a single performance campaign on the SMR apply path,
driven by the `hi-perf-cmp` comparison. It succeeded: the "~1,000× a flat
store" figure turned out to be ~90% a snapshot-retention artifact, and the real
trade is now ~30–50× batched. But the cost decomposition showed the residual
gap is two structural terms — per-value `Arc` allocation and O(log n) descent
versus O(1) slot addressing — that are irreducible while every commit is a
version. That line has reached diminishing returns.

This program redirects effort from engine performance to adoption: making
UltimaDB usable by, and legible to, the audience it is aimed at.

**Target audience:** Rust applications needing a fast embedded transactional
store — the slot occupied by redb, sled, and fjall, and by hand-rolled
`Arc<RwLock<HashMap>>` plus a bespoke WAL.

**Deliberately not chosen** (recorded, not discarded): SMR/replicated state
machines, embedded AI/vector, and MVCC time-travel as a *primary* niche.
Time-travel remains the headline differentiator for the chosen audience.

## Goals

1. Everything built since July is available to users.
2. A reader in the embedded-OLTP slot understands within thirty seconds what
   UltimaDB is, what it is not, and why they might choose it.
3. A user can model their data naturally, without an auto-increment `u64`
   forced between them and their domain keys.
4. The two directions set aside — apply-path performance and 1.0 hardening —
   are recorded in a retrievable form.

## Non-goals

A documentation site, a logo, blog posts, benchmarks-as-marketing, or an async
API. All are downstream of having a user. YAGNI applies with force here: the
project's failure mode is building more engine, not building less collateral.

---

## 1. Positioning

### The claim

> **UltimaDB is a fast embedded transactional store for Rust — and every commit
> is a permanent, free snapshot.**
>
> Typed tables, secondary indexes, real MVCC. Reads never block writers.
> Because the B-tree is copy-on-write, a snapshot costs nothing to keep and
> nothing to read: time-travel to any version, clone a whole database in O(1),
> and hand a consistent point-in-time view to another thread without copying
> it.

The claim is two-part because the advantage is two-part: **competitive on the
table stakes** (transactions, indexes, durability, speed), **unique on
versioning**.

The versioning half is what no competitor has. redb, sled, and fjall are
single-timeline stores — a read transaction can pin a snapshot, but none can
answer "what did this table look like at version 4,000?" a week later. Today
that property is one bullet in a Highlights list, below a benchmark table.

### The disclosure, and why it is positioning rather than a caveat

UltimaDB holds its dataset in memory; durability is a WAL and checkpoints on
top. redb, sled, and fjall are disk-resident with a page cache and serve
datasets far larger than RAM. **This must appear in the README above the
benchmark table.**

Stating it plainly is not a concession — it is what makes the performance
numbers credible instead of suspicious, and it sharpens the positioning:

- Most production OLTP databases fit comfortably in RAM on a modern machine.
  Disk-resident B-tree design is largely a constraint inherited from an era
  when they did not. Memory-residency is not a compromise UltimaDB makes; it is
  the design premise its performance follows from.
- So UltimaDB is not "a faster redb." It is an in-memory transactional store
  with real durability and permanent versioning — competing most directly with
  the hand-rolled in-process state stores that Rust services build for
  themselves.

The benchmark table stays, demoted from headline to evidence.

---

## 2. API work

### 2a. Arbitrary primary keys — `Table<R, K = u64>`

`Table<R>` is `BTree<u64, R>`: the primary key is always an auto-incrementing
`u64`. Every alternative in the target slot is an arbitrary-key store. A user
who wants `users` keyed by email, or a cache keyed by `String`, must define a
unique secondary index and perform a two-hop lookup. This is the one place
where UltimaDB is not merely *different* from the alternatives but *harder to
model in*.

**The defaulted type parameter is the core of the design.** `Table<R, K = u64>`
and `TableDef<R, K = u64>` leave every existing call site
(`open_table::<User>("users")`, `Table<R>`, `TableDef<R>`) compiling unchanged.
`BTree<K, V>` is already generic, so the storage layer needs nothing.

The work is in the layers that assumed `u64`:

- **Key contract.** A `PrimaryKey` trait: `Ord + Clone + Send + Sync +
  'static`, plus an order-preserving byte encoding when the `persistence`
  feature is on (the encoding must preserve `Ord` so that WAL/checkpoint replay
  and `from_sorted` see the same order the in-memory tree does). Blanket impls
  for the integer types, `String`, `Vec<u8>`, and tuples.
- **Auto-increment stays `u64`-only.** `insert(record) -> Result<u64>`,
  `next_id`, `set_next_id`, and the task51 right-spine bulk-append fast path in
  `insert_batch` are gated behind an `AutoKey` bound satisfied only by `u64`.
  Other key types use `put(key, record)`. This preserves the SMR id story and
  the bulk-append optimization exactly as they are.
- **OCC write-set: hash, do not generalize.** `BTreeMap<String,
  BTreeSet<u64>>` keeps its shape, but the `u64` becomes a 64-bit hash of the
  key. A hash collision produces a *spurious* conflict, never a missed one, so
  the detector stays sound and the entire OCC / intents / SSI path is
  untouched. This is the largest cost avoidance in the design, and the
  false-conflict behavior must be documented.
- **WAL/checkpoint format.** `WalOp::{Insert, Update, Delete}` carry encoded
  key bytes instead of `id: u64`. This is a format break. Bump the format
  version and have recovery reject older files with an actionable error rather
  than carrying a v1 compatibility branch — at 0.1.x, pre-1.0, a clean break
  costs less than the branch.
- **Indexes.** `UniqueStorage`'s `BTree<IK, u64>` becomes `BTree<IK, K>`;
  `NonUniqueStorage`'s composite `(IK, u64)` becomes `(IK, K)`. Mechanical, but
  it reaches `rebuild_from_sorted_data` and therefore `bulk_load` and
  `define_index`.

**Unchanged:** the B-tree, MVCC, the commit protocol, SSI, and the snapshot
streaming wire structure (its ordering is already "sorted by key").

**Version impact:** 0.2.0, breaking, on account of the WAL format.

### 2b. `Send` transactions — an audit, not a promised feature

`WriteTx` and `ReadTx` are `!Send + !Sync` via a `PhantomData<*const ()>`
marker, guarded by a negative test. The marker is a deliberate design choice,
not a constraint forced by unsafety. For the target audience this matters: a
large fraction is on tokio, and a transaction that cannot be held across an
`.await` is a real barrier.

Grounds for thinking `WriteTx: Send + !Sync` may be reachable: the contents are
`Arc<Snapshot>` and `Box<dyn MergeableTable: Send + Sync>`, and `RefCell<T:
Send>` is itself `Send` — it forces `!Sync`, not `!Send`. `Send + !Sync` is
also exactly the shape async needs: hold a transaction across an `.await` on
one task, never share it between tasks.

**This is scoped as investigation with an unstated outcome.** The marker exists
for a reason not yet traced — most likely thread affinity in intent
registration or the commit path. Three acceptable results:

1. It is safe → drop the marker, `WriteTx` becomes `Send`.
2. It is safe under a restriction → document the restriction.
3. It is genuinely load-bearing → document why, and the answer for async users
   is `spawn_blocking`.

The spec promises the audit, not a particular outcome.

**Cost split:** 2a is the bulk of the program. 2b is roughly a day, producing
either a one-line change or a paragraph of documentation.

---

## 3. Release and documentation

### Release

**Ship 0.1.2 first, ahead of the key work.** Fifty-three commits including two
new public APIs and the FixedVec/fanout change are unreleased. It is small,
self-contained, and has an existing checklist from the 0.1.0 release; it must
not queue behind a multi-week API change.

Keys then land as **0.2.0**, breaking, with the WAL format bump.

### Documentation deliverables

The smallest set that does the job:

- **README restructure** — the claim from §1, the memory-resident disclosure
  above the benchmark table, then evidence.
- **`examples/time_travel.rs`** — the differentiator has no runnable demo.
  Commit a series of versions, read each back, show O(1) clone and a
  `VersionPin` handoff to another thread.
- **`examples/durable_store.rs`** — there is no persistence example at all,
  despite persistence being what makes the crate production-usable.
- **`docs/guide.md`** — one page, `cargo add` to a durable multi-table
  transaction. Not a book.
- **`lib.rs` crate docs pass** — docs.rs is the real landing page for most
  readers, and it does not currently mirror the README's framing.

---

## 4. Sequence

1. **Release 0.1.2.** Days, near-zero risk, existing checklist.
2. **Positioning pass** — README, `lib.rs` crate docs, `examples/time_travel.rs`,
   `examples/durable_store.rs`. Cheap, and the step most likely to produce
   feedback before weeks are committed to the key work.
3. **`Send` audit.** One day, independent, and its outcome determines what the
   guide can say about async.
4. **`Table<R, K = u64>`** → 0.2.0. Internal order: `PrimaryKey` trait +
   generic `Table` → indexes → `bulk_load`/`snapshot_stream` → WAL/checkpoint
   format bump → migration note.
5. **`docs/guide.md`**, written against the 0.2.0 API so it is not written
   twice.

### Plan decomposition

This spec covers a program, not a single change. **Each numbered step gets its
own implementation plan**, written when that step starts rather than up front —
steps 4 and 5 in particular should be planned only after step 2 has had a
chance to produce feedback. Steps 1–3 are small and independent enough to be
planned together.

### Accepted trade-off

The WAL format break in step 4 lands *after* 0.1.2 has been announced in step
2. Any early adopter with persisted data will have to rebuild it. Pre-1.0 with
a documented break, this is acceptable; the alternative — keys first, silence
until 0.2.0 — trades feedback for tidiness. **The 0.1.2 release notes must say
that an on-disk format break is coming in 0.2.0.**

---

## 5. Deferred directions

Recorded in `docs/BACKLOG.md` so they are retrievable rather than remembered:

- **Apply-path performance.** The residual gap versus a flat store, its cost
  decomposition, and the candidate attacks. Note the interaction with §2a: a
  dense-integer-key fast path becomes *easier* once the key type is generic,
  because there is finally something to specialize on.
- **1.0 hardening.** SSI gaps, WAL fault-injection testing, T-parametric formal
  development, the small-T builder bug, Gate B harness rot, CJK tokenization.

## 6. Testing

- **§2a** is covered by the existing suite by construction: the defaulted type
  parameter means every current test exercises the `K = u64` path unchanged. New
  tests cover non-`u64` keys across insert/get/update/delete/range, secondary
  indexes over a non-`u64` primary key, `bulk_load` ordering, WAL round-trip and
  recovery for each blanket-impl key type, rejection of a stale WAL format
  version, and MultiWriter conflict/no-conflict on hashed write-set keys
  (including a forced-collision case proving a collision yields a spurious
  conflict rather than a missed one).
- **§2b** is answered by a compile test either way: the negative test is either
  updated to assert `Send + !Sync` or kept with a comment recording why.
- **Examples** are gated by CI already building `examples/`; both new examples
  must run to completion, not merely compile.
- Gates unchanged: `cargo test`, `cargo test --features persistence`,
  `cargo clippy --all-targets --features persistence -- -D warnings`, and the
  consistency and formal workflows.
