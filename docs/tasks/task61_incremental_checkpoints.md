# task61: Incremental Checkpoints via CoW Structural Diff

**Status:** Implemented (Tasks 1–8, feature-complete and tested); crossover and
recovery-time measurement pending on the bench host (see "Measurements" below).
**Related:** `docs/superpowers/specs/2026-08-08-incremental-checkpoints.md` (plan),
`docs/superpowers/specs/2026-08-08-incremental-checkpoints-design.md` (full design spec),
`docs/tasks/task13_persistence.md`, `docs/tasks/task15_three_phase_consistent_persistence.md`,
`docs/tasks/task53_version_pin_gc.md` (base-retention's memory story shares its vocabulary),
`docs/tasks/task59_table_lifecycle_races.md` (the reference for which table
create/drop/recreate interleavings this design's lifecycle handling must cover).

---

## 1. Motivation

`Store::checkpoint()` wrote a *full* serialized snapshot of every registered
table on every call (`serialize_table` walks the whole B-tree and
bincode-encodes every row). A store that commits ten rows between checkpoints
re-serialized all N rows to move a ten-row watermark. Checkpoint cost scaled
with dataset size instead of change volume, and it sits on the critical path
of two things operators care about: WAL prune cadence (the WAL cannot trim
past the last checkpoint) and recovery time. The workaround was to checkpoint
less often, which trades one problem for the other.

---

## 2. The CoW-diff insight

UltimaDB is copy-on-write. When a commit forks a new snapshot, every subtree
it did not touch is *literally the same `Arc<BTreeNode>`* as in the previous
snapshot — not a structurally-equal copy, the same allocation. Two snapshots
of the same table can therefore be diffed by pointer identity: at each pair
of cursors, if the two subtree roots are `Arc::ptr_eq`, the whole subtree is
skipped — everything under it is provably unchanged, by construction, with no
recursion into it. Complexity is `O(changed + height)`, not `O(N)`; nothing
about the diff depends on how large the unchanged part of the tree is.

`BTree::diff` (`src/btree.rs:554`) implements this as a dual-cursor ordered
merge over two trees, yielding a `Change<K, V>` (`Added` / `Updated` /
`Removed`) iterator in ascending key order. `Updated` still needs one more
check beyond "keys differ": two equal keys can carry value `Arc`s that are
themselves `ptr_eq` (a key whose row commit round-tripped it back to the same
value), so `diff` treats those as unchanged too rather than emitting a
same-value update.

This is a direct payoff of the CoW design, not something bolted on. It is
also strictly cheaper than the answer the LSM world has to the same problem.
WiscKey-style key/value separation exists to stop compaction from rewriting
whole *values* just to move a *key's* position in the merge order — the value
itself didn't change, but the LSM has no way to know that without comparing
it, so it gets copied along with the keys around it. UltimaDB never has that
problem: an unshared `Arc` *is* the "this changed" signal, for free, with no
comparison of value bytes at all. There is nothing here for a value log to
avoid rewriting, because nothing unchanged is ever touched in the first
place. (Origin note: this design was prompted by reading a WiscKey/LSM
survey; the transferable idea was "stop paying to move what didn't change,"
and the CoW structural diff is the UltimaDB-native form of that idea — see
the design spec's "Origin" section.)

---

## 3. File format

Deltas share the `checkpoint_{version}.bin` filename namespace with full
checkpoints, distinguished by a `kind` byte in the header
(`src/checkpoint.rs:1-24`):

```text
[magic: 4 bytes "ULDB"]
[format_version: u32]              // 2
[kind: u8]                         // CheckpointKind: Full=0, Delta=1
[snapshot_version: u64]
[base_version: u64]                // Delta only
[num_tables: u32]
for each table:
    [entry_kind: u8]               // TableEntryKind: Unchanged=0, Delta=1, Full=2, Dropped=3
    [name_len: u32][name: bytes]
    [data_len: u64][serialized table data: bytes]   // Full/Delta entries only
[crc32: u32]
```

`FULL` is byte-identical to the pre-task61 format apart from the
version/kind bytes, so a full checkpoint is a self-contained chain of length
one by construction — nothing about reading one changed.

**The four `TableEntryKind` values** (`src/checkpoint.rs:72-81`), one per
table per checkpoint file:

- **`Unchanged`** — byte-identical to the base version; no payload at all.
  The common case for a delta over a store where most tables are quiet
  between checkpoints.
- **`Delta`** — changed rows only, produced by `TableInfo::diff_table`
  (`src/registry.rs:560`), which runs `BTree::diff` against the base table
  and encodes each change as `[klen][key][op: Put=0|Del=1][Put: rlen, record]`,
  prefixed by an explicit `next_id` field (see §5). Per-key length caps
  (`check_encoded_key_len`) are enforced on this path too — a key the delta
  accepts and the WAL refuses would be a row that survives one durability
  path and destroys the other (the task56 hazard, reused here rather than
  re-litigated).
- **`Full`** — the whole table inline, via the same `serialize_table` a
  from-scratch checkpoint uses. Used for a table created since the base (no
  prior version to diff against), and for a table dropped and recreated with
  a different concrete `R`/`K` (detected by `diff_table`'s downcast on the
  *base* side failing — the registry's `key_type_id`/`key_type_name`
  guard already existed for exactly this identity check, so the delta path
  reuses it instead of inventing a second one).
- **`Dropped`** — present in the base, gone in this version; no payload.

Deliberately sharing the filename namespace (rather than giving deltas their
own pattern) is a safety property, not a naming convenience: an *older*
binary that doesn't know about deltas reads whichever file is newest, sees a
`kind`/`format_version` byte it doesn't recognize, and fails its format check
loudly. The alternative — an old binary that can't see delta files at all —
would silently fall back to an older full checkpoint whose WAL tail has
already been pruned, which is the exact silent-data-loss failure this design
exists to avoid (see §5).

---

## 4. Chain resolution and recovery

`find_head_chain` (`src/checkpoint.rs:698`) resolves the directory's newest
checkpoint file backward to its nearest `Full` ancestor by following each
delta's `base_version` back-pointer, reading only each file's small fixed
header (`HEADER_PREFIX_LEN`, `src/checkpoint.rs:534`) — a chain walk costs
`O(chain length)` in bytes read, not `O(sum of every file's full size)`, even
though a chain can have arbitrarily many full-table-sized deltas behind the
head. Each hop is pinned to the filename it was reached by (`expected_version`
in `walk_chain_from`) specifically to make a two-file reference cycle
detectable rather than an infinite loop — two files can each individually
satisfy "my base is older than me" while still pointing at each other, so
that check alone does not bound the walk.

`load_chain` (`src/checkpoint.rs:891`) then applies the resolved chain
base-first: load the full, then replay each delta's `Put`/`Del`/`next_id` in
version order, honoring `Unchanged`/`Full`/`Dropped` table entries as it goes.

---

## 5. Why a broken chain is fatal, not degraded

A missing or corrupt mid-chain file is `Error::CheckpointChainBroken`
(`src/error.rs:139`), a hard failure. It is **not** handled by falling back
to an older full checkpoint plus the WAL, and this is a deliberate design
choice, not a missing feature:

By the time a delta checkpoint exists, `checkpoint()` has already pruned the
WAL up to the delta's version (the ordering is write delta → fsync → verify
ancestors present → prune WAL → cleanup — see `src/store.rs:1010` onward).
If the chain that delta depends on breaks, an older full checkpoint plus
whatever WAL survives reconstructs **less state than was actually
committed** — the commits that happened between the older full and the
broken delta are gone from the WAL and gone from the broken chain. Recovery
returning success with a truncated dataset is silent data loss with no error
anywhere an operator would see it. Refusing to start is strictly better:
it turns a would-be-silent gap into a loud, diagnosable failure at the moment
it matters most.

The `tmp`+rename write discipline means this is not a routine occurrence: a
crash during a delta write leaves either the complete previous file or the
complete new file, never a partial one, so a *missing* file below the walk's
starting point just means the head is the previous (complete) checkpoint —
a normal, fully recoverable chain. `CheckpointChainBroken` only fires when a
file the walk has already committed to, by reading a `Delta` header that
names it as `base_version`, turns out not to exist — genuine corruption
(disk-level, or a checkpoint file deleted out from under a live chain by
something other than `cleanup_old_checkpoints`, which itself is built to
never delete a chain ancestor — see §6).

---

## 6. Base-retention memory trade

Diffing needs the last-checkpointed snapshot to still be in memory, so
`checkpoint()` holds the `Arc<Snapshot>` it just wrote as the diff base for
the *next* call (no separate `VersionPin` type was introduced for this —
`checkpoint()` already holds the `Arc`, and holding it already **is** the
pin; a newtype over the same `Arc` would add a type without adding a
guarantee). This is the design's real cost: a retained base snapshot keeps
its unshared nodes alive even after the live tree has moved past them, and
the longer the gap between checkpoints, the more of the base is unshared and
therefore pinned. Incremental checkpointing trades **memory for I/O**.

`StoreConfig::checkpoint_chain_max: usize` (`src/store.rs:179`) bounds this:
one full checkpoint followed by at most `checkpoint_chain_max - 1` deltas
before the next `checkpoint()` call is forced back to a full, which drops
the pin and starts the trade over from zero. **The default is `1`** — every
checkpoint is full, `checkpoint_chain_len` never advances past the point
where a base is retained, and `Store::new` rejects `0` outright. At the
default, incremental checkpoints are **not observable at all**: the code
path, the file format's `kind` byte, and the memory profile are exactly what
they were before this feature existed. Turning the feature on is opt-in via
raising the knob, not a behavior change anyone gets by upgrading.

Cleanup (`cleanup_old_checkpoints`, `src/checkpoint.rs:934`) keeps its
pre-existing invariant — never delete a checkpoint file newer than the
version a caller told it to keep — and adds a second one: never delete a
file that is an ancestor of the resolved head chain, even if that file is
older than `keep_version`. A delta's ancestors are exactly what makes it
loadable; deleting one out from under a live chain is precisely the
condition §5 exists to make loud rather than silent.

Two situations force a full checkpoint regardless of chain position, both
because there is no honest base to diff against:

- **After `recover()`.** The snapshot recovery rebuilds is not the exact
  snapshot any on-disk file holds (WAL replay carries it past the chain
  head's version), so nothing on disk is a base a delta could truthfully
  name.
- **After `bulk_load`.** A bulk load installs a wholly new tree; diffing it
  against the pre-load tree would degenerate into rewriting almost
  everything anyway, so `checkpoint_and_prune_after_bulk` goes straight to
  full rather than paying diff overhead for no benefit.

---

## 7. The `next_id` subtlety

This is the least obvious part of the design, and it cost a Critical bug
during implementation: **a row inserted and deleted inside the same delta
interval is invisible to `BTree::diff`.** The row never appears in the base
tree and never appears in the new tree either — it existed only transiently,
between two checkpoints, and the diff (correctly) reports no `Added`/`Removed`
for a key that is absent on both sides. That is the right answer for the
*rows*. It is the wrong answer for the *auto-increment counter*: the id that
row consumed must never be reissued, or two different rows across two
different recovery paths (chain replay vs. full-checkpoint-at-the-same-version)
would end up with different, non-reproducible contents for the same primary
key — a correctness break, not just a cosmetic one.

The fix is that `diff_table` writes the table's `next_id` **explicitly and
unconditionally** into every `Delta` entry's payload (`src/registry.rs:581`),
independent of whatever the row-level diff does or does not emit. Recovery's
chain replay applies this field via `replay_advance_next_id`
(`Table::advance_next_id_to`, taking the max against whatever the accumulator
already holds — never regressing it), rather than deriving `next_id` from the
replayed rows. A rows-only equivalence test — "recovered rows match a full
checkpoint's rows" — does not catch this: rows can match exactly while
`next_id` silently drifts, and the next insert after recovery reissues an id
that was already used and released. The regression test that exists for this
(`a_chain_replays_next_id_past_a_row_inserted_and_deleted_within_one_interval`,
`src/checkpoint.rs:2154`) constructs exactly that interval — insert id 3,
delete id 3, all within one delta — and asserts the chain-recovered table's
`next_id` matches the full-checkpoint path's, not just that both tables have
the same rows.

---

## 8. Testing

The correctness risk concentrates in `diff` and in chain recovery, so both
get oracle-based property tests (`proptest`, a new dev-dependency justified
by a diff whose failure mode — a silently dropped key — is exactly the kind
of thing shrinking is good at isolating) rather than hand-picked examples:

| Test | What it covers |
|---|---|
| Diff oracle (proptest) | Random op sequences on two snapshots; `diff` output must equal the brute-force difference of full key/value scans |
| Chain equivalence (proptest) | Random op sequences checkpointed under a random chain policy; recovered store must be byte-identical to one recovered from a full checkpoint at the same version — the test that would catch a dropped key |
| Subtree-skip effectiveness | `diff` must visit `O(changed)` nodes, not `O(N)` — correctness alone isn't the bar; a diff that is right but silently walks the whole tree delivers none of the benefit |
| Chain break | Delete/corrupt a mid-chain delta; recovery must fail with `CheckpointChainBroken`, never succeed with partial data |
| Lifecycle | Create / drop / drop-and-recreate-with-a-different-type across a delta boundary |
| Forced-full paths | Post-`recover()` and post-`bulk_load` checkpoints are full |
| Downgrade | A delta-headed directory read by the v1-format-check path fails loudly, CRC-honestly |
| `next_id` across a create+delete interval | See §7 |

---

## 9. Measurements

Checkpoint cost is serialize-plus-I/O, and per `CLAUDE.md` the local sandbox
has a **±2x noise floor** — no perf conclusion, ratio, or "X is faster than
Y" claim may be drawn from a run there. The two questions below can only be
answered on the `bench-infra` NVMe host, and **have not been run there yet**
(that requires the repository owner's explicit authorization to provision
billable AWS resources, which this task did not seek). The bench that will
answer them is ready and lives at `benches/checkpoint_delta.rs`; it was run
locally only to confirm it compiles and completes without panicking (it does
— `cargo bench --bench checkpoint_delta --features persistence`, full 8-cell
crossover sweep plus the 5-length recovery sweep, no errors), and no timing
from that run is reported here or anywhere else.

### 9a. Crossover: `checkpoint()` cost vs. dirty fraction

**PENDING — fill in from a `bench-infra` run.**

What goes here: a table (or the criterion HTML report path) with `checkpoint()`
wall time and bytes written, for dirty fractions `{1%, 10%, 50%, 100%}` of a
fixed ~100k-row dataset, at `checkpoint_chain_max = 1` (always full) versus
`checkpoint_chain_max = 8`, both durability tiers. The datapoint this task
plan cares about most is **where the crossover sits** — the dirty fraction
above which a delta stops being cheaper than a full, since a 100%-dirty delta
is expected to lose (it pays the ordered diff *and* per-key framing on top of
what a full write already pays).

Produced by:

```bash
cd bench-infra && make bench-oneshot TARGET=checkpoint-delta
make status     # confirm nothing is left running afterward
```

Results land in `bench-out/dist/<ts>/`; `benches/checkpoint_delta.rs`'s doc
comment has the equivalent local-only (non-authoritative) invocation.

### 9b. Recovery time vs. chain length

**PENDING — fill in from a `bench-infra` run.**

What goes here: `Store::recover()` wall time for chain lengths
`{1, 2, 4, 8, 16}` (one full plus 0–15 deltas, each dirtying ~1% of a fixed
row count — see `bench_recovery_vs_chain_length` in `benches/checkpoint_delta.rs`
for the exact construction). This is the curve that should set §10's
recommended default, once it exists.

---

## 10. Recommended `checkpoint_chain_max`

**Remains `1` (always full — pre-task61 behavior, unchanged).** This is not
a conservative placeholder pending a "real" answer computed elsewhere in this
doc — it is the actual, currently-justified recommendation, because §9's
crossover and recovery-time curves have not been measured yet. Raising the
default is a follow-up change, and it must be justified by §9b's recovery-time
curve specifically (the memory-vs-I/O trade in §6 is what a higher chain
length buys and what it costs; the recovery-time curve is what bounds how
high is still safe to default to) — not by an unmeasured guess about where
the crossover in §9a probably sits. Anyone changing this default should
replace the "PENDING" blocks in §9 with the real bench-host numbers first.

---

## 11. Files changed

| File | Change |
|---|---|
| `src/btree.rs` | `BTree::diff`, `Change<K, V>`, the subtree-skipping dual-cursor primitive |
| `src/registry.rs` | `diff_table`, `TableInfo::diff_table` fn pointer, `next_id` carried explicitly in delta payloads |
| `src/checkpoint.rs` | v2 format (`CheckpointKind`, `TableEntryKind`), `write_delta_checkpoint`, `find_head_chain`/`walk_chain_from`, `load_chain`, chain-aware `cleanup_old_checkpoints` |
| `src/store.rs` | `StoreConfig::checkpoint_chain_max` (builder + default), chain-length bookkeeping in `checkpoint()`, forced-full paths (post-recovery, post-bulk-load) |
| `src/error.rs` | `Error::CheckpointChainBroken`, `Error::TableTypeChanged` |
| `benches/checkpoint_delta.rs` | Crossover and recovery-vs-chain-length bench (this task) |
| `docs/tasks/task61_incremental_checkpoints.md` | This file |
| `docs/superpowers/specs/2026-08-08-incremental-checkpoints-design.md` | Retained design history; not changed by this task |
