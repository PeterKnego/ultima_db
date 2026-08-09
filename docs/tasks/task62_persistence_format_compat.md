# task62: Persistence Format Backward Compatibility

**Status:** Implemented (Tasks 1–5).
**Related:** `docs/superpowers/specs/2026-08-09-persistence-format-compat-design.md` (design spec),
`docs/superpowers/plans/2026-08-09-persistence-format-compat.md` (implementation plan),
`docs/tasks/task61_incremental_checkpoints.md` (the checkpoint-container v2 bump this undoes the
read-side break of), `docs/tasks/task56_arbitrary_primary_keys.md` (the table-payload v2 bump),
`docs/how-to/migrate-from-0-2-to-0-3.md` (operator-facing migration guide).

---

## 1. Motivation

Task61 (incremental checkpoints) bumped the checkpoint container's `FORMAT_VERSION` 1 → 2 and,
correctly at the time, rejected anything still at v1. That rejection was broader than the format
change actually required: nothing about delta chains makes a v1 *container* unreadable, and the
outright rejection stranded every persistence directory ever written before task61 — which is
every 0.2.0 and 0.3.0 deployment that exists. This task adds a reader for the old container and
both generations of table payload it can hold, so upgrading no longer requires an export/re-import
detour for the checkpoint half of the data. It deliberately does **not** extend the same treatment
to the WAL — see §4.

## 2. Two independent version axes

A checkpoint file has two version numbers that were bumped in different releases, for different
reasons, and a reader has to track both:

- **Container format** (`FORMAT_VERSION` in `src/checkpoint.rs:39`, currently `2`): the file-level
  framing — magic, `CheckpointKind`, per-table `TableEntryKind`, whether an entry is a delta or a
  full payload. Bumped once, by task61, to add delta-chain support. The module doc comment at
  `src/checkpoint.rs:9-22` is the current (v2) framing; v1 (pre-task61) had no `kind` byte, no
  `base_version`, and no per-table entry kind — every v1 file is an implicit, self-contained full
  checkpoint (`src/checkpoint.rs:310-313`).
- **Table payload format** (`TABLE_FORMAT_V2` in `src/registry.rs:466`, currently `2`): the
  per-table byte layout inside one checkpoint entry — row keys, record bytes, the auto-increment
  counter. Bumped once, by task56 (0.3.0, arbitrary primary keys), to add a `key_type` tag and
  variable-length key framing in place of task56's predecessor's fixed 8-byte `u64` id.

Because these two axes moved independently, **a v1 container can hold either generation of table
payload**, and both combinations are real, not hypothetical:

| Release | Container | Table payload |
|---|---|---|
| 0.2.0 | v1 | v1 (`u64` ids, no key-type tag) |
| 0.3.0 | v1 (task61 hadn't shipped yet) | v2 (arbitrary-key tag, task56) |
| this build | v2 | v2 |

`tests/format_compat.rs`'s `a_0_3_0_checkpoint_recovers` test exists specifically to pin the
0.3.0 combination (v1 container / v2 payload) — the one a reader that only ever tested against a
hand-rolled "old format" fixture would be most likely to get wrong, because it does not match
either endpoint of the version bump that motivated writing the reader in the first place.

## 3. Why the dispatch is exact, not heuristic

Two independent dispatch points, both keyed off a byte that is provably absent from the other
format rather than a version counter that could coincidentally collide:

- **Container**: `deserialize_snapshot` (`src/checkpoint.rs:161`) reads the `format_version`
  varint and matches on it directly (`src/checkpoint.rs:205-214`) — `1` routes to
  `deserialize_snapshot_v1` (`src/checkpoint.rs:314-370`), `FORMAT_VERSION` continues into the v2
  body, anything else is refused (see §5). `read_header` (`src/checkpoint.rs:609`, the
  bounded-prefix reader `find_head_chain`/`find_chain_for_version` walk chains with) duplicates
  this same three-way branch independently, because it cannot delegate — it only reads
  `HEADER_PREFIX_LEN` bytes, not the whole file. A v1 file is reported as `(Full, version, None)`
  (`src/checkpoint.rs:626-635`) so the chain walk stops there instead of looking for a
  `base_version` that a v1 header has no field for.
- **Table payload**: `deserialize_table` (`src/registry.rs:759-767`) looks at the payload's first
  byte. `TABLE_MAGIC_V2` is `0xFF` (`src/registry.rs:463`), and `0xFF` is not a legal `bincode`
  varint tag — a v1 payload's first field is a `next_id: u64` varint, whose leading byte is either
  a literal `0..=250` or a width marker `251..=253` (`254` is u128-only, `255` is not a legal tag
  at all). So no v1 payload can ever begin with `0xFF`, and the branch is exhaustive: `0xFF` means
  v2, anything else means v1 (`src/registry.rs:753-767`). This mirrors the identical `WAL_ENTRY_MAGIC`
  decision in `src/wal.rs:184-201` for the same reason, and it's why v1 acceptance needed no
  changes to how v2 payloads are recognized — every non-`0xFF` leading byte already meant "not v2"
  before this task; it used to mean "reject," and now means "route to `deserialize_table_v1`."

## 4. The key-type check, and the corruption it prevents

`deserialize_table_v1` (`src/registry.rs:785-848`) is the riskiest new code in this task, because
a v1 payload carries no key-type tag at all — task56 added that tag, and v1 predates task56. The
function's first move, before decoding a single row, is to check that the *destination* table's
key type is `u64` (`src/registry.rs:786-794`):

```rust
if K::KEY_TYPE_ID != <u64 as PrimaryKey>::KEY_TYPE_ID {
    return Err(Error::Persistence(format!(
        "v1 table payloads are u64-keyed (they predate arbitrary primary keys), but this \
         table is registered with key type {}. ...",
        ...
    )));
}
```

Without this check, reading a v1 payload into a `String`-keyed (or any non-`u64`-keyed) table
would reinterpret each row's raw 8 bytes as a `String`/`Vec<u8>`/`i64`/etc. encoding. That is not
a crash: `PrimaryKey` encodings are order-preserving, so the reinterpreted keys still pass
`deserialize_table_v1`'s own strict-ascending-order validation (`src/registry.rs:836-845`, the
same check the v2 reader applies) — the corrupted table looks internally consistent all the way
through. This is precisely the failure mode `TABLE_MAGIC_V2`'s v2-era key-type check
(`src/registry.rs:889-892`, `key_type_mismatch_msg`) already guards against for two *v2* tables
with different `K`; `deserialize_table_v1` is the same guard extended to the one case v2's own
tag can't cover, because the tag doesn't exist yet at that layer. It is also the reason the
original outright-rejection-of-v1 existed in the first place — accepting v1 unconditionally would
have reintroduced the exact silent-corruption path the all-formats-record-and-validate-key-type
design (task56, see the CHANGELOG's 0.3.0 entry) was built to close.

`tests/format_compat.rs::a_0_2_0_checkpoint_is_refused_for_a_differently_keyed_table` and
`src/registry.rs`'s `a_v1_payload_is_refused_for_a_non_u64_keyed_table` /
`v1_key_type_rejection_message_explains_the_hazard` pin this from both the integration and unit
level.

## 5. What is deliberately not covered: the WAL

The WAL format also changed in 0.3.0 (task56): v1 addressed rows by a bare `u64` id with no
format marker at all; v2 prefixes every entry payload with `[magic 0xFF][format 2]`
(`src/wal.rs:184-210`). Unlike the checkpoint table payload, **v1's absence of a marker is not
recoverable by inference** — `check_entry_header` (`src/wal.rs:219-253`) says so directly: a
leading byte that isn't `0xFF` "is either a pre-0.3.0 WAL or a corrupted one... this byte alone
cannot tell them apart." Reading a WAL entry has to trust that its length and CRC framing are
intact before it can even ask "what format is this," and a v1 entry offers nothing upstream of
that framing to distinguish "old but valid" from "torn write" from "bit rot." Extending this
task's read support to the WAL was ruled out of scope for that reason, not from a level-of-effort
judgment — there is no exact dispatch to write, only a heuristic one, and this codebase's stance
elsewhere (the `0xFF` magic bytes in §3, `formal/tla/wal/`'s existence at all) is that WAL
correctness does not get heuristics.

**Consequence — the partial-recovery boundary:** the operative gate is earlier than `recover()`.
`Store::new` (`src/store.rs:548`) constructs the store's `WalHandle` inline, for every
`Persistence::Standalone` config, in every `Durability`/`WalWrite` combination
(`src/store.rs:560-592`) — and that construction opens the WAL sink (`FileSink`/`BufferedFileSink`/
`PreallocFileSink`), which calls `reject_unreadable_wal` (`src/wal.rs:1034-1075`) before the store
object exists at all. `reject_unreadable_wal` inspects the first record's header
(`check_entry_header`, `src/wal.rs:219-253`) and is deliberately *not* deferred to `recover()`: the
doc comment on it spells out why — without an open-time check, a store on a legacy directory would
construct cleanly, accept commits, and return `Ok` from a `Durability::Consistent` `commit()`
before the next restart's `recover()` ever discovers the WAL is unreadable, by which point the
only remedy destroys exactly the commits that were just acknowledged as durable. So for a
non-empty legacy WAL, `Store::new` itself returns `Err` — `recover()` is never reached, and its own
independent `scan_wal` call (`src/store.rs:1284`, which would *also* reject the same file, since
`scan_wal` runs every entry through `check_entry_header` before filtering by version,
`src/store.rs:1288-1291`) never gets the chance to run. Net practical shape of "recovery reaches
the last checkpoint and no further":

- an **empty or absent** `wal.bin` (freshly checkpointed, no writes since) — `reject_unreadable_wal`
  accepts it (nothing to misread), `Store::new` succeeds, and `Store::recover()` afterward loads
  exactly the checkpoint's state;
- a **non-empty legacy-format** `wal.bin` — `Store::new` fails outright, on the WAL's own error
  (unrelated to this task's checkpoint changes), before a store object — and so before
  `recover()` — exists at all. The checkpoint being readable doesn't help until the incompatible
  WAL is out of the way (moved aside, once its rows are known not to matter, or exported through
  the old binary first).

This is why the operator-facing guidance (`docs/how-to/migrate-from-0-2-to-0-3.md`) spells out
both cases rather than asserting a single "recovery stops at the checkpoint" story — the second
case is a hard error, not a graceful degrade, and documentation that implied otherwise would send
an operator into `recover()` expecting a partial success they will not get.

A 0.3.0 directory has no such boundary: its WAL was already v2 (task56 shipped before task61), so
`Store::recover()` reads it in full — the same call that only gets a ≤0.2.x directory to its last
checkpoint recovers a 0.3.0 directory completely.

## 6. The rejection messages

The two checkpoint version-check sites (`src/checkpoint.rs:205-214` in `deserialize_snapshot`,
`src/checkpoint.rs:636-641` in `read_header`) previously described the export/re-import migration
task61 required for *any* pre-task61 checkpoint. Now that v1 is a genuinely readable format, that
prose no longer applies to any file this build can be handed short of one from a release that
doesn't exist yet — the `_` arm in both sites is reached only when `fmt_version` is neither `1`
nor `FORMAT_VERSION`, i.e. a container version *above* what this build understands. Both messages
were rewritten to say exactly that: the file is from a newer UltimaDB, upgrade the binary. The
literal prefix `"unsupported format version: {fmt_version}"` is unchanged — `a_future_format_version_is_still_refused`
and `a_future_format_version_is_still_refused_by_read_header` (`src/checkpoint.rs`, `#[cfg(test)]`
module) assert on that substring, and both still pass unmodified.

The third, structurally similar site in `apply_delta_file` (`src/checkpoint.rs:828-834`) was
already short — deltas didn't exist before task61, so there is no old-format case for it to
describe, and it was left as-is.

A sibling message in `src/wal.rs`'s `check_entry_header` (the "no v2 format marker" rejection —
see §5) made the same claim in the opposite direction and went stale for the same reason: it
told an operator hitting a v1 WAL that "pre-0.3.0 checkpoints are rejected too, so checkpointing
with the old build does not migrate the data." That was true when written (before this task) and
false afterward, and it is the one message an operator in exactly the ≤0.2.x situation actually
sees, so leaving it stale would have told them the checkpoint they now have working access to was
useless. Rewritten to state plainly that this build *can* read the checkpoint, only the WAL is
the problem, and to lead with the actionable remedy (move `wal.bin` aside and recover from the
checkpoint, losing anything after it) before the fallback (export/re-import to keep those rows).
The leading text existing tests pin — `"no v2 format marker"`, `"pre-0.3.0"`, `"corrupt"`,
`"Store::bulk_load"` — is preserved; see `strict_scan_rejects_a_genuine_v1_wal`
(`src/wal.rs`, `#[cfg(test)]`).

## 7. The fixture rule

`tests/fixtures/formats/README.md` states the rule this feature's tests depend on: a fixture must
be real bytes produced by an actual released build, not a hand-rolled reconstruction of what the
old format is believed to have been — the belief is exactly what the fixture exists to check.
`v0_2_0/checkpoint_2.bin` and `v0_3_0/checkpoint_2.bin` were generated from `5cfae9a` (0.2.0
release commit) and tag `v0.3.0` respectively, both still buildable under the current toolchain.
Only the checkpoint file is committed from each generated directory; `wal.bin` is deliberately
excluded, so the fixture set cannot accidentally imply WAL coverage that does not exist. The
README's forward-looking rule — a future format bump must add a fixture for the version it
supersedes *in the same commit* — is what keeps this guarantee from lapsing silently on the next
refactor; a compatibility claim with no committed old-format bytes behind it is unverifiable by
construction.

## 8. Testing

| Test | What it covers |
|---|---|
| `tests/format_compat.rs::a_0_2_0_checkpoint_recovers` | v1 container + v1 payload, the 0.2.0 shape |
| `tests/format_compat.rs::a_0_3_0_checkpoint_recovers` | v1 container + v2 payload, the 0.3.0 shape — the combination that only exists because the two axes bumped in different releases |
| `tests/format_compat.rs::an_old_checkpoint_round_trips_through_the_current_writer` | reading is lossless: recover → checkpoint → recover again reproduces the same rows |
| `tests/format_compat.rs::an_old_checkpoint_preserves_next_id` | the auto-increment counter survives, not just the rows (task61 had a `next_id`-specific regression) |
| `tests/format_compat.rs::a_0_2_0_checkpoint_is_refused_for_a_differently_keyed_table` | the key-type guard (§4) fires end-to-end |
| `src/checkpoint.rs`: `a_v1_container_is_read_as_a_full_checkpoint`, `a_v1_container_reports_as_full_with_no_base`, `a_v1_container_with_a_broken_crc_is_still_refused` | container-level v1 handling, including that CRC verification still applies to v1 files |
| `src/checkpoint.rs`: `a_future_format_version_is_still_refused[_by_read_header]` | backward compatibility does not weaken forward rejection — both message sites (§6) |
| `src/registry.rs`: `deserialize_accepts_v1_format`, `deserialize_reads_a_v1_payload_whose_first_byte_collides_with_the_v2_format_byte`, `a_v1_table_payload_round_trips_into_the_current_shape`, `an_empty_v1_table_payload_is_read_as_an_empty_table`, `a_v1_payload_is_refused_for_a_non_u64_keyed_table`, `v1_key_type_rejection_message_explains_the_hazard`, `a_truncated_v1_payload_errors_rather_than_panicking`, `a_v1_payload_with_out_of_order_rows_errors_rather_than_corrupting_the_tree`, `a_v1_payload_with_duplicate_row_ids_errors_rather_than_corrupting_the_tree`, `a_v1_payload_with_trailing_junk_errors_rather_than_silently_ignoring_it`, `a_v1_payload_with_understated_count_errors_rather_than_losing_rows` | table-payload-level v1 reader: dispatch, round-trip, and every trust-boundary check the v2 reader already had |

## 9. Files changed

| File | Change |
|---|---|
| `src/checkpoint.rs` | `deserialize_snapshot_v1`; v1 branch in `deserialize_snapshot`'s dispatch and in `read_header`; rejection messages at both version-check sites rewritten for the forward-only case |
| `src/registry.rs` | `deserialize_table_v1`; leading-byte dispatch in `deserialize_table`; key-type guard |
| `src/wal.rs` | `check_entry_header`'s v1-WAL rejection message rewritten: it no longer claims checkpoints are rejected too, and leads with the move-`wal.bin`-aside remedy |
| `tests/fixtures/formats/` | Golden `checkpoint_2.bin` fixtures from 0.2.0 and 0.3.0, plus the provenance/regeneration README |
| `tests/format_compat.rs` | Integration tests reading the fixtures end-to-end |
| `tests/checkpoint_chain_equivalence.rs` | Adjusted for the now-successful v1 read path |
| `docs/how-to/migrate-from-0-2-to-0-3.md` | Split into the 0.3.0 (just upgrade) and ≤0.2.x (checkpoint-only, WAL still blocks unattended `recover()`, with the explicit move-`wal.bin`-aside/data-loss tradeoff) cases |
| `docs/reference/key-encoding-and-formats.md` | Corrected two stale claims: "pre-0.3.0 data is refused with no compatibility branches" (checkpoint side now has one) and "a v1 checkpoint is rejected at `recover()`" (table payload) |
| `CHANGELOG.md` | `Unreleased` entry recording the compatibility restoration; task61's breaking-change entry amended to say it no longer fires for any reachable checkpoint version |
| `docs/tasks/task62_persistence_format_compat.md` | This file |
