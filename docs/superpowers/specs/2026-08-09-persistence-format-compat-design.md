# Persistence Format Compatibility: Newer Releases Read Older Formats

**Status:** design
**Date:** 2026-08-09
**Task doc (on completion):** `docs/tasks/task62_persistence_format_compat.md`
**Depends on:** task61 (incremental checkpoints, PR #26) — which introduced the break that motivated this

## Problem

Every versioned on-disk format in UltimaDB currently **rejects** older versions:

| Surface | Constant | Old-version handling |
|---|---|---|
| Checkpoint file | `src/checkpoint.rs` — `FORMAT_VERSION: u32 = 2` | v1 rejected |
| Table payload | `src/registry.rs` — `TABLE_FORMAT_V2: u8 = 2` | v1 rejected |
| WAL entry | `src/wal.rs` — `WAL_FORMAT_VERSION: u8 = 2` | v1 rejected ("no compatibility branch") |

Each rejection was individually defensible — they fail loudly rather than misreading, and a store that cannot start never prunes its WAL. But the accumulated policy is that **an upgrade strands the operator's data**, with the only path being "keep the old binary around, re-export, re-import". That is not a policy anyone chose; it is three local decisions adding up.

The policy going forward: **a newer release reads every older format version it has ever written.** Writes are always current.

## The version matrix — the fact that shapes everything

The two formats were bumped in *different* releases, so the combinations that exist on disk are not a single lineage:

| Written by | Checkpoint file | Table payload | WAL |
|---|---|---|---|
| ≤ 0.2.x | v1 | v1 | v1 (no marker) |
| **0.3.0 (released, live on crates.io)** | **v1** | **v2** | **v2** |
| task61 (unreleased) | v2 | v2 | v2 |

Two consequences:

1. **A checkpoint-v1 file may contain *either* v1 or v2 table payloads.** The reader cannot infer the payload version from the container version; it must detect per payload. Fortunately that is exact — see below.
2. **The 0.3.0 case is the one that matters most and is the easier half.** Its table payloads and WAL are already current; only the *container* changed. Restoring a 0.3.0 directory needs nothing but a checkpoint-v1 container reader.

## Scope

**In scope:** reading checkpoint-file v1, and reading table-payload v1.

**Out of scope: the WAL.** Pre-0.3.0 WAL entries carry *no version marker at all*, so a v1 entry is byte-ambiguous with a corrupt one — `src/wal.rs` says exactly this: "this byte alone cannot tell them apart". Distinguishing them means structural guessing, and a wrong guess replays garbage as committed data. That risk is not worth taking for a format two releases old.

**State the consequence honestly rather than implying full coverage:**

- **A 0.3.0 directory becomes fully recoverable.** Its WAL is already v2, so checkpoint + WAL replay both work.
- **A ≤ 0.2.x directory becomes recoverable only up to its last checkpoint.** Rows committed after it are in a v1 WAL and stay unreadable. The existing migrate-by-export guidance remains correct for those, and `docs/how-to/migrate-from-0-2-to-0-3.md` should say so rather than being quietly superseded.

**Also out of scope:** writing old formats (no downgrade path). New features would have to degrade into old containers, doubling the writer surface for a case that rarely comes up.

## The guarantee

> A release reads any checkpoint file and any table payload written by any earlier release. It always writes the current format. Reading an older format is lossless for the data that format could represent; it is never silently lossy.

"Never silently lossy" is the operative half. Where an old format cannot express something the current one needs, the reader supplies the value the old format *implied* — and where no such value exists, it errors rather than guessing.

## Design

### 1. Detection

**Checkpoint file** — the container already begins `[magic "ULDB"][format_version: u32]`, so dispatch on `format_version`: `1 → read_v1`, `2 → read_v2`, anything higher → the existing "written by a newer UltimaDB, upgrade the binary" error. The whole-file CRC32 is verified before any version-driven parsing in both paths, exactly as today.

**Table payload** — dispatch on the first byte, and this is exact rather than heuristic. `TABLE_MAGIC_V2 = 0xFF` was chosen precisely because `0xFF` is not a legal bincode varint tag, and a v1 payload opens with the varint tag of `next_id` (`0..=250`, or a width marker `251..=254`). So:

- first byte `0xFF` → v2, parse as today
- any other first byte → v1
- empty payload → error

The existing comment in `src/registry.rs` records why a bare version byte of `2` would *not* have been sufficient (a v1 table that took exactly one insert encodes `next_id = 2` as `0x02`). That reasoning is what makes this dispatch safe; preserve it.

### 2. Checkpoint v1 container reader

v1 layout is the pre-task61 format:

```text
[magic "ULDB"][format_version: u32 = 1][snapshot_version: u64][num_tables: u32]
for each table: [name_len: u32][name][data_len: u64][table payload]
[crc32: u32]
```

Read it as a v2 `Kind::Full` whose every table entry is `TableEntryKind::Full` — that is what a v1 file *is*, and expressing it that way means one loader downstream rather than two. No `base_version`, no chain: a v1 file is always a complete chain of length one, so `find_head_chain` treats it as a base and never looks for ancestors.

The same checked-arithmetic discipline the v2 reader uses applies unchanged: every length in the payload is attacker-controlled, and the CRC only guards accidental corruption.

### 3. Table payload v1 reader

v1 layout, all bincode-varint:

```text
[next_id: u64][count: u64][id: u64, record]*
```

Three things it does not carry, and what the reader supplies:

- **No key type.** v1 predates task56, so `u64` is the *only* key type it could have held. The reader therefore decodes keys as `u64` — and **must verify the table is registered with `K = u64`**, erroring otherwise. This is the original rejection's real concern and it must survive: without the check, a v1 payload read into a `String`-keyed table would reinterpret bytes into garbage keys, and because the encoding is order-preserving the garbage would still pass ascent validation. The check turns a silent corruption into a named error.
- **No `has_next_id` flag.** v1 always carried a counter, so the reconstructed table gets `Some(next_id)`.
- **No key encoding.** v1 ids are raw `u64`; convert through `PrimaryKey::encode` so the in-memory table is identical to one built from a v2 payload.

Reuse `Table::from_bulk` on the decoded rows, as the v2 path does. Secondary indexes are not stored in either version and are rebuilt/redefined the same way — this change does not alter that (see the note in the task61 doc).

### 4. Where the version knowledge lives

Put the v1 readers behind the same registry closures the v2 readers use (`deserialize_table`), so callers stay version-agnostic. **No caller outside `checkpoint.rs`/`registry.rs` should learn that v1 exists.** A version check leaking into `Store::recover` is how the next format bump acquires a second, divergent dispatch site.

## Golden fixtures and CI — the part that keeps this from rotting

A compatibility guarantee with no committed old-format bytes is a guarantee that silently lapses on the next refactor. This repo has no fixture convention yet, so establish one:

- `tests/fixtures/formats/<version>/` holding **real bytes**, committed.
- Generate them **once, from the actual released tags** (`v0.1.1`, `v0.3.0` — both exist), not by hand: check out the tag, write a small store, copy the resulting `checkpoint_*.bin`. Hand-rolled fixtures encode what we *believe* the old format was, which is exactly the belief the test exists to check. Record the generating commit and procedure alongside them.
- A test that loads every fixture and asserts the recovered rows, `next_id`, and `latest_version` — the same triple task61 learned to compare, because a rows-only assertion passes while the counter is wrong.
- Additionally keep a **test-only v1 writer** for constructing edge cases (empty table, `next_id` at a varint width boundary, a table whose first byte would collide with a version marker). Validate the writer against the committed golden file so it cannot drift into fiction.
- Wire the fixture test into the `ci` workflow's persistence pass. It is milliseconds; there is no reason for it to be optional.

**The rule to write down:** a future format bump must add a fixture for the version it is superseding, in the same commit that bumps it. That is the discipline that would have prevented this whole task.

## Testing

1. **Golden-file recovery** — every committed fixture recovers to the expected rows/`next_id`/`latest_version`.
2. **Cross-product** — checkpoint v1 + table v1, and checkpoint v1 + table v2 (the 0.3.0 shape). Both exist in the wild; both need a fixture.
3. **Key-type refusal** — a v1 payload for a table registered with a non-`u64` key errors with a named error, and never returns `Ok`. Assert on the message, not just the variant.
4. **Round-trip through the current writer** — load v1, checkpoint, reload: the twice-loaded state equals the once-loaded state. This is what proves reading is lossless rather than approximately right.
5. **Corruption still detected in the v1 path** — a CRC-broken v1 file fails as loudly as a v1 file did before, and a truncated/hostile length errors rather than panicking.
6. **Forward rejection preserved** — a file claiming version 3 still fails with the "upgrade the binary" message. Backward compatibility must not weaken forward rejection.

## Risks

| Risk | Mitigation |
|---|---|
| A v1 payload silently read into a non-`u64`-keyed table | Explicit registered-key-type check; named error; dedicated test |
| Fixtures hand-rolled and therefore fictional | Generate from real released tags; validate the test-only writer against them |
| Guarantee rots on the next bump | Fixture-per-superseded-version rule, enforced in CI |
| v1 reader becomes a second parsing surface with its own bugs | Convert v1 into the v2 in-memory shape at the boundary; one loader downstream |
| Operators assume a ≤0.2.x directory is now fully recoverable | Documented explicitly: recoverable only to its last checkpoint; the v1 WAL stays unreadable |

## Estimate

~2–3 days. The container reader and the table reader are each small and mechanical; the fixture generation (checking out two old tags and producing real bytes) and the CI wiring are the bulk, and the key-type safety check is the part that deserves care.

## Follow-on, explicitly not included

`docs/how-to/migrate-from-0-2-to-0-3.md` and the rejection messages added in task61 both describe export/re-import as the only path. Once this lands, the 0.3.0 half of that guidance is obsolete and should be rewritten to say "just upgrade" — but the ≤0.2.x half remains correct. Update them in the same change that ships this, or they become actively misleading.
