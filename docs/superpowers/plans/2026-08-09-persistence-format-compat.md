# Persistence Format Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make this build read checkpoint files and table payloads written by every earlier UltimaDB release, so upgrading the binary no longer strands an existing data directory.

**Architecture:** Two independent version axes, each dispatched at its own boundary and converted into the current in-memory shape immediately, so nothing downstream learns that v1 exists. The checkpoint *container* dispatches on its `format_version` field; the *table payload* dispatches on its first byte, which is exact because `TABLE_MAGIC_V2 = 0xFF` is not a legal bincode varint tag. Correctness is pinned by golden fixtures generated from real released tags rather than hand-rolled bytes.

**Tech Stack:** Rust 2024, `bincode` 2 (varint `standard()` config for v1, explicit big-endian framing for v2), `crc32fast` via `crate::wal::crc32`.

**Spec:** `docs/superpowers/specs/2026-08-09-persistence-format-compat-design.md`

## Global Constraints

- `cargo clippy --all-targets --features persistence -- -D warnings` must pass; the default-feature build must also be clean. CI-gated.
- **Do not run `cargo fmt`.** Repo-wide rustfmt-version drift, no fmt gate — it rewrites unrelated lines. Match the file you are editing (4-space indent in `src/checkpoint.rs`, `src/registry.rs`).
- Persistence code is behind `#[cfg(feature = "persistence")]`. Test with `cargo test --features persistence`.
- **Backward compatibility must not weaken forward rejection.** A file claiming a version *newer* than this build must still fail with the "upgrade the binary" error. Every task that touches a version check must leave that path intact.
- **Never silently lossy.** Where an old format cannot express something the current one needs, supply the value the old format *implied*; where no such value exists, error. Never guess.
- The whole-file CRC32 is verified **before** any length-driven parsing, in every path. Lengths in a payload are attacker-controlled; the CRC only guards accidental corruption.
- Doc comments explain *why*, not *what*.
- Commit after every task.

## Ground truth (already captured — do not re-derive)

Both old trees build under the current toolchain (verified: ~9s each), and real fixtures were generated from them.

**0.2.0** (commit `5cfae9a`, never tagged) — checkpoint v1 **and** table payload v1:

```
554c4442  "ULDB"
01        format_version = 1        (bincode varint)
02        snapshot_version = 2
01        num_tables = 1
05 7573657273                        name_len=5, "users"
10        data_len = 16
          -- table payload v1, all bincode varint --
          03                         next_id = 3
          02                         count = 2
          01  05 616c696365  1e      id=1, "alice", age=30
          02  03 626f62      19      id=2, "bob",   age=25
e8405178  crc32
```

**v0.3.0** (tagged, released, live on crates.io) — checkpoint v1 containing table payload **v2**:

```
554c4442 "ULDB" | 01 fmt=1 | 02 ver | 01 ntables | 05 "users" | 47 data_len=71
  payload: ff 02          TABLE_MAGIC_V2, TABLE_FORMAT_V2
           00000004       key_type = u64's KEY_TYPE_ID
           01 00000008 0000000000000003    has_next_id, len, next_id = 3
           0000000000000002                num_entries = 2
           00000008 0000000000000001 00000007 05 616c696365 1e
           00000008 0000000000000002 00000005 03 626f62 19
1e3b5b81 crc32
```

This is the combination the spec calls out: **a v1 container may hold either v1 or v2 payloads**, so the two dispatches are genuinely independent.

## File structure

| File | Responsibility |
|---|---|
| `src/registry.rs` | Table-payload dispatch + the v1 payload reader (Task 2) |
| `src/checkpoint.rs` | Container dispatch + the v1 container reader, in both `deserialize_snapshot` and `read_header` (Task 3) |
| `tests/fixtures/formats/README.md` | How the fixtures were generated, and the rule for future bumps (Task 1) |
| `tests/fixtures/formats/v0_2_0/`, `v0_3_0/` | Committed real bytes (Task 1) |
| `tests/format_compat.rs` | Fixture-driven recovery tests (Tasks 1, 4) |

---

### Task 1: Golden fixtures from real releases

**Files:**
- Create: `tests/fixtures/formats/v0_2_0/checkpoint_2.bin`, `tests/fixtures/formats/v0_3_0/checkpoint_2.bin`
- Create: `tests/fixtures/formats/README.md`
- Create: `tests/format_compat.rs`

**Interfaces:**
- Consumes: nothing
- Produces: the fixture paths above, and a `User` record shape `{ name: String, age: u32 }` on a table named `users` that later tasks assert against — rows `(1, "alice", 30)` and `(2, "bob", 25)`, `next_id = 3`, snapshot version `2`.

**Why this task is first, and why its test asserts *rejection*.** The fixtures must be proven to be genuinely old-format bytes before any reader exists to interpret them. A fixture that the current build already accepts is not a v1 fixture. So this task lands the bytes plus a test asserting the current build **rejects** them — which then flips to acceptance in Task 4. That red-to-green transition across tasks is the evidence that the readers do something.

- [ ] **Step 1: Generate the fixtures from the real releases**

Do **not** hand-write these bytes. Hand-rolled fixtures encode what we believe v1 was, which is precisely the belief the test exists to check.

```bash
SC=$(mktemp -d)
git worktree add "$SC/v020" 5cfae9a
git worktree add "$SC/v030" v0.3.0
```

In **each** worktree write `examples/genfix.rs`:

```rust
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User { name: String, age: u32 }

fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let cfg = StoreConfig::builder()
        .persistence(Persistence::standalone(
            std::path::PathBuf::from(&dir), Durability::Consistent, WalWrite::PerEntry))
        .build();
    let store = Store::new(cfg).unwrap();
    store.register_table::<User>("users").unwrap();
    store.recover().unwrap();
    for (n, a) in [("alice", 30u32), ("bob", 25)] {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap()
            .insert(User { name: n.into(), age: a }).unwrap();
        wtx.commit().unwrap();
    }
    let v = store.checkpoint().unwrap();
    println!("checkpointed v{v}");
}
```

Then, per worktree (separate `CARGO_TARGET_DIR` so the two builds cannot share artifacts):

```bash
cd "$SC/v020" && CARGO_TARGET_DIR="$SC/t020" cargo run --features persistence --example genfix -- "$SC/fix020"
cd "$SC/v030" && CARGO_TARGET_DIR="$SC/t030" cargo run --features persistence --example genfix -- "$SC/fix030"
```

Copy **only** `checkpoint_2.bin` from each into the fixture directories. Do not copy `wal.bin` — WAL compatibility is explicitly out of scope, and shipping a WAL fixture would imply coverage that does not exist.

Clean up: `git worktree remove "$SC/v020"` and likewise for `v030`.

- [ ] **Step 2: Verify the fixtures against the ground truth above**

```bash
xxd tests/fixtures/formats/v0_2_0/checkpoint_2.bin | head -3
xxd tests/fixtures/formats/v0_3_0/checkpoint_2.bin | head -6
```

The v0_2_0 bytes must begin `554c 4442 0102 0105 7573 6572 7310 03…` and the v0_3_0 bytes `554c 4442 0102 0105 7573 6572 7347 ff02…`. If they differ, stop and report — either the generation used the wrong commit or the record shape drifted; do not "fix" it by editing bytes.

- [ ] **Step 3: Write the README that keeps this from rotting**

`tests/fixtures/formats/README.md` must record: which commit/tag produced each fixture, the exact `genfix.rs` above, the expected leading bytes, and this rule stated plainly:

> A future on-disk format bump adds a fixture for the version it supersedes, **in the same commit that bumps it**. A compatibility guarantee with no committed old-format bytes silently lapses on the next refactor.

- [ ] **Step 4: Write the tests — asserting current rejection**

`tests/format_compat.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![cfg(feature = "persistence")]

//! Reading checkpoint files written by earlier releases.
//!
//! The fixtures under `tests/fixtures/formats/` are real bytes produced by
//! the actual released builds (see that directory's README), not
//! reconstructions — a hand-rolled fixture would encode what we *believe*
//! the old format was, which is the belief these tests exist to check.

mod common;

use std::path::{Path, PathBuf};
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

fn fixture(version: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(version)
        .join("checkpoint_2.bin")
}

/// Copy a fixture into a fresh scratch dir and open a store over it.
fn store_over_fixture(version: &str) -> (common::test_scratch::ScratchDir, Store) {
    let dir = common::test_scratch::scratch_dir();
    std::fs::copy(fixture(version), dir.path().join("checkpoint_2.bin")).unwrap();
    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build();
    let store = Store::new(config).unwrap();
    store.register_table::<User>("users").unwrap();
    (dir, store)
}

#[test]
fn v0_2_0_checkpoint_is_currently_rejected() {
    let (_dir, store) = store_over_fixture("v0_2_0");
    let err = store.recover().unwrap_err();
    assert!(
        format!("{err}").contains("unsupported format version"),
        "expected a named version rejection, got {err:?}"
    );
}

#[test]
fn v0_3_0_checkpoint_is_currently_rejected() {
    let (_dir, store) = store_over_fixture("v0_3_0");
    let err = store.recover().unwrap_err();
    assert!(
        format!("{err}").contains("unsupported format version"),
        "expected a named version rejection, got {err:?}"
    );
}
```

Confirm `common::test_scratch::scratch_dir()`'s real return type and adjust the helper's signature to match — check `tests/common/mod.rs` rather than assuming the name `ScratchDir`.

- [ ] **Step 5: Run the tests**

Run: `cargo test --features persistence --test format_compat`
Expected: PASS (2 tests) — they assert today's rejection.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/formats tests/format_compat.rs
git commit -m "test(compat): golden checkpoint fixtures from 0.2.0 and 0.3.0

Real bytes from the actual released builds, not reconstructions: a
hand-rolled fixture encodes what we believe the old format was, which is
the belief these tests exist to check.

Asserting today's REJECTION on purpose. These flip to acceptance when the
readers land, and that red-to-green transition is the evidence the readers
do something."
```

---

### Task 2: Table payload v1 reader

**Files:**
- Modify: `src/registry.rs` — `deserialize_table` (currently at `:754`, rejecting non-`0xFF` leading bytes at `:759`)
- Test: `src/registry.rs` `mod tests`

**Interfaces:**
- Consumes: nothing from Task 1 (that task is tests-only)
- Produces: `deserialize_table::<R, K>(bytes) -> Result<Table<R, K>>` now accepts v1 payloads. Behaviour later tasks rely on: a v1 payload yields `next_id = Some(..)` always, and errors with a message containing `"v1 table payloads are u64-keyed"` when `K` is not `u64`.

**Background.** `TABLE_MAGIC_V2 = 0xFF` was chosen because `0xFF` is not a legal bincode varint tag — a v1 payload opens with the varint tag of `next_id` (`0..=250`, or width markers `251..=254`). The existing comment in `src/registry.rs` records why a bare version byte of `2` would *not* have been enough: a v1 table that took exactly one insert encodes `next_id = 2` as `0x02`. That reasoning is what makes this dispatch exact rather than heuristic — preserve it.

- [ ] **Step 1: Write the failing tests**

Add to `src/registry.rs`'s `mod tests`:

```rust
    /// Bytes in the v1 table layout: `[next_id][count][id, rec]*`, all
    /// bincode varint. Mirrors what 0.2.0 actually wrote (see
    /// tests/fixtures/formats/v0_2_0).
    fn v1_payload(next_id: u64, rows: &[(u64, TestRecord)]) -> Vec<u8> {
        let config = bincode::config::standard();
        let mut buf = Vec::new();
        bincode::encode_into_std_write(next_id, &mut buf, config).unwrap();
        bincode::encode_into_std_write(rows.len() as u64, &mut buf, config).unwrap();
        for (id, rec) in rows {
            bincode::encode_into_std_write(*id, &mut buf, config).unwrap();
            buf.extend_from_slice(&bincode::serde::encode_to_vec(rec, config).unwrap());
        }
        buf
    }

    #[test]
    fn a_v1_table_payload_round_trips_into_the_current_shape() {
        let rows = [
            (1u64, TestRecord { name: "alice".into() }),
            (2u64, TestRecord { name: "bob".into() }),
        ];
        let table: Table<TestRecord, u64> =
            deserialize_table(&v1_payload(3, &rows)).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&1).unwrap().name, "alice");
        assert_eq!(table.get(&2).unwrap().name, "bob");
        // v1 always carried a counter, so the reconstructed table must too.
        assert_eq!(table.next_id_opt(), Some(3));
    }

    #[test]
    fn an_empty_v1_table_payload_is_read_as_an_empty_table() {
        let table: Table<TestRecord, u64> = deserialize_table(&v1_payload(1, &[])).unwrap();
        assert_eq!(table.len(), 0);
        assert_eq!(table.next_id_opt(), Some(1));
    }

    /// The safety property the original rejection existed to protect. A v1
    /// payload carries no key type, so reading it into a non-u64 table would
    /// reinterpret bytes into garbage keys — and because the encoding is
    /// order-preserving, the garbage would still pass ascent validation.
    #[test]
    fn a_v1_payload_is_refused_for_a_non_u64_keyed_table() {
        let rows = [(1u64, TestRecord { name: "alice".into() })];
        let err = deserialize_table::<TestRecord, String>(&v1_payload(2, &rows)).unwrap_err();
        assert!(
            format!("{err}").contains("v1 table payloads are u64-keyed"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn a_v2_payload_still_round_trips_unchanged() {
        let mut t = Table::<TestRecord, u64>::new();
        t.put(1, TestRecord { name: "alice".into() });
        let bytes = serialize_table(&t).unwrap();
        assert_eq!(bytes[0], TABLE_MAGIC_V2, "precondition: this is a v2 payload");
        let back: Table<TestRecord, u64> = deserialize_table(&bytes).unwrap();
        assert_eq!(back.get(&1).unwrap().name, "alice");
    }

    #[test]
    fn a_truncated_v1_payload_errors_rather_than_panicking() {
        let rows = [(1u64, TestRecord { name: "alice".into() })];
        let full = v1_payload(2, &rows);
        for cut in 1..full.len() {
            let err = deserialize_table::<TestRecord, u64>(&full[..cut]);
            assert!(err.is_err(), "truncation at {cut} must error, not succeed");
        }
    }
```

Check `TestRecord`'s real field set and `Table::put`/`get`/`next_id_opt` signatures in the surrounding tests before using them; adjust rather than assuming.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib --features persistence registry::tests::a_v1_table`
Expected: FAIL — the current code rejects any non-`0xFF` leading byte.

- [ ] **Step 3: Split the existing reader into a dispatch plus a v2 body**

Rename the current body to `deserialize_table_v2::<R, K>` unchanged, and make `deserialize_table` the dispatch:

```rust
/// Deserialize a `Table<R, K>` from bytes written by any release.
///
/// Dispatch is exact rather than heuristic: `TABLE_MAGIC_V2` (`0xFF`) is not
/// a legal bincode varint tag, and a v1 payload opens with the varint tag of
/// its `next_id`, so no v1 payload can begin with it. This is the same
/// property that made `0xFF` the right magic in the first place.
fn deserialize_table<R: Record, K: PrimaryKey>(bytes: &[u8]) -> Result<Table<R, K>> {
    match bytes.first() {
        None => Err(Error::Persistence(
            "table payload is empty: cannot determine its format version".into(),
        )),
        Some(&TABLE_MAGIC_V2) => deserialize_table_v2::<R, K>(bytes),
        Some(_) => deserialize_table_v1::<R, K>(bytes),
    }
}
```

- [ ] **Step 4: Implement the v1 reader**

```rust
/// Read a pre-0.3.0 table payload: `[next_id][count][id, rec]*`, all
/// bincode-varint, row keys fixed `u64` ids.
///
/// Three things v1 does not carry, and what is supplied for them:
/// * **No key type.** v1 predates arbitrary primary keys (task56), so `u64`
///   is the only key type it could have held — but that is *verified*, not
///   assumed. Reading a v1 payload into a differently-keyed table would
///   reinterpret its bytes into garbage keys, and because `PrimaryKey`
///   encodings are order-preserving the garbage would still pass the
///   ascending-order validation. This check is what the old outright
///   rejection was really protecting, and it outlives it.
/// * **No `has_next_id` flag.** v1 always carried a counter, so the result
///   is always `Some`.
/// * **No key encoding.** v1 ids are raw `u64`; they are routed through
///   `PrimaryKey::encode`/`decode` so the resulting table is
///   indistinguishable from one built from a v2 payload.
fn deserialize_table_v1<R: Record, K: PrimaryKey>(bytes: &[u8]) -> Result<Table<R, K>> {
    if K::KEY_TYPE_ID != <u64 as PrimaryKey>::KEY_TYPE_ID {
        return Err(Error::Persistence(format!(
            "v1 table payloads are u64-keyed (they predate arbitrary primary keys), but this \
             table is registered with key type {}. A pre-0.3.0 checkpoint cannot be read into \
             a differently-keyed table: its 8-byte row ids would be reinterpreted as {} keys.",
            std::any::type_name::<K>(),
            std::any::type_name::<K>(),
        )));
    }

    let config = bincode::config::standard();
    let mut at = 0usize;

    let (next_id, read): (u64, _) = bincode::decode_from_slice(&bytes[at..], config)
        .map_err(|e| Error::Persistence(format!("v1 table payload: next_id: {e}")))?;
    at += read;
    let (count, read): (u64, _) = bincode::decode_from_slice(&bytes[at..], config)
        .map_err(|e| Error::Persistence(format!("v1 table payload: count: {e}")))?;
    at += read;

    let mut rows: Vec<(K, std::sync::Arc<R>)> = Vec::new();
    for i in 0..count {
        let (id, read): (u64, _) = bincode::decode_from_slice(&bytes[at..], config)
            .map_err(|e| Error::Persistence(format!("v1 table payload: row {i} key: {e}")))?;
        at += read;
        let (rec, read): (R, _) = bincode::serde::decode_from_slice(&bytes[at..], config)
            .map_err(|e| Error::Persistence(format!("v1 table payload: row {i} record: {e}")))?;
        at += read;
        rows.push((u64_as_key::<K>(id)?, std::sync::Arc::new(rec)));
    }

    Table::from_bulk(rows, Some(u64_as_key::<K>(next_id)?), Vec::new())
}

/// Reinterpret a v1 `u64` row id as `K`, which the caller has already
/// verified *is* `u64` by key-type id. Goes through the encode/decode pair
/// rather than a transmute so the conversion is the same one every other
/// persistence path uses.
fn u64_as_key<K: PrimaryKey>(id: u64) -> Result<K> {
    K::decode(&id.encode())
}
```

Confirm `PrimaryKey::decode`'s exact signature and error type before writing `u64_as_key`; adapt the body rather than the intent if it differs.

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib --features persistence registry::tests`
Expected: PASS, including the pre-existing v2 tests.

- [ ] **Step 6: Clippy and commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/registry.rs
git commit -m "feat(registry): read v1 table payloads

Dispatch on the leading byte, which is exact rather than heuristic: 0xFF is
not a legal bincode varint tag, so no v1 payload can begin with the v2 magic.

v1 carries no key type, so u64 is supplied — but verified, not assumed. The
old outright rejection existed to stop a v1 payload being reinterpreted into
garbage keys that still pass ascent validation (the encodings are
order-preserving); that protection outlives the rejection as a named error."
```

---

### Task 3: Checkpoint container v1 reader

**Files:**
- Modify: `src/checkpoint.rs` — `deserialize_snapshot` (`:161`, version check at `:198`) and `read_header` (`:539`, version check at `:556`)
- Test: `src/checkpoint.rs` `mod tests`

**Interfaces:**
- Consumes: `deserialize_table` accepting v1 payloads (Task 2)
- Produces: `deserialize_snapshot` and `read_header` accept `format_version == 1`. A v1 file reports as `CheckpointKind::Full` with `base_version: None`, so `find_head_chain` treats it as a complete chain of length one and never looks for ancestors.

**The v1 container layout** (confirmed against real bytes):

```text
[magic "ULDB"][format_version: u32 = 1][snapshot_version: u64][num_tables: u32]
for each table: [name_len: u32][name][data_len: u64][table payload]
[crc32: u32]
```

All scalars are bincode varint (`bincode::config::standard()`), matching how the current v2 reader decodes its own header fields. There is no `kind` byte and no per-table entry-kind byte — v1 predates both.

- [ ] **Step 1: Write the failing tests**

```rust
    /// Build a v1 container around an already-encoded table payload.
    fn v1_checkpoint(version: u64, tables: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let config = bincode::config::standard();
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(1u32, &mut buf, config).unwrap();
        bincode::encode_into_std_write(version, &mut buf, config).unwrap();
        bincode::encode_into_std_write(tables.len() as u32, &mut buf, config).unwrap();
        for (name, payload) in tables {
            bincode::encode_into_std_write(*name, &mut buf, config).unwrap();
            bincode::encode_into_std_write(payload.len() as u64, &mut buf, config).unwrap();
            buf.extend_from_slice(payload);
        }
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn a_v1_container_is_read_as_a_full_checkpoint() {
        // A v1 *container* may legitimately hold a v2 *payload* — that is the
        // released-0.3.0 shape, since the two formats were bumped in
        // different releases. Build exactly that here.
        let mut registry = TableRegistry::default();
        registry.register::<TestRecord>("users").unwrap();
        let info = registry.get("users").unwrap();

        let mut t = Table::<TestRecord, u64>::new();
        t.put(1, TestRecord { name: "alice".into() });
        let payload = (info.serialize_table)(&t as &dyn std::any::Any).unwrap();

        let snap =
            deserialize_snapshot(&v1_checkpoint(2, &[("users", payload)]), &registry).unwrap();
        assert_eq!(snap.version, 2);
        assert!(snap.tables.contains_key("users"));
    }

    #[test]
    fn a_v1_container_reports_as_full_with_no_base() {
        // read_header must classify v1 as Full/None so the chain walk stops.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint_2.bin");
        std::fs::write(&path, v1_checkpoint(2, &[])).unwrap();
        let (kind, version, base) = read_header(&path).unwrap();
        assert_eq!(kind, CheckpointKind::Full);
        assert_eq!(version, 2);
        assert_eq!(base, None);
    }

    #[test]
    fn a_v1_container_with_a_broken_crc_is_still_refused() {
        let mut bytes = v1_checkpoint(2, &[]);
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        let registry = TableRegistry::default();
        let Err(err) = deserialize_snapshot(&bytes, &registry) else {
            panic!("a CRC-broken v1 file must be refused");
        };
        assert!(matches!(err, Error::CheckpointCorrupted(ref m) if m.contains("CRC")));
    }

    #[test]
    fn a_future_format_version_is_still_refused() {
        // Backward compatibility must not weaken forward rejection.
        let config = bincode::config::standard();
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(99u32, &mut buf, config).unwrap();
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let registry = TableRegistry::default();
        let Err(err) = deserialize_snapshot(&buf, &registry) else {
            panic!("a future version must be refused");
        };
        assert!(format!("{err}").contains("unsupported format version"));
    }
```

Fill the first test's registry/payload setup from the neighbouring v2 tests in the same module — they already build a registry and a serialized table.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib --features persistence checkpoint::tests::a_v1_container`
Expected: FAIL — `unsupported format version: 1`.

- [ ] **Step 3: Accept v1 in `deserialize_snapshot`**

Replace the `fmt_version != FORMAT_VERSION` rejection with a three-way match. Keep the *forward* rejection arm's message text byte-for-byte as it is today — only the `1` arm is new. `payload` is the CRC-stripped slice the existing code already computed, and `offset` is its cursor positioned immediately after the `format_version` field:

```rust
    match fmt_version {
        1 => return deserialize_snapshot_v1(payload, offset, registry),
        v if v == FORMAT_VERSION => {}
        _ => {
            return Err(Error::CheckpointCorrupted(format!(
                // the existing forward-rejection text, unchanged
                "unsupported format version: {fmt_version} (checkpoint; this build reads \
                 v{FORMAT_VERSION}). …"
            )));
        }
    }
```

Then implement:

```rust
/// Read a pre-task61 checkpoint container.
///
/// `payload` is the whole file minus its trailing CRC (already verified by
/// the caller); `at` points just past `format_version`. v1 has no `kind`
/// byte and no per-table entry kind — it is always a full checkpoint with
/// every table inline — so it is converted into exactly that shape here and
/// nothing downstream needs to know v1 existed.
fn deserialize_snapshot_v1(
    payload: &[u8],
    mut at: usize,
    registry: &TableRegistry,
) -> Result<Snapshot> {
```

Read `snapshot_version: u64`, `num_tables: u32`, then per table `name: String`, `data_len: u64`, and the payload slice, resolving each through `registry.get(&name)` and `(info.deserialize_table)(bytes)` — the same call the v2 `TableEntryKind::Full` path makes. Return `Snapshot { version, tables }`.

Preserve the existing checked-arithmetic discipline verbatim: every length is attacker-controlled, and the surrounding code is careful about `checked_add` and bounds before slicing. Do not introduce a raw `offset + len` anywhere.

- [ ] **Step 4: Accept v1 in `read_header`**

`read_header` reads only the fixed-size prefix. For `fmt_version == 1` there is no `kind` byte and no `base_version`, so return `(CheckpointKind::Full, snapshot_version, None)`. Keep its forward-rejection arm unchanged.

This is what makes `find_head_chain` treat a v1 file as a self-contained chain of length one — it is `Full`, so the walk stops there and never looks for an ancestor.

- [ ] **Step 5: Run the tests**

Run: `cargo test --features persistence`
Expected: PASS, whole suite. Task 1's two fixture tests will now **fail**, because they assert rejection and the readers now accept — that is the expected red-to-green transition, and Task 4 flips those assertions. Note which two failed and confirm they are exactly those.

- [ ] **Step 6: Clippy and commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/checkpoint.rs
git commit -m "feat(checkpoint): read v1 checkpoint containers

A v1 file is a Full checkpoint with every table inline, so it is converted
into exactly that shape at the boundary and nothing downstream learns v1
exists. read_header reports it as Full/None, which makes find_head_chain
treat it as a complete chain of length one.

Forward rejection is untouched: a file claiming a newer version still fails
with the upgrade-the-binary message.

Task 1's fixture tests now fail by design — they assert the old rejection
and flip in the next commit."
```

---

### Task 4: End-to-end fixture recovery, round-trip, and CI

**Files:**
- Modify: `tests/format_compat.rs` (flip the assertions from Task 1, add the rest)
- Modify: `.github/workflows/ci.yml` if the new test file is not already covered by the persistence pass

**Interfaces:**
- Consumes: everything from Tasks 1-3
- Produces: nothing later tasks depend on

- [ ] **Step 1: Flip the fixture assertions and add the real coverage**

Replace the two rejection tests with:

```rust
/// The pre-0.3.0 shape: v1 container, v1 table payload.
#[test]
fn a_0_2_0_checkpoint_recovers() {
    let (_dir, store) = store_over_fixture("v0_2_0");
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().name, "alice");
    assert_eq!(t.get(2).unwrap().name, "bob");
    assert_eq!(rtx.version(), 2);
}

/// The released-0.3.0 shape, and the one that matters most: v1 container
/// wrapping a *v2* table payload. The two version axes were bumped in
/// different releases, so this combination is not hypothetical.
#[test]
fn a_0_3_0_checkpoint_recovers() {
    let (_dir, store) = store_over_fixture("v0_3_0");
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().name, "alice");
    assert_eq!(rtx.version(), 2);
}

/// Reading must be *lossless*, not approximately right: checkpointing what
/// we read and reloading it must produce the same state.
#[test]
fn an_old_checkpoint_round_trips_through_the_current_writer() {
    let (dir, store) = store_over_fixture("v0_2_0");
    store.recover().unwrap();
    store.checkpoint().unwrap();
    drop(store);

    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build();
    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    store2.recover().unwrap();
    let rtx = store2.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().name, "alice");
    assert_eq!(t.get(2).unwrap().name, "bob");
}

/// `next_id` must survive, or the recovered store reissues used primary
/// keys. A rows-only assertion passes while this is broken — that exact
/// failure shipped once already (see task61's `next_id` finding).
#[test]
fn an_old_checkpoint_preserves_next_id() {
    let (_dir, store) = store_over_fixture("v0_2_0");
    store.recover().unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    let id = wtx
        .open_table::<User>("users")
        .unwrap()
        .insert(User { name: "carol".into(), age: 41 })
        .unwrap();
    wtx.commit().unwrap();
    assert_eq!(id, 3, "the next id must continue from the old counter, not restart");
}

/// A v1 payload carries no key type. Reading it into a differently-keyed
/// table must fail loudly rather than reinterpret 8-byte ids as String keys.
#[test]
fn a_0_2_0_checkpoint_is_refused_for_a_differently_keyed_table() {
    let dir = common::test_scratch::scratch_dir();
    std::fs::copy(fixture("v0_2_0"), dir.path().join("checkpoint_2.bin")).unwrap();
    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build();
    let store = Store::new(config).unwrap();
    store.register_table_keyed::<User, String>("users").unwrap();
    let err = store.recover().unwrap_err();
    assert!(
        format!("{err}").contains("v1 table payloads are u64-keyed"),
        "expected the key-type refusal, got {err:?}"
    );
}
```

Confirm `insert`'s return type and `register_table_keyed`'s turbofish shape against the real API before relying on them.

- [ ] **Step 2: Run**

Run: `cargo test --features persistence --test format_compat`
Expected: PASS (5 tests).

- [ ] **Step 3: Prove the tests have teeth**

Mutate, confirm failure, revert. **Do this in a git worktree with its own `CARGO_TARGET_DIR`** — mutating `src/` in place while any other build runs makes that build compile against the mutant, which has already produced one phantom "flaky test" investigation in this repo.

Mutate each and record the result:
1. Make `deserialize_table_v1` ignore the decoded `next_id` and pass `None` → `an_old_checkpoint_preserves_next_id` must fail.
2. Remove the `K::KEY_TYPE_ID` check → `a_0_2_0_checkpoint_is_refused_for_a_differently_keyed_table` must fail.
3. Make `read_header` return `base_version: Some(0)` for v1 → a chain-related failure must appear.

- [ ] **Step 4: Confirm CI covers the new test**

`.github/workflows/ci.yml`'s persistence pass runs the whole suite, so a new `tests/*.rs` file is picked up automatically. Verify that by reading the workflow rather than assuming; if it enumerates test targets explicitly, add this one.

- [ ] **Step 5: Commit**

```bash
git add tests/format_compat.rs
git commit -m "test(compat): old checkpoints recover, round-trip, and keep next_id

Flips Task 1's rejection assertions now that the readers exist. Covers both
real shapes: 0.2.0 (v1 container + v1 payload) and 0.3.0 (v1 container + v2
payload), the latter being the released version that actually matters.

next_id is asserted separately because a rows-only check passes while the
counter is wrong — that exact failure shipped once already."
```

---

### Task 5: Documentation and messages

**Files:**
- Modify: `src/checkpoint.rs` — the two rejection messages added by task61
- Modify: `docs/how-to/migrate-from-0-2-to-0-3.md`
- Modify: `CHANGELOG.md`
- Create: `docs/tasks/task62_persistence_format_compat.md`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: everything above
- Produces: the canonical per-feature record

**Why this is its own task.** The messages and how-to currently tell operators to export and re-import, which becomes actively misleading for the half of cases that now just work. Wrong guidance is worse than none.

- [ ] **Step 1: Rewrite the rejection messages**

Task61 added long messages at the two version-check sites naming an export/re-import migration. Those now fire only for a version this build genuinely cannot read — i.e. a *newer* one. Rewrite them to say that, and delete the migration prose, which no longer applies to any reachable case. Keep the literal prefix `"unsupported format version: {fmt_version}"`, because existing tests assert on that substring.

- [ ] **Step 2: Correct the migration how-to**

`docs/how-to/migrate-from-0-2-to-0-3.md` must now distinguish two cases plainly:

- A **0.3.0** directory: just upgrade. Its checkpoints and its WAL are both readable.
- A **≤0.2.x** directory: its *checkpoints* are readable, but its **WAL is not** — pre-0.3.0 entries carry no version marker at all and are byte-ambiguous with corruption. So recovery reaches the last checkpoint and no further; rows committed after it still need the old-binary export path.

Do not imply an old directory is fully recoverable. That is the one claim this work must not overstate.

- [ ] **Step 3: CHANGELOG**

Under `Unreleased`, record that this build reads 0.2.x and 0.3.0 checkpoints, and amend the task61 breaking-change entry — the break it describes is now largely undone. Say precisely what remains broken (pre-0.3.0 WALs).

- [ ] **Step 4: Task doc**

`docs/tasks/task62_persistence_format_compat.md`, following `docs/tasks/task37_wal_preallocation.md`'s shape. Record: the two independent version axes and why a v1 container can hold a v2 payload; why the leading-byte dispatch is exact; the key-type check and the silent-corruption it prevents; what is deliberately not covered (the WAL) and the resulting partial-recovery boundary; and the fixture rule.

**Verify every `src/` line citation you write against the real file.** Cite drift is a recurring problem in this repository — it has bitten three times recently, including a CI failure. Check them, do not estimate them.

- [ ] **Step 5: CLAUDE.md**

One sentence in the Persistence bullet: this build reads checkpoints from 0.2.x onward; pre-0.3.0 WALs are not readable. Match the surrounding density.

- [ ] **Step 6: Final verification**

```bash
cargo test
cargo test --features persistence
cargo clippy --all-targets --features persistence -- -D warnings
cargo test -p ultima-vector
python3 formal/scripts/check-cites.py
```

The cite check matters: this task edits `src/checkpoint.rs`, and any line shift there breaks `formal/tla/wal/`'s anchors. If it fails, re-anchor by **reading** each cited site — never by applying a uniform offset.

- [ ] **Step 7: Commit**

```bash
git add src/checkpoint.rs docs/ CHANGELOG.md CLAUDE.md
git commit -m "docs(task62): format compatibility — messages, how-to, canonical doc"
```

---

## Notes for the reviewer

- **The fixtures are the load-bearing part.** They were generated from real released builds (`5cfae9a` for 0.2.0, tag `v0.3.0`), both of which still compile under the current toolchain. If a future change makes them unreadable, that is the guarantee breaking — not the test being stale.
- **Task 1 deliberately asserts the opposite of the goal.** Its tests assert today's rejection so that the fixtures are proven genuinely old-format before any reader exists. Task 4 flips them. A reviewer seeing Task 1 in isolation should not read that as a bug.
- **The riskiest single line** is the `K::KEY_TYPE_ID` check in Task 2. Without it, a v1 payload read into a `String`-keyed table produces garbage keys that still pass ascending-order validation, because `PrimaryKey` encodings are order-preserving. That is a silent-corruption path, and it is the reason the original outright rejection existed.
