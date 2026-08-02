# Key encoding and storage formats

This page states the `PrimaryKey` encoding rules and the on-disk / wire format
versions as of ultima-db 0.3.0. API details of the `PrimaryKey` trait itself
are in the rustdoc (`ultima_db::primary_key`).

## `PrimaryKey` implementations

| Key type | `KEY_TYPE_ID` | `ENCODED_LEN` | Encoding |
|---|--:|---|---|
| `u8` | 1 | 1 | big-endian bytes |
| `u16` | 2 | 2 | big-endian bytes |
| `u32` | 3 | 4 | big-endian bytes |
| `u64` | 4 | 8 | big-endian bytes |
| `u128` | 5 | 16 | big-endian bytes |
| `i8` | 6 | 1 | big-endian bytes, sign bit flipped |
| `i16` | 7 | 2 | big-endian bytes, sign bit flipped |
| `i32` | 8 | 4 | big-endian bytes, sign bit flipped |
| `i64` | 9 | 8 | big-endian bytes, sign bit flipped |
| `i128` | 10 | 16 | big-endian bytes, sign bit flipped |
| `String` | 11 | variable | UTF-8 bytes |
| `Vec<u8>` | 12 | variable | raw bytes |
| 2-tuples `(A, B)` | derived, high bit set | fixed iff all elements fixed | concatenation with framing (below) |
| 3-tuples `(A, B, C)` | derived, high bit set | fixed iff all elements fixed | concatenation with framing (below) |

`u64` is the only `AutoKey` type: tables keyed by `u64` support `insert` /
`insert_batch` (auto-increment ids); all other key types use `put(key, record)`.

Encoded byte order equals `Ord` order for every implementation. WAL replay
and `BTree::from_sorted` depend on this property.

## Tuple framing

Tuple elements are concatenated in order. A non-final element is framed only
if its encoded length varies:

- Fixed-width elements (`ENCODED_LEN = Some(n)`) are self-delimiting and
  appended as-is.
- Variable-length elements are escaped and terminated: every `0x00` byte in
  the element's encoding becomes `0x00 0xFF`, and the element ends with the
  terminator `0x00 0x01`. The terminator's first byte (`0x00`) sorts before
  any literal byte, and against an escaped zero the second byte decides
  (`0x01 < 0xFF`), so framing preserves lexicographic order.
- The final element is never framed.

Length prefixes are not used anywhere in key encoding: they are not
order-preserving.

Tuple `KEY_TYPE_ID`s are derived by FNV-mixing the tuple arity and the
element type ids, with the high bit (`0x8000_0000`) forced on.

### `KEY_TYPE_ID` ranges

| Range | Meaning |
|---|---|
| `0` | Reserved; must not be used. |
| `1..=12` | Built-in scalar types (table above). |
| `13..=63` | Reserved for future built-in types. |
| `64..=0x7FFF_FFFF` | Available to third-party key types. The value is persisted and must never change once data exists. |
| `>= 0x8000_0000` | Produced by the tuple implementations. |

## Key length cap

`MAX_ENCODED_KEY_LEN` = 64 KiB. Every format that carries an encoded key
enforces the cap on write as well as on read; an over-long key fails at the
offending mutation with `Error::KeyTooLong { len, max, context }` and the
table is left untouched. The write-side checks live in
`TableWriter::{put, update, delete, update_batch, delete_batch}`,
`wal::serialize_entry`, and the checkpoint serializer.

## Format versions (0.3.0)

All three persistent formats are at version 2. Pre-0.3.0 (v1) data is refused
with a named error; there are no compatibility branches — see
[How to migrate from 0.2.x](../how-to/migrate-from-0-2-to-0-3.md). Each
format stamps the table's `KEY_TYPE_ID` and validates it on read: encoded
keys are opaque bytes that several key types decode without complaint, so a
mismatch is refused rather than silently reinterpreted (the reasoning is in
[the architecture explanation](../explanation/architecture.md)).

### Checkpoint table payload — v2

```
[magic u8 = 0xFF][version u8 = 2][key_type u32-be][has_next_id u8]
[next_id_len u32-be, next_id_bytes]?
[num_entries u64-be]
[key_len u32-be, key_bytes, rec_len u32-be, rec_bytes]*
```

All lengths are explicit and big-endian. `has_next_id` is `0` for an
explicitly-keyed table and `1` for an auto-increment one. A v1 checkpoint is
rejected at `recover()` with an error naming the table.

### WAL entry payload — v2

Each payload opens with `[magic 0xFF][format 2]`. Each key-carrying `WalOp`
carries `[key_type varint][len-prefixed encoded key]`; the tag is per op
because one transaction can write tables with different key types. The WAL
file has no header; the format marker lives at the front of each payload.

A v1 WAL is refused at store open, in all three `WalWrite` modes — not at
`recover()`.

### Snapshot stream — `FILE_FORMAT_V` 2

Rows are `key_len(u32) | key | val_len(u32) | val`. `TableHeader` carries
public `key_type_id` and `key_type` (name) fields. Version 2 is a
live-replication break as well as an on-disk one: a 0.2.x follower and a
0.3.0 leader reject each other's streams cleanly in both directions. The
install path additionally compares `TypeId` between the destination registry
and its live table.
