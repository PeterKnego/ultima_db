# Security review: code-review fix series (2026-06-12)

**Scope:** commit series `1de711a..9bf9a1c` — the code-review fix series
(phase-2 commit restructure, bulk-load OCC visibility + WAL marker, WAL
prune serialization, HNSW entry-point/tombstone fixes, journal truncate
intent-file protocol, corruption-injection hardening, metrics/API
cleanup).

**Method:** focused security pass over every new parsing and
file-handling surface introduced by the series, with data-flow tracing
from untrusted inputs to sensitive operations, followed by adversarial
false-positive filtering. Only findings with ≥80% exploitability
confidence qualify for the report.

**Threat model:** UltimaDB is an embedded database library — no network
surface, no authentication layer. The realistic untrusted inputs are:

1. attacker-crafted or corrupted on-disk files read at recovery time
   (`wal.bin`, `checkpoint_*.bin`, journal `seg-*.log`,
   `truncate.intent`),
2. record payloads and keys passed through the public API by the
   embedding application,
3. bincode/serde deserialization of those files.

## Findings

**No HIGH or MEDIUM security vulnerabilities were identified.**

## Attack surfaces examined and cleared

| New surface | Verdict |
|---|---|
| `truncate.intent` parser (`ultima_journal/src/journal/mod.rs`) | Fixed filename (no data-derived path components), strict 20-byte length + magic + CRC32 checks, fixed-width extraction — no panics, no data-sized allocations, no path influence. Planting a forged intent requires journal-directory write access, which already grants full control of the segment files themselves (same trust boundary). |
| `WalOp::BulkLoad` deserialization arm (`src/wal.rs`) | Decoded table names flow only into in-memory keys and an error message — never into file paths or commands. |
| WAL prune rewrite (`src/wal.rs`) | Temp filename derived from the constant WAL path, not data; atomic rename + dir fsync; serialized onto the background writer thread. Net hardening. |
| Checkpoint cleanup (`src/checkpoint.rs`) | Filenames matched by prefix/suffix/numeric parse within the store's own directory; no traversal possible. |
| Commit/PromoteGate/OCC restructure (`src/store.rs`) | Pure in-process concurrency correctness; removes races rather than adding them; no new untrusted-input handling. |
| `unsafe` code | The series adds zero compiled `unsafe` (the only `unsafe` snippet in the diff is inside a fenced code block of a design document). |

## Security improvements landed by the series

- `src/checkpoint.rs` — `data_len` now uses `usize::try_from` +
  `checked_add`: crafted-but-checksummed length fields yield
  `CheckpointCorrupted` instead of overflow/wrapped-slice behavior.
- `src/wal.rs` — `op_count` preallocation capped: a forged count can no
  longer drive a multi-GB allocation attempt (previously a process
  abort).
- `src/index.rs` — public-API-reachable `panic!` on bulk-loading
  custom-indexed tables replaced with `Err(InvalidBulkLoadInput)`.
- `tests/corruption_recovery.rs` — the attacker-crafted-file paths in
  the threat model are now explicitly regression-tested (bit flips,
  truncations, forged length/count fields).

## Informational note (Low severity)

The intent-file mechanism makes silent, acknowledged-data-destroying
log truncation expressible via a 20-byte file drop in the journal
directory. This is acceptable under the current trust model — directory
write access already implies full segment control, and CRC32 is
documented as accidental-corruption protection only, not tamper
resistance. Revisit if journal segments ever gain stronger integrity
protection (e.g. authenticated checksums): the intent file would then
become the weakest link.

## Conclusion

The series is a net security improvement on the recovery-time parsing
paths and introduces no exploitable vulnerability.
