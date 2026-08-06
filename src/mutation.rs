//! Test-only fault injection, with two consumers:
//!
//! * the Elle mutation-testing harness (task47) — the three isolation/merge
//!   logic switches, which prove the consistency checks have teeth;
//! * the in-flight WAL fault tests (task60, `tests/wal_fault_*.rs`) — the three
//!   payload-carrying I/O faults, which make a failing syscall reachable from a
//!   test.
//!
//! Compiled ONLY under the `mutation-testing` cargo feature and selected at
//! runtime by the `ULTIMA_MUTATION` env var. Feature-on + var-unset = no
//! mutation, so a mutation-testing build with the var unset behaves normally.
//!
//! `active()` memoises in a `OnceLock`, so **one mutation value per process**:
//! a test binary can arm exactly one fault, and a second `set_var` in the same
//! process is silently ignored. See `docs/tasks/task60_wal_inflight_faults.md`
//! §3 before adding a fault or a test.

use std::sync::OnceLock;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mutation {
    /// SSI bug: `validate_read_set` never fires — read-set validation disabled.
    SkipReadSetValidation,
    /// OCC bug: `validate_write_set` never reports a conflict — lost updates.
    SkipWriteSetValidation,
    /// Merge bug: `Table::merge_keys_from` silently drops one of the writer's
    /// edited keys during the commit slow-path merge — a lost update below the
    /// isolation layer (the write-set validation still passes).
    DropMergeKey,
    /// I/O fault: the next `write_all` in the WAL write path writes `n` bytes
    /// and then returns `ENOSPC`. Models a disk filling mid-operation, which
    /// leaves the file longer than the sink's in-memory `capacity`.
    FailWriteAfter(u64),
    /// I/O fault: the next `sync_all`/`sync_data` in the WAL write path returns
    /// an error instead of succeeding.
    FailSync,
    /// I/O fault: the sink's positioned batch write is truncated at this byte
    /// offset — a torn frame, produced while the sink still believes it wrote
    /// the whole batch.
    TearFrameAt(u64),
}

/// Pure mapping from the env-var value to a mutation (testable without env).
fn parse(v: Option<&str>) -> Option<Mutation> {
    match v {
        Some("skip-readset-validation") => Some(Mutation::SkipReadSetValidation),
        Some("skip-writeset-validation") => Some(Mutation::SkipWriteSetValidation),
        Some("drop-merge-key") => Some(Mutation::DropMergeKey),
        Some(s) if s.starts_with("fail-write-after=") => s["fail-write-after=".len()..]
            .parse()
            .ok()
            .map(Mutation::FailWriteAfter)
            .or_else(|| panic!("unknown ULTIMA_MUTATION value: {s}")),
        Some("fail-sync") => Some(Mutation::FailSync),
        Some(s) if s.starts_with("tear-frame-at=") => s["tear-frame-at=".len()..]
            .parse()
            .ok()
            .map(Mutation::TearFrameAt)
            .or_else(|| panic!("unknown ULTIMA_MUTATION value: {s}")),
        None | Some("") => None,
        Some(other) => panic!("unknown ULTIMA_MUTATION value: {other}"),
    }
}

/// The active mutation for this process, read once from `ULTIMA_MUTATION`.
pub(crate) fn active() -> Option<Mutation> {
    static M: OnceLock<Option<Mutation>> = OnceLock::new();
    *M.get_or_init(|| parse(std::env::var("ULTIMA_MUTATION").ok().as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_maps_known_values() {
        assert_eq!(
            parse(Some("skip-readset-validation")),
            Some(Mutation::SkipReadSetValidation)
        );
        assert_eq!(
            parse(Some("skip-writeset-validation")),
            Some(Mutation::SkipWriteSetValidation)
        );
        assert_eq!(parse(Some("drop-merge-key")), Some(Mutation::DropMergeKey));
        assert_eq!(parse(None), None);
        assert_eq!(parse(Some("")), None);
    }

    #[test]
    #[should_panic(expected = "unknown ULTIMA_MUTATION")]
    fn parse_panics_on_unknown() {
        let _ = parse(Some("bogus"));
    }

    #[test]
    fn parses_the_io_fault_variants() {
        assert_eq!(parse(Some("fail-write-after=0")), Some(Mutation::FailWriteAfter(0)));
        assert_eq!(parse(Some("fail-write-after=65536")), Some(Mutation::FailWriteAfter(65536)));
        assert_eq!(parse(Some("fail-sync")), Some(Mutation::FailSync));
        assert_eq!(parse(Some("tear-frame-at=12")), Some(Mutation::TearFrameAt(12)));
    }

    #[test]
    #[should_panic(expected = "unknown ULTIMA_MUTATION")]
    fn rejects_an_io_fault_with_no_payload() {
        // `fail-write-after` without `=<n>` is a typo, not a default — an
        // unparameterised fault would silently fail the *first* write and make
        // every test using it pass for the wrong reason.
        let _ = parse(Some("fail-write-after"));
    }
}
