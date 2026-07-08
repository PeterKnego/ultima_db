//! Test-only fault injection for the Elle mutation-testing harness (task47).
//! Compiled ONLY under the `mutation-testing` cargo feature and selected at
//! runtime by the `ULTIMA_MUTATION` env var. Feature-on + var-unset = no
//! mutation, so a mutation-testing build with the var unset behaves normally.

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
}

/// Pure mapping from the env-var value to a mutation (testable without env).
fn parse(v: Option<&str>) -> Option<Mutation> {
    match v {
        Some("skip-readset-validation") => Some(Mutation::SkipReadSetValidation),
        Some("skip-writeset-validation") => Some(Mutation::SkipWriteSetValidation),
        Some("drop-merge-key") => Some(Mutation::DropMergeKey),
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
}
