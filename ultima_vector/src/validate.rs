// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Input validation helpers.

use crate::error::{Error, Result};

/// Reject NaN and ±Inf. Every embedding/query is validated once at its
/// `VectorCollection` entry point, so the HNSW graph can never contain a
/// non-finite value and the hot distance loops stay check-free.
pub(crate) fn ensure_finite(v: &[f32]) -> Result<()> {
    match v.iter().position(|x| !x.is_finite()) {
        Some(index) => Err(Error::NonFinite {
            index,
            value: v[index],
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_vector_passes() {
        assert!(ensure_finite(&[1.0, -2.5, 0.0, f32::MIN, f32::MAX]).is_ok());
        assert!(ensure_finite(&[]).is_ok());
    }

    #[test]
    fn nan_rejected_with_index() {
        let err = ensure_finite(&[1.0, f32::NAN, 3.0]).unwrap_err();
        match err {
            Error::NonFinite { index, value } => {
                assert_eq!(index, 1);
                assert!(value.is_nan());
            }
            other => panic!("expected NonFinite, got {other:?}"),
        }
    }

    #[test]
    fn pos_and_neg_infinity_rejected() {
        assert!(matches!(
            ensure_finite(&[f32::INFINITY]).unwrap_err(),
            Error::NonFinite { index: 0, .. }
        ));
        assert!(matches!(
            ensure_finite(&[0.0, 0.0, f32::NEG_INFINITY]).unwrap_err(),
            Error::NonFinite { index: 2, .. }
        ));
    }

    #[test]
    fn error_message_names_index_and_value() {
        let msg = ensure_finite(&[f32::INFINITY]).unwrap_err().to_string();
        assert_eq!(msg, "non-finite value (inf) at index 0 in vector");
    }
}
