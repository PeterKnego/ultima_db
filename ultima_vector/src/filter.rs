//! Helpers for building `RoaringTreemap` filters that gate HNSW search.
//!
//! Re-exports `RoaringTreemap` so callers don't have to add `roaring` as a
//! direct dependency.

pub use roaring::RoaringTreemap;

/// Build a treemap from any iterator of u64 ids.
pub fn from_ids<I: IntoIterator<Item = u64>>(ids: I) -> RoaringTreemap {
    let mut bm = RoaringTreemap::new();
    for id in ids {
        bm.insert(id);
    }
    bm
}

/// Build a treemap from `(u64, &R)` pairs (the shape returned by
/// `TableReader::index_range` and friends), discarding the records.
pub fn from_id_record_pairs<'a, R, I>(pairs: I) -> RoaringTreemap
where
    I: IntoIterator<Item = (u64, &'a R)>,
    R: 'a,
{
    let mut bm = RoaringTreemap::new();
    for (id, _) in pairs {
        bm.insert(id);
    }
    bm
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ids_collects() {
        let bm = from_ids([1u64, 5, 3, 5]); // duplicate 5 is fine
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(1));
        assert!(bm.contains(3));
        assert!(bm.contains(5));
        assert!(!bm.contains(2));
    }

    #[test]
    fn from_id_record_pairs_drops_records() {
        let pairs: Vec<(u64, &i32)> = vec![(10, &100), (20, &200)];
        let bm = from_id_record_pairs(pairs.iter().map(|(id, r)| (*id, *r)));
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(10));
        assert!(bm.contains(20));
    }
}
