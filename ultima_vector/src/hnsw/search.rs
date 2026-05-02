// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! HNSW search: greedy descent through upper layers, then `ef`-bounded
//! beam search at the target layer.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use roaring::RoaringTreemap;
use ultima_db::{ReadTx, Record};

use super::{Candidate, NodeAccess};
use crate::Distance;
use crate::error::{Error, Result};
use crate::row::{EntryPoint, VectorRow};

/// Layer search per Malkov/Yashunin Algorithm 2.
///
/// Returns up to `ef` closest nodes to `query` within `layer`, starting from
/// `entry_points`. Tombstoned nodes are skipped. If `filter` is `Some`,
/// traversal restricts to ids in the bitmap.
pub(crate) fn search_layer<T, Meta, D>(
    nodes: &T,
    distance: &D,
    query: &[f32],
    entry_points: &[Candidate],
    layer: u8,
    ef: usize,
    filter: Option<&RoaringTreemap>,
) -> Result<Vec<Candidate>>
where
    T: NodeAccess<Meta>,
    Meta: Record,
    D: Distance,
{
    if ef == 0 || entry_points.is_empty() {
        return Ok(Vec::new());
    }

    let mut visited: HashSet<u64> = HashSet::with_capacity(ef * 4);
    // candidates: min-heap by distance (closest first to explore next).
    let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
    // results: max-heap by distance (farthest first, bounded at `ef`).
    let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

    for ep in entry_points {
        if !visited.insert(ep.id) {
            continue;
        }
        candidates.push(Reverse(*ep));
        if filter.is_none_or(|f| f.contains(ep.id)) {
            results.push(*ep);
            if results.len() > ef {
                results.pop();
            }
        }
    }

    while let Some(Reverse(c)) = candidates.pop() {
        let worst = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
        if c.dist > worst && results.len() >= ef {
            break;
        }

        let row = nodes.get_row(c.id).ok_or(Error::NodeNotFound(c.id))?;
        let neighbors: Vec<u64> = row.hnsw.neighbors(layer).to_vec();

        for n in neighbors {
            if !visited.insert(n) {
                continue;
            }
            if let Some(f) = filter
                && !f.contains(n)
            {
                continue;
            }
            let nrow = match nodes.get_row(n) {
                Some(r) => r,
                None => continue,
            };
            if nrow.hnsw.is_tombstoned() {
                continue;
            }
            let nd = distance.distance(query, &nrow.embedding);
            let worst = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
            if nd < worst || results.len() < ef {
                let cand = Candidate { id: n, dist: nd };
                candidates.push(Reverse(cand));
                results.push(cand);
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    let mut out: Vec<Candidate> = results.into_sorted_vec();
    out.truncate(ef);
    Ok(out)
}

/// Below this filter cardinality, search bypasses the HNSW graph and
/// brute-forces over the filter's ids. Avoids the well-known recall
/// collapse of HNSW under highly selective filters.
pub const BRUTE_FORCE_THRESHOLD: u64 = 128;

/// Public read-path entry point. Reads the entry-point row, descends from
/// the top layer with `ef=1`, then runs the bounded beam search at layer 0.
///
/// `filter` is applied during traversal. If the filter cardinality is below
/// [`BRUTE_FORCE_THRESHOLD`], HNSW is bypassed entirely and the filter's
/// ids are scanned linearly.
#[allow(clippy::too_many_arguments)]
pub fn search<Meta, D>(
    tx: &ReadTx,
    data_name: &str,
    entry_name: &str,
    distance: &D,
    query: &[f32],
    k: usize,
    ef: usize,
    filter: Option<&RoaringTreemap>,
) -> Result<Vec<(u64, f32)>>
where
    Meta: Record,
    D: Distance,
{
    // Empty store: no entry table -> empty result.
    let entry = match tx.open_table::<EntryPoint>(entry_name) {
        Ok(e) => e,
        Err(ultima_db::Error::KeyNotFound) => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let ep = entry.get(1).cloned().unwrap_or_default();
    drop(entry);

    let Some(start_id) = ep.node_id else {
        return Ok(Vec::new());
    };

    let table = tx.open_table::<VectorRow<Meta>>(data_name)?;

    // Brute-force fallback for highly selective filters.
    if let Some(f) = filter
        && f.len() < BRUTE_FORCE_THRESHOLD
    {
        return brute_force_filtered::<Meta, D>(&table, distance, query, k, f);
    }

    let start_row = table.get(start_id).ok_or(Error::NodeNotFound(start_id))?;
    let start_dist = distance.distance(query, &start_row.embedding);
    let mut current = vec![Candidate { id: start_id, dist: start_dist }];

    // Greedy descent through upper layers (no filter).
    let mut layer = ep.max_level;
    while layer > 0 {
        let next = search_layer(&table, distance, query, &current, layer, 1, None)?;
        if !next.is_empty() {
            current = next;
        }
        layer -= 1;
    }

    // Layer-0 beam search with optional filter.
    let ef = ef.max(k);
    let mut top = search_layer(&table, distance, query, &current, 0, ef, filter)?;
    top.truncate(k);
    Ok(top.into_iter().map(|c| (c.id, c.dist)).collect())
}

fn brute_force_filtered<Meta, D>(
    table: &ultima_db::TableReader<'_, VectorRow<Meta>>,
    distance: &D,
    query: &[f32],
    k: usize,
    filter: &RoaringTreemap,
) -> Result<Vec<(u64, f32)>>
where
    Meta: Record,
    D: Distance,
{
    // Gather live (id, embedding) pairs first; tombstoned rows are skipped
    // *before* scoring to avoid paying the distance compute on them.
    let cap = filter.len() as usize;
    let mut ids: Vec<u64> = Vec::with_capacity(cap);
    let mut targets: Vec<&[f32]> = Vec::with_capacity(cap);
    for id in filter.iter() {
        let Some(row) = table.get(id) else { continue };
        if row.hnsw.is_tombstoned() {
            continue;
        }
        ids.push(id);
        targets.push(&row.embedding);
    }

    let mut scores = vec![0.0f32; targets.len()];
    distance.distance_many(query, &targets, &mut scores);

    let mut scored: Vec<(u64, f32)> = ids.into_iter().zip(scores).collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);
    Ok(scored)
}
