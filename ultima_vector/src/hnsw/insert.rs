// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! HNSW insert: sample level, descend through upper layers, build adjacency
//! at every layer from `new_level` down to 0, M-prune neighbors as the new
//! node's edges are added back-references on existing nodes.
//!
//! Implements the layer-search + heuristic neighbor selection from
//! Malkov & Yashunin (2018), Algorithms 1, 3, 4.

use rand::Rng;
use ultima_db::{Record, TableWriter, WriteTx};

use super::level::LevelSampler;
use super::params::HnswParams;
use super::search::search_layer;
use super::{Candidate, NodeAccess};
use crate::Distance;
use crate::error::{Error, Result};
use crate::row::{EntryPoint, HnswState, VectorRow};

/// Insert a new vector into the HNSW graph. Returns the row id.
///
/// `data_name` and `entry_name` are the names of the data and entry-point
/// tables in the underlying store; both are opened (one at a time) on `tx`.
#[allow(clippy::too_many_arguments)]
pub fn insert<Meta, D, R>(
    tx: &mut WriteTx,
    data_name: &str,
    entry_name: &str,
    distance: &D,
    embedding: Vec<f32>,
    meta: Meta,
    params: &HnswParams,
    rng: &mut R,
) -> Result<u64>
where
    Meta: Record + Clone,
    D: Distance,
    R: Rng + ?Sized,
{
    if embedding.len() != params.dim {
        return Err(Error::DimMismatch { expected: params.dim, got: embedding.len() });
    }

    let sampler = LevelSampler::new(params.m, params.max_level);
    let new_level = sampler.sample(rng);

    let ep = read_entry_point(tx, entry_name)?;

    let new_id = {
        let mut data = tx.open_table::<VectorRow<Meta>>(data_name)?;
        let new_row = VectorRow {
            embedding: embedding.clone(),
            meta,
            hnsw: HnswState::empty(new_level),
        };
        let new_id = data.insert(new_row)?;
        if ep.node_id.is_some() {
            connect_node(&mut data, distance, &embedding, new_id, new_level, &ep, params)?;
        }
        new_id
    };

    if ep.node_id.is_none() || new_level > ep.max_level {
        let new_ep = EntryPoint {
            node_id: Some(new_id),
            max_level: new_level.max(ep.max_level),
        };
        write_entry_point(tx, entry_name, new_ep)?;
    }

    Ok(new_id)
}

/// Update the embedding at an existing id while preserving the user's
/// metadata. Resamples a fresh level and rebuilds adjacency. Stale
/// back-references on other nodes at layers above the new level are left
/// in place — they degrade to dead ends during traversal but don't break
/// correctness.
#[allow(clippy::too_many_arguments)]
pub fn update_embedding<Meta, D, R>(
    tx: &mut WriteTx,
    data_name: &str,
    entry_name: &str,
    distance: &D,
    id: u64,
    embedding: Vec<f32>,
    params: &HnswParams,
    rng: &mut R,
) -> Result<()>
where
    Meta: Record + Clone,
    D: Distance,
    R: Rng + ?Sized,
{
    if embedding.len() != params.dim {
        return Err(Error::DimMismatch { expected: params.dim, got: embedding.len() });
    }

    let sampler = LevelSampler::new(params.m, params.max_level);
    let new_level = sampler.sample(rng);

    let ep = read_entry_point(tx, entry_name)?;

    {
        let mut data = tx.open_table::<VectorRow<Meta>>(data_name)?;
        let current = data.get(id).ok_or(Error::NodeNotFound(id))?.clone();
        let fresh = VectorRow {
            embedding: embedding.clone(),
            meta: current.meta.clone(),
            hnsw: HnswState::empty(new_level),
        };
        data.update(id, fresh)?;

        // If the only node in the graph is the one being updated, there's no
        // graph to connect into — entry point handling below covers it.
        let has_other = ep.node_id.is_some_and(|epid| epid != id);
        if has_other {
            connect_node(&mut data, distance, &embedding, id, new_level, &ep, params)?;
        }
    }

    if ep.node_id == Some(id) || new_level > ep.max_level {
        // Either we just rebuilt the entry-point node itself, or this node
        // now sits above the previous top.
        let new_ep = EntryPoint {
            node_id: Some(id),
            max_level: new_level.max(ep.max_level),
        };
        write_entry_point(tx, entry_name, new_ep)?;
    }

    Ok(())
}

/// Tombstone a node. Adjacency repair is lazy — searches skip tombstoned
/// nodes; future inserts may also drop them during M-pruning.
///
/// If the tombstoned node is the current entry point, scan the table for
/// the highest-level non-tombstoned node and promote it. Linear in the
/// table size; rare in practice (only on entry-point delete).
pub fn delete<Meta>(
    tx: &mut WriteTx,
    data_name: &str,
    entry_name: &str,
    id: u64,
) -> Result<()>
where
    Meta: Record + Clone,
{
    {
        let mut data = tx.open_table::<VectorRow<Meta>>(data_name)?;
        let mut row = data.get(id).ok_or(Error::NodeNotFound(id))?.clone();
        if row.hnsw.is_tombstoned() {
            return Ok(()); // already deleted; idempotent
        }
        row.hnsw.tombstone();
        data.update(id, row)?;
    }

    let ep = read_entry_point(tx, entry_name)?;
    if ep.node_id != Some(id) {
        return Ok(());
    }

    let replacement: Option<(u64, u8)> = {
        let data = tx.open_table::<VectorRow<Meta>>(data_name)?;
        let mut best: Option<(u64, u8)> = None;
        for (rid, r) in data.iter() {
            if r.hnsw.is_tombstoned() {
                continue;
            }
            let l = r.hnsw.level();
            if best.is_none_or(|(_, bl)| l > bl) {
                best = Some((rid, l));
            }
        }
        best
    };

    let new_ep = match replacement {
        Some((rid, l)) => EntryPoint { node_id: Some(rid), max_level: l },
        None => EntryPoint::default(),
    };
    write_entry_point(tx, entry_name, new_ep)?;
    Ok(())
}

pub(crate) fn read_entry_point(tx: &mut WriteTx, name: &str) -> Result<EntryPoint> {
    let entry = tx.open_table::<EntryPoint>(name)?;
    Ok(entry.get(1).cloned().unwrap_or_default())
}

pub(crate) fn write_entry_point(tx: &mut WriteTx, name: &str, ep: EntryPoint) -> Result<()> {
    let mut entry = tx.open_table::<EntryPoint>(name)?;
    if entry.get(1).is_some() {
        entry.update(1, ep)?;
    } else {
        let id = entry.insert(ep)?;
        debug_assert_eq!(id, 1, "entry-point singleton expected at id=1");
    }
    Ok(())
}

/// Wire an existing row at `node_id` into the graph: greedy descent, then
/// per-layer ef_construction search, neighbor selection, M-pruning. Assumes
/// the row already exists at `node_id` with empty adjacency lists at every
/// layer in its level. Caller handles entry-point updates and the case
/// where `ep.node_id` is `None` (graph empty).
fn connect_node<Meta, D>(
    data: &mut TableWriter<'_, VectorRow<Meta>>,
    distance: &D,
    embedding: &[f32],
    node_id: u64,
    new_level: u8,
    ep: &EntryPoint,
    params: &HnswParams,
) -> Result<()>
where
    Meta: Record + Clone,
    D: Distance,
{
    let ep_id = ep.node_id.expect("connect_node requires existing entry point");

    let start_dist = {
        let r = data.get(ep_id).ok_or(Error::NodeNotFound(ep_id))?;
        distance.distance(embedding, &r.embedding)
    };
    let mut current = vec![Candidate { id: ep_id, dist: start_dist }];

    // Greedy descent through layers above new_level.
    let mut layer = ep.max_level;
    while layer > new_level {
        let next = search_layer(&*data, distance, embedding, &current, layer, 1, None)?;
        if !next.is_empty() {
            current = next;
        }
        if layer == 0 {
            break;
        }
        layer -= 1;
    }

    // For each layer min(new_level, ep.max_level)..=0, find ef_construction
    // candidates, select up to m, write adjacency, update neighbors.
    let start_layer = new_level.min(ep.max_level);
    for l in (0..=start_layer).rev() {
        let m_lim = if l == 0 { params.m_max0 } else { params.m };
        let candidates = search_layer(
            &*data,
            distance,
            embedding,
            &current,
            l,
            params.ef_construction,
            None,
        )?;
        let selected = select_neighbors_heuristic(&*data, distance, &candidates, m_lim)?;

        // Write this node's adjacency at this layer.
        let mut nrow = data.get(node_id).ok_or(Error::NodeNotFound(node_id))?.clone();
        nrow.hnsw.set_neighbors(l, selected.iter().map(|c| c.id).collect());
        data.update(node_id, nrow)?;

        // Add back-reference on each chosen neighbor; M-prune if over capacity.
        for nbr in &selected {
            if nbr.id == node_id {
                continue;
            }
            let mut nbr_row = data.get(nbr.id).ok_or(Error::NodeNotFound(nbr.id))?.clone();
            let mut nbr_adj = nbr_row.hnsw.neighbors(l).to_vec();
            if !nbr_adj.contains(&node_id) {
                nbr_adj.push(node_id);
            }
            if nbr_adj.len() > m_lim {
                let anchor_emb = nbr_row.embedding.clone();
                let mut as_cands: Vec<Candidate> = Vec::with_capacity(nbr_adj.len());
                for id in &nbr_adj {
                    let r = data.get(*id).ok_or(Error::NodeNotFound(*id))?;
                    let d = distance.distance(&anchor_emb, &r.embedding);
                    as_cands.push(Candidate { id: *id, dist: d });
                }
                as_cands.sort();
                let pruned = select_neighbors_heuristic(&*data, distance, &as_cands, m_lim)?;
                nbr_adj = pruned.into_iter().map(|c| c.id).collect();
            }
            // Skip the level-out-of-range case: nbr may have a lower level
            // than `l` if levels disagree. Setting neighbors above the
            // node's level would panic; gate on nbr's actual layer count.
            if usize::from(l) < nbr_row.hnsw.layers_len() {
                nbr_row.hnsw.set_neighbors(l, nbr_adj);
                data.update(nbr.id, nbr_row)?;
            }
        }

        current = selected;
    }

    Ok(())
}

/// Heuristic neighbor selection (Malkov & Yashunin Alg. 4) with
/// `keep_pruned_connections = true`.
fn select_neighbors_heuristic<T, Meta, D>(
    nodes: &T,
    distance: &D,
    candidates: &[Candidate],
    m: usize,
) -> Result<Vec<Candidate>>
where
    T: NodeAccess<Meta>,
    Meta: Record,
    D: Distance,
{
    if candidates.len() <= m {
        return Ok(candidates.to_vec());
    }

    let mut sorted = candidates.to_vec();
    sorted.sort();

    let mut selected: Vec<Candidate> = Vec::with_capacity(m);
    let mut discarded: Vec<Candidate> = Vec::new();

    'next: for c in sorted {
        if selected.len() >= m {
            discarded.push(c);
            continue;
        }
        let c_row = nodes.get_row(c.id).ok_or(Error::NodeNotFound(c.id))?;
        for r in &selected {
            let r_row = nodes.get_row(r.id).ok_or(Error::NodeNotFound(r.id))?;
            let d_cr = distance.distance(&c_row.embedding, &r_row.embedding);
            if d_cr < c.dist {
                discarded.push(c);
                continue 'next;
            }
        }
        selected.push(c);
    }

    if selected.len() < m {
        for c in discarded {
            if selected.len() >= m {
                break;
            }
            selected.push(c);
        }
    }

    Ok(selected)
}
