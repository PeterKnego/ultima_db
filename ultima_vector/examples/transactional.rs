// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Composing vector ops with a user-driven UltimaDB transaction.
//!
//! Uses the `_in` variants (`upsert_in`, `search_in`) so a vector insert
//! can ride alongside an audit-log row in the *same* atomic commit. If
//! anything fails, both writes roll back together.
//!
//!     cargo run --example transactional -p ultima-vector

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
struct AuditRow {
    actor: String,
    action: String,
    vector_id: u64,
}

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn main() {
    const DIM: usize = 8;
    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<String, Cosine> =
        VectorCollection::open(store.clone(), "vec", HnswParams::for_dim(DIM), Cosine).unwrap();

    let mut rng = StdRng::seed_from_u64(0xFAB);

    // Insert a vector AND record an audit-log entry in one transaction.
    // UltimaDB allows only one TableWriter at a time per WriteTx, so the
    // sequence is: open the vector path (closes its writer internally), then
    // open the audit table, then commit.
    let id = {
        let mut tx = store.begin_write(None).unwrap();

        let id = coll
            .upsert_in(&mut tx, random_unit_vec(&mut rng, DIM), "doc-1".into())
            .unwrap();

        let mut audit = tx.open_table::<AuditRow>("audit").unwrap();
        audit
            .insert(AuditRow {
                actor: "alice".into(),
                action: "upsert".into(),
                vector_id: id,
            })
            .unwrap();
        drop(audit);

        tx.commit().unwrap();
        id
    };
    println!("committed vector id={id} with matching audit row");

    // A read transaction can drive both `search_in` and a direct table read,
    // so the two are observed at the same snapshot.
    {
        let tx = store.begin_read(None).unwrap();

        let q = random_unit_vec(&mut rng, DIM);
        let top = coll.search_in(&tx, &q, 1, None, None).unwrap();
        println!("top-1 nearest at this snapshot: {:?}", top);

        let audit = tx.open_table::<AuditRow>("audit").unwrap();
        println!("audit log size at this snapshot: {}", audit.len());
        for (aid, row) in audit.iter() {
            println!(
                "  audit#{aid}: actor={} action={} vector_id={}",
                row.actor, row.action, row.vector_id
            );
        }
    }
}
