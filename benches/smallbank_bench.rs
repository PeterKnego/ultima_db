#![allow(clippy::drop_non_drop, clippy::redundant_iter_cloned)]
// drop(TableWriter) releases borrow, needed for multi-table txns.
// redundant_iter_cloned: clippy's suggested fix doesn't compile when the
// cloned Vec needs to move into a 'static thread::spawn closure.

//! SmallBank benchmark for UltimaDB — multi-table transactional workload.
//!
//! Three tables (accounts, savings, checking) and six transaction types.
//! Read-only reference: Alomari et al., "The Cost of Serializability on
//! Platforms That Use Snapshot Isolation" (ICDE 2008).
//!
//! Uses `WriterMode::MultiWriter` for contention runs — each writer runs on
//! its own OS thread and retries on `WriteConflict`.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{Criterion, criterion_group, criterion_main};
use ultima_db::{IndexKind, Store, StoreConfig, WriterMode};

#[path = "smallbank_common.rs"]
mod smallbank_common;
use smallbank_common::*;

// ---------------------------------------------------------------------------
// Benchmark configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SmallBankConfig {
    name: String,
    store_config: StoreConfig,
    #[cfg(feature = "persistence")]
    persistence: ultima_db::Persistence,
}

impl SmallBankConfig {
    fn inmemory() -> Self {
        Self {
            name: "inmemory".into(),
            store_config: StoreConfig {
                num_snapshots_retained: 2,
                auto_snapshot_gc: true,
                ..StoreConfig::default()
            },
            #[cfg(feature = "persistence")]
            persistence: ultima_db::Persistence::None,
        }
    }

    #[cfg(feature = "persistence")]
    fn standalone_consistent() -> Self {
        Self {
            name: "standalone_consistent".into(),
            store_config: StoreConfig {
                num_snapshots_retained: 2,
                auto_snapshot_gc: true,
                ..StoreConfig::default()
            },
            persistence: ultima_db::Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::Consistent,
            },
        }
    }

    #[cfg(feature = "persistence")]
    fn standalone_eventual() -> Self {
        Self {
            name: "standalone_eventual".into(),
            store_config: StoreConfig {
                num_snapshots_retained: 2,
                auto_snapshot_gc: true,
                ..StoreConfig::default()
            },
            persistence: ultima_db::Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::Eventual,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Ultima engine
// ---------------------------------------------------------------------------

struct UltimaEngine {
    name: String,
    store: Store,
    _tmpdir: Option<tempfile::TempDir>,
}

impl UltimaEngine {
    fn new(config: &SmallBankConfig, writer_mode: WriterMode) -> Self {
        #[allow(unused_mut)]
        let mut store_config = config.store_config.clone();
        store_config.writer_mode = writer_mode;
        let tmpdir;

        #[cfg(feature = "persistence")]
        {
            tmpdir = match &config.persistence {
                ultima_db::Persistence::None => None,
                ultima_db::Persistence::Standalone { durability, .. } => {
                    let dir = tempfile::tempdir().unwrap();
                    store_config.persistence = ultima_db::Persistence::Standalone {
                        dir: dir.path().to_path_buf(),
                        durability: *durability,
                    };
                    Some(dir)
                }
                ultima_db::Persistence::Smr { .. } => None,
            };
        }
        #[cfg(not(feature = "persistence"))]
        {
            tmpdir = None;
        }

        let store = Store::new(store_config).unwrap();

        #[cfg(feature = "persistence")]
        {
            store.register_table::<Account>("accounts").unwrap();
            store.register_table::<Savings>("savings").unwrap();
            store.register_table::<Checking>("checking").unwrap();
        }

        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut accounts = wtx.open_table::<Account>("accounts").unwrap();
            accounts
                .define_index("customer_id", IndexKind::Unique, |a: &Account| a.customer_id)
                .unwrap();
            let batch: Vec<Account> = (1..=NUM_ACCOUNTS)
                .map(|i| Account {
                    customer_id: i,
                    name: format!("Customer_{i}"),
                })
                .collect();
            accounts.insert_batch(batch).unwrap();
        }
        {
            let mut savings = wtx.open_table::<Savings>("savings").unwrap();
            savings
                .define_index("customer_id", IndexKind::Unique, |s: &Savings| s.customer_id)
                .unwrap();
            let batch: Vec<Savings> = (1..=NUM_ACCOUNTS)
                .map(|i| Savings {
                    customer_id: i,
                    balance: INITIAL_SAVINGS,
                })
                .collect();
            savings.insert_batch(batch).unwrap();
        }
        {
            let mut checking = wtx.open_table::<Checking>("checking").unwrap();
            checking
                .define_index("customer_id", IndexKind::Unique, |c: &Checking| {
                    c.customer_id
                })
                .unwrap();
            let batch: Vec<Checking> = (1..=NUM_ACCOUNTS)
                .map(|i| Checking {
                    customer_id: i,
                    balance: INITIAL_CHECKING,
                })
                .collect();
            checking.insert_batch(batch).unwrap();
        }
        wtx.commit().unwrap();

        Self {
            name: config.name.clone(),
            store,
            _tmpdir: tmpdir,
        }
    }

    fn execute_single_op(store: &Store, op: &SmallBankOp) {
        match op {
            SmallBankOp::Balance(cid) => {
                let rtx = store.begin_read(None).unwrap();
                let savings = rtx.open_table::<Savings>("savings").unwrap();
                let checking = rtx.open_table::<Checking>("checking").unwrap();
                let sb = savings.get_unique::<u64>("customer_id", cid).unwrap();
                let cb = checking.get_unique::<u64>("customer_id", cid).unwrap();
                black_box((sb.map(|(_, s)| s.balance), cb.map(|(_, c)| c.balance)));
            }
            SmallBankOp::DepositChecking(cid, amount) => {
                let mut wtx = store.begin_write(None).unwrap();
                {
                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let new = Checking {
                            balance: rec.balance + amount,
                            ..*rec
                        };
                        let _ = checking.update(id, new);
                    }
                }
                wtx.commit().unwrap();
            }
            SmallBankOp::TransactSavings(cid, amount) => {
                let mut wtx = store.begin_write(None).unwrap();
                {
                    let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                    if let Some((id, rec)) =
                        savings.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let new = Savings {
                            balance: rec.balance + amount,
                            ..*rec
                        };
                        let _ = savings.update(id, new);
                    }
                }
                wtx.commit().unwrap();
            }
            SmallBankOp::Amalgamate { source, dest } => {
                let mut wtx = store.begin_write(None).unwrap();
                {
                    let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                    let source_amount =
                        if let Some((id, rec)) =
                            savings.get_unique::<u64>("customer_id", source).unwrap()
                        {
                            let amt = rec.balance;
                            let _ = savings.update(
                                id,
                                Savings {
                                    balance: 0.0,
                                    ..*rec
                                },
                            );
                            amt
                        } else {
                            0.0
                        };
                    drop(savings);

                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", dest).unwrap()
                    {
                        let _ = checking.update(
                            id,
                            Checking {
                                balance: rec.balance + source_amount,
                                ..*rec
                            },
                        );
                    }
                }
                wtx.commit().unwrap();
            }
            SmallBankOp::WriteCheck(cid, amount) => {
                let mut wtx = store.begin_write(None).unwrap();
                {
                    let savings = wtx.open_table::<Savings>("savings").unwrap();
                    let sbal = savings
                        .get_unique::<u64>("customer_id", cid)
                        .unwrap()
                        .map(|(_, s)| s.balance)
                        .unwrap_or(0.0);
                    drop(savings);

                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let total = sbal + rec.balance;
                        let penalty = if total < *amount { 1.0 } else { 0.0 };
                        let _ = checking.update(
                            id,
                            Checking {
                                balance: rec.balance - amount - penalty,
                                ..*rec
                            },
                        );
                    }
                }
                wtx.commit().unwrap();
            }
            SmallBankOp::SendPayment { source, dest, amount } => {
                let mut wtx = store.begin_write(None).unwrap();
                {
                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((src_id, src_rec)) =
                        checking.get_unique::<u64>("customer_id", source).unwrap()
                        && src_rec.balance >= *amount
                    {
                        let src_new = Checking {
                            balance: src_rec.balance - amount,
                            ..*src_rec
                        };
                        if let Some((dst_id, dst_rec)) =
                            checking.get_unique::<u64>("customer_id", dest).unwrap()
                        {
                            let dst_new = Checking {
                                balance: dst_rec.balance + amount,
                                ..*dst_rec
                            };
                            let _ = checking.update(src_id, src_new);
                            let _ = checking.update(dst_id, dst_new);
                        }
                    }
                }
                wtx.commit().unwrap();
            }
        }
    }

    /// Execute ops within a single WriteTx. Used by the multi-writer burst
    /// path — each thread wraps its whole op slice in one transaction, so the
    /// thread's writes are atomic end-to-end and the retry loop retries the
    /// full slice on conflict.
    fn execute_ops_on_tx(wtx: &mut ultima_db::WriteTx, ops: &[SmallBankOp]) {
        for op in ops {
            match op {
                SmallBankOp::Balance(_) => {}
                SmallBankOp::DepositChecking(cid, amount) => {
                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let new = Checking { balance: rec.balance + amount, ..*rec };
                        let _ = checking.update(id, new);
                    }
                }
                SmallBankOp::TransactSavings(cid, amount) => {
                    let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                    if let Some((id, rec)) =
                        savings.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let new = Savings { balance: rec.balance + amount, ..*rec };
                        let _ = savings.update(id, new);
                    }
                }
                SmallBankOp::Amalgamate { source, dest } => {
                    let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                    let source_amount = if let Some((id, rec)) =
                        savings.get_unique::<u64>("customer_id", source).unwrap()
                    {
                        let amt = rec.balance;
                        let _ = savings.update(id, Savings { balance: 0.0, ..*rec });
                        amt
                    } else {
                        0.0
                    };
                    drop(savings);

                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", dest).unwrap()
                    {
                        let _ = checking.update(
                            id,
                            Checking { balance: rec.balance + source_amount, ..*rec },
                        );
                    }
                }
                SmallBankOp::WriteCheck(cid, amount) => {
                    let savings = wtx.open_table::<Savings>("savings").unwrap();
                    let sbal = savings
                        .get_unique::<u64>("customer_id", cid)
                        .unwrap()
                        .map(|(_, s)| s.balance)
                        .unwrap_or(0.0);
                    drop(savings);

                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let total = sbal + rec.balance;
                        let penalty = if total < *amount { 1.0 } else { 0.0 };
                        let _ = checking.update(
                            id,
                            Checking {
                                balance: rec.balance - amount - penalty,
                                ..*rec
                            },
                        );
                    }
                }
                SmallBankOp::SendPayment { source, dest, amount } => {
                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((src_id, src_rec)) =
                        checking.get_unique::<u64>("customer_id", source).unwrap()
                        && src_rec.balance >= *amount
                    {
                        let src_new = Checking { balance: src_rec.balance - amount, ..*src_rec };
                        if let Some((dst_id, dst_rec)) =
                            checking.get_unique::<u64>("customer_id", dest).unwrap()
                        {
                            let dst_new = Checking { balance: dst_rec.balance + amount, ..*dst_rec };
                            let _ = checking.update(src_id, src_new);
                            let _ = checking.update(dst_id, dst_new);
                        }
                    }
                }
            }
        }
    }
}

impl SmallBankEngine for UltimaEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn verify(&self) -> StateHash {
        let rtx = self.store.begin_read(None).unwrap();
        let savings = rtx.open_table::<Savings>("savings").unwrap();
        let checking = rtx.open_table::<Checking>("checking").unwrap();
        let mut accounts: std::collections::BTreeMap<u64, AccountState> =
            std::collections::BTreeMap::new();
        for (_, s) in savings.iter() {
            accounts.entry(s.customer_id).or_default().savings = s.balance;
        }
        for (_, c) in checking.iter() {
            accounts.entry(c.customer_id).or_default().checking = c.balance;
        }
        hash_accounts(accounts)
    }

    fn execute(&mut self, ops: &[SmallBankOp]) {
        for op in ops {
            Self::execute_single_op(&self.store, op);
        }
    }

    fn execute_burst(&mut self, op_sets: &[Vec<SmallBankOp>]) -> BurstResult {
        let barrier = Arc::new(Barrier::new(op_sets.len()));
        let handles: Vec<_> = op_sets
            .iter()
            .cloned()
            .map(|ops| {
                let store = self.store.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut retries = 0u64;
                    loop {
                        let mut wtx = store.begin_write(None).unwrap();
                        Self::execute_ops_on_tx(&mut wtx, &ops);
                        match wtx.commit() {
                            Ok(_) => return (1u64, retries),
                            Err(ultima_db::Error::WriteConflict { .. }) => {
                                retries += 1;
                                continue;
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                })
            })
            .collect();

        let mut committed = 0u64;
        let mut aborted = 0u64;
        for h in handles {
            let (c, r) = h.join().unwrap();
            committed += c;
            aborted += r;
        }
        BurstResult { committed, aborted }
    }
}

// ---------------------------------------------------------------------------
// Criterion entry
// ---------------------------------------------------------------------------

fn bench_smallbank(c: &mut Criterion) {
    // Correctness gate — every `cargo bench` run confirms the engine matches
    // the reference implementation before burning cycles on timings.
    {
        let config = SmallBankConfig::inmemory();
        let mut engine = UltimaEngine::new(&config, WriterMode::SingleWriter);
        assert_matches_reference(&mut engine);
    }

    let fixture = generate_fixture(FIXTURE_POOL_SIZE);
    let configs = vec![
        SmallBankConfig::inmemory(),
        #[cfg(feature = "persistence")]
        SmallBankConfig::standalone_consistent(),
        #[cfg(feature = "persistence")]
        SmallBankConfig::standalone_eventual(),
    ];

    for config in &configs {
        {
            let mut engine = UltimaEngine::new(config, WriterMode::SingleWriter);
            bench_workloads(c, &mut engine, &fixture);
        }
        {
            let mut engine = UltimaEngine::new(config, WriterMode::MultiWriter);
            bench_contention(c, &mut engine, &fixture);
        }
    }
}

criterion_group! {
    name = smallbank;
    config = smallbank_criterion();
    targets = bench_smallbank
}
criterion_main!(smallbank);
