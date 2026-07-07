//! elle-history: generate Elle list-append histories against UltimaDB
//! MultiWriter transactions (SI or Serializable), for checking with the
//! vendored elle-cli. See docs/tasks/task41_elle_consistency_harness.md.

use clap::Parser;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use ultima_db::{CommitWaiter, Error, IsolationLevel, Store, StoreConfig, WriterMode};

#[derive(Parser)]
#[command(about = "Generate an Elle list-append history from UltimaDB MultiWriter transactions")]
struct Args {
    /// Isolation level: "si" or "serializable" (always MultiWriter).
    #[arg(long)]
    isolation: String,
    /// Writer threads (= Elle processes).
    #[arg(long, default_value_t = 8)]
    threads: usize,
    /// Number of pre-seeded rows (small => contention).
    #[arg(long, default_value_t = 16)]
    keys: usize,
    /// Attempted transactions per thread.
    #[arg(long, default_value_t = 1500)]
    txns_per_thread: usize,
    /// Ops per transaction.
    #[arg(long, default_value_t = 4)]
    ops_per_txn: usize,
    /// Probability an op is a read (rest are appends).
    #[arg(long, default_value_t = 0.4)]
    read_ratio: f64,
    /// Probability a transaction reads via a full table scan (`range`) instead
    /// of point `get`s. Exercises SSI's coarse `table_scan` read tracking
    /// (a scan taints the whole table for conflict detection). Default 0.0
    /// preserves the point-read workload exactly.
    #[arg(long, default_value_t = 0.0)]
    scan_ratio: f64,
    /// PRNG seed (per-thread stream: seed + thread index).
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Output path for the EDN history.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ElleRow {
    list: Vec<u64>,
}

#[derive(Clone, Debug)]
enum Op {
    Append { key: u64, value: u64 },
    Read { key: u64, result: Option<Vec<u64>> },
}

enum EventType {
    Invoke,
    Ok,
    Fail,
    Info,
}

struct Event {
    typ: EventType,
    process: usize,
    time_ns: u128,
    value: Vec<Op>,
}

/// SplitMix64: tiny deterministic PRNG so autobench needs no rand dependency.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, p: f64) -> bool {
        (self.next() as f64) < p * (u64::MAX as f64)
    }
}

fn gen_ops(
    rng: &mut SplitMix64,
    keys: &[u64],
    ops_per_txn: usize,
    read_ratio: f64,
    values: &AtomicU64,
) -> Vec<Op> {
    (0..ops_per_txn)
        .map(|_| {
            let key = keys[rng.below(keys.len() as u64) as usize];
            if rng.chance(read_ratio) {
                Op::Read { key, result: None }
            } else {
                Op::Append { key, value: values.fetch_add(1, Ordering::Relaxed) }
            }
        })
        .collect()
}

fn edn_event(index: usize, e: &Event) -> String {
    let typ = match e.typ {
        EventType::Invoke => ":invoke",
        EventType::Ok => ":ok",
        EventType::Fail => ":fail",
        EventType::Info => ":info",
    };
    let mut v = String::new();
    for (i, op) in e.value.iter().enumerate() {
        if i > 0 {
            v.push(' ');
        }
        match op {
            Op::Append { key, value } => write!(v, "[:append {key} {value}]").unwrap(),
            Op::Read { key, result: None } => write!(v, "[:r {key} nil]").unwrap(),
            Op::Read { key, result: Some(list) } => {
                write!(v, "[:r {key} [").unwrap();
                for (j, x) in list.iter().enumerate() {
                    if j > 0 {
                        v.push(' ');
                    }
                    write!(v, "{x}").unwrap();
                }
                v.push_str("]]");
            }
        }
    }
    format!(
        "{{:index {index}, :type {typ}, :f :txn, :process {p}, :time {t}, :value [{v}]}}",
        p = e.process,
        t = e.time_ns
    )
}

enum TxnFail {
    /// WriteConflict at the write site or at commit: definitely not committed.
    Conflict(Option<CommitWaiter>),
    /// SSI read-set validation failure at commit: definitely not committed.
    Serialization,
    /// Anything else: outcome treated as indeterminate.
    Other(Error),
}

/// Run one list-append transaction. Returns completed ops (reads filled in)
/// on commit. When `scan` is true the transaction reads the whole table once
/// via `range(..)` (registering the SSI coarse `table_scan` read) and serves
/// every read and append from a local working copy, preserving
/// read-your-writes; otherwise each op does a point `get`.
fn run_txn(store: &Store, ops: &[Op], scan: bool) -> Result<Vec<Op>, TxnFail> {
    let mut tx = store.begin_write(None).map_err(TxnFail::Other)?;
    let mut completed = Vec::with_capacity(ops.len());
    {
        let mut t = tx.open_table::<ElleRow>("elle").map_err(TxnFail::Other)?;
        // Scan mode reads the table up front; `local` is the transaction's own
        // working copy so later reads observe earlier appends in this txn.
        let mut local: std::collections::HashMap<u64, Vec<u64>> = if scan {
            t.range(..).map(|(id, row)| (id, row.list.clone())).collect()
        } else {
            std::collections::HashMap::new()
        };
        for op in ops {
            match op {
                Op::Read { key, .. } => {
                    let list = if scan {
                        local.get(key).cloned().unwrap_or_default()
                    } else {
                        match t.get(*key) {
                            Some(row) => row.list.clone(),
                            None => Vec::new(),
                        }
                    };
                    completed.push(Op::Read { key: *key, result: Some(list) });
                }
                Op::Append { key, value } => {
                    let next = if scan {
                        // Read-modify-write against the local working copy; the
                        // up-front scan already registered the read.
                        let list = local.entry(*key).or_default();
                        list.push(*value);
                        list.clone()
                    } else {
                        // get() both fetches the list and registers the SSI point read.
                        let mut list = match t.get(*key) {
                            Some(row) => row.list.clone(),
                            None => Vec::new(),
                        };
                        list.push(*value);
                        list
                    };
                    match t.update(*key, ElleRow { list: next }) {
                        Ok(()) => completed.push(op.clone()),
                        Err(Error::WriteConflict { wait_for, .. }) => {
                            return Err(TxnFail::Conflict(wait_for));
                        }
                        Err(e) => return Err(TxnFail::Other(e)),
                    }
                }
            }
        }
    }
    match tx.commit() {
        Ok(_) => Ok(completed),
        Err(Error::WriteConflict { wait_for, .. }) => Err(TxnFail::Conflict(wait_for)),
        Err(Error::SerializationFailure { .. }) => Err(TxnFail::Serialization),
        Err(e) => Err(TxnFail::Other(e)),
    }
}

#[derive(Default)]
struct Stats {
    ok: AtomicU64,
    write_conflict: AtomicU64,
    serialization_failure: AtomicU64,
    info: AtomicU64,
    scan_txns: AtomicU64,
}

struct Shared {
    store: Store,
    keys: Vec<u64>,
    history: Mutex<Vec<Event>>,
    values: AtomicU64,
    stats: Stats,
    epoch: Instant,
}

impl Shared {
    fn push(&self, typ: EventType, process: usize, value: Vec<Op>) {
        let time_ns = self.epoch.elapsed().as_nanos();
        self.history
            .lock()
            .unwrap()
            .push(Event { typ, process, time_ns, value });
    }
}

fn worker(shared: &Shared, args: &Args, process: usize) {
    let mut rng = SplitMix64(args.seed.wrapping_add(process as u64));
    for _ in 0..args.txns_per_thread {
        let ops = gen_ops(
            &mut rng,
            &shared.keys,
            args.ops_per_txn,
            args.read_ratio,
            &shared.values,
        );
        let scan = rng.chance(args.scan_ratio);
        if scan {
            shared.stats.scan_txns.fetch_add(1, Ordering::Relaxed);
        }
        shared.push(EventType::Invoke, process, ops.clone());
        match run_txn(&shared.store, &ops, scan) {
            Ok(completed) => {
                shared.stats.ok.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Ok, process, completed);
            }
            Err(TxnFail::Conflict(waiter)) => {
                shared.stats.write_conflict.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Fail, process, ops);
                if let Some(w) = waiter {
                    w.wait();
                }
            }
            Err(TxnFail::Serialization) => {
                shared
                    .stats
                    .serialization_failure
                    .fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Fail, process, ops);
            }
            Err(TxnFail::Other(e)) => {
                // Indeterminate outcome: record :info and retire this process
                // (a Jepsen process may not issue ops after an :info).
                eprintln!("elle-history: process {process}: unexpected error, retiring: {e}");
                shared.stats.info.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Info, process, ops);
                return;
            }
        }
    }
}

fn main() {
    let args = Args::parse();
    let isolation = match args.isolation.as_str() {
        "si" => IsolationLevel::SnapshotIsolation,
        "serializable" => IsolationLevel::Serializable,
        other => {
            eprintln!("elle-history: --isolation must be 'si' or 'serializable', got '{other}'");
            std::process::exit(2);
        }
    };

    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .isolation_level(isolation)
            .build(),
    )
    .expect("store construction");

    // Pre-seed the keyspace: TableWriter has no insert-at-key, and update()
    // requires the key to exist, so the workload is pure get+update.
    let keys = {
        let mut tx = store.begin_write(None).expect("setup begin_write");
        let ids = {
            let mut t = tx.open_table::<ElleRow>("elle").expect("setup open_table");
            t.insert_batch(vec![ElleRow { list: Vec::new() }; args.keys])
                .expect("setup insert_batch")
        };
        tx.commit().expect("setup commit");
        ids
    };

    let shared = Arc::new(Shared {
        store,
        keys,
        history: Mutex::new(Vec::with_capacity(args.threads * args.txns_per_thread * 2)),
        values: AtomicU64::new(1),
        stats: Stats::default(),
        epoch: Instant::now(),
    });

    std::thread::scope(|s| {
        for process in 0..args.threads {
            let shared = Arc::clone(&shared);
            let args = &args;
            s.spawn(move || worker(&shared, args, process));
        }
    });

    let history = shared.history.lock().unwrap();
    let mut out = String::with_capacity(history.len() * 96);
    for (index, event) in history.iter().enumerate() {
        out.push_str(&edn_event(index, event));
        out.push('\n');
    }
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent).expect("create output dir");
    }
    std::fs::write(&args.out, out).expect("write history");

    let s = &shared.stats;
    eprintln!(
        "elle-history: isolation={} scan_ratio={} events={} ok={} write_conflict={} serialization_failure={} info={} scan_txns={}",
        args.isolation,
        args.scan_ratio,
        history.len(),
        s.ok.load(Ordering::Relaxed),
        s.write_conflict.load(Ordering::Relaxed),
        s.serialization_failure.load(Ordering::Relaxed),
        s.info.load(Ordering::Relaxed),
        s.scan_txns.load(Ordering::Relaxed),
    );
    if s.info.load(Ordering::Relaxed) > 0 {
        eprintln!("elle-history: degraded run (unexpected errors above)");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn edn_invoke_event() {
        let e = Event {
            typ: EventType::Invoke,
            process: 3,
            time_ns: 1200,
            value: vec![
                Op::Append { key: 7, value: 42 },
                Op::Read { key: 3, result: None },
            ],
        };
        assert_eq!(
            edn_event(0, &e),
            "{:index 0, :type :invoke, :f :txn, :process 3, :time 1200, :value [[:append 7 42] [:r 3 nil]]}"
        );
    }

    #[test]
    fn edn_ok_event_with_read_result() {
        let e = Event {
            typ: EventType::Ok,
            process: 0,
            time_ns: 5,
            value: vec![Op::Read { key: 1, result: Some(vec![12, 42]) }],
        };
        assert_eq!(
            edn_event(9, &e),
            "{:index 9, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 [12 42]]]}"
        );
    }

    #[test]
    fn edn_empty_list_read() {
        let e = Event {
            typ: EventType::Ok,
            process: 0,
            time_ns: 5,
            value: vec![Op::Read { key: 1, result: Some(vec![]) }],
        };
        assert_eq!(
            edn_event(0, &e),
            "{:index 0, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 []]]}"
        );
    }

    #[test]
    fn splitmix_is_deterministic() {
        let (mut a, mut b) = (SplitMix64(42), SplitMix64(42));
        for _ in 0..100 {
            assert_eq!(a.next(), b.next());
        }
        assert_ne!(SplitMix64(1).next(), SplitMix64(2).next());
    }

    #[test]
    fn gen_ops_respects_bounds_and_unique_values() {
        let keys = vec![10, 20, 30];
        let counter = AtomicU64::new(0);
        let mut rng = SplitMix64(7);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            let ops = gen_ops(&mut rng, &keys, 4, 0.4, &counter);
            assert_eq!(ops.len(), 4);
            for op in &ops {
                match op {
                    Op::Append { key, value } => {
                        assert!(keys.contains(key));
                        assert!(seen.insert(*value), "append values must be unique");
                    }
                    Op::Read { key, result } => {
                        assert!(keys.contains(key));
                        assert!(result.is_none());
                    }
                }
            }
        }
    }

    #[test]
    fn gen_ops_read_ratio_extremes() {
        let keys = vec![1];
        let counter = AtomicU64::new(0);
        let mut rng = SplitMix64(7);
        assert!(gen_ops(&mut rng, &keys, 8, 0.0, &counter)
            .iter()
            .all(|op| matches!(op, Op::Append { .. })));
        assert!(gen_ops(&mut rng, &keys, 8, 1.0, &counter)
            .iter()
            .all(|op| matches!(op, Op::Read { .. })));
    }

    /// A scan-mode transaction reads the table once up front, so it MUST serve
    /// later reads (and appends) from a local working copy — otherwise a read
    /// after an append in the same transaction would miss its own write and
    /// manufacture a false Elle anomaly. This is the correctness invariant the
    /// scan branch of `run_txn` has to preserve.
    #[test]
    fn scan_txn_reads_its_own_appends() {
        let store = Store::new(
            StoreConfig::builder()
                .writer_mode(WriterMode::MultiWriter)
                .isolation_level(IsolationLevel::Serializable)
                .build(),
        )
        .unwrap();
        let key = {
            let mut tx = store.begin_write(None).unwrap();
            let id = {
                let mut t = tx.open_table::<ElleRow>("elle").unwrap();
                t.insert(ElleRow { list: vec![] }).unwrap()
            };
            tx.commit().unwrap();
            id
        };
        let ops = vec![
            Op::Append { key, value: 100 },
            Op::Read { key, result: None },
            Op::Append { key, value: 200 },
            Op::Read { key, result: None },
        ];
        let completed = match run_txn(&store, &ops, true) {
            Ok(c) => c,
            Err(_) => panic!("uncontended scan txn should commit"),
        };
        match &completed[1] {
            Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![100]),
            other => panic!("expected read of [100], got {other:?}"),
        }
        match &completed[3] {
            Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![100, 200]),
            other => panic!("expected read of [100, 200], got {other:?}"),
        }
    }
}
