//! elle-history: generate Elle list-append histories against UltimaDB
//! MultiWriter transactions (SI or Serializable), for checking with the
//! vendored elle-cli. See docs/tasks/task45_elle_consistency_harness.md.

use clap::Parser;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use ultima_db::{CommitWaiter, Error, IndexKind, IsolationLevel, Store, StoreConfig, WriterMode};

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
    /// Number of index buckets for predicate reads (rows are assigned
    /// `id_index % buckets` at seed).
    #[arg(long, default_value_t = 4)]
    buckets: usize,
    /// Probability a transaction reads a bucket group via the `bucket` index
    /// (`get_by_index`/`index_range`) instead of point/scan. Exercises the
    /// index read path; degrades to the same SSI `table_scan` tracking as a
    /// full scan. Default 0.0 preserves the point/scan workload.
    #[arg(long, default_value_t = 0.0)]
    predicate_ratio: f64,
    /// PRNG seed (per-thread stream: seed + thread index).
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Output path for the EDN history.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ElleRow {
    /// Static bucket, assigned once at seed (`i % buckets`), never mutated.
    /// Indexed for predicate reads.
    bucket: u64,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Predicate {
    /// Equality lookup on one bucket (`get_by_index`).
    Equality(u64),
    /// Inclusive bucket range (`index_range`), `lo <= hi`.
    Range(u64, u64),
}

/// Static bucket membership: `members[b]` = ids at seed positions `i` where
/// `i % buckets == b`, in seed order. Mirrors `seed`'s assignment exactly.
fn bucket_members(ids: &[u64], buckets: usize) -> Vec<Vec<u64>> {
    let mut m = vec![Vec::new(); buckets];
    for (i, &id) in ids.iter().enumerate() {
        m[i % buckets].push(id);
    }
    m
}

/// Deterministically choose an equality or range predicate (50/50) and resolve
/// its expected key set from static membership. Consumes RNG draws in a fixed
/// order so a given RNG state always yields the same predicate.
fn select_predicate(
    rng: &mut SplitMix64,
    buckets: usize,
    members: &[Vec<u64>],
) -> (Predicate, Vec<u64>) {
    if rng.chance(0.5) {
        let b = rng.below(buckets as u64);
        (Predicate::Equality(b), members[b as usize].clone())
    } else {
        let lo = rng.below(buckets as u64);
        let hi = lo + rng.below(buckets as u64 - lo); // lo..=hi, within [0, buckets)
        let mut keys = Vec::new();
        for b in lo..=hi {
            keys.extend_from_slice(&members[b as usize]);
        }
        (Predicate::Range(lo, hi), keys)
    }
}

fn gen_ops(
    rng: &mut SplitMix64,
    keys: &[u64],
    ops_per_txn: usize,
    read_ratio: f64,
    values: &AtomicU64,
) -> Vec<Op> {
    if keys.is_empty() {
        return Vec::new();
    }
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

#[derive(Debug)]
enum TxnFail {
    /// WriteConflict at the write site or at commit: definitely not committed.
    Conflict(Option<CommitWaiter>),
    /// SSI read-set validation failure at commit: definitely not committed.
    Serialization,
    /// A predicate index read returned a membership other than the static
    /// expectation — an index-maintenance bug, surfaced loudly.
    IndexIntegrity(String),
    /// Anything else: outcome treated as indeterminate.
    Other(Error),
}

enum Mode {
    Point,
    Scan,
    /// Predicate read over the `bucket` index. `expected` is the statically
    /// known membership the index read must return (integrity check).
    Predicate { query: Predicate, expected: Vec<u64> },
}

/// Run one list-append transaction. Returns completed ops (reads filled in) on
/// commit. In `Scan`/`Predicate` mode the transaction reads a group of rows up
/// front (registering the SSI coarse `table_scan` read) and serves reads/appends
/// from a local working copy, preserving read-your-writes; `Point` mode uses per-
/// op `get`. Predicate mode additionally asserts the index returned exactly the
/// statically-known bucket membership.
fn run_txn(store: &Store, ops: &[Op], mode: &Mode) -> Result<Vec<Op>, TxnFail> {
    let mut tx = store.begin_write(None).map_err(TxnFail::Other)?;
    let mut completed = Vec::with_capacity(ops.len());
    {
        let mut t = tx.open_table::<ElleRow>("elle").map_err(TxnFail::Other)?;
        let mut local: std::collections::HashMap<u64, ElleRow> = match mode {
            Mode::Scan => t.range(..).map(|(id, row)| (id, row.clone())).collect(),
            Mode::Predicate { query, expected } => {
                // The index read registers the SSI coarse `table_scan` read.
                let result = match query {
                    Predicate::Equality(b) => {
                        t.get_by_index("bucket", b).map_err(TxnFail::Other)?
                    }
                    Predicate::Range(lo, hi) => {
                        t.index_range("bucket", *lo..=*hi).map_err(TxnFail::Other)?
                    }
                };
                let mut got: Vec<u64> = result.iter().map(|(id, _)| *id).collect();
                let seeded: std::collections::HashMap<u64, ElleRow> =
                    result.iter().map(|(id, row)| (*id, (*row).clone())).collect();
                got.sort_unstable();
                let mut exp = expected.clone();
                exp.sort_unstable();
                if got != exp {
                    return Err(TxnFail::IndexIntegrity(format!(
                        "predicate {query:?} returned ids {got:?}, expected {exp:?}"
                    )));
                }
                seeded
            }
            Mode::Point => std::collections::HashMap::new(),
        };
        let from_local = !matches!(mode, Mode::Point);
        for op in ops {
            match op {
                Op::Read { key, .. } => {
                    let list = if from_local {
                        local.get(key).map(|r| r.list.clone()).unwrap_or_default()
                    } else {
                        t.get(*key).map(|r| r.list.clone()).unwrap_or_default()
                    };
                    completed.push(Op::Read { key: *key, result: Some(list) });
                }
                Op::Append { key, value } => {
                    let next = if from_local {
                        let row = local.get_mut(key).expect("local seeds every op key");
                        row.list.push(*value);
                        row.clone()
                    } else {
                        // get() both fetches the row and registers the SSI point read.
                        let mut row = t.get(*key).cloned().expect("seeded key exists");
                        row.list.push(*value);
                        row
                    };
                    match t.update(*key, next) {
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
    predicate_txns: AtomicU64,
}

struct Shared {
    store: Store,
    keys: Vec<u64>,
    members: Vec<Vec<u64>>,
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

fn build_store(isolation: IsolationLevel) -> Store {
    Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .isolation_level(isolation)
            .build(),
    )
    .expect("store construction")
}

/// Seed `keys` rows with round-robin buckets (`ids[i]` has bucket `i % buckets`)
/// and define the non-unique `"bucket"` index. Returns row ids in seed order.
fn seed(store: &Store, keys: usize, buckets: usize) -> Vec<u64> {
    let mut tx = store.begin_write(None).expect("setup begin_write");
    let ids = {
        let mut t = tx.open_table::<ElleRow>("elle").expect("setup open_table");
        let rows: Vec<ElleRow> = (0..keys)
            .map(|i| ElleRow { bucket: (i % buckets) as u64, list: Vec::new() })
            .collect();
        let ids = t.insert_batch(rows).expect("setup insert_batch");
        t.define_index("bucket", IndexKind::NonUnique, |r: &ElleRow| r.bucket)
            .expect("setup define_index");
        ids
    };
    tx.commit().expect("setup commit");
    ids
}

fn worker(shared: &Shared, args: &Args, process: usize) {
    let mut rng = SplitMix64(args.seed.wrapping_add(process as u64));
    for _ in 0..args.txns_per_thread {
        // Mode is rolled before op generation because predicate mode restricts
        // ops to the selected bucket's keys. predicate -> scan -> point.
        let mode = if rng.chance(args.predicate_ratio) {
            let (query, expected) = select_predicate(&mut rng, args.buckets, &shared.members);
            shared.stats.predicate_txns.fetch_add(1, Ordering::Relaxed);
            Mode::Predicate { query, expected }
        } else if rng.chance(args.scan_ratio) {
            shared.stats.scan_txns.fetch_add(1, Ordering::Relaxed);
            Mode::Scan
        } else {
            Mode::Point
        };
        let key_subset: &[u64] = match &mode {
            Mode::Predicate { expected, .. } => expected,
            _ => &shared.keys,
        };
        let ops = gen_ops(
            &mut rng,
            key_subset,
            args.ops_per_txn,
            args.read_ratio,
            &shared.values,
        );
        shared.push(EventType::Invoke, process, ops.clone());
        match run_txn(&shared.store, &ops, &mode) {
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
            Err(TxnFail::IndexIntegrity(msg)) => {
                eprintln!("elle-history: process {process}: INDEX INTEGRITY VIOLATION: {msg}");
                shared.stats.info.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Info, process, ops);
                return;
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

    if args.buckets == 0 {
        eprintln!("elle-history: --buckets must be >= 1");
        std::process::exit(2);
    }

    let store = build_store(isolation);

    // Pre-seed the keyspace: TableWriter has no insert-at-key, and update()
    // requires the key to exist, so the workload is pure get+update.
    let keys = seed(&store, args.keys, args.buckets);
    let members = bucket_members(&keys, args.buckets);

    let shared = Arc::new(Shared {
        store,
        keys,
        members,
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
        "elle-history: isolation={} scan_ratio={} predicate_ratio={} events={} ok={} write_conflict={} serialization_failure={} info={} scan_txns={} predicate_txns={}",
        args.isolation,
        args.scan_ratio,
        args.predicate_ratio,
        history.len(),
        s.ok.load(Ordering::Relaxed),
        s.write_conflict.load(Ordering::Relaxed),
        s.serialization_failure.load(Ordering::Relaxed),
        s.info.load(Ordering::Relaxed),
        s.scan_txns.load(Ordering::Relaxed),
        s.predicate_txns.load(Ordering::Relaxed),
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
                t.insert(ElleRow { bucket: 0, list: vec![] }).unwrap()
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
        let completed = match run_txn(&store, &ops, &Mode::Scan) {
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

    #[test]
    fn point_append_preserves_bucket_and_index_membership() {
        let store = build_store(IsolationLevel::SnapshotIsolation);
        let ids = seed(&store, 3, 3); // ids[1] has bucket 1

        let ops = vec![Op::Append { key: ids[1], value: 5 }];
        run_txn(&store, &ops, &Mode::Point).expect("uncontended append commits");

        let mut tx = store.begin_write(None).unwrap();
        let t = tx.open_table::<ElleRow>("elle").unwrap();
        assert_eq!(t.get(ids[1]).unwrap().bucket, 1, "append must not change bucket");
        let bucket1: Vec<u64> =
            t.get_by_index("bucket", &1u64).unwrap().iter().map(|(id, _)| *id).collect();
        assert!(bucket1.contains(&ids[1]), "row must still be in its bucket after append");
    }

    #[test]
    fn seed_assigns_round_robin_buckets_and_indexes_them() {
        use std::collections::BTreeSet;
        let store = build_store(IsolationLevel::Serializable);
        let ids = seed(&store, 6, 3); // buckets: 0,1,2,0,1,2

        let mut tx = store.begin_write(None).unwrap();
        let t = tx.open_table::<ElleRow>("elle").unwrap();
        let bucket0: BTreeSet<u64> =
            t.get_by_index("bucket", &0u64).unwrap().iter().map(|(id, _)| *id).collect();
        assert_eq!(bucket0, BTreeSet::from([ids[0], ids[3]]));
        let bucket2: BTreeSet<u64> =
            t.get_by_index("bucket", &2u64).unwrap().iter().map(|(id, _)| *id).collect();
        assert_eq!(bucket2, BTreeSet::from([ids[2], ids[5]]));
    }

    #[test]
    fn bucket_members_round_robin() {
        let ids = vec![10, 20, 30, 40, 50];
        let m = bucket_members(&ids, 2);
        assert_eq!(m, vec![vec![10, 30, 50], vec![20, 40]]);
    }

    #[test]
    fn select_predicate_equality_and_range_within_bounds() {
        let ids = vec![10, 20, 30, 40];
        let members = bucket_members(&ids, 2); // [[10,30],[20,40]]
        let mut rng = SplitMix64(1);
        for _ in 0..200 {
            let (p, keys) = select_predicate(&mut rng, 2, &members);
            match p {
                Predicate::Equality(b) => {
                    assert!(b < 2);
                    assert_eq!(keys, members[b as usize]);
                }
                Predicate::Range(lo, hi) => {
                    assert!(lo <= hi && hi < 2);
                    let mut expected = Vec::new();
                    for b in lo..=hi {
                        expected.extend_from_slice(&members[b as usize]);
                    }
                    assert_eq!(keys, expected);
                }
            }
        }
    }

    #[test]
    fn gen_ops_empty_keys_returns_no_ops() {
        let counter = AtomicU64::new(0);
        let mut rng = SplitMix64(7);
        assert!(gen_ops(&mut rng, &[], 4, 0.4, &counter).is_empty());
    }

    #[test]
    fn predicate_txn_reads_its_own_appends_equality() {
        let store = build_store(IsolationLevel::Serializable);
        let ids = seed(&store, 4, 2); // bucket 0 = {ids[0], ids[2]}
        let members = bucket_members(&ids, 2);

        // Equality predicate on bucket 0, ops within that bucket.
        let mode = Mode::Predicate { query: Predicate::Equality(0), expected: members[0].clone() };
        let ops = vec![
            Op::Append { key: ids[0], value: 100 },
            Op::Read { key: ids[0], result: None },
            Op::Append { key: ids[2], value: 200 },
            Op::Read { key: ids[2], result: None },
        ];
        let completed = run_txn(&store, &ops, &mode).expect("uncontended predicate txn commits");
        match &completed[1] {
            Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![100]),
            other => panic!("expected read of [100], got {other:?}"),
        }
        match &completed[3] {
            Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![200]),
            other => panic!("expected read of [200], got {other:?}"),
        }
    }

    #[test]
    fn predicate_txn_reads_its_own_appends_range() {
        let store = build_store(IsolationLevel::Serializable);
        let ids = seed(&store, 4, 2);
        let members = bucket_members(&ids, 2);
        let mut expected = members[0].clone();
        expected.extend_from_slice(&members[1]); // range 0..=1 = all keys

        let mode = Mode::Predicate { query: Predicate::Range(0, 1), expected };
        let ops = vec![
            Op::Append { key: ids[1], value: 7 },
            Op::Read { key: ids[1], result: None },
        ];
        let completed = run_txn(&store, &ops, &mode).expect("range predicate txn commits");
        match &completed[1] {
            Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![7]),
            other => panic!("expected read of [7], got {other:?}"),
        }
    }

    #[test]
    fn predicate_txn_preserves_bucket_after_append() {
        let store = build_store(IsolationLevel::SnapshotIsolation);
        let ids = seed(&store, 4, 2);
        let members = bucket_members(&ids, 2);

        let mode = Mode::Predicate { query: Predicate::Equality(1), expected: members[1].clone() };
        let ops = vec![Op::Append { key: ids[1], value: 9 }];
        run_txn(&store, &ops, &mode).expect("commit");

        // The row is still in bucket 1, so a fresh equality predicate still finds it.
        let mut tx = store.begin_write(None).unwrap();
        let t = tx.open_table::<ElleRow>("elle").unwrap();
        let b1: Vec<u64> =
            t.get_by_index("bucket", &1u64).unwrap().iter().map(|(id, _)| *id).collect();
        assert!(b1.contains(&ids[1]));
    }

    #[test]
    fn generation_is_reproducible_for_a_fixed_seed() {
        let ids = vec![10, 20, 30, 40];
        let members = bucket_members(&ids, 2);

        let run = || {
            let counter = AtomicU64::new(1);
            let mut rng = SplitMix64(99);
            let (p, keys) = select_predicate(&mut rng, 2, &members);
            let ops = gen_ops(&mut rng, &keys, 4, 0.4, &counter);
            (p, keys, format!("{ops:?}"))
        };
        assert_eq!(run(), run(), "same seed must produce identical predicate + ops");
    }
}
