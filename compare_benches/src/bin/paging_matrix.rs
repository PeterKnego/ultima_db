// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `paging_matrix` — one cell of the "OS paging as a memory tier" baseline.
//!
//! Question: with a dataset larger than the memory budget, how does the
//! in-memory UltimaDB tree, tiered to swap by the kernel, compare to itself
//! unconstrained (the ceiling) and to purpose-built disk engines
//! (ReDB / RocksDB / Fjall) given the *same* budget?
//!
//! The process runs one cell: `load` → barrier → `run` → cgroup snapshot →
//! `drop`. The memory budget is applied *from outside* (a cgroup v2 scope,
//! see `scripts/paging_matrix.sh`): after loading, the harness writes its
//! RSS to `<barrier>.rss` and blocks until `<barrier>.go` appears, so the
//! driver can lower `memory.max` on the live scope before the measured phase.
//! Loading unconstrained and tightening afterwards means the load never pays
//! for swap; the kernel reclaims cold pages once, then the workload runs.
//!
//! Output: one JSON object on stdout; a human summary on stderr.
//!
//! Not criterion: criterion's warm-up/sampling loop is pathological under
//! swap, and the metrics that matter here (major faults per op, swap-ins)
//! are read from `/proc`, around a single long run with an op cap and a
//! wall-clock timeout.

use std::hint::black_box;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use ultima_bench_workloads::ycsb::ZipfianGenerator;

#[cfg(feature = "bench-mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();
const PAGE: u64 = 4096;

// ---------------------------------------------------------------------------
// Row: 64 bytes, no inner heap allocation. "Many small rows" — the per-row
// cost that matters is UltimaDB's own `Arc<V>` allocation, not the payload.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Row {
    a: u64,
    b: u64,
    pad: [u64; 6],
}

impl Row {
    fn new(seed: u64) -> Self {
        let x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        Row {
            a: seed,
            b: x,
            pad: [x ^ 1, x ^ 2, x ^ 3, x ^ 4, x ^ 5, x ^ 6],
        }
    }
}

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

fn encode_row(r: &Row) -> Vec<u8> {
    bincode::serde::encode_to_vec(r, BINCODE_CFG).expect("serialize")
}

// ---------------------------------------------------------------------------
// Engines
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LoadMode {
    /// `Store::bulk_load` (O(N) `from_sorted`) — the shape a *recovered*
    /// store has: values allocated in key order in one pass.
    Bulk,
    /// Per-transaction inserts in chunks — the shape a store built by
    /// traffic has (CoW churn scatters node allocations).
    Insert,
}

trait Engine {
    fn name(&self) -> &'static str;
    fn load(&mut self, rows: u64, mode: LoadMode);
    /// Must *consume* the value (return a field of it), not just locate it:
    /// UltimaDB's leaf holds `Arc<V>` pointers, so an `is_some()` check
    /// touches only the leaf page and leaves the value pages in swap.
    fn read(&self, key: u64) -> Option<u64>;
    fn update(&mut self, key: u64, seed: u64);
    /// DIAG mode needs the concrete store.
    fn as_ultima(&self) -> Option<&UltimaEngine> {
        None
    }
    /// On-disk directory of a disk engine (None for in-memory engines).
    fn disk_dir(&self) -> Option<&Path> {
        None
    }
}

/// Total size of the files under `dir` (recursive).
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
    }
    total
}

fn first_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().expect("value >= 8 bytes"))
}

// --- UltimaDB (in-memory, no persistence: the pure "OS does the tiering" case)

struct UltimaEngine {
    store: ultima_db::Store,
}

impl UltimaEngine {
    fn new() -> Self {
        let store = ultima_db::Store::new(ultima_db::StoreConfig::builder().build())
            .expect("Store::new");
        UltimaEngine { store }
    }
}

impl Engine for UltimaEngine {
    fn name(&self) -> &'static str {
        if cfg!(feature = "bench-mimalloc") {
            "ultima-mimalloc"
        } else {
            "ultima"
        }
    }

    fn as_ultima(&self) -> Option<&UltimaEngine> {
        Some(self)
    }

    fn load(&mut self, rows: u64, mode: LoadMode) {
        match mode {
            LoadMode::Bulk => {
                use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource};
                let src = BulkSource::Sorted(Box::new((1..=rows).map(|i| (i, Row::new(i)))));
                let opts = BulkLoadOptions {
                    create_if_missing: true,
                    ..Default::default()
                };
                self.store
                    .bulk_load::<Row>("rows", BulkLoadInput::Replace(src), opts)
                    .expect("bulk_load");
            }
            LoadMode::Insert => {
                const CHUNK: u64 = 10_000;
                let mut next = 1u64;
                while next <= rows {
                    let end = (next + CHUNK - 1).min(rows);
                    let mut wtx = self.store.begin_write(None).expect("begin_write");
                    {
                        let mut t = wtx.open_table::<Row>("rows").expect("open_table");
                        for i in next..=end {
                            t.put(i, Row::new(i)).expect("put");
                        }
                    }
                    wtx.commit().expect("commit");
                    next = end + 1;
                }
            }
        }
    }

    fn read(&self, key: u64) -> Option<u64> {
        let rtx = self.store.begin_read(None).expect("begin_read");
        let t = rtx.open_table::<Row>("rows").expect("open_table");
        t.get(key).map(|r| black_box(r.a ^ r.pad[5]))
    }

    fn update(&mut self, key: u64, seed: u64) {
        let mut wtx = self.store.begin_write(None).expect("begin_write");
        {
            let mut t = wtx.open_table::<Row>("rows").expect("open_table");
            t.update(key, Row::new(seed)).expect("update");
        }
        wtx.commit().expect("commit");
    }
}

// --- ReDB (on-disk CoW B-tree, values inline in leaves)

struct RedbEngine {
    db: redb::Database,
    _dir: tempfile::TempDir,
}

const REDB_TABLE: redb::TableDefinition<[u8; 8], &[u8]> = redb::TableDefinition::new("rows");

impl RedbEngine {
    fn new(disk_dir: &Path) -> Self {
        let dir = tempfile::tempdir_in(disk_dir).expect("tempdir");
        let db = redb::Database::create(dir.path().join("rows.redb")).expect("redb create");
        RedbEngine { db, _dir: dir }
    }
}

impl Engine for RedbEngine {
    fn name(&self) -> &'static str {
        "redb"
    }

    fn disk_dir(&self) -> Option<&Path> {
        Some(self._dir.path())
    }

    fn load(&mut self, rows: u64, _mode: LoadMode) {
        let mut tx = self.db.begin_write().expect("begin_write");
        tx.set_durability(redb::Durability::None).expect("durability");
        {
            let mut t = tx.open_table(REDB_TABLE).expect("open_table");
            for i in 1..=rows {
                t.insert(encode_key(i), encode_row(&Row::new(i)).as_slice())
                    .expect("insert");
            }
        }
        tx.commit().expect("commit");
    }

    fn read(&self, key: u64) -> Option<u64> {
        use redb::ReadableDatabase;
        let tx = self.db.begin_read().expect("begin_read");
        let t = tx.open_table(REDB_TABLE).expect("open_table");
        let v = t.get(encode_key(key)).expect("get");
        v.map(|g| black_box(first_u64(g.value())))
    }

    fn update(&mut self, key: u64, seed: u64) {
        let mut tx = self.db.begin_write().expect("begin_write");
        tx.set_durability(redb::Durability::None).expect("durability");
        {
            let mut t = tx.open_table(REDB_TABLE).expect("open_table");
            t.insert(encode_key(key), encode_row(&Row::new(seed)).as_slice())
                .expect("insert");
        }
        tx.commit().expect("commit");
    }
}

// --- RocksDB (LSM; default block cache, buffered I/O so page cache is charged)

struct RocksEngine {
    db: rocksdb::DB,
    _dir: tempfile::TempDir,
}

impl RocksEngine {
    fn new(disk_dir: &Path) -> Self {
        let dir = tempfile::tempdir_in(disk_dir).expect("tempdir");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(256 * 1024 * 1024);
        let db = rocksdb::DB::open(&opts, dir.path()).expect("rocksdb open");
        RocksEngine { db, _dir: dir }
    }
}

impl Engine for RocksEngine {
    fn name(&self) -> &'static str {
        "rocksdb"
    }

    fn disk_dir(&self) -> Option<&Path> {
        Some(self._dir.path())
    }

    fn load(&mut self, rows: u64, _mode: LoadMode) {
        let mut wo = rocksdb::WriteOptions::default();
        wo.disable_wal(true);
        for i in 1..=rows {
            self.db
                .put_opt(encode_key(i), encode_row(&Row::new(i)), &wo)
                .expect("put");
        }
        // Flush + full compaction so the read path is a settled tree, not a
        // pile of L0 files from the sequential load.
        self.db.flush().expect("flush");
        self.db.compact_range::<&[u8], &[u8]>(None, None);
    }

    fn read(&self, key: u64) -> Option<u64> {
        self.db
            .get(encode_key(key))
            .expect("get")
            .map(|v| black_box(first_u64(&v)))
    }

    fn update(&mut self, key: u64, seed: u64) {
        self.db
            .put(encode_key(key), encode_row(&Row::new(seed)))
            .expect("put");
    }
}

// --- Fjall (LSM)

struct FjallEngine {
    keyspace: fjall::Keyspace,
    _db: fjall::Database,
    _dir: tempfile::TempDir,
}

impl FjallEngine {
    fn new(disk_dir: &Path) -> Self {
        let dir = tempfile::tempdir_in(disk_dir).expect("tempdir");
        let db = fjall::Database::builder(dir.path()).open().expect("fjall open");
        let keyspace = db
            .keyspace("rows", fjall::KeyspaceCreateOptions::default)
            .expect("keyspace");
        FjallEngine {
            keyspace,
            _db: db,
            _dir: dir,
        }
    }
}

impl Engine for FjallEngine {
    fn name(&self) -> &'static str {
        "fjall"
    }

    fn disk_dir(&self) -> Option<&Path> {
        Some(self._dir.path())
    }

    fn load(&mut self, rows: u64, _mode: LoadMode) {
        for i in 1..=rows {
            self.keyspace
                .insert(encode_key(i), encode_row(&Row::new(i)))
                .expect("insert");
        }
    }

    fn read(&self, key: u64) -> Option<u64> {
        self.keyspace
            .get(encode_key(key))
            .expect("get")
            .map(|v| black_box(first_u64(&v)))
    }

    fn update(&mut self, key: u64, seed: u64) {
        self.keyspace
            .insert(encode_key(key), encode_row(&Row::new(seed)))
            .expect("insert");
    }
}

// ---------------------------------------------------------------------------
// /proc and cgroup readers
// ---------------------------------------------------------------------------

fn read_to_string(p: impl AsRef<Path>) -> Option<String> {
    std::fs::read_to_string(p).ok()
}

/// `/proc/self/stat` field (1-based, counting `pid` as 1).
fn stat_field(n: usize) -> u64 {
    let s = read_to_string("/proc/self/stat").unwrap_or_default();
    // Everything after the last ')' is whitespace-separated, starting at
    // `state` (field 3).
    let tail = s.rsplit(')').next().unwrap_or("");
    tail.split_whitespace()
        .nth(n - 3)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Major page faults (did I/O) of this process — field 12.
fn majflt() -> u64 {
    stat_field(12)
}

/// Minor page faults (no I/O: swap-cache hits from readahead, fresh
/// zero pages, CoW) of this process — field 10.
fn minflt() -> u64 {
    stat_field(10)
}

/// Resident set size in bytes (`/proc/self/statm` field 2 × page size).
fn rss_bytes() -> u64 {
    read_to_string("/proc/self/statm")
        .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
        .unwrap_or(0)
        * PAGE
}

/// System-wide counter from `/proc/vmstat` (pages).
fn vmstat(key: &str) -> u64 {
    read_to_string("/proc/vmstat")
        .unwrap_or_default()
        .lines()
        .find_map(|l| {
            let mut it = l.split_whitespace();
            (it.next()? == key).then(|| it.next()?.parse().ok())?
        })
        .unwrap_or(0)
}

fn cgroup_rel_path() -> Option<String> {
    let s = read_to_string("/proc/self/cgroup")?;
    let line = s.lines().find(|l| l.starts_with("0::"))?;
    Some(line[3..].trim().to_string())
}

fn cgroup_file(name: &str) -> Option<String> {
    let rel = cgroup_rel_path()?;
    read_to_string(format!("/sys/fs/cgroup{rel}/{name}")).map(|s| s.trim().to_string())
}

fn cgroup_stat(name: &str, key: &str) -> Option<u64> {
    cgroup_file(name)?.lines().find_map(|l| {
        let mut it = l.split_whitespace();
        (it.next()? == key).then(|| it.next()?.parse().ok())?
    })
}

/// `some avg10=… avg60=… total=…` → total stall microseconds.
fn cgroup_pressure_total_us() -> Option<u64> {
    cgroup_file("memory.pressure")?.lines().find_map(|l| {
        l.starts_with("some ").then(|| {
            l.split_whitespace()
                .find_map(|kv| kv.strip_prefix("total=")?.parse().ok())
        })?
    })
}

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------

struct Args {
    engine: String,
    rows: u64,
    dist: String,
    workload: String,
    ops: u64,
    timeout: Duration,
    load: LoadMode,
    barrier: Option<PathBuf>,
    disk_dir: Option<PathBuf>,
    ratio: String,
    seed: u64,
    /// Key range the workload draws from (default: all rows). A small
    /// value pins the workload to a hot subset, isolating write-path
    /// faults from key-coldness faults.
    keys: Option<u64>,
}

fn parse_args() -> Args {
    let mut a = Args {
        engine: "ultima".into(),
        rows: 1_000_000,
        dist: "zipf".into(),
        workload: "C".into(),
        ops: 2_000_000,
        timeout: Duration::from_secs(60),
        load: LoadMode::Bulk,
        barrier: None,
        disk_dir: None,
        ratio: "unlimited".into(),
        seed: 42,
        keys: None,
    };
    for arg in std::env::args().skip(1) {
        let (k, v) = arg
            .strip_prefix("--")
            .and_then(|s| s.split_once('='))
            .unwrap_or_else(|| panic!("bad arg {arg:?}; expected --key=value"));
        match k {
            "engine" => a.engine = v.into(),
            "rows" => a.rows = v.parse().expect("rows"),
            "dist" => a.dist = v.into(),
            "workload" => a.workload = v.to_ascii_uppercase(),
            "ops" => a.ops = v.parse().expect("ops"),
            "timeout-secs" => a.timeout = Duration::from_secs(v.parse().expect("timeout")),
            "load" => {
                a.load = match v {
                    "bulk" => LoadMode::Bulk,
                    "insert" => LoadMode::Insert,
                    _ => panic!("--load=bulk|insert"),
                }
            }
            "barrier" => a.barrier = Some(v.into()),
            "disk-dir" => a.disk_dir = Some(v.into()),
            "ratio" => a.ratio = v.into(),
            "seed" => a.seed = v.parse().expect("seed"),
            "keys" => a.keys = Some(v.parse().expect("keys")),
            _ => panic!("unknown arg --{k}"),
        }
    }
    assert!(
        matches!(a.workload.as_str(), "A" | "C" | "DIAG"),
        "--workload=A|C|DIAG (A: 50% read / 50% update; C: 100% read; DIAG: ultima-only per-phase fault decomposition)"
    );
    assert!(matches!(a.dist.as_str(), "zipf" | "uniform"), "--dist=zipf|uniform");
    a
}

// ---------------------------------------------------------------------------
// Barrier: publish RSS, wait for the driver to apply the budget.
// ---------------------------------------------------------------------------

fn barrier_wait(barrier: &Path, rss: u64) {
    let rss_path = barrier.with_extension("rss");
    let go_path = barrier.with_extension("go");
    let _ = std::fs::remove_file(&go_path);
    let body = format!(
        "rss_bytes={rss}\npid={}\ncgroup={}\n",
        std::process::id(),
        cgroup_rel_path().unwrap_or_default()
    );
    let tmp = barrier.with_extension("rss.tmp");
    std::fs::write(&tmp, body).expect("write barrier");
    std::fs::rename(&tmp, &rss_path).expect("rename barrier");
    eprintln!("[barrier] rss={} MiB; waiting for {}", rss >> 20, go_path.display());
    let start = Instant::now();
    while !go_path.exists() {
        std::thread::sleep(Duration::from_millis(20));
        if start.elapsed() > Duration::from_secs(600) {
            panic!("barrier timeout: driver never created {}", go_path.display());
        }
    }
    let _ = std::fs::remove_file(&go_path);
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Trajectory {
    t_secs: f64,
    ops: u64,
    majflt: u64,
}

#[derive(Serialize)]
struct Report {
    engine: &'static str,
    ratio: String,
    rows: u64,
    dist: String,
    keys: u64,
    workload: String,
    load: String,
    load_secs: f64,
    rss_after_load_bytes: u64,
    /// Bytes on disk after load (disk engines only). Compare against the
    /// budget: an LSM that compresses a compressible payload may fit its
    /// whole dataset in page cache at a ratio where the budget is meant
    /// to exclude it.
    disk_bytes_after_load: Option<u64>,
    memory_max: String,
    rss_after_tighten_bytes: u64,
    rss_after_run_bytes: u64,
    ops: u64,
    timed_out: bool,
    run_secs: f64,
    ops_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    majflt_run: u64,
    majflt_per_op: f64,
    minflt_run: u64,
    pswpin_run: u64,
    pswpout_run: u64,
    cg_anon_bytes: Option<u64>,
    cg_file_bytes: Option<u64>,
    cg_pgmajfault: Option<u64>,
    cg_events_max: Option<u64>,
    cg_events_oom: Option<u64>,
    cg_pressure_some_total_us: Option<u64>,
    trajectory: Vec<Trajectory>,
    drop_secs: f64,
    majflt_drop: u64,
    pswpin_drop: u64,
}

/// Per-phase major-fault decomposition of a cold-key update on UltimaDB:
/// read → update (buffered or CoW, depending on overlay) → commit (+auto gc).
/// Prints mean/max faults and time per phase. Ultima only.
fn diag(engine: Box<dyn Engine>, args: &Args) {
    let store = &engine.as_ultima().expect("DIAG is ultima-only").store;
    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    let keyspace = args.keys.unwrap_or(args.rows).min(args.rows);
    let n = args.ops.min(5000) as usize;
    let mut acc = [(0u64, 0u64, 0f64); 3]; // (sum faults, max faults, sum secs)
    let names = ["read", "update", "commit+gc"];
    for _ in 0..n {
        let key = rng.random_range(1..=keyspace);
        let mut phase = |i: usize, f: &mut dyn FnMut()| {
            let m0 = majflt();
            let t = Instant::now();
            f();
            let d = majflt() - m0;
            acc[i].0 += d;
            acc[i].1 = acc[i].1.max(d);
            acc[i].2 += t.elapsed().as_secs_f64();
        };
        phase(0, &mut || {
            let rtx = store.begin_read(None).unwrap();
            let t = rtx.open_table::<Row>("rows").unwrap();
            black_box(t.get(key).map(|r| r.a ^ r.pad[5]));
        });
        let mut wtx = Some(store.begin_write(None).unwrap());
        phase(1, &mut || {
            let w = wtx.as_mut().unwrap();
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(key, Row::new(key + 7)).unwrap();
        });
        phase(2, &mut || {
            wtx.take().unwrap().commit().unwrap();
        });
    }
    eprintln!(
        "[diag] rows={} keys={} n={} overlay_cap={} memory.max={}",
        args.rows,
        keyspace,
        n,
        std::env::var("ULTIMA_OVERLAY_CAP").unwrap_or_else(|_| "default(32)".into()),
        cgroup_file("memory.max").unwrap_or_else(|| "n/a".into())
    );
    for (i, name) in names.iter().enumerate() {
        eprintln!(
            "[diag]   {:<10} majflt mean={:>7.2} max={:>5}  mean_us={:>9.1}",
            name,
            acc[i].0 as f64 / n as f64,
            acc[i].1,
            acc[i].2 / n as f64 * 1e6
        );
    }
    let m0 = majflt();
    let t = Instant::now();
    drop(engine);
    eprintln!(
        "[diag]   drop       majflt={} secs={:.2}",
        majflt() - m0,
        t.elapsed().as_secs_f64()
    );
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)] as f64 / 1000.0
}

fn main() {
    let args = parse_args();
    let disk_dir = args
        .disk_dir
        .clone()
        .unwrap_or_else(ultima_bench_workloads::ycsb::bench_disk_dir);

    let mut engine: Box<dyn Engine> = match args.engine.as_str() {
        "ultima" => Box::new(UltimaEngine::new()),
        "redb" => Box::new(RedbEngine::new(&disk_dir)),
        "rocksdb" => Box::new(RocksEngine::new(&disk_dir)),
        "fjall" => Box::new(FjallEngine::new(&disk_dir)),
        other => panic!("unknown engine {other}"),
    };
    let name = engine.name();

    // --- load
    eprintln!("[{name}] loading {} rows ({:?})", args.rows, args.load);
    let t0 = Instant::now();
    engine.load(args.rows, args.load);
    let load_secs = t0.elapsed().as_secs_f64();
    let rss_after_load = rss_bytes();
    let disk_after_load = engine.disk_dir().map(dir_size);
    eprintln!(
        "[{name}] loaded in {load_secs:.1}s, rss={} MiB, disk={}",
        rss_after_load >> 20,
        disk_after_load.map_or("n/a".to_string(), |b| format!("{} MiB", b >> 20))
    );

    // --- barrier (driver lowers memory.max here)
    if let Some(b) = &args.barrier {
        barrier_wait(b, rss_after_load);
    }
    let memory_max = cgroup_file("memory.max").unwrap_or_else(|| "n/a".into());
    let rss_after_tighten = rss_bytes();
    eprintln!(
        "[{name}] memory.max={memory_max}, rss now {} MiB",
        rss_after_tighten >> 20
    );

    if args.workload == "DIAG" {
        diag(engine, &args);
        return;
    }

    // --- run
    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    let keyspace = args.keys.unwrap_or(args.rows).min(args.rows);
    let zipf = (args.dist == "zipf").then(|| ZipfianGenerator::new(keyspace, 0.99));
    let write_frac = if args.workload == "A" { 0.5 } else { 0.0 };

    let mut lat: Vec<u32> = Vec::with_capacity(args.ops.min(50_000_000) as usize);
    let mut trajectory = Vec::new();
    let majflt0 = majflt();
    let minflt0 = minflt();
    let pswpin0 = vmstat("pswpin");
    let pswpout0 = vmstat("pswpout");
    let run_start = Instant::now();
    let mut next_sample = Duration::from_secs(5);
    let mut ops = 0u64;
    let mut timed_out = false;
    let mut seed = args.rows + 1;

    while ops < args.ops {
        let key = match &zipf {
            Some(z) => z.next(&mut rng),
            None => rng.random_range(1..=keyspace),
        };
        let is_write = write_frac > 0.0 && rng.random_bool(write_frac);
        let t = Instant::now();
        if is_write {
            seed += 1;
            engine.update(key, seed);
        } else {
            let found = engine.read(key);
            debug_assert!(found.is_some(), "key {key} missing");
        }
        lat.push(t.elapsed().as_nanos().min(u32::MAX as u128) as u32);
        ops += 1;

        if ops & 0xFF == 0 {
            let el = run_start.elapsed();
            if el >= next_sample {
                trajectory.push(Trajectory {
                    t_secs: el.as_secs_f64(),
                    ops,
                    majflt: majflt() - majflt0,
                });
                next_sample += Duration::from_secs(5);
            }
            if el >= args.timeout {
                timed_out = true;
                break;
            }
        }
    }
    let run_secs = run_start.elapsed().as_secs_f64();
    let majflt_run = majflt() - majflt0;
    let minflt_run = minflt() - minflt0;
    let pswpin_run = vmstat("pswpin") - pswpin0;
    let pswpout_run = vmstat("pswpout") - pswpout0;
    trajectory.push(Trajectory {
        t_secs: run_secs,
        ops,
        majflt: majflt_run,
    });

    let rss_after_run = rss_bytes();

    // cgroup snapshot while the scope still exists
    let cg_anon = cgroup_stat("memory.stat", "anon");
    let cg_file = cgroup_stat("memory.stat", "file");
    let cg_pgmajfault = cgroup_stat("memory.stat", "pgmajfault");
    let cg_events_max = cgroup_stat("memory.events", "max");
    let cg_events_oom = cgroup_stat("memory.events", "oom");
    let cg_pressure = cgroup_pressure_total_us();

    lat.sort_unstable();
    let p50 = percentile(&lat, 0.50);
    let p99 = percentile(&lat, 0.99);
    let p999 = percentile(&lat, 0.999);
    let max = lat.last().copied().unwrap_or(0) as f64 / 1000.0;
    drop(lat);

    // --- drop: freeing a cold CoW tree faults every page in just to run
    // `Arc` destructors. Disk engines should be ~free here.
    let majflt_d0 = majflt();
    let pswpin_d0 = vmstat("pswpin");
    let td = Instant::now();
    drop(engine);
    let drop_secs = td.elapsed().as_secs_f64();
    let majflt_drop = majflt() - majflt_d0;
    let pswpin_drop = vmstat("pswpin") - pswpin_d0;

    let report = Report {
        engine: name,
        ratio: args.ratio.clone(),
        rows: args.rows,
        dist: args.dist.clone(),
        keys: keyspace,
        workload: args.workload.clone(),
        load: format!("{:?}", args.load).to_ascii_lowercase(),
        load_secs,
        rss_after_load_bytes: rss_after_load,
        disk_bytes_after_load: disk_after_load,
        memory_max,
        rss_after_tighten_bytes: rss_after_tighten,
        rss_after_run_bytes: rss_after_run,
        ops,
        timed_out,
        run_secs,
        ops_per_sec: ops as f64 / run_secs,
        p50_us: p50,
        p99_us: p99,
        p999_us: p999,
        max_us: max,
        majflt_run,
        majflt_per_op: majflt_run as f64 / ops.max(1) as f64,
        minflt_run,
        pswpin_run,
        pswpout_run,
        cg_anon_bytes: cg_anon,
        cg_file_bytes: cg_file,
        cg_pgmajfault,
        cg_events_max,
        cg_events_oom,
        cg_pressure_some_total_us: cg_pressure,
        trajectory,
        drop_secs,
        majflt_drop,
        pswpin_drop,
    };

    eprintln!(
        "[{name}] {}/{} ratio={} ops={} ({}) {:.0} ops/s p50={:.1}us p99={:.1}us p999={:.1}us majflt/op={:.3} minflt={} pswpin={} pswpout={} rss_end={}MiB psi_some={}ms drop={:.2}s (majflt {})",
        report.workload,
        report.dist,
        report.ratio,
        ops,
        if timed_out { "timeout" } else { "cap" },
        report.ops_per_sec,
        p50,
        p99,
        p999,
        report.majflt_per_op,
        minflt_run,
        pswpin_run,
        pswpout_run,
        rss_after_run >> 20,
        cg_pressure.unwrap_or(0) / 1000,
        drop_secs,
        majflt_drop,
    );
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, &report).expect("json");
    writeln!(out).expect("newline");
}
