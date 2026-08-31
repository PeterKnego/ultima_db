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
    /// Drop the live store and reopen + recover it from the same on-disk
    /// directory, timing the reopen+recover. `None` if this engine doesn't
    /// support the `--restart` protocol (every engine except `ultima-paged`,
    /// as of task16).
    fn restart(&mut self) -> Option<f64> {
        None
    }
    /// Cumulative data-page faults (`PagedStats::data_page_faults`) for a
    /// paged UltimaDB engine — `None` for every other engine, including the
    /// plain in-memory `ultima` engine.
    fn paged_data_faults(&self) -> Option<u64> {
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

// --- UltimaDB paged (on-disk B-tree node store; the store itself demotes
// leaves under a memory budget, instead of relying on the OS to page an
// in-memory tree out to swap). See `Persistence::paged`/`PagedOptions`.

struct UltimaPagedEngine {
    // `Option` so `restart()` can drop the live store (stopping its
    // background checkpointer thread) before opening a fresh one against
    // the same directory — two live `Store`s against the same page/WAL file
    // would race.
    store: Option<ultima_db::Store>,
    dir: tempfile::TempDir,
    budget: u64,
}

impl UltimaPagedEngine {
    fn new(disk_dir: &Path, budget: u64) -> Self {
        let dir = tempfile::tempdir_in(disk_dir).expect("tempdir");
        let store = Self::open(dir.path(), budget);
        UltimaPagedEngine {
            store: Some(store),
            dir,
            budget,
        }
    }

    /// `Store::new` + `register_table` against `path` — does *not* recover;
    /// callers that need the on-disk state loaded call `.recover()`
    /// themselves (so its cost can be timed separately, see `restart`).
    fn open(path: &Path, budget: u64) -> ultima_db::Store {
        let p = ultima_db::Persistence::standalone(
            path,
            ultima_db::Durability::Eventual,
            ultima_db::WalWrite::Coalesced,
        )
        .paged(ultima_db::PagedOptions::builder().memory_budget_bytes(budget).build())
        .expect("paged persistence");
        let store = ultima_db::Store::new(ultima_db::StoreConfig::builder().persistence(p).build())
            .expect("Store::new");
        store.register_table::<Row>("rows").expect("register_table");
        store
    }

    fn store(&self) -> &ultima_db::Store {
        self.store.as_ref().expect("store live between restart() calls")
    }

    fn store_mut(&mut self) -> &mut ultima_db::Store {
        self.store.as_mut().expect("store live between restart() calls")
    }
}

impl Engine for UltimaPagedEngine {
    fn name(&self) -> &'static str {
        "ultima-paged"
    }

    fn disk_dir(&self) -> Option<&Path> {
        Some(self.dir.path())
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
                self.store_mut()
                    .bulk_load::<Row>("rows", BulkLoadInput::Replace(src), opts)
                    .expect("bulk_load");
            }
            LoadMode::Insert => {
                // Batched `insert_batch` in write transactions, not a
                // put-loop — this is the "built by writes" criterion: CoW
                // churn scatters node allocations the way real traffic
                // would, rather than the single sorted `from_sorted` pass
                // `LoadMode::Bulk` takes.
                const CHUNK: u64 = 10_000;
                let store = self.store_mut();
                let mut next = 1u64;
                while next <= rows {
                    let end = (next + CHUNK - 1).min(rows);
                    let mut wtx = store.begin_write(None).expect("begin_write");
                    {
                        let mut t = wtx.open_table::<Row>("rows").expect("open_table");
                        let batch: Vec<Row> = (next..=end).map(Row::new).collect();
                        t.insert_batch(batch).expect("insert_batch");
                    }
                    wtx.commit().expect("commit");
                    next = end + 1;
                }
            }
        }
        // Write every dirty leaf to `pages.bin` and assign page ids. With
        // `memory_budget_bytes` configured this also runs phase-3 demotion,
        // so the run phase below starts from a genuinely paged tree instead
        // of relying on the background checkpointer's first tick.
        self.store().checkpoint().expect("checkpoint");
    }

    fn read(&self, key: u64) -> Option<u64> {
        let rtx = self.store().begin_read(None).expect("begin_read");
        let t = rtx.open_table::<Row>("rows").expect("open_table");
        t.get(key).map(|r| black_box(r.a ^ r.pad[5]))
    }

    fn update(&mut self, key: u64, seed: u64) {
        let store = self.store_mut();
        let mut wtx = store.begin_write(None).expect("begin_write");
        {
            let mut t = wtx.open_table::<Row>("rows").expect("open_table");
            t.update(key, Row::new(seed)).expect("update");
        }
        wtx.commit().expect("commit");
    }

    fn restart(&mut self) -> Option<f64> {
        let path = self.dir.path().to_path_buf();
        let budget = self.budget;
        self.store = None; // drop first: see the `store` field's doc
        let t0 = Instant::now();
        let store = Self::open(&path, budget);
        store.recover().expect("recover");
        let secs = t0.elapsed().as_secs_f64();
        self.store = Some(store);
        Some(secs)
    }

    fn paged_data_faults(&self) -> Option<u64> {
        self.store().paged_stats().map(|s| s.data_page_faults)
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
    /// `PagedOptions::memory_budget_bytes` for `--engine=ultima-paged`.
    /// Required for that engine (there is no sane default: it is the
    /// whole point of the cell).
    paged_budget: Option<u64>,
    /// After the run phase, drop the store, reopen + recover it, then run
    /// the same workload again — reports `recover_secs`/`ops_per_sec_2`/
    /// `pf_per_op_2` alongside the first run's numbers.
    restart: bool,
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
        paged_budget: None,
        restart: false,
    };
    let mut load_explicit = false;
    for arg in std::env::args().skip(1) {
        // `--restart` is a bare flag, not `--key=value`.
        if arg == "--restart" {
            a.restart = true;
            continue;
        }
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
                load_explicit = true;
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
            "paged-budget" => a.paged_budget = Some(v.parse().expect("paged-budget")),
            _ => panic!("unknown arg --{k}"),
        }
    }
    assert!(
        matches!(a.workload.as_str(), "A" | "C" | "DIAG"),
        "--workload=A|C|DIAG (A: 50% read / 50% update; C: 100% read; DIAG: ultima-only per-phase fault decomposition)"
    );
    assert!(matches!(a.dist.as_str(), "zipf" | "uniform"), "--dist=zipf|uniform");
    if a.engine == "ultima-paged" {
        assert!(
            a.paged_budget.is_some(),
            "--engine=ultima-paged requires --paged-budget=BYTES"
        );
        // "Built by writes" is this engine's whole point (see `run` module
        // doc / task16 brief) — default to it unless the caller overrode.
        if !load_explicit {
            a.load = LoadMode::Insert;
        }
    }
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
    /// `PagedOptions::memory_budget_bytes` (`--engine=ultima-paged` only).
    paged_budget_bytes: Option<u64>,
    /// Data-page faults (`PagedStats::data_page_faults`) per op over the
    /// run phase — `--engine=ultima-paged` only, `null` for every other
    /// engine (they report OS-level `majflt_per_op` instead: a page-FILE
    /// fault here is a positioned `pread` against `pages.bin`, not a kernel
    /// major fault).
    pf_per_op: Option<f64>,
    /// `--restart`: time to drop the store and reopen + recover it from
    /// disk (`Store::new` + `register_table` + `Store::recover`).
    recover_secs: Option<f64>,
    /// `--restart`: `ops_per_sec` of the second run (after recovery).
    ops_per_sec_2: Option<f64>,
    /// `--restart`: `pf_per_op` of the second run (after recovery).
    pf_per_op_2: Option<f64>,
}

/// One pass through the op loop: same shape whether it's the first run or,
/// under `--restart`, the second run against a freshly recovered store.
struct RunOutcome {
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
    trajectory: Vec<Trajectory>,
    pf_per_op: Option<f64>,
}

/// Run `args.ops` (or until `args.timeout`) reads/updates against `engine`,
/// drawing keys from `args.dist`/`args.keys` with `rng_seed`. Shared by the
/// first run and, under `--restart`, the second (post-recovery) run.
fn run_workload(engine: &mut dyn Engine, args: &Args, rng_seed: u64) -> RunOutcome {
    let mut rng = rand::rngs::StdRng::seed_from_u64(rng_seed);
    let keyspace = args.keys.unwrap_or(args.rows).min(args.rows);
    let zipf = (args.dist == "zipf").then(|| ZipfianGenerator::new(keyspace, 0.99));
    let write_frac = if args.workload == "A" { 0.5 } else { 0.0 };

    let mut lat: Vec<u32> = Vec::with_capacity(args.ops.min(50_000_000) as usize);
    let mut trajectory = Vec::new();
    let majflt0 = majflt();
    let minflt0 = minflt();
    let pswpin0 = vmstat("pswpin");
    let pswpout0 = vmstat("pswpout");
    let pf0 = engine.paged_data_faults();
    let run_start = Instant::now();
    let mut next_sample = Duration::from_secs(5);
    let mut ops = 0u64;
    let mut timed_out = false;
    let mut wseed = args.rows + 1;

    while ops < args.ops {
        let key = match &zipf {
            Some(z) => z.next(&mut rng),
            None => rng.random_range(1..=keyspace),
        };
        let is_write = write_frac > 0.0 && rng.random_bool(write_frac);
        let t = Instant::now();
        if is_write {
            wseed += 1;
            engine.update(key, wseed);
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
    let pf_per_op = pf0
        .zip(engine.paged_data_faults())
        .map(|(before, after)| (after - before) as f64 / ops.max(1) as f64);

    lat.sort_unstable();
    let p50 = percentile(&lat, 0.50);
    let p99 = percentile(&lat, 0.99);
    let p999 = percentile(&lat, 0.999);
    let max = lat.last().copied().unwrap_or(0) as f64 / 1000.0;

    RunOutcome {
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
        trajectory,
        pf_per_op,
    }
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
        "ultima-paged" => Box::new(UltimaPagedEngine::new(
            &disk_dir,
            args.paged_budget.expect("checked in parse_args"),
        )),
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
    let keyspace = args.keys.unwrap_or(args.rows).min(args.rows);
    let run1 = run_workload(engine.as_mut(), &args, args.seed);
    let ops = run1.ops;
    let timed_out = run1.timed_out;
    let run_secs = run1.run_secs;
    let p50 = run1.p50_us;
    let p99 = run1.p99_us;
    let p999 = run1.p999_us;
    let minflt_run = run1.minflt_run;
    let pswpin_run = run1.pswpin_run;
    let pswpout_run = run1.pswpout_run;

    let rss_after_run = rss_bytes();

    // --- restart (opt-in): drop the store, reopen + recover, run the same
    // workload again. Only `ultima-paged` implements `Engine::restart`.
    let mut recover_secs = None;
    let mut ops_per_sec_2 = None;
    let mut pf_per_op_2 = None;
    if args.restart {
        let secs = engine
            .restart()
            .unwrap_or_else(|| panic!("--restart is not supported by engine {name}"));
        eprintln!("[{name}] recovered in {secs:.3}s");
        recover_secs = Some(secs);
        let run2 = run_workload(engine.as_mut(), &args, args.seed);
        eprintln!(
            "[{name}] (restart) {}/{} ops={} {:.0} ops/s pf/op={}",
            args.workload,
            args.dist,
            run2.ops,
            run2.ops_per_sec,
            run2.pf_per_op.map_or("n/a".to_string(), |v| format!("{v:.3}")),
        );
        ops_per_sec_2 = Some(run2.ops_per_sec);
        pf_per_op_2 = run2.pf_per_op;
    }

    // cgroup snapshot while the scope still exists (cumulative over both
    // runs above, if `--restart` was passed)
    let cg_anon = cgroup_stat("memory.stat", "anon");
    let cg_file = cgroup_stat("memory.stat", "file");
    let cg_pgmajfault = cgroup_stat("memory.stat", "pgmajfault");
    let cg_events_max = cgroup_stat("memory.events", "max");
    let cg_events_oom = cgroup_stat("memory.events", "oom");
    let cg_pressure = cgroup_pressure_total_us();

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
        max_us: run1.max_us,
        majflt_run: run1.majflt_run,
        majflt_per_op: run1.majflt_per_op,
        minflt_run,
        pswpin_run,
        pswpout_run,
        cg_anon_bytes: cg_anon,
        cg_file_bytes: cg_file,
        cg_pgmajfault,
        cg_events_max,
        cg_events_oom,
        cg_pressure_some_total_us: cg_pressure,
        trajectory: run1.trajectory,
        drop_secs,
        majflt_drop,
        pswpin_drop,
        paged_budget_bytes: args.paged_budget,
        pf_per_op: run1.pf_per_op,
        recover_secs,
        ops_per_sec_2,
        pf_per_op_2,
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
