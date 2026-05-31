// WAL-only microbenchmark — isolates the write-ahead-log I/O path from the rest
// of the store so we can measure the cost of a durable commit and compare write
// backends (sinks) across record sizes.
//
// Dimensions: sink {fswrite[,buffered,mmap,iouring]} x size {1,2,4,8,16 KiB} x
// view {consistent single, eventual single, eventual batch}.
//
// IMPORTANT: fsync on tmpfs is a no-op, which would make every number here
// meaningless. The WAL dir is pinned to `target/wal-bench` (real disk); the
// harness prints the backing filesystem at startup and panics if it resolves to
// tmpfs/ramfs.

use std::path::{Path, PathBuf};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::wal::{BenchWal, WalEntry, WalOp, WalSinkKind};

/// Record payload sizes to sweep (bytes).
const SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384];

/// Number of entries per Eventual fire-and-forget batch.
const EVENTUAL_BATCH: u64 = 256;

/// Sinks under test (fs-write baseline, buffered, mmap, and io_uring when enabled).
/// Array elements cannot carry cfg attributes, so we split into two const
/// definitions selected by target and feature flags at compile time.
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
    ("iouring", WalSinkKind::IoUring),
];
#[cfg(not(all(target_os = "linux", feature = "wal-iouring")))]
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
];

/// Build a single-op WAL entry whose payload is `payload` bytes.
fn make_entry(version: u64, payload: usize) -> WalEntry {
    WalEntry {
        version,
        ops: vec![WalOp::Insert { table: "bench".to_string(), id: version, data: vec![0u8; payload] }],
    }
}

/// Resolve the (mount point, fstype) backing `path` by scanning /proc/mounts.
fn backing_fs(path: &Path) -> (String, String) {
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    let mut best: (usize, String, String) = (0, String::new(), "unknown".to_string());
    for line in mounts.lines() {
        let mut f = line.split_whitespace();
        let _dev = f.next();
        let mp = match f.next() {
            Some(m) => m,
            None => continue,
        };
        let fstype = f.next().unwrap_or("unknown");
        if canon.starts_with(Path::new(mp)) && mp.len() >= best.0 {
            best = (mp.len(), mp.to_string(), fstype.to_string());
        }
    }
    (best.1, best.2)
}

/// Create and return the on-disk benchmark root, asserting it is not tmpfs/ramfs.
fn bench_root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target").join("wal-bench");
    std::fs::create_dir_all(&root).expect("create bench root");
    let (mount, fs) = backing_fs(&root);
    eprintln!("[wal_bench] WAL dir = {} | mount = {} | fs = {}", root.display(), mount, fs);
    assert!(
        fs != "tmpfs" && fs != "ramfs",
        "WAL benchmark dir is on {fs} (mount {mount}); fsync is a no-op there, so \
         results would be meaningless. Point the bench at real disk."
    );
    root
}

/// Open a fresh WAL under `root` with the given sink. The TempDir is returned so
/// the caller controls when it (and the background writer thread) is dropped.
fn new_wal(root: &Path, consistent: bool, kind: WalSinkKind) -> (tempfile::TempDir, BenchWal) {
    let dir = tempfile::Builder::new().prefix("wal").tempdir_in(root).expect("tempdir on bench root");
    let wal = BenchWal::new(dir.path(), consistent, kind).expect("open BenchWal");
    (dir, wal)
}

fn label(size: usize) -> String {
    format!("{}KiB", size / 1024)
}

fn wal_benches(c: &mut Criterion) {
    let root = bench_root();

    // --- Consistent: one durable commit (write + fsync wait) per iteration. ---
    {
        let mut g = c.benchmark_group("wal_commit_consistent");
        for &(kind_name, kind) in KINDS {
            for &size in SIZES {
                g.throughput(Throughput::Bytes(size as u64));
                g.bench_function(BenchmarkId::new(kind_name, label(size)), |b| {
                    b.iter_batched(
                        || new_wal(&root, true, kind),
                        |(dir, wal)| {
                            wal.commit_consistent(make_entry(1, size)).unwrap();
                            (dir, wal)
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
        g.finish();
    }

    // --- Eventual: caller-visible latency of one fire-and-forget commit (no
    // drain). The fsync runs on the bg thread and is forced off-clock at teardown.
    {
        let mut g = c.benchmark_group("wal_commit_eventual");
        for &(kind_name, kind) in KINDS {
            for &size in SIZES {
                g.throughput(Throughput::Bytes(size as u64));
                g.bench_function(BenchmarkId::new(kind_name, label(size)), |b| {
                    b.iter_batched(
                        || new_wal(&root, false, kind),
                        |(dir, wal)| {
                            wal.commit_eventual(make_entry(1, size)).unwrap();
                            (dir, wal)
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
        g.finish();
    }

    // --- Eventual batched: fire-and-forget batch + single drain (group commit). -
    {
        let mut g = c.benchmark_group("wal_eventual_batch");
        g.sample_size(20);
        for &(kind_name, kind) in KINDS {
            for &size in SIZES {
                g.throughput(Throughput::Bytes(size as u64 * EVENTUAL_BATCH));
                g.bench_function(BenchmarkId::new(kind_name, label(size)), |b| {
                    b.iter_batched(
                        || new_wal(&root, false, kind),
                        |(dir, wal)| {
                            for v in 1..=EVENTUAL_BATCH {
                                wal.commit_eventual(make_entry(v, size)).unwrap();
                            }
                            wal.wait_durable(EVENTUAL_BATCH).unwrap();
                            (dir, wal)
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
        g.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(4))
        .warm_up_time(Duration::from_secs(1));
    targets = wal_benches
}
criterion_main!(benches);
