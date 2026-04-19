//! Metrics and observability for UltimaDB.
//!
//! This module provides atomic counters for store-level and table-level
//! operations, along with snapshot types for reading the current values.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

// ---------------------------------------------------------------------------
// Public snapshot types
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of all store-level metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub commits: u64,
    pub rollbacks: u64,
    pub gc_runs: u64,
    pub snapshots_collected: u64,
    pub write_conflicts: u64,
    /// MultiWriter commit-phase cumulative wall time, in nanoseconds.
    /// Useful for profiling where commit-path time is actually spent.
    /// All four sum to approximately the total time inside `commit()`.
    pub commit_ns_phase0_acquire_locks: u64,
    pub commit_ns_phase1_read_validate: u64,
    pub commit_ns_phase2_merge: u64,
    pub commit_ns_phase3_install: u64,
    pub tables: HashMap<String, TableMetricsSnapshot>,
}

/// A point-in-time snapshot of per-table metrics.
#[derive(Debug, Clone)]
pub struct TableMetricsSnapshot {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub primary_key_reads: u64,
    pub primary_key_scans: u64,
    pub indexes: HashMap<String, IndexMetricsSnapshot>,
}

/// A point-in-time snapshot of per-index metrics.
#[derive(Debug, Clone)]
pub struct IndexMetricsSnapshot {
    pub reads: u64,
    pub range_scans: u64,
}

// ---------------------------------------------------------------------------
// Internal counter types
// ---------------------------------------------------------------------------

/// Per-index atomic counters.
struct IndexMetrics {
    reads: AtomicU64,
    range_scans: AtomicU64,
}

impl IndexMetrics {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            range_scans: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> IndexMetricsSnapshot {
        IndexMetricsSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            range_scans: self.range_scans.load(Ordering::Relaxed),
        }
    }
}

/// Per-table atomic counters plus per-index metrics.
struct TableMetrics {
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    primary_key_reads: AtomicU64,
    primary_key_scans: AtomicU64,
    indexes: RwLock<HashMap<String, IndexMetrics>>,
}

impl TableMetrics {
    fn new() -> Self {
        Self {
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            primary_key_reads: AtomicU64::new(0),
            primary_key_scans: AtomicU64::new(0),
            indexes: RwLock::new(HashMap::new()),
        }
    }

    fn snapshot(&self) -> TableMetricsSnapshot {
        let indexes = self
            .indexes
            .read()
            .unwrap()
            .iter()
            .map(|(name, m)| (name.clone(), m.snapshot()))
            .collect();
        TableMetricsSnapshot {
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            primary_key_reads: self.primary_key_reads.load(Ordering::Relaxed),
            primary_key_scans: self.primary_key_scans.load(Ordering::Relaxed),
            indexes,
        }
    }
}

/// Store-level atomic counters plus per-table metrics.
pub(crate) struct StoreMetrics {
    commits: AtomicU64,
    rollbacks: AtomicU64,
    gc_runs: AtomicU64,
    snapshots_collected: AtomicU64,
    write_conflicts: AtomicU64,
    commit_ns_phase0: AtomicU64,
    commit_ns_phase1: AtomicU64,
    commit_ns_phase2: AtomicU64,
    commit_ns_phase3: AtomicU64,
    tables: RwLock<HashMap<String, TableMetrics>>,
}

// ---------------------------------------------------------------------------
// Optional metrics-crate emission helper
// ---------------------------------------------------------------------------

#[cfg(feature = "metrics")]
fn emit(name: &'static str, labels: &[(&'static str, String)], val: u64) {
    metrics::counter!(name, labels).increment(val);
}

// ---------------------------------------------------------------------------
// StoreMetrics implementation
// ---------------------------------------------------------------------------

impl StoreMetrics {
    pub(crate) fn new() -> Self {
        Self {
            commits: AtomicU64::new(0),
            rollbacks: AtomicU64::new(0),
            gc_runs: AtomicU64::new(0),
            snapshots_collected: AtomicU64::new(0),
            write_conflicts: AtomicU64::new(0),
            commit_ns_phase0: AtomicU64::new(0),
            commit_ns_phase1: AtomicU64::new(0),
            commit_ns_phase2: AtomicU64::new(0),
            commit_ns_phase3: AtomicU64::new(0),
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn add_phase0(&self, ns: u64) {
        self.commit_ns_phase0.fetch_add(ns, Ordering::Relaxed);
    }
    pub(crate) fn add_phase1(&self, ns: u64) {
        self.commit_ns_phase1.fetch_add(ns, Ordering::Relaxed);
    }
    pub(crate) fn add_phase2(&self, ns: u64) {
        self.commit_ns_phase2.fetch_add(ns, Ordering::Relaxed);
    }
    pub(crate) fn add_phase3(&self, ns: u64) {
        self.commit_ns_phase3.fetch_add(ns, Ordering::Relaxed);
    }

    // --- Registration -------------------------------------------------------

    pub(crate) fn register_table(&self, name: &str) {
        let mut tables = self.tables.write().unwrap();
        tables.entry(name.to_string()).or_insert_with(TableMetrics::new);
    }

    pub(crate) fn register_index(&self, table: &str, index: &str) {
        let tables = self.tables.read().unwrap();
        if let Some(table_metrics) = tables.get(table) {
            let mut indexes = table_metrics.indexes.write().unwrap();
            indexes
                .entry(index.to_string())
                .or_insert_with(IndexMetrics::new);
        }
    }

    // --- Store-level increments ---------------------------------------------

    pub(crate) fn inc_commit(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        emit("ultima.commits", &[], 1);
    }

    pub(crate) fn inc_rollback(&self) {
        self.rollbacks.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        emit("ultima.rollbacks", &[], 1);
    }

    pub(crate) fn inc_gc_run(&self) {
        self.gc_runs.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        emit("ultima.gc_runs", &[], 1);
    }

    pub(crate) fn inc_snapshots_collected(&self, n: u64) {
        self.snapshots_collected.fetch_add(n, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        emit("ultima.snapshots_collected", &[], n);
    }

    pub(crate) fn inc_write_conflict(&self) {
        self.write_conflicts.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        emit("ultima.write_conflicts", &[], 1);
    }

    // --- Table-level increments ---------------------------------------------

    pub(crate) fn inc_inserts(&self, table: &str, n: u64) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            t.inserts.fetch_add(n, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            emit(
                "ultima.table.inserts",
                &[("table", table.to_string())],
                n,
            );
        }
    }

    pub(crate) fn inc_updates(&self, table: &str, n: u64) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            t.updates.fetch_add(n, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            emit(
                "ultima.table.updates",
                &[("table", table.to_string())],
                n,
            );
        }
    }

    pub(crate) fn inc_deletes(&self, table: &str, n: u64) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            t.deletes.fetch_add(n, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            emit(
                "ultima.table.deletes",
                &[("table", table.to_string())],
                n,
            );
        }
    }

    pub(crate) fn inc_primary_key_reads(&self, table: &str, n: u64) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            t.primary_key_reads.fetch_add(n, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            emit(
                "ultima.table.primary_reads",
                &[("table", table.to_string())],
                n,
            );
        }
    }

    pub(crate) fn inc_primary_key_scans(&self, table: &str) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            t.primary_key_scans.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            emit(
                "ultima.table.primary_scans",
                &[("table", table.to_string())],
                1,
            );
        }
    }

    // --- Index-level increments ---------------------------------------------

    pub(crate) fn inc_index_reads(&self, table: &str, index: &str) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            let indexes = t.indexes.read().unwrap();
            if let Some(idx) = indexes.get(index) {
                idx.reads.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "metrics")]
                emit(
                    "ultima.index.reads",
                    &[
                        ("table", table.to_string()),
                        ("index", index.to_string()),
                    ],
                    1,
                );
            }
        }
    }

    pub(crate) fn inc_index_range_scans(&self, table: &str, index: &str) {
        let tables = self.tables.read().unwrap();
        if let Some(t) = tables.get(table) {
            let indexes = t.indexes.read().unwrap();
            if let Some(idx) = indexes.get(index) {
                idx.range_scans.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "metrics")]
                emit(
                    "ultima.index.range_scans",
                    &[
                        ("table", table.to_string()),
                        ("index", index.to_string()),
                    ],
                    1,
                );
            }
        }
    }

    // --- Snapshot -----------------------------------------------------------

    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        let tables = self
            .tables
            .read()
            .unwrap()
            .iter()
            .map(|(name, m)| (name.clone(), m.snapshot()))
            .collect();
        MetricsSnapshot {
            commits: self.commits.load(Ordering::Relaxed),
            rollbacks: self.rollbacks.load(Ordering::Relaxed),
            gc_runs: self.gc_runs.load(Ordering::Relaxed),
            snapshots_collected: self.snapshots_collected.load(Ordering::Relaxed),
            write_conflicts: self.write_conflicts.load(Ordering::Relaxed),
            commit_ns_phase0_acquire_locks: self.commit_ns_phase0.load(Ordering::Relaxed),
            commit_ns_phase1_read_validate: self.commit_ns_phase1.load(Ordering::Relaxed),
            commit_ns_phase2_merge: self.commit_ns_phase2.load(Ordering::Relaxed),
            commit_ns_phase3_install: self.commit_ns_phase3.load(Ordering::Relaxed),
            tables,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_metrics_snapshot_starts_at_zero() {
        let m = StoreMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.commits, 0);
        assert_eq!(s.rollbacks, 0);
        assert_eq!(s.gc_runs, 0);
        assert_eq!(s.snapshots_collected, 0);
        assert_eq!(s.write_conflicts, 0);
        assert!(s.tables.is_empty());
    }

    #[test]
    fn store_level_increments() {
        let m = StoreMetrics::new();
        m.inc_commit();
        m.inc_commit();
        m.inc_rollback();
        m.inc_gc_run();
        m.inc_snapshots_collected(5);
        m.inc_write_conflict();
        m.inc_write_conflict();

        let s = m.snapshot();
        assert_eq!(s.commits, 2);
        assert_eq!(s.rollbacks, 1);
        assert_eq!(s.gc_runs, 1);
        assert_eq!(s.snapshots_collected, 5);
        assert_eq!(s.write_conflicts, 2);
    }

    #[test]
    fn table_level_increments() {
        let m = StoreMetrics::new();
        m.register_table("users");

        m.inc_inserts("users", 3);
        m.inc_updates("users", 2);
        m.inc_deletes("users", 1);
        m.inc_primary_key_reads("users", 10);
        m.inc_primary_key_scans("users");

        let s = m.snapshot();
        let t = s.tables.get("users").expect("users table should exist");
        assert_eq!(t.inserts, 3);
        assert_eq!(t.updates, 2);
        assert_eq!(t.deletes, 1);
        assert_eq!(t.primary_key_reads, 10);
        assert_eq!(t.primary_key_scans, 1);
        assert!(t.indexes.is_empty());
    }

    #[test]
    fn index_level_increments() {
        let m = StoreMetrics::new();
        m.register_table("orders");
        m.register_index("orders", "by_customer");

        m.inc_index_reads("orders", "by_customer");
        m.inc_index_reads("orders", "by_customer");
        m.inc_index_range_scans("orders", "by_customer");

        let s = m.snapshot();
        let t = s.tables.get("orders").expect("orders table should exist");
        let idx = t
            .indexes
            .get("by_customer")
            .expect("by_customer index should exist");
        assert_eq!(idx.reads, 2);
        assert_eq!(idx.range_scans, 1);
    }

    #[test]
    fn unregistered_table_increments_are_ignored() {
        let m = StoreMetrics::new();

        // None of these should panic
        m.inc_inserts("ghost", 5);
        m.inc_updates("ghost", 1);
        m.inc_deletes("ghost", 1);
        m.inc_primary_key_reads("ghost", 3);
        m.inc_primary_key_scans("ghost");
        m.inc_index_reads("ghost", "some_index");
        m.inc_index_range_scans("ghost", "some_index");

        let s = m.snapshot();
        assert!(s.tables.is_empty());
    }
}
