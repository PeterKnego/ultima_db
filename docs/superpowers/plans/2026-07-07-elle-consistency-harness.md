# Elle Consistency Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Opt-in `make consistency/elle` tier that generates Elle list-append histories against UltimaDB MultiWriter transactions (SI and Serializable) and checks them with a vendored elle-cli, per `docs/superpowers/specs/2026-07-07-elle-consistency-harness-design.md`.

**Architecture:** A new `elle-history` binary in the existing `ultima-autobench` crate runs N writer threads doing get+update list appends against pre-seeded rows, recording an EDN history; `scripts/elle_check.sh` runs the vendored `tools/elle-cli/elle-cli-0.1.9-standalone.jar` (Java 21 available) on a known-bad fixture plus both histories and asserts the verdicts.

**Tech Stack:** Rust (std threads, clap already in autobench, no new deps — inline SplitMix64 PRNG, hand-formatted EDN), bash, elle-cli 0.1.9 fat jar.

## Global Constraints

- No new Cargo dependencies anywhere (autobench already has `clap` 4 derive + `serde` derive; use only those and std).
- `cargo clippy -- -D warnings` must stay clean (run as `cargo clippy -p ultima-autobench --all-targets -- -D warnings`).
- Not part of `cargo test` / `make test`: the pipeline runs only via `make consistency/elle`.
- elle-cli verdict stdout format is `<file>\t<true|false|unknown>`; `unknown` is always a hard failure.
- Elle event shape: `{:index N, :type :invoke|:ok|:fail|:info, :f :txn, :process P, :time NANOS, :value [[:append K V] [:r K nil-or-[V...]]]}` — one map per line.
- Append values must be globally unique across the whole run (single `AtomicU64`).

---

### Task 1: Vendor elle-cli + known-bad fixture

**Files:**
- Create: `tools/elle-cli/elle-cli-0.1.9-standalone.jar` (binary, downloaded)
- Create: `tools/elle-cli/LICENSE` (upstream license, from the release/repo)
- Create: `tools/elle-cli/README.md`
- Create: `tools/elle-cli/fixtures/known_bad.edn`

**Interfaces:**
- Produces: jar at exactly `tools/elle-cli/elle-cli-0.1.9-standalone.jar`; fixture at exactly `tools/elle-cli/fixtures/known_bad.edn` (used by Task 3's script).

- [ ] **Step 1: Download and extract the release**

```bash
cd "$(git rev-parse --show-toplevel)"
mkdir -p tools/elle-cli/fixtures
curl -sSL -o /tmp/claude-elle-cli.zip https://github.com/ligurio/elle-cli/releases/download/0.1.9/elle-cli-bin-0.1.9.zip
unzip -o /tmp/claude-elle-cli.zip -d /tmp/claude-elle-cli
find /tmp/claude-elle-cli -type f   # locate the standalone jar and any LICENSE
```

Copy the standalone jar to `tools/elle-cli/elle-cli-0.1.9-standalone.jar` (rename if the extracted name differs — the script depends on this exact path). Copy the upstream `LICENSE` if present in the zip; otherwise fetch `https://raw.githubusercontent.com/ligurio/elle-cli/master/LICENSE`.

- [ ] **Step 2: Record provenance**

`tools/elle-cli/README.md`:

```markdown
# elle-cli (vendored)

Standalone CLI for Jepsen's Elle transactional-safety checker.

- Upstream: https://github.com/ligurio/elle-cli
- Version: 0.1.9 (release asset `elle-cli-bin-0.1.9.zip`)
- sha256(elle-cli-0.1.9-standalone.jar): <fill with actual `sha256sum` output>
- Requires: Java 11+ (repo toolchain: Temurin 21)
- Used by: `scripts/elle_check.sh` via `make consistency/elle`
  (see `docs/tasks/task41_elle_consistency_harness.md`)

`fixtures/known_bad.edn` is a hand-written list-append history containing a
lost update; the check script requires elle-cli to reject it before trusting
any real verdict.
```

Fill in the actual `sha256sum tools/elle-cli/elle-cli-0.1.9-standalone.jar` value.

- [ ] **Step 3: Write the known-bad fixture**

`tools/elle-cli/fixtures/known_bad.edn` — process 0 appends 1 to key 1 (acknowledged), process 1 appends 2, a later read sees only `[2]`: value 1 was lost. Invalid under every model elle-cli supports:

```edn
{:index 0, :type :invoke, :f :txn, :process 0, :time 0, :value [[:append 1 1]]}
{:index 1, :type :ok, :f :txn, :process 0, :time 10, :value [[:append 1 1]]}
{:index 2, :type :invoke, :f :txn, :process 1, :time 20, :value [[:append 1 2]]}
{:index 3, :type :ok, :f :txn, :process 1, :time 30, :value [[:append 1 2]]}
{:index 4, :type :invoke, :f :txn, :process 0, :time 40, :value [[:r 1 nil]]}
{:index 5, :type :ok, :f :txn, :process 0, :time 50, :value [[:r 1 [2]]]}
```

- [ ] **Step 4: Verify elle-cli runs and rejects the fixture**

```bash
git check-ignore tools/elle-cli/elle-cli-0.1.9-standalone.jar && echo "IGNORED — fix .gitignore" || echo "trackable"
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append --consistency-models serializable tools/elle-cli/fixtures/known_bad.edn
```

Expected: output line ending in `false` (tab-separated after the filename). If the flag spelling differs in 0.1.9, adjust and record the working invocation in `tools/elle-cli/README.md` — Task 3's script must use the same spelling.

- [ ] **Step 5: Commit**

```bash
git add tools/elle-cli
git commit -m "tools: vendor elle-cli 0.1.9 + known-bad fixture (task41)"
```

---

### Task 2: `elle-history` workload driver

**Files:**
- Create: `autobench/src/bin/elle-history.rs`

**Interfaces:**
- Consumes: `ultima_db::{Store, StoreConfig, WriterMode, IsolationLevel, Error, CommitWaiter}`; `TableWriter::{insert_batch, get, update}`; `WriteTx::{open_table, commit}`; `Store::begin_write(None)`.
- Produces: binary `elle-history` (cargo auto-discovers `src/bin/*.rs`), CLI per the spec table, EDN history file at `--out`. Exit 0 on clean run; exit 1 if any `:info` event was recorded.

- [ ] **Step 1: Write the failing unit tests (in the bin file)**

Create `autobench/src/bin/elle-history.rs` with the test module and only stubs missing — i.e. start the file with the full types + `todo!()`-free helpers written in Step 3; for strict TDD, write the tests first and let the file fail to compile:

```rust
#[cfg(test)]
mod tests {
    use super::*;

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
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ultima-autobench --bin elle-history`
Expected: compile error (types not defined yet).

- [ ] **Step 3: Write the implementation**

Full contents of `autobench/src/bin/elle-history.rs` (above the test module):

```rust
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

#[derive(Clone)]
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
/// on commit.
fn run_txn(store: &Store, ops: &[Op]) -> Result<Vec<Op>, TxnFail> {
    let mut tx = store.begin_write(None).map_err(TxnFail::Other)?;
    let mut completed = Vec::with_capacity(ops.len());
    {
        let mut t = tx
            .open_table::<ElleRow>("elle")
            .map_err(TxnFail::Other)?;
        for op in ops {
            match op {
                Op::Read { key, .. } => {
                    let list = match t.get(*key) {
                        Some(row) => row.list.clone(),
                        None => Vec::new(),
                    };
                    completed.push(Op::Read { key: *key, result: Some(list) });
                }
                Op::Append { key, value } => {
                    // get() both fetches the list and registers the SSI point read.
                    let mut list = match t.get(*key) {
                        Some(row) => row.list.clone(),
                        None => Vec::new(),
                    };
                    list.push(*value);
                    match t.update(*key, ElleRow { list }) {
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
        shared.push(EventType::Invoke, process, ops.clone());
        match run_txn(&shared.store, &ops) {
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

    let store = Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        isolation_level: isolation,
        ..StoreConfig::default()
    })
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
        history: Mutex::new(Vec::with_capacity(
            args.threads * args.txns_per_thread * 2,
        )),
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
        "elle-history: isolation={} events={} ok={} write_conflict={} serialization_failure={} info={}",
        args.isolation,
        history.len(),
        s.ok.load(Ordering::Relaxed),
        s.write_conflict.load(Ordering::Relaxed),
        s.serialization_failure.load(Ordering::Relaxed),
        s.info.load(Ordering::Relaxed),
    );
    if s.info.load(Ordering::Relaxed) > 0 {
        eprintln!("elle-history: degraded run (unexpected errors above)");
        std::process::exit(1);
    }
}
```

- [ ] **Step 4: Run the unit tests**

Run: `cargo test -p ultima-autobench --bin elle-history`
Expected: all 6 tests PASS.

- [ ] **Step 5: Smoke-run both isolation levels (small)**

```bash
cargo build --release -p ultima-autobench --bin elle-history
target/release/elle-history --isolation si --threads 4 --txns-per-thread 200 \
  --out /tmp/claude-elle-smoke/si.edn
target/release/elle-history --isolation serializable --threads 4 --txns-per-thread 200 \
  --out /tmp/claude-elle-smoke/ser.edn
head -4 /tmp/claude-elle-smoke/si.edn
```

Expected: exit 0, stderr summary with `info=0`; SI run shows `write_conflict > 0`; serializable run shows `serialization_failure > 0` (with 4 threads on 16 keys both conflict types occur); EDN lines match the event shape in Global Constraints. Feed the smoke histories to the vendored jar as an early end-to-end sanity check:

```bash
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
  --consistency-models snapshot-isolation /tmp/claude-elle-smoke/si.edn
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
  --consistency-models serializable /tmp/claude-elle-smoke/ser.edn
```

Expected: both `true`. If elle-cli rejects the EDN with a parse error, fix `edn_event` (and its unit tests) before proceeding.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy -p ultima-autobench --all-targets -- -D warnings
git add autobench/src/bin/elle-history.rs
git commit -m "feat(autobench): elle-history list-append workload driver (task41)"
```

---

### Task 3: `elle_check.sh` + `make consistency/elle`

**Files:**
- Create: `scripts/elle_check.sh` (mode 755)
- Modify: `Makefile` (add `consistency/elle` to `.PHONY` line 1 and the target near `bench/compare-engines`, ~line 136)

**Interfaces:**
- Consumes: `elle-history` binary (Task 2), jar + fixture paths (Task 1).
- Produces: `scripts/elle_check.sh <si-history.edn> <ser-history.edn>` exiting 0 iff all required verdicts hold; `make consistency/elle` end-to-end target.

- [ ] **Step 1: Write the script**

`scripts/elle_check.sh`:

```bash
#!/usr/bin/env bash
# Check elle-history EDN histories with the vendored elle-cli (task41).
# Usage: scripts/elle_check.sh <si-history.edn> <serializable-history.edn>
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
FIXTURE="$ROOT/tools/elle-cli/fixtures/known_bad.edn"

usage="usage: elle_check.sh <si-history.edn> <serializable-history.edn>"
SI_HIST="${1:?$usage}"
SER_HIST="${2:?$usage}"

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found (set JAVA=/path/to/java)" >&2; exit 1; }
[ -f "$JAR" ] || { echo "error: missing $JAR (vendored jar; see tools/elle-cli/README.md)" >&2; exit 1; }
[ -f "$FIXTURE" ] || { echo "error: missing $FIXTURE" >&2; exit 1; }

# verdict <consistency-model> <history-file>: echoes true|false|unknown.
# elle-cli stdout is "<file>\t<verdict>".
verdict() {
    local out
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" "$2")" \
        || { echo "error: elle-cli failed on $2" >&2; exit 1; }
    printf '%s\n' "$out" | awk -F'\t' 'END { print $NF }'
}

# require <expected> <actual> <label>: hard assertion (unknown never passes).
require() {
    if [ "$2" != "$1" ]; then
        echo "FAIL: $3 (verdict: $2, expected $1)" >&2
        [ "$2" = "unknown" ] && echo "hint: bump elle-cli --cycle-search-timeout or shrink the history" >&2
        exit 1
    fi
    echo "OK: $3"
}

echo "== fixture smoke test (checker must reject a known-bad history) =="
require false "$(verdict serializable "$FIXTURE")" "known-bad fixture rejected"

echo "== SI history vs snapshot-isolation (UltimaDB SI claim) =="
require true "$(verdict snapshot-isolation "$SI_HIST")" "SI history satisfies snapshot-isolation"

echo "== SI history vs serializable (write skew expected under SI) =="
v="$(verdict serializable "$SI_HIST")"
case "$v" in
    false) echo "OK: SI history is not serializable (write skew present, as expected)" ;;
    true)  echo "WARN: SI history unexpectedly serializable — no write skew observed;" \
                "raise contention (lower --keys, raise --threads/--txns-per-thread)" ;;
    *)     echo "FAIL: verdict '$v' on $SI_HIST (bump --cycle-search-timeout?)" >&2; exit 1 ;;
esac

echo "== Serializable history vs serializable (UltimaDB SSI claim) =="
require true "$(verdict serializable "$SER_HIST")" "Serializable history satisfies serializable"

echo "elle consistency check passed"
```

```bash
chmod +x scripts/elle_check.sh
```

- [ ] **Step 2: Verify the script fails on a bad input and passes on the smoke histories**

```bash
scripts/elle_check.sh tools/elle-cli/fixtures/known_bad.edn /tmp/claude-elle-smoke/ser.edn; echo "exit=$?"
```

Expected: `FAIL: SI history satisfies snapshot-isolation ...` (the fixture is invalid even under SI) and `exit=1`.

```bash
scripts/elle_check.sh /tmp/claude-elle-smoke/si.edn /tmp/claude-elle-smoke/ser.edn
```

Expected: exit 0; the write-skew line prints either OK or WARN (small smoke run may legitimately WARN).

- [ ] **Step 3: Add the Makefile target**

Append `consistency/elle` to the `.PHONY` list (Makefile line 1) and add after the `bench/compare-engines` block (~line 137):

```make
# Transactional consistency check (Elle list-append via vendored elle-cli,
# needs java) — opt-in tier, not part of `make test`. See task41.
consistency/elle:
	cargo build --release -p ultima-autobench --bin elle-history
	target/release/elle-history --isolation si $(ELLE_ARGS) --out target/elle/si/history.edn
	target/release/elle-history --isolation serializable $(ELLE_ARGS) --out target/elle/ser/history.edn
	scripts/elle_check.sh target/elle/si/history.edn target/elle/ser/history.edn
```

- [ ] **Step 4: Run the full pipeline with default parameters**

Run: `make consistency/elle`
Expected: both generation runs finish (~a minute total), all elle-cli checks pass, the SI-vs-serializable step prints `OK: ... write skew present` (with default 8×1500×4 contention this should fire; a WARN here means defaults need tuning — lower `--keys` default to 8 in Task 2 and rerun until write skew is observed reliably), final line `elle consistency check passed`.

- [ ] **Step 5: Commit**

```bash
git add scripts/elle_check.sh Makefile
git commit -m "feat: make consistency/elle — Elle check pipeline (task41)"
```

---

### Task 4: Documentation

**Files:**
- Create: `docs/tasks/task41_elle_consistency_harness.md`
- Modify: `CLAUDE.md` (autobench bullet, ~line 41)

**Interfaces:**
- Consumes: everything above (documents it).

- [ ] **Step 1: Write the task doc**

`docs/tasks/task41_elle_consistency_harness.md` covering (write real prose, not an outline):

- What is checked: MultiWriter+SI history must satisfy Elle's `snapshot-isolation` model; MultiWriter+SSI history must satisfy `serializable`; SI history checked against `serializable` is expected-invalid (write skew) as a contention sanity signal (WARN if valid).
- How the mapping works: pre-seeded keyspace (no public insert-at-key; `update` requires existing key), append = `get`+clone+push+`update` (the `get` registers the SSI point read), globally unique append values from one `AtomicU64`, `:fail` only for `WriteConflict`/`SerializationFailure` (both guarantee no commit), failed attempts are not retried — the next attempt is a fresh transaction with fresh values, `:info` retires the process.
- How to run: `make consistency/elle`, tuning via `ELLE_ARGS`, reading failures via elle-cli `--directory` anomaly dumps.
- Why the known-bad fixture exists (checker-actually-fires guard).
- Known limitations, copied from the spec's "Known limitations (v1)" section: in-memory only; point reads only (coarse index/range SSI tracking unexercised); Elle checks histories not final state (mw_commit_torture covers that); write-skew occurrence is probabilistic hence WARN not FAIL.

- [ ] **Step 2: Update CLAUDE.md**

In the `autobench/` bullet, append one sentence:

```
Also hosts the `elle-history` bin: `make consistency/elle` generates Elle list-append histories under MultiWriter SI/SSI and checks them with the vendored elle-cli (`tools/elle-cli/`, needs java) — see `docs/tasks/task41_elle_consistency_harness.md`.
```

- [ ] **Step 3: Commit**

```bash
git add docs/tasks/task41_elle_consistency_harness.md CLAUDE.md
git commit -m "docs: task41 Elle consistency harness"
```

---

## Verification (whole feature)

- `cargo test -p ultima-autobench` — green (includes the new bin's unit tests).
- `cargo clippy -p ultima-autobench --all-targets -- -D warnings` — clean.
- `make consistency/elle` — passes end-to-end with write-skew OK (not WARN).
- Root `cargo test` untouched (no root-crate changes).
