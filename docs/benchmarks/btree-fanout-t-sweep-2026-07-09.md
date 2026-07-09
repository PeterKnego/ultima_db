# B-tree fanout (`T`) sweep — bench-host results (2026-07-09)

Authoritative AWS-NVMe results behind the `T = 32 → 64` default bump (task50 §fanout,
`docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md` §2). Preserved here because
`bench-infra/bench-out/` is gitignored — the summary tables live in the task docs, but the raw
per-run data (below) would otherwise be lost.

**Host (all runs):** AWS local-NVMe, 8 vcpu, 15701 MB, kernel `6.17.0-1019-aws`. Provisioned +
torn down per run via `bench-infra` (`make bench-oneshot`-style, guaranteed teardown).

## TL;DR

- **Uncontended bulk ops (1M random keys):** larger `T` wins monotonically up to ~64. **T=64**
  is the balance point — get −18–28%, insert ~−8%, remove ~−6–19% vs T=32; T=128 helps writes a
  hair more but regresses reads.
- **Contended SMR-apply + read-under-load:** "apply" here is the **SMR apply path**
  (`Persistence::smr` — replaying *already-committed* batches from the consensus log into the CoW
  tree, not an interactive `WriteTx::commit`). **T=64 costs −22% apply throughput** (bigger nodes →
  bigger `Arc::make_mut` memmove/CoW per applied batch) but gives **−62% read p99** (shallower tree
  → consistent low tail). Both effects are tight and reproducible (15 runs/arm, non-overlapping).
- **T=48 is dominated:** ~full apply hit (−19%), *bimodal* p99 (median no better than T=32), only
  ~half the bulk get win. No useful middle ground — the metrics are non-monotonic in `T`.
- **Decision: keep `T=64`.** −22% SMR-apply accepted for the broad read/bulk/p99 wins.

---

## 1. Bulk fanout — full sweep `T ∈ {8,16,32,64,128}`, 1M random keys

Post in-place rebalancing (task50 §5.1). Times normalized to T=32, **lower = faster**.
Run `20260709T065146Z`, git `906e5a4`, rustc 1.96.1.

| T | MAX_KEYS | get | insert | remove |
|--:|--:|--:|--:|--:|
| 8 | 15 | 1.73 | 1.27 | 1.24 |
| 16 | 31 | 1.26 | 1.07 | 1.00 |
| **32** | **63** | **1.00** | **1.00** | **1.00** |
| 64 | 127 | 0.82 | 0.92 | 0.81 |
| 128 | 255 | 0.96 | 0.88 | 0.80 |

Absolute @T=32: get 213.0 ms, insert 512.5 ms, remove 450.4 ms.

## 2. Bulk fanout — `T ∈ {32,48,64}` (the T=48 point)

Separate run (`20260709T092723Z`, git `05b19ea`) — its T=64 numbers differ from §1 by run-to-run
noise (get 0.72 vs 0.82), so **T=48 must be read against *this* run's T=32/64, not spliced into §1.**

| T | MAX_KEYS | get | insert | remove |
|--:|--:|--:|--:|--:|
| **32** | **63** | **1.00** | **1.00** | **1.00** |
| 48 | 95 | 0.83 | 0.95 | 0.99 |
| 64 | 127 | 0.72 | 0.94 | 0.94 |

Absolute @T=32: get 179.0 ms, insert 411.4 ms, remove 333.3 ms. T=48 keeps ~half the get win and
essentially none of the insert/remove win.

## 3. SMR-apply + read-p99-under-load — `T ∈ {32,48,64}`, 15 runs/arm (authoritative)

Run `20260709T130609Z`, git `07cf6ba`, rustc 1.97.0. `median [min-max]`, ratio ×T32.
`apply_sw_batch_throughput` higher = better; `read_p99_under_load_ns` lower = better.

> **Where the fanout cost actually lands: the row update, not the commit.** Each timed txn is
> `begin_write → open_table → update → commit` (`smr_bench.rs` block (c)). The `T`-dependent cost is
> entirely in `tbl.update` → `BTree::insert_mut` (`table.rs:317`), which `Arc::make_mut`-copies every
> node on the root→leaf path still shared with the live snapshot — bigger `T` = bigger node `Vec` =
> bigger memmove per copied node. `commit_single_writer` (`store.rs:2638`) is a *fixed* cost
> independent of `T`: it Arc-clones the table map, wraps the already-built dirty tree, and inserts a
> snapshot — it never touches a B-tree node. And because this is SMR mode (no `wal_handle`), commit
> does **no fsync/parking**. So "apply throughput" here is the CoW path-copy rate, **not** a commit
> rate — which is exactly why fanout moves it.

| T | MAX_KEYS | apply throughput | read p99 (ns) |
|--:|--:|--:|--:|
| **32** | 63 | 346014 [343330–347444] · 1.00× | 2287 [2194–2367] · 1.00× |
| 48 | 95 | 280970 [279803–281393] · 0.81× | 2325 [993–2445] · 1.02× |
| 64 | 127 | 270670 [267266–271300] · 0.78× | 859 [715–958] · 0.38× |

**apply throughput** arms are razor-tight and non-overlapping → the −19%/−22% regression is real.
**read p99**: T=32 and T=48 overlap (T=48 is *bimodal* — see raw runs below: four ~1000 ns runs,
eleven ~2300 ns); only **T=64 is a clean, separated −62%**. The earlier "T=48 worse than both"
reading was p99 noise — the ranges show it's not.

> Note: a *sandbox* SMR-ab run showed T=64 p99 *worse* (+41%) — a VM-contention artifact. The
> bench host (dedicated NVMe) is authoritative and shows the opposite: T=64 p99 much better.

---

## Raw logs (verbatim, as fetched from the bench host)

### 3a. `smr-ab.log` — 15 runs/arm SMR-apply (run `20260709T130609Z`, git `07cf6ba`, rustc 1.97.0)

```
scripts/smr_apply_ab.sh
>>> building + benching T=32 (MAX_KEYS=63), 15 runs...
>>> building + benching T=48 (MAX_KEYS=95), 15 runs...
>>> building + benching T=64 (MAX_KEYS=127), 15 runs...

# SMR-apply A/B — 15 runs/arm, median [min-max], normalized to T=32

| T | MAX_KEYS | apply_sw_batch_throughput (higher=better) | read_p99_under_load_ns (lower=better) |
|--:|--:|--:|--:|
| **32** | 63 | 346014 [343330-347444]  1.00x | 2287 [2194-2367]  1.00x |
| 48 | 95 | 280970 [279803-281393]  0.81x | 2325 [993-2445]  1.02x |
| 64 | 127 | 270670 [267266-271300]  0.78x | 859 [715-958]  0.38x |

raw apply_sw_batch_throughput per run (sorted):
  T=32: 343330 344581 345223 345229 345491 345680 345759 346014 346022 346032 346101 346254 346745 347195 347444
  T=48: 279803 280104 280208 280463 280784 280838 280891 280970 281056 281065 281136 281148 281266 281320 281393
  T=64: 267266 269600 269875 269977 270419 270453 270509 270670 270733 270757 270899 270981 271177 271290 271300
raw read_p99_under_load_ns per run (sorted):
  T=32: 2194 2229 2237 2257 2278 2284 2285 2287 2319 2324 2329 2331 2354 2363 2367
  T=48: 993 998 1069 1130 2028 2081 2139 2325 2333 2362 2380 2391 2411 2426 2445
  T=64: 715 780 814 818 828 834 835 859 863 866 870 896 914 953 958
```

### 1a. `fanout.log` — full 8–128 sweep (run `20260709T065146Z`, git `906e5a4`, rustc 1.96.1)

```
scripts/fanout_ab.sh
>>> building + benching T=8 (MAX_KEYS=15)...
>>> building + benching T=16 (MAX_KEYS=31)...
>>> building + benching T=32 (MAX_KEYS=63)...
>>> building + benching T=64 (MAX_KEYS=127)...
>>> building + benching T=128 (MAX_KEYS=255)...

# fanout A/B @ 1,000,000 random keys — normalized to T=32 (lower=faster)

| T | MAX_KEYS | get | insert | remove |
|--:|--:|--:|--:|--:|
| 8 | 15 | 1.73 | 1.27 | 1.24 |
| 16 | 31 | 1.26 | 1.07 | 1.00 |
| **32** | 63 | 1.00 | 1.00 | 1.00 |
| 64 | 127 | 0.82 | 0.92 | 0.81 |
| 128 | 255 | 0.96 | 0.88 | 0.80 |

absolute @T=32: get 213.0 ms, insert 512.5 ms, remove 450.4 ms
```

### 2a. `fanout.log` — 32/48/64 sweep (run `20260709T092723Z`, git `05b19ea`, rustc 1.96.1)

```
scripts/fanout_ab.sh
>>> building + benching T=32 (MAX_KEYS=63)...
>>> building + benching T=48 (MAX_KEYS=95)...
>>> building + benching T=64 (MAX_KEYS=127)...

# fanout A/B @ 1,000,000 random keys — normalized to T=32 (lower=faster)

| T | MAX_KEYS | get | insert | remove |
|--:|--:|--:|--:|--:|
| **32** | 63 | 1.00 | 1.00 | 1.00 |
| 48 | 95 | 0.83 | 0.95 | 0.99 |
| 64 | 127 | 0.72 | 0.94 | 0.94 |

absolute @T=32: get 179.0 ms, insert 411.4 ms, remove 333.3 ms
```
