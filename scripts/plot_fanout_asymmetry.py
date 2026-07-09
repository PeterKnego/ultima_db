#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Plot the B-tree fanout (T) read-vs-write asymmetry from a fanout-micro run and
# check the measured curves against the formulas:
#   read (traverse)      ~ 1 / ln T        (bigger T faster)
#   warm write (CoW)     ~ T / ln T        (bigger T slower)
#   cold write (in-place) ~ height+width   (U-shape; NOT T/lnT — the corollary)
#
# Input: a file containing the raw per-run JSON lines emitted by
# examples/fanout_microbench (each line starts with '{'). Point it at the pulled
# bench-host log (results/fanout-micro.log) or a raw .jsonl.
#
# Usage: scripts/plot_fanout_asymmetry.py <log_or_jsonl> <out_png> [norm_T=32]
import sys, json, math, statistics
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

src, out_png = sys.argv[1], sys.argv[2]
norm = int(sys.argv[3]) if len(sys.argv) > 3 else 32

METRICS = ["get_ns", "insert_warm_ns", "update_warm_ns", "remove_warm_ns",
           "insert_cold_ns", "update_cold_ns", "remove_cold_ns"]
runs = defaultdict(lambda: defaultdict(list))  # T -> metric -> [vals]
for line in open(src):
    line = line.strip()
    if not line.startswith("{"):
        continue
    d = json.loads(line)
    T = int(d["t"])
    for k in METRICS:
        if k in d:
            runs[T][k].append(d[k])

Ts = sorted(runs)
med = {T: {k: statistics.median(runs[T][k]) for k in METRICS} for T in Ts}
lo  = {T: {k: min(runs[T][k]) for k in METRICS} for T in Ts}
hi  = {T: {k: max(runs[T][k]) for k in METRICS} for T in Ts}
base = med[norm]

def ratio(T, k):   return med[T][k] / base[k]
def err(T, k):
    r = ratio(T, k)
    return (r - lo[T][k]/base[k], hi[T][k]/base[k] - r)

# ---- fit-check tables (printed) ----
def norm_theory_read(T):  return (1/math.log(T)) / (1/math.log(norm))
def norm_theory_warm(T):  return (T/math.log(T)) / (norm/math.log(norm))

print(f"# Fit check (normalized to T={norm}); measured = median of {len(runs[Ts[0]]['get_ns'])} runs\n")
print("## Reads vs 1/lnT")
print("| T | get measured | 1/lnT theory | Δ% |")
print("|--:|--:|--:|--:|")
for T in Ts:
    m, th = ratio(T,"get_ns"), norm_theory_read(T)
    print(f"| {T} | {m:.2f} | {th:.2f} | {100*(m-th)/th:+.0f}% |")

warm_avg = {T: statistics.mean([ratio(T,k) for k in ("insert_warm_ns","update_warm_ns","remove_warm_ns")]) for T in Ts}
print("\n## Warm (CoW) writes vs T/lnT  [avg of insert/update/remove]")
print("| T | warm measured | T/lnT theory | Δ% |")
print("|--:|--:|--:|--:|")
for T in Ts:
    m, th = warm_avg[T], norm_theory_warm(T)
    print(f"| {T} | {m:.2f} | {th:.2f} | {100*(m-th)/th:+.0f}% |")

cold_avg = {T: statistics.mean([ratio(T,k) for k in ("insert_cold_ns","update_cold_ns","remove_cold_ns")]) for T in Ts}
print("\n## Cold (in-place) writes — corollary: should NOT track T/lnT")
print("| T | cold measured | T/lnT theory | (cold ignores the tax) |")
print("|--:|--:|--:|:--|")
for T in Ts:
    print(f"| {T} | {cold_avg[T]:.2f} | {norm_theory_warm(T):.2f} | |")

# ---- plot ----
plt.figure(figsize=(9, 6.2))
x = Ts
def series(k, **kw):
    y = [ratio(T,k) for T in Ts]
    yerr = [[err(T,k)[0] for T in Ts],[err(T,k)[1] for T in Ts]]
    plt.errorbar(x, y, yerr=yerr, capsize=3, **kw)

# reads (the win) — bold blue
series("get_ns", marker="o", lw=2.4, color="#1f6fb2", label="get (read)")
# warm writes (the tax) — warm solid
series("insert_warm_ns", marker="s", lw=2.0, color="#c0392b", label="insert — warm (CoW)")
series("update_warm_ns", marker="^", lw=2.0, color="#e67e22", label="update — warm (CoW)")
series("remove_warm_ns", marker="v", lw=2.0, color="#8e44ad", label="remove — warm (CoW)")
# cold writes (context) — dashed muted
series("insert_cold_ns", marker="s", lw=1.3, ls="--", color="#c0392b", alpha=0.45, label="insert — cold (in-place)")
series("update_cold_ns", marker="^", lw=1.3, ls="--", color="#e67e22", alpha=0.45, label="update — cold (in-place)")
series("remove_cold_ns", marker="v", lw=1.3, ls="--", color="#8e44ad", alpha=0.45, label="remove — cold (in-place)")

# theory overlays (thin dotted, no markers)
tt = [8,16,32,64,128,256]
tt = [t for t in tt if min(Ts) <= t <= max(Ts)]
plt.plot(tt, [norm_theory_read(t) for t in tt], ":", color="#1f6fb2", lw=1.4, alpha=0.8, label="read theory  ∝ 1/lnT")
plt.plot(tt, [norm_theory_warm(t) for t in tt], ":", color="#c0392b", lw=1.4, alpha=0.8, label="warm-write theory  ∝ T/lnT")

plt.axhline(1.0, color="gray", lw=0.8, alpha=0.6)
plt.xscale("log", base=2)
plt.yscale("log", base=2)
plt.xticks(Ts, [f"{t}\n({2*t-1})" for t in Ts])
plt.gca().set_yticks([0.5,0.7,1.0,1.5,2.0,3.0,4.0,5.0])
plt.gca().set_yticklabels(["0.5","0.7","1.0","1.5","2.0","3.0","4.0","5.0"])
plt.xlabel("fanout  T   (MAX_KEYS = 2T−1)")
plt.ylabel(f"cost per op, normalized to T={norm}  (log scale; lower = faster)")
plt.title("B-tree fanout: reads and CoW writes diverge with T\n"
          "same tree — reads traverse (∝1/lnT, faster), warm writes clone (∝T/lnT, slower)")
plt.grid(True, which="both", alpha=0.25)
plt.legend(fontsize=8, ncol=2, loc="upper left")
plt.annotate("bigger T → reads win", xy=(max(Ts), ratio(max(Ts),"get_ns")),
             xytext=(0,-14), textcoords="offset points", fontsize=8, color="#1f6fb2")
plt.tight_layout()
plt.savefig(out_png, dpi=130)
plt.savefig(out_png.rsplit(".",1)[0] + ".svg")
print(f"\nwrote {out_png} (+ .svg)")
