# Reading our benchmarks

Every database publishes a table in which it wins. UltimaDB publishes such
tables too — see [the performance reference](../reference/performance.md) for
the current ones — so this page exists to answer the uncomfortable question
directly: *why should you believe, or doubt, a database's benchmark table?*
It explains how our numbers are produced, which claims they can support, and
which claims they cannot, no matter how confident the bold text looks.

This page is about methodology and epistemics. It contains no numbers of its
own. The current results live in the
[performance reference](../reference/performance.md); the full historical run
records live in the dated archive under [`docs/benchmarks/`](../benchmarks/).

## Absolute numbers don't travel

The single most important thing to understand about any benchmark table —
ours or anyone's — is that the absolute numbers are a property of one machine
on one day. "42,000 transactions per second" bakes in the CPU generation, the
storage device and its firmware mood, the kernel version, the filesystem, the
allocator, and whatever else was running. Move any of those and the number
moves, sometimes by an integer factor. This is why our benchmark documents
repeat, almost as a liturgy, that only **same-host orderings and ratios** are
meaningful: "UltimaDB was faster than ReDB on workload E, by about 5×, on this
host, in this run" is an honest claim. "UltimaDB does 336,000 ops/sec" is not
a claim at all — it's a coordinate without a map.

This has a corollary that surprises people: two runs of *our own* suite on two
different hosts are also not comparable in absolute terms. When we re-ran the
competitor comparison in July 2026 on a fresh cloud instance, the write-up
(see [`competitor-nvme-2026-07-13.md`](../benchmarks/competitor-nvme-2026-07-13.md))
deliberately compared only rankings and ratios against the June run — and
noted that even single-row ratio moves of up to ~15% between runs are within
the combined noise of two independent arms. The rankings held on every row;
that, not any individual millisecond figure, was the finding.

## Noise floors, and why small deltas are fiction

Every measurement arm has a run-to-run variance floor. On our dedicated
NVMe bench host it is roughly ±2.5–9% per arm depending on the workload. On a
shared or virtualized development machine it is dramatically worse — the
sandbox environments we develop in show up to ~2× run-to-run swings on the
same code. A "7% improvement" measured once on a laptop is not a result; it is
a coin flip with a narrative attached.

The practical rule we hold ourselves to: a single-run delta below the noise
floor is meaningless, and a delta near the floor is at best directional. When
we do claim an improvement, we want it to clear the floor decisively — the WAL
preallocation work (see
[`wal-preallocation-ab-2026-06-20.md`](../benchmarks/wal-preallocation-ab-2026-06-20.md))
is the template. The effect there was ~2× and came with built-in controls: the
read-only workload stayed flat (reads never fsync, so if it had moved, the A/B
would have been measuring something else), and the eventual-durability tier
stayed flat (the change targets the fsync path, so a tier that doesn't block
on fsync should not care). A benchmark you should trust tends to carry its own
falsification apparatus: control arms that are *supposed* to show nothing, and
do. Even so, that same document explicitly downgraded its sandbox result to
"strong directional confirmation" and withheld the portable magnitude until
the run was repeated on real hardware.

## Why the remote rig exists

We run performance work in two tiers, and the split is about repeatability,
not convenience. Local runs — `cargo bench` on whatever machine is at hand —
are fine for smoke-testing a harness, catching a gross regression, or getting
a directional read on a change. They are never the basis for a published
number, a competitor ratio, or a recorded baseline, because the environment is
too noisy to distinguish a modest real effect from scheduling luck.

Anything that will be *claimed* runs on a provisioned cloud host with local
NVMe storage: one dedicated machine, OS-tuned, all engines built and run in
the same session on the same disk, torn down afterwards. Note what this buys
and what it doesn't. It buys a low, characterized noise floor and — crucially
— a level playing field for head-to-head comparisons, since every engine sees
the identical host in the identical hour. It does *not* make the absolute
numbers portable to your hardware. Nothing does. The rig narrows the error
bars around the ratios; the ratios are still the product.

## What the harness actually measures

Our headline comparison uses YCSB workloads, and the harness makes a specific
— and deliberately unflattering — choice: **every operation is its own
transaction**, and in the durable tier **every commit fsyncs**. That models an
application doing small, independent, individually-durable writes: the
worst-case shape for any engine, and a very different workload from batched
ingestion where hundreds of writes amortize one commit.

This choice has teeth. Engine improvements that help batched writes — cheaper
in-place mutation inside a transaction's privately-owned tree, bulk-load fast
paths — barely register on YCSB, because a transaction that touches one row
never gets to amortize anything. The July 2026 re-bench said this out loud:
the intervening engine work "was not expected to move YCSB, and didn't," and
its value was confirming *no regression* under the per-op regime. When you
read our table, read it as "per-operation transactional latency under this
commit discipline," not as the ceiling of what any of these engines can do
with batching. If your workload batches, the table understates everyone, and
not by the same factor.

The comparison runs two durability tiers, and the fairness reasoning behind
the second one is worth spelling out. In the strict tier every engine is
configured to make each commit durable before returning — a genuinely
like-for-like contract. In the eventual tier the competitors run their default
no-fsync write path, so UltimaDB runs `Durability::Eventual`: the WAL is still
written to real disk, but the fsync happens asynchronously and the commit
doesn't block on it. We could have run UltimaDB fully in-memory in that tier
and posted better numbers; that would have compared a database that does no
I/O against databases that do. Matching the *contract* — both sides write,
neither side waits for the sync — is the fair framing, and it is also the tier
where UltimaDB loses some rows to an LSM engine on write-heavy mixes. We keep
those rows in the table. A benchmark that never loses a row anywhere is
telling you about the benchmark's construction, not the software.

## Why the records are dated and immutable

Benchmark results in this repository are written once, dated, and never
edited into an evergreen "current performance" page. Each record carries its
provenance: the git SHA of the code measured, the host class and kernel, the
date, the exact configuration of every arm. When the code changes enough to
matter, we run again and write a *new* dated document that cites the old one.

The alternative — one living page of numbers, quietly updated — is how
benchmark rot happens. An evergreen number silently detaches from the code
that produced it, the machine it ran on, and the methodology in force at the
time; before long it is being quoted in support of a binary three versions
newer on hardware two generations different. Immutable records make the claim
checkable ("this ratio, this SHA, this host") and make regressions honest: if
a later run is worse, there is a prior document to be worse *than*, and the
comparison is explicit rather than a lost edit. The
[performance reference](../reference/performance.md) points at whichever
dated record is most recent; the record itself never moves.

## What we are not telling you

Honest limits, so you can weigh the claims:

- **Scope is YCSB.** Six synthetic key-value workloads over uniform-ish small
  records. Wider workloads (transactional contention à la SmallBank,
  multi-writer conflict behavior) have been measured occasionally but are not
  part of the routinely re-run suite; those older numbers age accordingly.
- **Single node, single process.** Nothing here speaks to replication,
  network overhead, or multi-node behavior.
- **Not measured at all:** memory footprint under load (material for an
  in-memory engine), recovery time, behavior at data sizes near or beyond
  RAM, large values, long-horizon effects such as fragmentation or — for the
  LSM competitors — compaction debt that a minutes-long benchmark never pays.
  Some of these cut in our favor, some against; the point is the table is
  silent on them.
- **Competitor tuning is default-ish.** Each competitor runs a reasonable
  configuration under the harness's commit discipline, not the config its
  vendor's performance team would produce for this exact workload. This is
  the standard limitation of every head-to-head ever published, including
  this one.

The compact summary: believe the orderings, weigh the ratios with their
error bars, ignore the absolute numbers unless you reproduce them on your own
hardware, and check that the workload's shape resembles yours before letting
any of it move a decision. That standard is the one we try to meet in
[`docs/benchmarks/`](../benchmarks/) — and the one we'd suggest applying to
everyone else's table too.
