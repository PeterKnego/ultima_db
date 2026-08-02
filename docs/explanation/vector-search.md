# Vector search: HNSW and the recall/speed trade-off

`ultima_vector` answers "which stored embeddings are most similar to this
query?" — and it answers *approximately*, on purpose. This page explains why
approximate nearest-neighbor search is the right trade, what the tuning knobs
actually buy and cost, why the distance metrics are shaped the way they are,
and where the honest edges of the approximation lie. For how to set a
collection up, see [Add vector search](../how-to/add-vector-search.md); this
page is about why it works the way it does.

## Why exact search loses

Exact k-nearest-neighbor is trivially correct and brutally linear: compare
the query against every stored vector, keep the best k. At a few thousand
vectors this is fine — fast, even. At a million 768-dimensional embeddings,
every single query pays a full scan of ~3 GB of floats, and no classical
index rescues you: the tree structures that make low-dimensional spatial
search sublinear (k-d trees and kin) degrade toward that same linear scan as
dimensionality climbs into the hundreds. In high dimensions, everything is
roughly equidistant from everything, and the pruning logic that makes trees
work has nothing to grip. This is the curse of dimensionality, and it is why
every serious vector store gives up exactness.

The trade on offer is steep in your favor: accept that the top-k you get back
is *almost always* the true top-k — recall of 0.95+ is routine — and query
cost drops from linear to roughly logarithmic in collection size. For search,
recommendations, and retrieval-augmented generation, a 95th-best match
occasionally swapping places with the 96th is invisible; a 300 ms scan per
query is not.

## The layered-graph idea

HNSW (Hierarchical Navigable Small World) is the approximation `ultima_vector`
uses, following Malkov & Yashunin's 2018 formulation. The intuition fits in a
paragraph: build a graph where each vector is a node connected to a handful of
near neighbors, and answer queries by greedy walking — start somewhere, move
to whichever neighbor is closest to the query, repeat until no neighbor
improves. On a single flat graph, greedy walks get stuck near where they
start. So HNSW stacks layers, like a skip list: a sparse top layer with a few
long-range nodes, denser layers beneath, every node present in the bottom
layer. A search enters at the top, takes big cheap hops to land in the right
region, then drops down a layer and refines, finishing with a careful beam
search at the base. Each node's layer is drawn from a geometric distribution
at insert time, which is what keeps the upper layers sparse without any
global coordination — a property that matters here, because inserts are just
row writes.

The result is a structure that touches a few hundred nodes per query instead
of all of them, at the price of being probabilistic: the walk can, rarely,
descend into the wrong neighborhood and miss a true neighbor. Every knob the
index exposes is a way of spending resources to make that rarer.

## What the knobs actually trade

There are really three ideas, whatever their names ([the how-to
guide](../how-to/add-vector-search.md) covers the concrete usage):

**Graph degree (`m`)** is how many neighbors each node keeps. More edges mean
more routes to the right answer — better recall, more robustness on lumpy
data distributions — but every edge is memory forever and more candidates to
evaluate per hop. This one is baked in at build time; you live with it.

**Build-time beam width (`ef_construction`)** is how carefully each insert
looks for its true neighbors before wiring itself in. A wider beam produces a
higher-quality graph — better recall at *every* future query setting — and
costs only build-time CPU. It's the cheapest quality you can buy if you can
afford slower ingestion, because it's paid once rather than per query.

**Search-time beam width (`ef_search`)** is the per-query recall/latency dial,
and the only one you can turn after the fact. The beam search at the base
layer keeps the best `ef` candidates seen so far; wider beams explore more,
miss less, and take proportionally longer. It's per-query, so one collection
can serve fast approximate lookups and slow careful ones from the same graph.

The shape worth internalizing: memory is set by degree, graph quality by the
build beam, and the query-time position on the recall/latency curve by the
search beam. Recall problems are usually cheapest to fix in that reverse
order — widen the search beam first, rebuild with a wider construction beam
second, raise the degree last.

## Distance metrics are semantics, not preference

The metric decides what "similar" *means*, and picking the wrong one is a
correctness bug that no recall tuning can fix. Cosine compares direction and
ignores magnitude — the right choice when embeddings encode meaning in their
angle, as most text-embedding models do. L2 (squared Euclidean here; squaring
preserves order and skips the square root) compares positions, so magnitude
matters — right for embeddings that live in a genuine metric space. Dot
product rewards both alignment *and* magnitude, which some recommendation
models train for deliberately, letting popular items carry larger norms.
`ultima_vector` negates dot product internally so that smaller-is-closer
holds uniformly across all metrics — one ordering convention, no special
cases in the search code.

`CosineNormalized` exists because cosine does redundant work when every
vector is already unit-length: the norms it computes per comparison are all
1.0, and cosine distance collapses to `1 − dot`. If you normalize embeddings
once at ingestion, that per-comparison norm work is pure waste on the hottest
loop in the system. The interesting design choice is that the fast path is a
*separate type*, not a flag on `Cosine`: the precondition — both stored
vectors and queries must be pre-normalized (the crate ships helpers for this)
— is encoded in the type system, so "I forgot to set the flag" and "I set the
flag but didn't normalize" become structurally different, and the first is
impossible. Feed it un-normalized vectors and you get silently wrong
rankings; that contract is the entire price of the shortcut, which is why it
is opt-in.

## Why SIMD dispatch happens at runtime

Distance is the inner loop of everything — each node visited during a walk
pays one distance call — so the kernels are SIMD-vectorized. The less obvious
choice is that the instruction set (AVX-512, AVX2, NEON, or scalar) is
selected at *runtime*, on the first call, from what the executing CPU
actually supports, rather than at compile time via cargo features.

Compile-time selection optimizes for the machine that built the binary, which
is frequently not the machine that runs it — CI builders, container images,
and published crates all cross that boundary constantly. Baking in AVX-512
produces a binary that crashes on older hardware; baking in the lowest common
denominator wastes the newer hardware silently. Runtime dispatch (via `pulp`,
which generates all the per-ISA variants from one kernel definition and
caches the CPU detection after the first call) means one binary is correct
and near-optimal everywhere, with no feature-flag matrix for users to get
wrong. The accepted cost is some ground given up to hand-tuned per-ISA
intrinsics — measured before committing at a few percent for
production-sized embeddings, worse only for tiny vectors off the hot path —
and judged a bargain against maintaining three hand-rolled kernels per
metric, each with its own `unsafe`.

## Why bad inputs are rejected at the door

The collection validates every incoming vector — insert, update, query, and
restore — rejecting dimension mismatches and any non-finite value (NaN or
±Inf). This looks like bureaucracy until you consider what NaN does to a
similarity index: every comparison against NaN is false, so a NaN distance
doesn't crash anything — it silently scrambles the candidate ordering the
graph walk depends on. One NaN embedding admitted at insert time poisons the
graph's *structure* permanently: the node gets wired to arbitrary neighbors,
and searches near it quietly return worse results forever, with no error
anywhere. Infinities are rejected on the same grounds — they manufacture NaN
downstream (`Inf − Inf`, `Inf × 0`) and are never legitimate embedding output.

Rejecting at the boundary rather than checking inside the kernels is the
performance half of the same decision: if non-finite values can never enter,
the distance loops that run millions of times per query need no defensive
checks at all. One O(dim) scan per insert buys a check-free hot path. In the
same spirit, the distance kernels assert on mismatched slice lengths in *all*
build profiles, not just debug — a panic is strictly better than computing a
plausible-looking distance over the shorter prefix and returning it as truth.

## Riding on MVCC

`ultima_vector` stores the HNSW graph *inside* ordinary UltimaDB tables: each
row carries its embedding, the caller's metadata, and that node's own
adjacency lists, with a singleton row holding the graph's entry point. This
was a deliberate rejection of the standard approach (an in-memory graph
structure beside the database), because a big mutable `Vec`-of-adjacency
blob defeats copy-on-write — every transaction commit would have to clone
the entire graph. Denormalized into rows, the graph inherits UltimaDB's
persistent-B-tree behavior: snapshots are O(1), and a mutation clones only
the touched rows.

The user-visible consequence is that search results are *snapshot-consistent*.
A search runs against one immutable version of the graph; a writer inserting
vectors concurrently builds new versions without disturbing it. You will
never observe a half-inserted node or a half-rewired neighborhood, and you
can deliberately search a pinned historical snapshot by supplying your own
read transaction. Persistence, WAL recovery, and multi-writer conflict
detection all come along for free, because to the storage engine a vector
collection is just two tables.

## Honest limits

- **Approximate means approximate.** Recall targets like 0.95 are empirical,
  not guaranteed, and depend on the data distribution — clustered or
  low-intrinsic-dimension data can behave worse than the uniform-random
  vectors that test suites (including ours) love.
- **Sharp filters break HNSW, so we stop using it.** Graph search under a
  highly selective metadata filter degrades badly — most neighbors are
  filtered out and the walk starves. Below a threshold of 128 candidate ids
  (`BRUTE_FORCE_THRESHOLD` in the source), the collection abandons the graph
  and scans the filtered set linearly, which is *exact*. So paradoxically,
  your most selective queries are your most accurate ones.
- **Deletes and updates degrade gently, not freely.** Deleted nodes are
  tombstoned, and updates leave stale back-references at upper layers as
  harmless dead ends; there is currently no background compaction to reclaim
  either. A collection with heavy churn slowly accumulates dead weight until
  rebuilt.
- **Scale target is in-process:** thousands to low millions of vectors, full
  precision `f32`, no quantization or IVF/PQ-style compression yet. If your
  corpus is hundreds of millions of vectors, this is the wrong tool — by
  design, since the point is vector search *inside* a process that already
  has UltimaDB, not a standalone vector service.
