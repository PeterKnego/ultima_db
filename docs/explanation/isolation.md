# Isolation in UltimaDB

Why is `SnapshotIsolation` the default, and what does `Serializable` actually buy you? This page answers that question from first principles: what the classical isolation anomalies are, how UltimaDB's MVCC design makes most of them structurally impossible, why write skew is the one that slips through, and what the opt-in Serializable mode trades to catch it. It is about *understanding* the two levels — for the exact anomaly matrices and measured numbers see the [isolation levels reference](../reference/isolation-levels.md), and for the correct coding pattern see [preventing write skew](../how-to/prevent-write-skew.md).

## The four SQL levels, and what they are really about

SQL's four isolation levels — Read Uncommitted, Read Committed, Repeatable Read, Serializable — are best read not as a menu of features but as a ladder of *anomalies ruled out*. Each level is defined negatively: it is the set of interleavings it forbids.

At the bottom, Read Uncommitted permits a **dirty read**: transaction B reads a value A has written but not yet committed. If A rolls back, B has observed data that never existed — not stale data, but *fictional* data. Read Committed forbids that, but still permits a **nonrepeatable read**: B reads the same row twice and gets two different answers, because A committed a change in between. B's view of the world is real at every instant but inconsistent *across* instants. Repeatable Read pins individual rows, but classically still permits the **phantom read**: B re-runs a range query and rows have appeared or vanished, because pinning rows you have seen says nothing about rows you haven't seen yet.

And then there is the top rung, which is qualitatively different. Serializable does not just forbid a list of named read anomalies — it makes the positive promise that the outcome of any concurrent execution equals the outcome of *some* serial one. The gap between Repeatable Read and Serializable is where the interesting anomaly of this page lives.

The historical footnote that matters for UltimaDB: databases built on multiversion concurrency (PostgreSQL being the famous example) discovered that snapshot-based Repeatable Read is *stronger* than the SQL standard requires — it prevents phantoms too — while still falling short of Serializable. That intermediate point has a name, **Snapshot Isolation**, and it is exactly where UltimaDB's default sits.

## How snapshot isolation falls out of the architecture

Most databases *implement* snapshot isolation with effort — undo logs, read timestamps, visibility checks per row. In UltimaDB it is closer to a corollary of the data structure.

Every commit publishes a new immutable `Snapshot` — a complete, versioned view of all tables, built from copy-on-write B-trees that share unchanged subtrees with their predecessors (see [architecture](architecture.md) for how that sharing works). A read transaction does one thing at creation time: it grabs an `Arc` to one of those snapshots. From then on, every read it performs walks that one frozen tree. There is no visibility logic, no "is this row committed yet?" check, because the snapshot *cannot contain* uncommitted data — a writer's changes live in its own private working copies until commit, and commit is an atomic publication of a new snapshot, never a mutation of an old one.

Run the three read anomalies against this design and each dies structurally rather than by enforcement. A dirty read is impossible because uncommitted writes exist only inside the writer's private copy, which no reader can reach. A nonrepeatable read is impossible because later commits create *new* snapshots; the reader's pinned snapshot is never touched. A phantom is impossible for the same reason — a range scan iterates an immutable tree, and no concurrent insert can appear in it, because concurrent inserts happen to a different tree.

This is why snapshot isolation is the default and costs nothing: it isn't a feature layered on top with tracking and validation, it's what you get for free once versions are immutable values. Making the default *weaker* (Read Committed) would require extra work for a worse guarantee. Making it *stronger* — Serializable — genuinely does require extra work, and that is the trade this page is about.

## Write skew: the anomaly SI lets through

Snapshot isolation has one famous blind spot, and it is worth internalizing the canonical story, because the pattern shows up in real applications constantly under different costumes.

A hospital requires at least one doctor on call at all times. Two are currently on call, Alice and Bob, and both want the evening off. Each runs the same transaction: *count the doctors on call; if at least two, take myself off.* Run serially, this is safe — whoever goes second counts one and stays. Run concurrently under snapshot isolation:

```
Alice: count on-call doctors        // her snapshot says 2
Bob:   count on-call doctors        // his snapshot also says 2
Alice: set alice = off_call
Bob:   set bob   = off_call
Alice: commit → succeeds
Bob:   commit → succeeds            // disjoint keys — no write conflict!
// result: zero doctors on call — a state no serial order could produce
```

Both transactions read overlapping data (the whole table) and wrote *disjoint* rows (each only their own). UltimaDB's optimistic concurrency check compares **write** sets, and the write sets don't intersect — so as far as conflict detection can see, these transactions are perfectly compatible. Each one's premise ("there are two on call") was true *in its snapshot* and false in the world its commit helped create. That is **write skew**: not a corrupted read, not a lost update, but two individually correct decisions whose combination violates an invariant neither transaction alone touched.

The insidious part is that every read involved was perfectly consistent. Write skew is invisible if you audit reads and writes separately; it only appears when you ask whether the *decision* each transaction made was still justified at commit time. Which points directly at the fix.

## What Serializable adds: remember what you read

If the problem is "my premise went stale," the remedy is to remember the premise. Under `IsolationLevel::Serializable`, each write transaction records what it *read* — its read set — alongside the write set it was already keeping. At commit, in addition to the usual write-write conflict check, the store asks: since this transaction's snapshot was taken, did any committed writer modify something this transaction read? If yes, the premise may be stale, and the commit aborts with a serialization failure. The application retries against a fresh snapshot — and on the retry, Bob counts one doctor and stays on call. This is Serializable Snapshot Isolation (SSI): SI's zero-cost read guarantees, plus commit-time validation of read premises.

Two design choices inside that sentence deserve unpacking, because both are deliberate trade-offs rather than limitations discovered later.

### Coarse scans: choosing which way to be wrong

Point reads — "get row 7" — are tracked precisely, key by key. But scans, ranges, counts, and index lookups are recorded as a single coarse fact: *this transaction observed the table*. Validation then treats *any* concurrent commit touching that table as a conflict with the scan, even a commit to a row the scan's predicate would never have matched.

This coarseness produces **false positives** — aborts that a perfectly precise tracker would have allowed — and never **false negatives**. That direction was chosen on purpose, and the reasoning generalizes: the two failure modes are wildly asymmetric in cost. A false positive costs a retry — bounded, visible, and handled by machinery the application already needs, because even flawless SSI aborts transactions and demands a retry loop. A false negative is a silent invariant violation: zero doctors on call, discovered in production, with no error ever raised. When one error direction is a nuisance and the other is corruption, you buy precision only where it's cheap (point reads) and round *up* everywhere else. Precise range tracking — recording scan bounds rather than a table-level flag — is a known future refinement; it would reduce retries on scan-heavy contended workloads, not change any correctness property.

### Why only writers track reads

Read sets live on `WriteTx` only. A read-only transaction tracks nothing, validates nothing, and can never be aborted — under either isolation level. This follows from what write skew *is*: a bad decision made durable. A transaction that writes nothing makes nothing durable; its premise can go as stale as it likes and the database's invariants are untouched. Tracking reads on read-only transactions would cost bookkeeping on the cheapest, most common operation in the system and could never prevent anything.

But the choice has a consequence that every SSI user must understand, because it shapes how correct code is written: **the deciding read must happen on the write transaction that acts on it.** If you count the doctors through a separate read transaction, then open a write transaction to take yourself off call, the write transaction's read set is empty — it never read anything — and SSI will wave the commit through. The system validates what *this writer* observed, not what your application observed somewhere nearby. Read-your-premise-on-the-writer is the whole discipline; the concrete pattern is in [preventing write skew](../how-to/prevent-write-skew.md).

There is a second, quieter consequence of the same design: under `SingleWriter` mode, Serializable silently costs and changes nothing. Write skew needs two concurrent writers, so with one writer slot the validation could never fire — and UltimaDB skips both the tracking and the check entirely rather than paying for a guarantee already provided by mutual exclusion.

## So which should you use?

The default is the answer until you can name the invariant that says otherwise. Snapshot isolation is free — no tracking, no validation, no new failure mode — and its guarantees cover every anomaly except one. Reach for `Serializable` when your transactions *decide based on what they read and write somewhere else*: threshold checks ("at least N on call"), uniqueness enforced by look-then-insert, budget or quota checks across rows, any read-verify-then-write-elsewhere shape. Those are the write-skew shapes; SSI turns their silent corruption into a retryable error. The measured overhead on a write-heavy contended workload is on the order of a few percent — see the [isolation levels reference](../reference/isolation-levels.md) for the numbers and their caveats.

The honest summary: SI gives you consistent views for free; Serializable makes your *decisions* safe, at the price of retries — some of them, under coarse scan tracking, retries you didn't strictly owe. UltimaDB defaults to the free guarantee and lets you buy the stronger one exactly where your invariants demand it.
