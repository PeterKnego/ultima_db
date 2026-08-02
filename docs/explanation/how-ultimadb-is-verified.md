# How UltimaDB is verified

Why should you trust an embedded database this young with your data? That is a
fair question, and this page is the answer to it. It is not a list of tests to
run or an API to browse — it is an account of the *correctness case*: the
distinct verification layers the engine carries, why each one exists, what
class of bug it catches that the others cannot, and what each one honestly
does not cover. The scope here is the single-node engine — the B-tree, the
MVCC/OCC transaction machinery, the key encoding, and the crash-recovery path.

The short version of the argument: no single technique gives a total
guarantee, so UltimaDB does not rest on one. It rests on several partial
guarantees that overlap, chosen so that each one's blind spot sits inside
another one's coverage — and, unusually, one layer whose whole job is to prove
that the other layers can actually fail.

## The conventional floor

The base layer is the one every serious database has, and it is worth exactly
one paragraph. A large integration-test suite exercises the store,
persistence, bulk-load, and snapshot-stream paths, including deliberately
hostile inputs — truncated headers, malformed UTF-8 in table names, corrupted
stream payloads — because a restore path that trusts its input is a restore
path that installs garbage. A dedicated allocation-count guard test pins the
point-read hot path at zero allocations, because in a noisy environment
timings lie but allocation counts are deterministic; a regression that adds
one allocation per read multiplies by N and cannot hide. Criterion benchmarks
run against committed baselines so performance regressions gate merges rather
than surfacing in user reports (see [the performance
reference](../reference/performance.md)). All of this catches the bugs someone
thought to write a test for. The remaining layers exist because the worst
database bugs are precisely the ones nobody thought of.

## Property checking: Elle against real histories

Isolation guarantees are the hardest claims a database makes, and the hardest
to test. A unit test checks an interleaving its author imagined; a subtle bug
in the optimistic-concurrency merge path would produce anomalies only under
interleavings nobody scripted, at 3 a.m., in production. So UltimaDB's two
isolation claims — Snapshot Isolation by default, Serializable as opt-in (see
[the isolation-levels reference](../reference/isolation-levels.md)) — are
checked by [Elle](https://github.com/jepsen-io/elle), the transactional-safety
checker behind the published Jepsen analyses of PostgreSQL, MySQL, and
CockroachDB.

The approach treats the store as a black box. Many concurrent writer threads
run Elle's list-append workload through real transactions, and Elle
mathematically searches the recorded history for dependency cycles that
falsify the claimed isolation model. Crucially, the check asserts the *exact*
anomaly set, not a bare pass/fail: the Snapshot Isolation history must show
write skew and *nothing worse* — write skew is the one anomaly SI permits, so
its presence is evidence the harness has real contention, while a lost update
or dirty read fails the check — and the Serializable history must show no
anomalies at all. The workload deliberately covers three read shapes — point
reads, full scans, and secondary-index reads — because each exercises a
different read-set-tracking path, and a checker that only ever probes the easy
path proves nothing about the others. These checks gate every pull request.

Two things make this layer more than a demo. First, it has already paid for
itself: the scan workload surfaced a real quiescence deadlock in the
write-intent table — threads parked forever on keys whose waiters belonged to
already-dropped transactions — a rare, load-dependent hang that no scripted
test would have found, reproduced deterministically and fixed. Second, and
more unusual: the harness is itself verified by mutation testing. Three known
bugs can be injected into the commit path — disabling read-set validation,
disabling write-conflict detection, and silently dropping a key in the commit
merge below the isolation layer — and the suite confirms Elle *catches each
one*. A checker that never fails is worthless; this is the layer that
validates the validator. The injected faults are compiled out of every normal
build and provably inert even when compiled in.

What property checking honestly does not give you: it samples histories rather
than exhausting them, so it is probabilistic evidence, not proof — a
sufficiently rare interleaving could still slip past. That gap is one reason
the next layer exists.

## Machine-checked proofs: the B-tree kernel and the key encoding

Every table and every secondary index in UltimaDB sits on the same persistent
copy-on-write B-tree. An off-by-one in the median-split logic, a child
mis-slotted after a rebalance — these corrupt key order in ways that manifest
only on specific key distributions after specific split cascades, which is
exactly the shape of bug that randomized testing samples for and formal proof
excludes outright.

So the B-tree's insert and delete algorithms are proved correct in Lean 4,
against a mechanical Rust-to-Lean translation produced by the
[Aeneas](https://github.com/AeneasVerif/aeneas) toolchain — meaning the
theorems are about a translation of real algorithm code, not a hand-written
model that can quietly drift from it. The proved statements amount to complete
functional correctness of the mutating API: insert behaves exactly as a map
update and remove exactly as a map deletion — the ordering invariant, height
uniformity, and the minimum-occupancy balance invariant are all preserved
through splits, rotations, and merges; the touched key reads back correctly;
every other key is untouched; and remove provably never fails on a
well-formed tree. Because the translation makes every effect explicit, the
theorems also rule out panics, overflow, and out-of-bounds indexing on those
paths. The proof of deletion even forced a *stronger* invariant than the code
comments claimed, because the naive theorem turned out to be false on
degenerate trees no insert sequence ever builds — a gap testing could never
have surfaced, since testing only ever sees trees that inserts built.

The same treatment covers the order-preserving primary-key encoding: the
property that encoded bytes compare in exactly the same order as the keys
themselves, which WAL replay, sorted bulk-load, and every composite-key index
scan silently depend on (see [the key-encoding
reference](../reference/key-encoding-and-formats.md)). The theorems establish
round-trips, injectivity, strict order-preservation for the fixed-width and
sign-flipped encodings, and — the subtle one — that the escape-and-terminate
framing for variable-length elements inside composite keys has *no confusion
cases*: a framed prefix that has ended never compares wrongly against a
continuation, for any suffixes. That is precisely the property a length-prefix
encoding would violate, and the reason the framing exists at all.

Now the boundary, stated as precisely as the theorems themselves. The proofs
cover *extracted kernel models*: the insert/delete core of `src/btree.rs` and
the encoding functions of `src/primary_key.rs`, each ported to the safe-Rust
subset the translator accepts, with every delta from the shipped code
documented and each kernel anchored to reality by a differential test against
an independent oracle. They do not cover range iterators, the store, OCC, the
WAL, or anything concurrent. Only the shipped fanout is instantiated; some
integer widths and tuple arities follow the same argument shape but are not
separately machine-checked. The full inventory of what is and is not proved
lives in [formal/README.md](../../formal/README.md), and the narrative of how
the proofs were built in [formal/WRITEUP.md](../../formal/WRITEUP.md).

Two mechanisms keep this layer from rotting into a museum piece. A drift guard
runs in CI on every pull request: a change to the verified files without a
matching change on the proof side fails the build, so the kernels cannot
silently fall behind the shipped source — divergence must be either mirrored
or explicitly acknowledged in the open. And the proofs themselves are held to
a mechanical integrity check: no `sorry` placeholders, and every top-level
theorem must depend on only Lean's three standard axioms — a check CI re-runs,
because several tempting proof shortcuts mint hidden axioms that would quietly
turn "proved" into "assumed."

## The crash-recovery contract: two failure modes, two answers

Durability (see [setting up durable
persistence](../how-to/set-up-durable-persistence.md)) raises a question the
other layers don't: what should recovery do with a damaged write-ahead log?
UltimaDB's answer is a deliberately split contract, pinned by a suite of
corruption-injection tests that damage the WAL and checkpoint files the way
real failures actually damage them.

A *torn tail* — a truncated, zero-filled, or garbage-filled end of the log —
is the normal signature of a crash: the machine died mid-write. Recovery
handles it silently, restoring the durable prefix, because a database that
refuses to start after every power cut is not durable in any useful sense; the
lost tail was never acknowledged as committed. But *mid-file corruption* — a
bit flip inside an entry whose successors are intact — is not something a
crash produces. It is disk rot or an outside writer, and it means data the
store *did* acknowledge is damaged. Recovery fails loudly rather than
guessing, because silently dropping a committed transaction from the middle of
history is the one thing a durable store must never do, and no heuristic can
repair data whose only copy is wrong.

The split matters because either uniform policy would be wrong. Always
recovering silently converts bit rot into invisible data loss; always failing
loudly converts every crash into an outage requiring manual intervention. The
contract distinguishes the failure you must tolerate from the failure you must
report, and the test suite pins the distinction so it survives refactoring.

## The standpoint: overlapping partial guarantees

Notice what each layer cannot do, and which other layer covers it. Unit and
integration tests miss unimagined interleavings; Elle searches real histories
for them. Elle samples rather than exhausts; the Lean proofs are exhaustive
over the kernel's entire input space. The proofs are bounded to sequential
kernels; the concurrent machinery above them is exactly what Elle exercises.
A property checker could itself be broken and green forever; mutation testing
demonstrates it fails when it should. And the differential tests under the
proof kernels anchor the one thing a proof cannot establish about itself —
that the model corresponds to the shipped code — while the drift guard keeps
that correspondence from decaying.

This is the honest shape of trust in a young system: not "it is verified,"
full stop, but a lattice of specific, checkable claims, each with a stated
boundary, arranged so the boundaries don't coincide. A mature database earns
trust through years of production exposure; a young one has to earn it by
making its correctness argument unusually explicit and unusually falsifiable.
Every claim above is backed by an artifact in this repository that CI re-runs.

## What this does not cover

The limits, plainly. The Serializable implementation tracks scan and
secondary-index reads at *table* granularity: any index read registers a
whole-table read-set entry, so SSI prevents phantoms by being coarse, at the
cost of extra aborts under contention — there is no fine-grained predicate
tracking today, and the Elle predicate pass verifies the coarse behavior
rather than something finer. The formal proofs stop at the kernel boundary:
nothing concurrent — the OCC merge protocol, commit-version promotion
ordering, WAL recovery — is proved, only property-checked and tested; those
protocol models are the natural next frontier, and the harder one. The
Elle harness verifies isolation, not crash durability — the
corruption-recovery tests inject damage into files at rest, which is not the
same as a kill-during-commit torture harness. And everything on this page is
single-node: replication and consensus live in a separate cluster layer with
its own, separate verification story. Those boundaries are stated here for
the same reason the axiom check exists — a correctness case you can trust is
one that tells you where it ends.
