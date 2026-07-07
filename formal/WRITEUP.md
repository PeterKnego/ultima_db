# Machine-Checked Proofs for UltimaDB's B-Tree: Leanstral, Aeneas, and a Parallel Proof Factory

*Working notes / raw material for an article. Sessions of 2026-07-06 and 2026-07-07.*

---

## 0. TL;DR

In roughly two working days we went from "can AI tooling formally verify anything about UltimaDB?" to a **machine-checked, sorry-free proof that the core B-tree insertion algorithm preserves the full B-tree ordering invariant** — including the subtle 64-entry median-split path — checked by the Lean 4 kernel against code mechanically translated from the actual Rust by the Aeneas toolchain.

Along the way we built a compile-feedback harness for **Leanstral** (Mistral's specialized Lean 4 proof model, released days earlier) and got a clean negative result: 0-for-2 as an autonomous prover through a naive harness, despite finding the right proof architecture within two rounds. The proofs that landed were done by a general-purpose frontier model (Claude) orchestrating **five parallel subagents** over a shared lemma stack, with the Lean compiler as the arbiter of every step.

Final integrity check: `#print axioms` on the top-level theorem reports only `propext`, `Classical.choice`, `Quot.sound` — Lean's three standard axioms. No `sorry` anywhere in the chain.

---

## 1. Context: why formally verify UltimaDB's B-tree?

**UltimaDB** is an MVCC embedded store built on a persistent copy-on-write B-tree (Rust). Mutations create new tree roots that share unchanged subtrees via `Arc`; old versions stay alive for free, which is what snapshot isolation physically rests on. The B-tree (`src/btree.rs`, T=32, MAX_KEYS=63) uses bottom-up splitting with recursive `InsertResult::Split` propagation.

This code is correctness-critical in a way tests handle poorly:

- An ordering bug in the split/median-promotion logic (an off-by-one in the median index, a child mis-slotted after a split) could corrupt the key order in ways that only manifest on specific key distributions after specific split cascades.
- The same structure underlies every table and secondary index in the store.
- The codebase already has strong conventional assurance (differential tests, torture tests, autobench perf gates) — the interesting question was whether *proof-level* assurance had become practical for a small team.

Two tool arrivals made the question timely:

1. **Leanstral** (Mistral AI, June 2026; v1.5 released 2026-07-06 — the day this work started): the first open-source code agent specialized for Lean 4. 119B-parameter MoE with only ~6B active per token, Apache 2.0 weights, free API endpoint. Trained with RL inside a propose → compile → read-errors → refine loop against the Lean compiler, plus an agentic environment (filesystem, language server). Headline numbers: saturates miniF2F, 587/672 on PutnamBench, and — most relevant — claims strength on *proof engineering in realistic repositories*, not just competition math.
2. **Aeneas** (AeneasVerif project, actively developed): a verification toolchain that compiles a safe-Rust subset from MIR (via the Charon front-end) into *pure functional Lean definitions*. You prove theorems about a mechanical translation of your actual code instead of a hand-written model — closing the "model fidelity" gap that is the classic weakness of model-based verification.

The plan that emerged: use Aeneas to get the real algorithm into Lean, and evaluate Leanstral as the proof grinder, with a general-purpose model (Claude, running the session) as the orchestrator.

---

## 2. Part I — Leanstral: setup and honest evaluation

### 2.1 Access mechanics (worth documenting; all of it was discovered the hard way)

- The free endpoint is a **Labs model** (`labs-leanstral-1-5`). Calls fail with a 403 (`labs_not_enabled`) until an org admin enables Labs models in Mistral's console — a privacy toggle, because Labs endpoints may train on submitted data.
- **`reasoning_effort: "high"` is essential.** Without it the model answers in 3–17 seconds with 700–3,700 tokens and makes careless mistakes (wrong constructor arities, misremembered lemma names). With it, responses run 10–43k tokens over 1–4 minutes and the proof plans become sound.
- Responses arrive as **typed content chunks** (`thinking` + `text` parts), not a flat string — a naive `message.content` parser breaks.
- `max_tokens` up to 60,000 is accepted; at 32k a hard proof can burn the entire budget on thinking and emit no code (`finish_reason: length`).

### 2.2 The harness

We built a ~200-line Python driver (`leanstral-demo/drive.py`, later adapted as `proofs/drive_proofs.py`) implementing the loop Leanstral was trained for:

1. Send the Lean file with `sorry`s + system instructions.
2. Extract the returned ```lean block.
3. **Anti-cheat guards**: reject any `sorry`/`admit`/`axiom` (checked on comment-stripped code with word boundaries — an early version rejected every attempt because the file's own header comment contained the word "sorry"); require the original theorem statements verbatim (whitespace-insensitive), so the model can't weaken what it proves.
4. Compile with Lean; on failure, feed the *actual compiler diagnostics* back.
5. **Sliding-window context**: keep only the latest attempt + its errors, not the pile of failed attempts — accumulated failure history visibly degraded the model (by round 8 of one run it had started rewriting the theorem statements themselves).
6. On truncation, resample the round rather than poisoning the history.

### 2.3 Experiment 1: sorted-insert toy theorems — FAIL (12 rounds)

Target: a self-contained, Mathlib-free Lean file defining an inductive `Sorted` predicate and `insertSorted`, with four theorems to prove (sortedness preserved, membership, no key loss, length+1). Difficulty: an exercise from a first Lean course.

Result: 12 rounds, no verified proof. The failure mode was consistent: **plausible proof structure, fatal last-mile errors** — binder-name mistakes (`Unknown identifier h_tail`), constructor arity errors, `simp` invocations that loop or make no progress, Mathlib lemma names used against core-only Lean (`add_assoc` vs `Nat.add_assoc`). Each round produced *fresh* mistakes rather than converging on the previous round's fixes.

### 2.4 Experiment 2: `find_pos_ok` over Aeneas output — FAIL (10 rounds), but instructive

Target: totality of the translated binary search (`∃ r, find_pos entries k = ok r`) — a real loop-invariant proof over Aeneas's monadic encoding. We provided the relevant library lemma (`loop.spec_decr_nat`) and the invariant hint in the prompt.

Result: no verified proof in 10 rounds, **but the proof architecture was right by round 2**: correct invariant (`lo ≤ hi ∧ hi ≤ entries.len`), correct termination measure (`(if found then 0 else 1) + (hi − lo)` — including the non-obvious found-flag trick for the loop's one extra exit iteration), correct top-level lemma. It lost on: exact namespaces (`spec_imp_exists` lives in `Aeneas.Std.WP` — our hint omitted the namespace, partly our fault), the curried-vs-tupled shape of the generated loop body, and eventually regression into `simp`-loop flailing.

For calibration: the human-plus-frontier-model side proved the same theorem in 5 compile iterations — but crucially, *by first reading the Aeneas library source* to learn its idioms (`step*`, `scalar_tac`, the `⦃ ⦄` spec notation), which the API-only Leanstral could not do.

### 2.5 Verdict on Leanstral (as of 1.5, free endpoint, July 2026)

- **0/2 as an autonomous end-to-end prover** through a compiler-error-only harness.
- **Genuinely useful as a skeleton generator**: its round-2 invariant/measure for `find_pos_ok` was correct and was, in fact, the same shape used in the successful human-driven proof.
- The bottleneck is not mathematical insight but **grounding**: no goal-state introspection (it saw error text, not proof states), no library browsing, no `exact?`/`loogle`-style search. Its trained environment (Mistral Vibe + `lean-lsp-mcp`) provides exactly these; our harness didn't. The fair conclusion is *"the harness matters more than the model"* — which is itself an interesting finding, and suggests a rematch with goal-state feedback via `lean-lsp-mcp` would look different.
- Economic note: every experiment ran on the free endpoint. Total cost of all Leanstral usage: $0.

---

## 3. Part II — Aeneas: getting the real Rust into Lean

### 3.1 Toolchain setup (much easier than expected)

The published path is an OCaml source build, but **both projects ship nightly prebuilt binaries** (GitHub releases): `aeneas-linux-x86_64`, `charon-linux-x86_64`, and — the big time-saver — a prebuilt Lean library build (`lean-build-aeneas-*.tar.gz`, 360MB of `.lake/build` artifacts), so neither OCaml nor the Aeneas Lean library needed compiling. Mathlib came from its Azure binary cache (8,297 files). Lean itself: `elan`-managed v4.30.0-rc2 (pinned by the Aeneas library).

**Gotcha #1 — version pinning:** Aeneas requires the *exact* Charon it pins (`charon-pin` file in the Aeneas repo). Same-day nightlies mismatched (LLBC format v0.1.219 vs expected v0.1.218). Resolution: read `charon-pin` at the Aeneas release commit, date the pinned Charon commit (2026-07-01), take the next day's Charon nightly (2026.07.02). Charon additionally needs its pinned Rust nightly (`nightly-2026-06-01` with `rustc-dev` + `llvm-tools-preview`) via rustup.

Pipeline once assembled: `charon cargo --preset=aeneas` (produces `btree_kernel.llbc`) → `aeneas -backend lean btree_kernel.llbc` (produces `BtreeKernel.lean` in ~0.7s).

### 3.2 The verification kernel: porting `btree.rs` into the Aeneas subset

Aeneas handles a *safe-Rust subset*. The insert/get core of `src/btree.rs` was extracted into a dependency-free crate (`kernel/`, 404 lines including tests) with deliberately minimal, documented deltas:

| Delta | Reason | Fidelity argument |
|---|---|---|
| `Arc<BTreeNode>` → `Box<Node>` | Aeneas has no shared-ownership support | The Arcs are never mutated through (persistent structure); sharing vs deep copy is observationally identical for a pure structure. The CoW/aliasing safety this "loses" is instead guaranteed wholesale: the translated code is pure, so old snapshots *cannot* be disturbed. |
| `K, V` monomorphized to `u64` | Generic trait-clause support is partial | Matches actual use: `Table` instantiates `BTree<u64, R>`. |
| `children: Vec<Arc<Node>>` → custom `Children` cons-list enum | **Recursive types through `Vec` don't translate**: Aeneas models `Vec α` as a length-bounded subtype of `List α`; a recursive occurrence sits inside the subtype's predicate (negative position) and the Lean kernel rejects it as not strictly positive. Aeneas's own flagship betree example uses a custom list for the same reason. | Entries stay `Vec<(u64,u64)>` (non-recursive). The cons-list is a faithful sequence model. |
| Derived `Clone` → hand-written `clone_node`/`clone_children` functions | Charon rejects mutually recursive *trait impls* ("mixed declaration groups"); plain mutually recursive functions are fine | Same semantics; later *proved* to be the identity (`clone_node_spec : clone_node n ⦃ m => m = n ⦄`). |
| `binary_search_by` → hand-rolled `find_pos`; `Vec::insert`/slice `to_vec` → loop helpers | No closures; no early `return` from loops | Same algorithms; the helpers get their own full functional specs. |
| `is_empty()` → `len() == 0` | `Vec::is_empty` isn't in Aeneas's std model — it would have been emitted as an **axiom**, silently weakening every downstream theorem | `len` is modeled. Final translation: **zero axioms**. |

**Keeping the port honest:** the kernel carries a differential test — 2,000 randomized inserts (keyspace 512, forcing multi-level splits) compared element-by-element against `std::collections::BTreeMap`, plus length tracking. This test was re-run after every port change. It's the cheap empirical anchor underneath the expensive formal one.

### 3.3 What the translation looks like

581 lines of Lean. Every function is total-with-explicit-effects in Aeneas's `Result` monad (`ok` / `fail` / `div`): arithmetic carries overflow checks, indexing carries bounds checks, panics become `fail`. Rust `while` loops become explicit tail-recursive `loop`-combinator applications over a `ControlFlow` state (`find_pos_loop.body` + `find_pos_loop`); recursive functions (`get_in_node`, `insert_into_node`, the `Children` helpers) become `partial_fixpoint` definitions. This means **totality is a theorem, not an assumption**: proving `f x ⦃ post ⦄` simultaneously proves f doesn't panic, overflow, or diverge on that input.

---

## 4. Part III — The proofs

### 4.1 Warm-ups (day 1)

- `new_get_none` — lookup on a fresh tree returns `ok none`. 4 lines, 3 iterations. Proved the pipeline end-to-end.
- `find_pos_ok` — binary search never fails: `loop.spec_decr_nat` with invariant `lo ≤ hi ∧ hi ≤ len` and measure `(if found then 0 else 1) + (hi − lo)`. The discovery here was the Aeneas automation: **`step*`** (auto-discharges monadic side conditions using `@[step]`-tagged spec lemmas — every overflow and bounds obligation vanished) and **`scalar_tac`** (omega-style closure over scalar coercions). The proof is ~20 lines.

### 4.2 The main theorem: sortedness invariant preservation (day 2)

**The invariant** (`BtreeInvariant.lean`, 83 lines) is the full B-tree ordering discipline, defined as a *mutual* inductive predicate over the translated mutual types `Node`/`Children`:

- `NodeInv lo hi n`: entries strictly sorted (`List.Pairwise` on key `.val`s); every key strictly inside the open interval `(lo, hi)` (`Option Nat`, `none` = unbounded); **arity ≤ 63** (`MAX_KEYS` — also needed so `insert` can't overflow a `Vec` push); children (if any) aligned.
- `Aligned lo hi entries children`: children positionally interleave entries — child *i* is a valid subtree strictly between entry *i−1* and entry *i*, with the outer bounds at the ends. The inductive shape forces `#children = #entries + 1` for free.

A key design decision: `Aligned` is stated over `List Node` (via a `clist : Children → List Node` view) rather than over the `Children` cons-list directly. All the helper specs naturally produce `clist out = <take/drop/set surgery on lists>` equalities, so alignment reassembly becomes pure list manipulation. This one choice probably saved a day.

**The architecture: a parallel proof factory.** The proof decomposes into a helper-lemma stack whose elements are independent given the definitions. Five subagents were dispatched concurrently, each with: the exact theorem statements to prove (so merging is trivial), the worked `find_pos_ok` example as a style template, a cheat-sheet of Aeneas idioms, the compile command, and instructions to iterate until exit 0. Each agent worked in its own file against prebuilt oleans.

| Agent | Deliverable | Content |
|---|---|---|
| A | `AgentFindPos.lean` (125 lines, 1 theorem) | **Full binary-search correctness**: under sortedness, `find_pos` returns either the exact hit position or the insertion point with two-sided bracketing (`∀ i < pos: key_i < k` and `∀ i ≥ pos: k < key_i`). The hardest single lemma; ~110k tokens, 26 tool calls. |
| B | `AgentEntryHelpers.lean` (282 lines, 7 theorems) | Loop specs for the entry-vector helpers: `insert_entry_at = take ++ x :: drop`, `replace_entry = set`, `prefix/suffix = take/drop`, each via `loop.spec_decr_nat` with accumulator invariants. |
| C | `AgentListLemmas.lean` (139 lines, 12 theorems) | Pure list layer: sortedness of take/drop/set/insert-at-point, median bracketing (`mem_take_lt_median` etc.), bounds propagation and tightening (`InB.of_take/of_drop/tighten_*`). |
| D | `AgentChildrenHelpers.lean` (212 lines, 13 theorems) | Cons-list specs: `children_len/child_at/replace_child/replace_and_insert_child/prefix/suffix` in terms of `clist`, plus the mutual `clone_node_spec`/`clone_children_spec` identity proofs and the size lemma for induction. Invented its own eliminator (see 4.3). |
| E | `AgentAligned.lean` (173 lines, 11 theorems) | Alignment surgery: `Aligned.length_eq`, `Aligned.child` (slot extraction with `lbnd/rbnd` interval selectors), `Aligned.set_child`, `Aligned.insert_split` (the split-insertion surgery), `Aligned.split` (median split of an aligned family). |

**All five came back complete and sorry-free.** Total agent output: ~930 lines of verified Lean, ~44 theorems, produced concurrently in under 15 minutes of wall time each.

**The integration layer** (`BtreeInsertInv.lean`, 560 lines, done in the main session): glue lemmas (`Aligned.congr_keys` — alignment only reads keys, so replacing an entry with an equal-keyed one preserves it; `bracket_of_InB_bnd` — adjacent slot bounds imply full bracketing by sortedness transitivity; `InB_outer_of_bnd`), the `MAX_KEYS`/`CloneVec` monadic facts, then:

- **`maybe_split_spec`**: the split lemma. Post-condition `InsPost`: a `Fit` yields `NodeInv lo hi`; a `Split l m r` yields `NodeInv lo (m.key) l ∧ NodeInv (m.key) hi r ∧ m.key ∈ (lo, hi)`. Covers all four cases (fit/split × leaf/internal); the interesting case rebuilds alignment of 33+32 children around the promoted median via `Aligned.split`.
- **`insert_into_node_inv`**: the main induction. Fuel-based (`fuel ≥ Node.size`), since the recursion descends through `child_at` rather than structurally. Four cases: hit-in-leaf (replace, `SortedE_set`), miss-in-leaf (insert at the `find_pos` point, then `maybe_split_spec`), hit-in-internal (replace + `Aligned.congr_keys`), miss-in-internal (recurse into slot `pos` with bounds `(lbnd, rbnd)` from `Aligned.child`, then either `Aligned.set_child` on a `Fit` or `insert_entry_at` + `replace_and_insert_child` + `Aligned.insert_split` + `maybe_split_spec` on a `Split`).
- **`BTree.insert_inv`**: the API-level corollary, `BTreeInv t → t.insert k v ⦃ t' => BTreeInv t' ⦄` (given `t.len < usize::MAX` for the length counter), including the root-split case that builds a fresh root `[median]` with two children. Compiled on the first attempt — a decent signal that the abstraction layers were right.

### 4.3 Lean-engineering findings (the part practitioners will want)

These cost the most wall-clock time and are the reusable knowledge:

1. **Mutual inductives break `induction` and sometimes `cases`.** Lean 4 refuses `induction h` for predicates/types in `mutual` blocks. Workarounds used: induct on a list/Nat and `cases` the derivation inside (agents C, E, main session); or prove a custom eliminator by well-founded recursion on size (`children_ind`, agent D) and use `induction c using children_ind`.
2. **Private do-notation matchers resist `simp`/`dsimp`/`split`.** After rewriting `find_pos ... = ok (hit, pos)` into the goal, the continuation is a *matcher* private to the imported module applied to the pair; no simp-set unfolds it (fresh matchers in your own file *do* reduce — which makes the failure confusing). Reliable fix: `show` (or `change`) with the explicitly written reduced do-block — definitional equality bridges it. Used four times in the main induction plus twice for `InsertResult` match arms.
3. **Dependent `getElem` proofs break `rw` motives.** Rewriting `out.val = entries.val.set pos x` under `(out.val[i]'h).1` fails ("motive is not type correct"). Fix: `revert` the bounds proof, rewrite, re-`intro`.
4. **`@[step]`-tag your own spec lemmas.** Aeneas's `step*` composes any conclusion of shape `f args ⦃ post ⦄`, emitting hypothesis-named posts (`i_post : ↑i = ↑hi - ↑lo`) and leaving preconditions as side goals for `scalar_tac`. Tagging the five agents' specs made `maybe_split_spec`'s eleven-bind monadic chain almost fully automatic.
5. **Irreducible generated globals** (`@[global_simps, irreducible] def MAX_KEYS : Result Usize`) need `unseal MAX_KEYS T in` before their spec proof.
6. **`subst` direction surprises**: `subst (h : ent = entries)` can eliminate `entries` (renaming half the context) rather than `ent`; `rw` is the predictable tool.
7. **Watch for library sorries.** The Aeneas library itself ships two `sorry`-carrying lemmas (`Slice.lean`) plus one in `StringIter.lean`. Nothing we used depends on them — verified by `#print axioms`, which is the *only* trustworthy final check: every theorem reports exactly `[propext, Classical.choice, Quot.sound]`.

### 4.4 Statistics

| Artifact | Lines | Theorems |
|---|---|---|
| Rust kernel (`kernel/src/lib.rs`, incl. differential test) | 404 | — |
| Generated Lean (`BtreeKernel.lean`) | 581 | — |
| Invariant definitions (`BtreeInvariant.lean`) | 83 | — |
| Warm-ups (`BtreeProofs.lean`) | 51 | 2 |
| Agent lemma stack (5 files) | 931 | 44 |
| Integration + main theorems (`BtreeInsertInv.lean`) | 560 | 9 |
| **Total hand-written proof code** | **~1,625** | **~55** |

Leanstral harnesses: 416 lines of Python across two drivers.

Wall-clock: pipeline + warm-ups in one session (~half a day of elapsed time, mostly downloads and compile cycles); the sortedness theorem in a second session (~4 hours elapsed), of which the five agents ran concurrently for ~10–15 minutes and the rest was integration and the Lean-engineering fights above. Compile roundtrip per iteration: 30–60s.

---

## 5. Significance

### 5.1 For UltimaDB

This is the **first machine-checked correctness result about UltimaDB's core algorithm**. Concretely, the theorem rules out — for the translated kernel, and by the fidelity argument for `btree.rs` — the entire class of bugs where insertion corrupts the search-tree discipline: median off-by-ones, children mis-slotted after splits, bound violations across promoted keys, sorted-order breakage on replace. It additionally proves the insert path panic-free and overflow-free for every tree satisfying the invariant (which the arity bound makes self-sustaining).

The honest caveat is the **twin-code gap**: we verified a port with five documented, individually-argued deltas, not `btree.rs` itself. Two mitigations are available and worth doing: (a) a CI check that structurally diffs the kernel against `btree.rs`'s insert path so drift is caught; (b) longer-term, inverting the relationship — make the verified kernel the canonical algorithm reference. The differential test against `std::BTreeMap` already anchors behavioral equivalence empirically.

**Update (day 2, continued):** get-after-insert is now also proved — `BTree.insert_get`: after `insert k v`, `get k` returns `ok (some v)` (864 further lines; `GetPost` handles the subtle case where the inserted pair becomes a promoted split median; navigation rests on `find_pos` completeness + uniqueness from sortedness). Axiom check identical. One process lesson worth an article aside: a `grep -c … && python-edit` chain silently skipped two edits (grep -c exits nonzero on zero matches), producing an illusory "compiled first try" on the unmodified file — caught only by the end-of-run `#print axioms` discipline, which is precisely the point of doing verification this way.

**Update (day 2, final):** the frame property is also proved — `BTree.insert_frame`: `insert k v` leaves `get k'` unchanged for every `k' ≠ k` (1,200 further lines incl. bracket-transport lemmas for median insertion). Together, `insert_inv` + `insert_get` + `insert_frame` constitute **complete functional correctness of insertion**: the translated B-tree insert behaves exactly as a map update, machine-checked. Cumulative verified Lean: ~4,900 hand-written lines, ~85 theorems, across three sessions-worth of work in two days.

What this does *not* cover (roadmap, in value order): the `remove`/rebalancing path (the hairiest code in `btree.rs`, unported); range iterators; and everything concurrent — the MultiWriter OCC merge protocol, commit-version promotion ordering, WAL recovery. Those last three are out of Aeneas's scope (its unsafe/concurrency support is future work) and need hand-written protocol models — where the model-fidelity gap returns and the payoff is arguably even higher.

### 5.2 For the "AI + formal methods" story

1. **The economics have flipped.** Aeneas's comparable betree case study was a research-paper-scale effort by verification experts. Here, a database side-project got a nontrivial invariant proof in ~2 days of AI-assisted work, ~$0 in specialized-model costs, with the human role reduced to direction-setting. The bottleneck is no longer proof labor; it's deciding what's worth proving and designing invariants.
2. **Verification is the ideal AI workload** because the trust story doesn't depend on the AI. Every step — Leanstral's attempts, subagent output, the orchestrator's own proofs — passes through the Lean kernel. A proof that compiles is correct no matter which model wrote it, how many hallucinated lemma names preceded it, or whether the process was reviewable. The anti-cheat guards (no `sorry`/`axiom`, statements verbatim) plus a final `#print axioms` make the acceptance criterion fully mechanical. This is the inverse of most AI coding, where plausible-but-wrong survives review.
3. **Specialist vs. generalist, July-2026 snapshot.** The specialized prover (Leanstral) went 0/2 through a naive harness while a generalist frontier model completed everything — but the comparison is unfair in an instructive way: the generalist could *read the library source*, learn `step*`/`scalar_tac` idioms, and adapt its harness on the fly; Leanstral saw only compiler stderr. Its proof *architecture* was right almost immediately. The actionable conclusion: **harness quality (goal states, library search, idiom priming) dominates model choice**, and hybrid designs — generalist orchestrates and integrates, specialist grinds well-framed subgoals through a rich harness — are the obvious next experiment.
4. **The parallel proof factory pattern worked on the first try.** Decompose the theorem into an independent lemma stack; freeze exact statements up front (merges become mechanical); give each agent a worked example and an idiom cheat-sheet; let the compiler adjudicate; integrate in one context that owns the invariant design. Five out of five agents returned complete proofs. The pattern generalizes to any goal with a stable spec boundary.
5. **Mechanical translation is the fidelity unlock.** Hand-written models drift; Aeneas-translated code can't (modulo the subset deltas). The practical recipe for Rust projects: extract algorithmic kernels into dependency-free crates written in the supported subset, differential-test them against the originals, translate, prove. The subset limitations we hit (no `Arc`, no recursive-through-`Vec`, no mutually recursive derived impls, no closures) all had local workarounds — none blocked the project.

### 5.3 One-paragraph version (for the article lede)

We took the copy-on-write B-tree at the heart of UltimaDB, mechanically translated its insertion algorithm from Rust into Lean 4 with the Aeneas toolchain, and produced a kernel-checked proof — no `sorry`, no extra axioms — that insertion preserves the full B-tree ordering invariant, median splits included, and can never panic or overflow along the way. The proof was built in about two days by a frontier LLM orchestrating five parallel proof agents, with the Lean compiler as the only authority; a purpose-built Lean prover (Mistral's freshly-released Leanstral) was evaluated in the same loop and, through a naive harness, designed the right proofs but couldn't land them — a result that says less about the model than about where the real leverage in AI-assisted verification lies: in the harness, the invariant design, and the decomposition.

---

## 6. Appendix: artifact inventory & reproduction

```
formal/.toolchain/                   # prebuilt aeneas + charon + Lean backend library
                                     #   (gitignored; populate via formal/scripts/fetch-toolchain.sh)
formal/kernel/                       # Rust verification kernel + differential test
formal/proofs/                       # lake package: everything below
    BtreeKernel.lean                 # GENERATED — never edit
    BtreeInvariant.lean              # NodeInv/Aligned/InB/SortedE/lbnd/rbnd definitions
    BtreeProofs.lean                 # new_get_none, find_pos_ok
    FindPosSpec.lean                 # find_pos_spec (binary search correctness)
    EntrySpecs.lean                  # entry-vector loop specs
    ListLemmas.lean                  # pure sortedness/bounds lemmas
    ChildrenSpecs.lean               # cons-list specs + clone identity + children_ind
    AlignedLemmas.lean               # alignment surgery lemmas
    TransportLemmas.lean             # bracket/found transport across median insertion
    BtreeInsertInv.lean              # maybe_split_spec, insert_into_node_inv, BTree.insert_inv
    BtreeInsertGet.lean              # navigation lemmas, insert_get_same, BTree.insert_get
    BtreeInsertFrame.lean            # GetR, frame lemmas, insert_frame, BTree.insert_frame
~/ultima/leanstral-demo/             # Leanstral experiments incl. drive.py harness (outside repo)
```
(The `Agent*.lean` names in the narrative above were the working names of the
parallel-subagent deliverables; they were renamed to the descriptive names on
merge into the repo.)

Reproduce the pipeline:
```bash
cd formal/kernel
cargo test                                        # differential test vs std BTreeMap
PATH=$PWD/../.toolchain/charon-bin:$PATH charon cargo --preset=aeneas
../.toolchain/aeneas -backend lean btree_kernel.llbc
cp BtreeKernel.lean ../proofs/
cd ../proofs && lake build                        # checks every theorem
```

Integrity check:
```lean
import BtreeInsertInv
#print axioms btree_kernel.BTree.insert_inv
-- 'btree_kernel.BTree.insert_inv' depends on axioms: [propext, Classical.choice, Quot.sound]
```

Key theorem statements (verbatim):
```lean
theorem insert_into_node_inv (fuel : Nat) {lo hi : Option Nat}
    (node : Node) (k v : Std.U64)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hk : InB lo hi k.val) :
    insert_into_node node k v ⦃ r => InsPost lo hi r ⦄

theorem BTree.insert_inv (t : BTree) (k v : Std.U64)
    (hinv : BTreeInv t) (hcap : t.len.val < Std.Usize.max) :
    BTree.insert t k v ⦃ t' => BTreeInv t' ⦄
```
