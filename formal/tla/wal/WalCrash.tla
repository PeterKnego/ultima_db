---------------------------- MODULE WalCrash ----------------------------
(***************************************************************************)
(* F-DB-2 scout, S1 / Task 1: UltimaDB's Standalone persistence pipeline,   *)
(* steady state only -- NO crash and NO recovery yet (those are Task 2).    *)
(*                                                                         *)
(* Ground truth: docs/tasks/task15_three_phase_consistent_persistence.md    *)
(* ("Three-phase commit protocol", "Why this is safe", "Promotion ordering  *)
(* (lost-update fix)"), and the commit path in src/store.rs                 *)
(* (commit_single_writer ~3737, commit_multi_writer phase 3 ~3977).         *)
(*                                                                         *)
(* The protocol, as modelled:                                              *)
(*                                                                         *)
(*   Begin   -- begin_write(None) allocates a candidate version from       *)
(*              next_version and bumps it; SingleWriter admits one writer.  *)
(*   Submit  -- Phase 1 PREPARE, under the store write lock: finalize the   *)
(*              version (bump past max(latest_version, last_submitted) and  *)
(*              allocate from next_version), append the WAL entry to the    *)
(*              buffered log (no fsync), take a PromoteGate ticket.         *)
(*   Fsync   -- the WAL background thread's durability barrier: it drains a *)
(*              batch and issues one sync for the whole batch, so a barrier *)
(*              covers a non-empty PREFIX of the buffered log.              *)
(*   Promote -- Phase 3 PROMOTE: the FIFO ticket at the head of the gate,   *)
(*              once its entry is covered by a barrier, forks from the      *)
(*              latest AT PROMOTE TIME, installs, and advances              *)
(*              latest_version.                                             *)
(*                                                                         *)
(* The load-bearing rule the whole model exists to check (task15):          *)
(*   "latest_version strictly advances at every promotion, and every        *)
(*    promotion forks from the latest at promote time."                     *)
(* PromotionFaithful is exactly that sentence.                             *)
(*                                                                         *)
(* Deliberate faithfulness choice: Promote installs UNCONDITIONALLY at its  *)
(* version and only advances latest_version when strictly greater -- this   *)
(* mirrors `snapshots.insert(v, ..); if v > latest_version { .. }` in       *)
(* src/store.rs. The model does NOT assume the good property; the invariant *)
(* detects its absence. That is what gives the M1-M5 mutations teeth.       *)
(*                                                                         *)
(* NON-GOALS (§4 of the scout brief), not modelled here:                    *)
(*   SMR / explicit-version commits; MultiWriter OCC validation semantics   *)
(*   (Elle covers those); byte-level framing; snapshot contents (a commit   *)
(*   is an opaque <version, table> pair); GC; the bench-only WAL sinks;     *)
(*   random mid-WAL corruption.                                            *)
(*                                                                         *)
(* NOT YET modelled, arriving in later tasks of this plan:                  *)
(*   Crash + recovery (Task 2) -- `crashed` is declared and pinned FALSE    *)
(*     here so Task 2 adds an action, not a variable, to every config.      *)
(*   SinkKind behaviour -- declared and carried, but FsWrite / Coalesced /  *)
(*     CoalescedPrealloc only diverge under a crash (sync_data vs sync_all, *)
(*     torn tails), so at steady state all three share one barrier rule.    *)
(*   Fsync FAILURE (the "advance the gate without promoting" rule) -- a     *)
(*     failure path, adjacent to crash; Task 2+.                            *)
(*   MUTATION -- declared now so Tasks 4/5 gate behaviour on it without     *)
(*     retrofitting every config. No mutation exists yet; "NONE" only.      *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
    MaxCommits,     \* bound on commits per behaviour (<= 4)
    Tables,         \* set of table identities (<= 2)
    Durability,     \* "Consistent" | "ConsistentInline" | "Eventual"
    SinkKind,       \* "FsWrite" | "Coalesced" | "CoalescedPrealloc"
    WriterMode,     \* "SingleWriter" | "MultiWriter"
    MUTATION        \* "NONE" | "M1".."M5" -- calibration mutation selector

ASSUME MaxCommits \in Nat
ASSUME Durability \in {"Consistent", "ConsistentInline", "Eventual"}
ASSUME SinkKind   \in {"FsWrite", "Coalesced", "CoalescedPrealloc"}
ASSUME WriterMode \in {"SingleWriter", "MultiWriter"}
ASSUME MUTATION   \in {"NONE", "M1", "M2", "M3", "M4", "M5"}

Cids == 1..MaxCommits

VARIABLES
    walBuffered,    \* Seq of frames written to the WAL, not yet barriered
    walDurable,     \* Seq of frames covered by a durability barrier
    begun,          \* set of writers past begin_write, not yet submitted
    submitted,      \* Seq of entries in WAL-submission order (== ticket order)
    parked,         \* Seq: the PromoteGate FIFO -- submitted, not yet promoted
    promoted,       \* Seq: the promotion log (the snapshot chain)
    latestVersion,  \* StoreInner::latest_version
    lastSubmitted,  \* StoreInner::last_submitted_version
    nextVersion,    \* StoreInner::next_version
    acked,          \* set of cids whose commit() returned Ok
    crashed         \* Task 2 seam; always FALSE here

vars == <<walBuffered, walDurable, begun, submitted, parked, promoted,
          latestVersion, lastSubmitted, nextVersion, acked, crashed>>

Max2(a, b) == IF a > b THEN a ELSE b

(* Stores whose commits can never park skip the gate entirely and hold the  *)
(* write lock continuously from version assignment through promotion        *)
(* (StoreInner::commit_may_park, src/store.rs:405).                         *)
CommitMayPark == Durability \in {"Consistent", "ConsistentInline"}

SubmittedCids == { submitted[i].cid : i \in 1..Len(submitted) }
BegunCids     == { r.cid : r \in begun }
UsedCids      == BegunCids \cup SubmittedCids

(* Phase 2's wait condition: the entry is covered by a durability barrier. *)
IsDurable(c) == \E i \in 1..Len(walDurable) : walDurable[i].cid = c

SubIndex(c) == CHOOSE i \in 1..Len(submitted) : submitted[i].cid = c

(* SingleWriter holds the writer slot (active_writer_count) from            *)
(* begin_write through promotion -- through the fsync wait -- so a second   *)
(* writer cannot be admitted and fork from a latest that lacks the parked   *)
(* commit (task15, "Promotion ordering", failure mode 1).                   *)
WriterSlotFree == (WriterMode = "MultiWriter") \/ (begun = {} /\ parked = <<>>)

----------------------------------------------------------------------------

Init ==
    /\ walBuffered   = <<>>
    /\ walDurable    = <<>>
    /\ begun         = {}
    /\ submitted     = <<>>
    /\ parked        = <<>>
    /\ promoted      = <<>>
    /\ latestVersion = 0        \* the genesis snapshot
    /\ lastSubmitted = 0
    /\ nextVersion   = 1
    /\ acked         = {}
    /\ crashed       = FALSE

(* begin_write(None), src/store.rs:656: allocate the candidate commit       *)
(* version from next_version and keep next_version ahead of it.             *)
Begin(c, t) ==
    /\ ~crashed
    /\ c \notin UsedCids
    /\ WriterSlotFree
    /\ begun'       = begun \cup {[cid |-> c, ver |-> nextVersion, tbl |-> t]}
    /\ nextVersion' = nextVersion + 1
    /\ UNCHANGED <<walBuffered, walDurable, submitted, parked, promoted,
                   latestVersion, lastSubmitted, acked, crashed>>

(* Phase 1 PREPARE (src/store.rs:3977 ff). Under the write lock: finalize   *)
(* the version against max(last_submitted, latest) allocating from          *)
(* next_version, submit the WAL entry (no fsync), take a ticket.            *)
(* Under Eventual / no-WAL the lock is never released, so phases 2-3        *)
(* collapse into this same step.                                            *)
Submit(r) ==
    /\ ~crashed
    /\ r \in begun
    /\ LET bump == r.ver <= Max2(lastSubmitted, latestVersion)
           v    == IF bump THEN nextVersion ELSE r.ver
           e    == [cid |-> r.cid, ver |-> v, tbl |-> r.tbl]
       IN /\ nextVersion'   = IF bump THEN nextVersion + 1 ELSE nextVersion
          /\ lastSubmitted' = Max2(lastSubmitted, v)
          /\ begun'         = begun \ {r}
          /\ submitted'     = Append(submitted, e)
          /\ walBuffered'   = Append(walBuffered, [cid |-> r.cid, ver |-> v])
          /\ IF CommitMayPark
               THEN /\ parked' = Append(parked, e)
                    /\ UNCHANGED <<promoted, latestVersion, acked>>
               ELSE /\ parked'   = parked
                    /\ promoted' = Append(promoted,
                           [cid        |-> r.cid,
                            ver        |-> v,
                            tbl        |-> r.tbl,
                            sub        |-> Len(submitted) + 1,
                            forkedFrom |-> latestVersion])
                    /\ latestVersion' = Max2(latestVersion, v)
                    /\ acked'         = acked \cup {r.cid}
    /\ UNCHANGED <<walDurable, crashed>>

(* The WAL background thread: recv() the first entry, drain the rest with   *)
(* try_recv(), then ONE sync for the whole batch (task15, "Background WAL   *)
(* writer"). A barrier therefore covers a non-empty prefix of the buffered  *)
(* log; the batch boundary is nondeterministic.                             *)
Fsync ==
    /\ ~crashed
    /\ walBuffered # <<>>
    /\ \E n \in 1..Len(walBuffered) :
         /\ walDurable'  = walDurable \o SubSeq(walBuffered, 1, n)
         /\ walBuffered' = SubSeq(walBuffered, n + 1, Len(walBuffered))
    /\ UNCHANGED <<begun, submitted, parked, promoted, latestVersion,
                   lastSubmitted, nextVersion, acked, crashed>>

(* Phase 3 PROMOTE. Only the head of the PromoteGate FIFO may promote, and  *)
(* only once its entry is durable. It forks from latestVersion AS OF NOW    *)
(* (not from its own base) and installs; latest_version advances only when  *)
(* strictly greater -- exactly `if v > inner.latest_version`.               *)
Promote ==
    /\ ~crashed
    /\ parked # <<>>
    /\ LET h == Head(parked)
       IN /\ IsDurable(h.cid)
          /\ parked'   = Tail(parked)
          /\ promoted' = Append(promoted,
                 [cid        |-> h.cid,
                  ver        |-> h.ver,
                  tbl        |-> h.tbl,
                  sub        |-> SubIndex(h.cid),
                  forkedFrom |-> latestVersion])
          /\ latestVersion' = Max2(latestVersion, h.ver)
          /\ acked'         = acked \cup {h.cid}
    /\ UNCHANGED <<walBuffered, walDurable, begun, submitted, lastSubmitted,
                   nextVersion, crashed>>

Next ==
    \/ \E c \in Cids, t \in Tables : Begin(c, t)
    \/ \E r \in begun : Submit(r)
    \/ Fsync
    \/ Promote

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Frame  == [cid : Cids, ver : Nat]
Writer == [cid : Cids, ver : Nat, tbl : Tables]
Snap   == [cid : Cids, ver : Nat, tbl : Tables, sub : Nat, forkedFrom : Nat]

TypeOK ==
    /\ \A i \in 1..Len(walBuffered) : walBuffered[i] \in Frame
    /\ \A i \in 1..Len(walDurable)  : walDurable[i]  \in Frame
    /\ \A r \in begun               : r \in Writer
    /\ \A i \in 1..Len(submitted)   : submitted[i] \in Writer
    /\ \A i \in 1..Len(parked)      : parked[i]    \in Writer
    /\ \A i \in 1..Len(promoted)    : promoted[i]  \in Snap
    /\ latestVersion \in Nat
    /\ lastSubmitted \in Nat
    /\ nextVersion   \in Nat
    /\ acked \subseteq Cids
    /\ crashed \in BOOLEAN

(***************************************************************************)
(* VACUITY CANARY. Must be VIOLATED. A model in which nothing ever promotes *)
(* satisfies every safety property below it, trivially and silently. Run    *)
(* this first, every time; if TLC reports it holds, the model is inert and  *)
(* every green underneath it is meaningless.                                *)
(***************************************************************************)
NoCommitEverPromotes == promoted = <<>>

(***************************************************************************)
(* S2 PromotionFaithful -- task15's load-bearing rule, clause by clause.    *)
(*  (1) the promoted chain is exactly submission order (the PromoteGate     *)
(*      FIFO: promotion i is the i'th submission);                          *)
(*  (2) latest_version strictly advances at every promotion;                *)
(*  (3) every promotion forked from the latest at promote time (the fork    *)
(*      source is the immediately preceding promotion, genesis 0 for the    *)
(*      first) -- this is what fails when a promotion forks from a latest   *)
(*      that lags behind a parked commit;                                   *)
(*  (4) no snapshot is ever replaced at the same version (a duplicate       *)
(*      `snapshots.insert(v, ..)` silently erases the earlier commit).      *)
(***************************************************************************)
PromotionFaithful ==
    /\ \A i \in 1..Len(promoted) : promoted[i].sub = i
    /\ \A i \in 2..Len(promoted) : promoted[i].ver > promoted[i-1].ver
    /\ \A i \in 1..Len(promoted) :
           promoted[i].forkedFrom = IF i = 1 THEN 0 ELSE promoted[i-1].ver
    /\ \A i, j \in 1..Len(promoted) : (i # j) => promoted[i].ver # promoted[j].ver

=============================================================================
