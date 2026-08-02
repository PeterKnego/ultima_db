---------------------------- MODULE WalCrash ----------------------------
(***************************************************************************)
(* F-DB-2 scout, S1 / Tasks 1-2: UltimaDB's Standalone persistence          *)
(* pipeline -- the steady-state three-phase commit (Task 1) plus crash and   *)
(* recovery (Task 2).                                                       *)
(*                                                                         *)
(* Ground truth: docs/tasks/task15_three_phase_consistent_persistence.md    *)
(* ("Three-phase commit protocol", "Why this is safe", "Promotion ordering  *)
(* (lost-update fix)"), the commit path in src/store.rs                     *)
(* (commit_single_writer ~3737, commit_multi_writer phase 3 ~3977), and for *)
(* Task 2 the recovery path Store::recover (src/store.rs:984-1163) plus     *)
(* wal::scan_wal (src/wal.rs:561-610).                                      *)
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
(* Task 2 adds, on top of that:                                             *)
(*                                                                         *)
(*   Crash   -- at most once per behaviour. Volatile state (the snapshot    *)
(*              chain, latest/next version, in-flight and parked writers,   *)
(*              the unbarriered write-back cache) is lost. Every frame      *)
(*              already covered by a durability barrier survives intact;    *)
(*              every merely-buffered frame independently survives, TEARS   *)
(*              (present-but-CRC-bad), or is absent.                        *)
(*   Recover -- Store::recover (src/store.rs:984): install the latest       *)
(*              checkpoint, scan_wal, replay the entries whose version      *)
(*              exceeds the checkpoint's. tail_tolerant is passed TRUE      *)
(*              exactly for CoalescedPrealloc (src/store.rs:1017-1022).     *)
(*                                                                         *)
(* CAUTION for later tasks -- the post-Recover value of `promoted` is the   *)
(* REPLAY SEQUENCE, not the Rust's snapshot chain. Recovery installs        *)
(* exactly ONE snapshot, at latest_version (src/store.rs:1150-1156);        *)
(* intermediate replayed versions never enter inner.snapshots. Reusing      *)
(* `promoted` for the replay makes all four RecoverySound clauses           *)
(* expressible and is harmless for them, but do NOT build a property like   *)
(* "every acked version is READABLE after recovery" on it -- in the Rust    *)
(* those intermediate versions are not readable, and the model would say    *)
(* they are.                                                                *)
(*                                                                         *)
(* BOUND: at most one crash per behaviour AND no operation after recovery.  *)
(* Every steady-state action requires ~crashed and Recover requires         *)
(* ~recovered, so a restarted store never commits again. "Does a restarted  *)
(* store re-issue versions safely?" is outside this state space.            *)
(*                                                                         *)
(* SinkKind is now load-bearing, but only through TailTolerant: FsWrite and *)
(* Coalesced are both strict scans and remain indistinguishable at this     *)
(* altitude (they differ in write granularity, not in sync granularity or   *)
(* in what the recovery scan does).                                         *)
(*                                                                         *)
(* NOT YET modelled, arriving in later tasks of this plan:                  *)
(*   Fsync FAILURE (the "advance the gate without promoting" rule). Crash   *)
(*     did NOT give it a natural home: a crash removes a parked commit by   *)
(*     destroying the whole gate, whereas a failed fsync must remove ONE    *)
(*     ticket and let the rest of the FIFO proceed. That needs a per-ticket *)
(*     outcome on Fsync, not a crash. L1 (liveness) stays inexpressible     *)
(*     until then. S2 work; deliberately NOT added here.                    *)
(*   Checkpointing -- `checkpointVersion` is carried (Recover honours it as *)
(*     the replay floor, src/store.rs:1027-1030) but no action moves it off *)
(*     0, so no committed config exercises a non-zero floor. A Checkpoint   *)
(*     action also drags in WAL pruning (src/wal.rs:628 prune_wal), which   *)
(*     is where checkpoint/prune/crash interleavings would actually bite.   *)
(*     Out of scope for Task 2's brief; see the report.                     *)
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
    crashed,        \* a crash has happened (at most one per behaviour)
    recovered,      \* Store::recover has run
    recoverErr,     \* ...and returned Err (strict scan hit a CRC mismatch)
    checkpointVersion, \* the replay floor: version of the latest checkpoint
    walAfterCrash   \* the on-disk log the recovery scan reads

vars == <<walBuffered, walDurable, begun, submitted, parked, promoted,
          latestVersion, lastSubmitted, nextVersion, acked, crashed,
          recovered, recoverErr, checkpointVersion, walAfterCrash>>

crashVars == <<crashed, recovered, recoverErr, checkpointVersion,
               walAfterCrash>>

(* `submitted` and `acked` are HISTORY variables: they record what happened *)
(* and what the caller was told, not implementation state, so they survive  *)
(* the crash. Everything else in `vars` is store state and is destroyed.    *)

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

(***************************************************************************)
(* Which protection applies to which writer mode -- this asymmetry is the   *)
(* whole reason M1 is a distinct calibration bug from M2/M3.                *)
(*                                                                         *)
(* commit_multi_writer (src/store.rs:3844) has BOTH the version bump        *)
(* (:3992) and the PromoteGate FIFO (:4050 take, :4104/:4121 wait).         *)
(* commit_single_writer (src/store.rs:3737-3843) has NEITHER. Its only      *)
(* protection is holding the writer slot through the fsync wait             *)
(* (:3786-3801, and begin_write's active_writer_count check at :668).       *)
(*                                                                         *)
(* Modelling the bump and the gate unconditionally would hand SingleWriter  *)
(* two protections the code does not have, and breaking its one real        *)
(* protection would then change nothing observable -- M1 would be masked.   *)
(***************************************************************************)
GateApplies == WriterMode = "MultiWriter"
BumpApplies == WriterMode = "MultiWriter"

(* SingleWriter holds the writer slot (active_writer_count) from            *)
(* begin_write through PROMOTION -- through the fsync wait -- so a second   *)
(* writer cannot be admitted and fork from a latest that lacks the parked   *)
(* commit (task15, "Promotion ordering", failure mode 1).                   *)
(*                                                                         *)
(* M1 re-creates the pre-fix code: the slot was released in phase 1, at     *)
(* submission, so a second writer was admitted while the first was still    *)
(* parked in the fsync wait. Only the `parked = <<>>` conjunct drops --     *)
(* one writer at a time before submission is unchanged.                     *)
WriterSlotFree ==
    \/ WriterMode = "MultiWriter"
    \/ IF MUTATION = "M1" THEN begun = {} ELSE (begun = {} /\ parked = <<>>)

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
    /\ recovered     = FALSE
    /\ recoverErr    = FALSE
    /\ checkpointVersion = 0    \* no checkpoint: the floor is genesis
    /\ walAfterCrash = <<>>

(* begin_write(None), src/store.rs:656: allocate the candidate commit       *)
(* version from next_version and keep next_version ahead of it.             *)
Begin(c, t) ==
    /\ ~crashed
    /\ c \notin UsedCids
    /\ WriterSlotFree
    /\ begun'       = begun \cup {[cid |-> c, ver |-> nextVersion, tbl |-> t]}
    /\ nextVersion' = nextVersion + 1
    /\ UNCHANGED <<walBuffered, walDurable, submitted, parked, promoted,
                   latestVersion, lastSubmitted, acked, crashVars>>

(* Phase 1 PREPARE (src/store.rs:3977 ff). Under the write lock: finalize   *)
(* the version against max(last_submitted, latest) allocating from          *)
(* next_version, submit the WAL entry (no fsync), take a ticket.            *)
(* Under Eventual / no-WAL the lock is never released, so phases 2-3        *)
(* collapse into this same step.                                            *)
Submit(r) ==
    /\ ~crashed
    /\ r \in begun
    /\ LET bump == BumpApplies /\ r.ver <= Max2(lastSubmitted, latestVersion)
           v    == IF bump THEN nextVersion ELSE r.ver
           e    == [cid |-> r.cid, ver |-> v, tbl |-> r.tbl]
       IN /\ nextVersion'   = IF bump THEN nextVersion + 1 ELSE nextVersion
          \* `last_submitted_version` is maintained only by commit_multi_writer
          \* (src/store.rs:3997). Kept unconditional here because it is read
          \* only by the bump, which BumpApplies already gates off.
          /\ lastSubmitted' = Max2(lastSubmitted, v)
          /\ begun'         = begun \ {r}
          /\ submitted'     = Append(submitted, e)
          \* The WAL entry names the table it touches (wal::WalOp::Insert
          \* { table, .. }), so replay can rebuild it; the frame carries tbl.
          /\ walBuffered'   = Append(walBuffered,
                                 [cid |-> r.cid, ver |-> v, tbl |-> r.tbl])
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
    /\ UNCHANGED <<walDurable, crashVars>>

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
                   lastSubmitted, nextVersion, acked, crashVars>>

(* Phase 3 PROMOTE. A parked commit may promote once its entry is durable.  *)
(* Under MultiWriter the PromoteGate restricts that to the FIFO head        *)
(* (k = 1); commit_single_writer has no gate at all, so any parked commit   *)
(* may promote as its own fsync wait completes. At MUTATION = "NONE" that   *)
(* is behaviour-preserving -- WriterSlotFree keeps Len(parked) <= 1 under   *)
(* SingleWriter, so k = 1 is the only choice anyway. It stops being         *)
(* behaviour-preserving under M1, which is exactly the point.               *)
(*                                                                         *)
(* Promotion forks from latestVersion AS OF NOW (not from its own base) and *)
(* installs; latest_version advances only when strictly greater -- exactly  *)
(* `snapshots.insert(v, ..); if v > inner.latest_version { .. }`.           *)
Promote ==
    /\ ~crashed
    /\ parked # <<>>
    /\ \E k \in 1..Len(parked) :
       /\ GateApplies => (k = 1)
       /\ LET h == parked[k]
          IN /\ IsDurable(h.cid)
             /\ parked'   = SubSeq(parked, 1, k - 1)
                            \o SubSeq(parked, k + 1, Len(parked))
             /\ promoted' = Append(promoted,
                    [cid        |-> h.cid,
                     ver        |-> h.ver,
                     tbl        |-> h.tbl,
                     sub        |-> SubIndex(h.cid),
                     forkedFrom |-> latestVersion])
             /\ latestVersion' = Max2(latestVersion, h.ver)
             /\ acked'         = acked \cup {h.cid}
    /\ UNCHANGED <<walBuffered, walDurable, begun, submitted, lastSubmitted,
                   nextVersion, crashVars>>

----------------------------------------------------------------------------
(***************************************************************************)
(*                       CRASH AND RECOVERY (Task 2)                        *)
(***************************************************************************)

(* A frame as the RECOVERY SCAN sees it after a crash. `st` is what became  *)
(* of the record at that OFFSET -- a flag, not bytes; the model has no      *)
(* byte-level framing (§4 non-goal).                                        *)
(*   "present" -- the whole record landed and its CRC verifies;             *)
(*   "torn"    -- the record is there but the CRC does not verify           *)
(*                (src/wal.rs:585);                                         *)
(*   "absent"  -- nothing landed at this offset: a zero len-prefix or a     *)
(*                short tail (src/wal.rs:576-581).                          *)
FrameSt   == {"present", "torn", "absent"}
CrashFrame == [cid : Cids, ver : Nat, tbl : Tables, st : FrameSt]

(* Frames covered by a durability barrier survive a crash unaltered.        *)
Intact(s) == [i \in 1..Len(s) |->
                 [cid |-> s[i].cid, ver |-> s[i].ver, tbl |-> s[i].tbl,
                  st  |-> "present"]]

(* The per-frame crash outcome for the unbarriered (write-back cached)      *)
(* tail: each buffered frame independently landed whole, landed torn, or    *)
(* never landed.                                                            *)
(*                                                                         *)
(* POSITIONAL, deliberately: an absent frame is a HOLE at its offset, not a *)
(* removal that slides later frames forward. Every sink assigns a frame its *)
(* byte offset at append time, in submission order (PreallocFileSink seeks  *)
(* to its own write_head, src/wal.rs:1045), and scan_wal walks offsets      *)
(* strictly in order, breaking at the first record it cannot accept         *)
(* (src/wal.rs:574-607) -- there is no skip-and-continue branch. So a frame *)
(* that never landed makes every LATER frame unreachable to recovery,       *)
(* whether or not its own bytes reached the platter. Modelling absence as   *)
(* compaction instead (the shape Task 2's brief sketches) manufactures a    *)
(* log with a hole at offset 0, which no filesystem produces; it violates   *)
(* RecoverySound (a) under MultiWriter as a pure model artifact. See the    *)
(* Task 2 report, "Adjudication".                                           *)
SurvivingFrames(buf, outcome) ==
    [i \in 1..Len(buf) |->
        [cid |-> buf[i].cid, ver |-> buf[i].ver, tbl |-> buf[i].tbl,
         st  |-> outcome[i]]]

(* Store::recover passes tail_tolerant = TRUE exactly for CoalescedPrealloc *)
(* (src/store.rs:1017-1022).                                                *)
TailTolerant(sk) == sk = "CoalescedPrealloc"

(* scan_wal walks frames at increasing offsets and STOPS at the first frame *)
(* it cannot accept (src/wal.rs:574-607). ScanLen is the length of the      *)
(* longest accepted prefix.                                                 *)
ScanLen(log) == CHOOSE n \in 0..Len(log) :
    /\ \A i \in 1..n : log[i].st = "present"
    /\ (n < Len(log)) => log[n+1].st # "present"

(* WHICH stop it is decides whether recovery is silent or loud.             *)
(* An absent record -- zero len-prefix or short tail -- is a clean          *)
(* end-of-log in BOTH modes (src/wal.rs:576-581, unconditional `break`).    *)
(* A TORN record is end-of-log only when tail_tolerant (src/wal.rs:586-588);*)
(* strict mode returns Error::WalCorrupted (:589-591), which Store::recover *)
(* propagates with `?` (src/store.rs:1023) -- so NOTHING is replayed, not   *)
(* even the frames the scan had already accepted.                           *)
ScanFails(log, tolerant) ==
    /\ ~tolerant
    /\ ScanLen(log) < Len(log)
    /\ log[ScanLen(log) + 1].st = "torn"

(* The replay: the accepted prefix, filtered to entries whose version       *)
(* exceeds the checkpoint floor (src/store.rs:1027-1030), applied in order. *)
(* Each replayed entry is built on the state left by its predecessor, hence *)
(* the forkedFrom chain; `sub` is looked up in the submission history so    *)
(* that "replay order = submission order" stays a CHECKED property rather   *)
(* than one the construction assumes.                                       *)
Replay(log, cpVer, tolerant) ==
    LET accepted == SubSeq(log, 1, ScanLen(log))
        live     == SelectSeq(accepted, LAMBDA fr : fr.ver > cpVer)
    IN [i \in 1..Len(live) |->
           [cid        |-> live[i].cid,
            ver        |-> live[i].ver,
            tbl        |-> live[i].tbl,
            sub        |-> SubIndex(live[i].cid),
            forkedFrom |-> IF i = 1 THEN cpVer ELSE live[i-1].ver]]

(* Every buffered frame independently survives, tears, or vanishes; durable *)
(* frames always survive. All volatile store state resets -- the process is *)
(* gone. `submitted` and `acked` are history and survive by construction:   *)
(* `acked` records what the CALLER was told, and RecoverySound exists to    *)
(* compare that against what actually came back.                            *)
Crash ==
    /\ ~crashed
    /\ \E outcome \in [1..Len(walBuffered) -> {"absent", "torn", "present"}] :
         walAfterCrash' = Intact(walDurable)
                          \o SurvivingFrames(walBuffered, outcome)
    /\ crashed'       = TRUE
    /\ walBuffered'   = <<>>    \* the write-back cache is gone
    /\ walDurable'    = <<>>    \* the log now lives only in walAfterCrash
    /\ begun'         = {}
    /\ parked'        = <<>>    \* parked commits are volatile -- their acks are not
    /\ promoted'      = <<>>    \* the snapshot chain was in memory
    /\ latestVersion' = 0
    /\ lastSubmitted' = 0
    /\ nextVersion'   = 1
    /\ UNCHANGED <<submitted, acked, recovered, recoverErr, checkpointVersion>>

(* Store::recover (src/store.rs:984): install the latest checkpoint, then   *)
(* in Standalone mode scan the WAL and replay past the checkpoint version.  *)
(* On a strict-mode scan error the checkpoint has ALREADY been installed    *)
(* (src/store.rs:1005-1009 runs before the scan at :1023) but recover()     *)
(* returns Err and no WAL entry is applied.                                 *)
Recover ==
    /\ crashed /\ ~recovered
    /\ recovered' = TRUE
    /\ LET tolerant == TailTolerant(SinkKind)
           chain    == Replay(walAfterCrash, checkpointVersion, tolerant)
       IN IF ScanFails(walAfterCrash, tolerant)
            THEN /\ recoverErr'    = TRUE
                 /\ promoted'      = <<>>
                 /\ latestVersion' = checkpointVersion
            ELSE /\ recoverErr'    = FALSE
                 /\ promoted'      = chain
                 \* src/store.rs:1146-1157: latest_version is the version of
                 \* the last entry replayed, or the checkpoint's if none.
                 /\ latestVersion' = IF chain = <<>>
                                       THEN checkpointVersion
                                       ELSE chain[Len(chain)].ver
    /\ UNCHANGED <<walBuffered, walDurable, begun, submitted, parked,
                   lastSubmitted, nextVersion, acked, crashed,
                   checkpointVersion, walAfterCrash>>

----------------------------------------------------------------------------

Next ==
    \/ \E c \in Cids, t \in Tables : Begin(c, t)
    \/ \E r \in begun : Submit(r)
    \/ Fsync
    \/ Promote
    \/ Crash
    \/ Recover

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Frame  == [cid : Cids, ver : Nat, tbl : Tables]
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
    /\ recovered \in BOOLEAN
    /\ recoverErr \in BOOLEAN
    /\ checkpointVersion \in Nat
    /\ \A i \in 1..Len(walAfterCrash) : walAfterCrash[i] \in CrashFrame
    /\ recovered => crashed          \* recovery only follows a crash

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

(***************************************************************************)
(* CRASH-REACHABILITY CANARY (Task 2). Must be VIOLATED. If it HOLDS, no    *)
(* behaviour ever crashes and then recovers, and RecoverySound is being     *)
(* checked over zero crash behaviours -- a green with no content.           *)
(***************************************************************************)
NoCrashThenRecover == ~(crashed /\ recovered)

(* What an ack PROMISES. task15: Consistent / ConsistentInline block the    *)
(* committing thread until the entry is fsynced, so an ack means durable.   *)
(* Eventual returns as soon as the snapshot is promoted, with the WAL entry *)
(* still only buffered (task15, "Eventual mode: snapshot promoted           *)
(* immediately, WAL fsynced asynchronously") -- an Eventual ack promises    *)
(* nothing about a crash, so clause (b) does not apply to it.               *)
DurableAck == Durability \in {"Consistent", "ConsistentInline"}

(***************************************************************************)
(* S1 RecoverySound -- the acked-write-loss property, clause by clause.     *)
(*                                                                         *)
(*  (a) the recovered state is the replay of a PREFIX of submission order:  *)
(*      no reordering, and no hole (recovering commit 2 without commit 1);  *)
(*  (b) it contains every Consistent / ConsistentInline acked commit -- the *)
(*      failure class this scout exists for: the store said "durable" and   *)
(*      it was not;                                                         *)
(*  (c) no partial commit: at this altitude a commit is one WAL frame, so   *)
(*      "partial" is "torn", and replaying a torn frame is replaying half a *)
(*      commit record. This is what M4 (replay past a torn frame) attacks;  *)
(*  (d) recovered versions are strictly monotone.                           *)
(*                                                                         *)
(* Scoped to a SUCCESSFUL recovery: when recover() returns Err there is no  *)
(* recovered state to predicate over, and folding that case in would turn a *)
(* safety claim into an availability one. The exclusion is deliberate --    *)
(* but the excluded case is NOT benign, and it has its own named invariant  *)
(* below: StrictScanErrLosesDurableAck.                                     *)
(***************************************************************************)
RecoverySound ==
    (recovered /\ ~recoverErr) =>
      /\ Len(promoted) <= Len(submitted)
      /\ \A i \in 1..Len(promoted) :
             /\ promoted[i].cid = submitted[i].cid
             /\ promoted[i].ver = submitted[i].ver
             \* The WAL entry names its table and Replay carries it through;
             \* without this a table mix-up during replay would pass.
             /\ promoted[i].tbl = submitted[i].tbl
      /\ DurableAck =>
             \A c \in acked : \E i \in 1..Len(promoted) : promoted[i].cid = c
      /\ \A i \in 1..Len(walAfterCrash) :
             (\E k \in 1..Len(promoted) : promoted[k].cid = walAfterCrash[i].cid)
                 => walAfterCrash[i].st = "present"
      /\ \A i \in 2..Len(promoted) : promoted[i].ver > promoted[i-1].ver

(***************************************************************************)
(* OWED PROPERTY -- expected RED, and checked so that it stays known.       *)
(*                                                                         *)
(* Under a STRICT scan (every sink except CoalescedPrealloc, which is every *)
(* sink except the opt-in one -- so this includes the DEFAULT Standalone    *)
(* configuration, Consistent + WalWrite::PerEntry), a single torn frame     *)
(* anywhere in the log costs the WHOLE log, including durable commits the   *)
(* scan had ALREADY ACCEPTED and whose commit() returned Ok under           *)
(* Consistent. src/wal.rs:589-591 returns Err(WalCorrupted); Store::recover *)
(* propagates it with `?` at src/store.rs:1023, before any entry is         *)
(* applied. A full-length-but-CRC-bad tail is physically ordinary on an     *)
(* appending sink, so this is not an exotic state.                          *)
(*                                                                         *)
(* This is NOT a RecoverySound clause: recover() returned Err, so there is  *)
(* no recovered state, and the loss is loud (the store refuses to open)     *)
(* rather than silent. It is an AVAILABILITY property about durably-acked   *)
(* data, and nobody has yet decided whether the behaviour should change.    *)
(* Until someone does, it is checked as a known gap: TLC must report it     *)
(* VIOLATED. A green here means either the strict error path stopped being  *)
(* reachable (the model rotted) or the behaviour changed (write the real    *)
(* property and delete this one).                                          *)
(***************************************************************************)
StrictScanErrLosesDurableAck ==
    ~(recovered /\ recoverErr /\ DurableAck /\ acked # {})

=============================================================================
