---------------------------- MODULE WalCrash ----------------------------
(***************************************************************************)
(* F-DB-2 scout, S1 / Tasks 1-3: UltimaDB's Standalone persistence          *)
(* pipeline -- the steady-state three-phase commit (Task 1), crash and       *)
(* recovery (Task 2), and the three production WAL sinks with the            *)
(* sync_data / sync_all barrier split (Task 3).                              *)
(*                                                                         *)
(* Ground truth: docs/tasks/task15_three_phase_consistent_persistence.md    *)
(* ("Three-phase commit protocol", "Why this is safe", "Promotion ordering  *)
(* (lost-update fix)"), the commit path in src/store.rs                     *)
(* (commit_single_writer ~3737, commit_multi_writer phase 3 ~3977), and for *)
(* Task 2 the recovery path Store::recover (src/store.rs:1037-1219) plus    *)
(* wal::scan_wal (src/wal.rs:677-726).                                      *)
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
(* detects its absence. That is what gives the M1-M7 mutations teeth.       *)
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
(*   Recover -- Store::recover (src/store.rs:1037): install the latest      *)
(*              checkpoint, scan_wal, replay the entries whose version      *)
(*              exceeds the checkpoint's. tail_tolerant is passed TRUE      *)
(*              exactly for CoalescedPrealloc (src/store.rs:1073-1078).     *)
(*                                                                         *)
(* CAUTION for later tasks -- the post-Recover value of `promoted` is the   *)
(* REPLAY SEQUENCE, not the Rust's snapshot chain. Recovery installs        *)
(* exactly ONE snapshot, at latest_version (src/store.rs:1206-1212);        *)
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
(* Task 3 adds the sink layer:                                              *)
(*                                                                         *)
(*   SyncData -- fdatasync. A barrier over frame DATA only. Reached by      *)
(*               exactly one sink: PreallocFileSink's steady state, where   *)
(*               the batch fits the already-zero-filled region so the file  *)
(*               size does not change (src/wal.rs:1255-1256).               *)
(*   SyncAll  -- fsync. A barrier over data AND metadata (the physical file *)
(*               size). Every append-mode sink uses it for every batch --   *)
(*               FileSink (src/wal.rs:1079-1083) and BufferedFileSink with  *)
(*               datasync=false, which is what WalSinkKind::Coalesced       *)
(*               selects (src/wal.rs:1133-1137) -- because an append ALWAYS *)
(*               changes the size. PreallocFileSink reaches it only as      *)
(*               preallocate_to's sync (src/wal.rs:665), i.e. only on an    *)
(*               extend.                                                    *)
(*   Extend   -- the batch overruns `capacity`, so grow by whole chunks of  *)
(*               physically-written zeros (src/wal.rs:1207-1228, 619-667).  *)
(*                                                                         *)
(* THE DISTINCTION THIS TASK EXISTS FOR. sync_data does not make a new file *)
(* SIZE durable. So bytes past the last sync_all-covered size can vanish    *)
(* WHOLESALE on a crash -- including frames a sync_data "covered", because  *)
(* the filesystem may still present the old size and those offsets simply   *)
(* are not there. That is why preallocate_to sync_all's BEFORE any record   *)
(* is written into the new region (src/wal.rs:665, and the ordering at      *)
(* src/wal.rs:1211 preceding the write at :1245) and why task37 §4 states   *)
(* it as a barrier-discipline invariant. `syncedCapacity` is that frontier; *)
(* SlotSafe is the crash rule; PreallocInvariant is task37 §4.              *)
(*                                                                         *)
(* At MUTATION = "NONE" the discipline holds by construction (SyncData is   *)
(* guarded on metaDurable), so `writeHead <= syncedCapacity` is an          *)
(* INVARIANT, not a reachable state. M5 (Task 5) is the mutation that drops *)
(* that guard; PreallocInvariant clause (2) is where it breaks, and         *)
(* RecoverySound is where the harm lands.                                   *)
(*                                                                         *)
(* Sink faithfulness notes, so the modelling choices are auditable:         *)
(*   - FsWrite and Coalesced STILL do not diverge, and now for a stated     *)
(*     reason: both are O_APPEND sinks that sync_all every batch and are    *)
(*     scanned strictly, so they agree on every variable this model has.    *)
(*     They differ in WHEN bytes reach the file -- FsWrite write_all's the  *)
(*     entry's framed bytes per entry at append (src/wal.rs:1074-1078),     *)
(*     Coalesced buffers them in a Vec and write_all's the whole batch      *)
(*     inside sync (src/wal.rs:1125, 1129-1132). That is invisible here     *)
(*     because the crash rule already gives EVERY unbarriered frame an      *)
(*     independent {present, torn, absent} outcome, which is a superset of  *)
(*     both "some subset of the per-entry writes landed" and "a byte prefix *)
(*     of one big write landed".                                            *)
(*   - FRAMING NOW HAPPENS AT COMMIT TIME (task57), not inside the sink.    *)
(*     WalHandle::write serializes the entry into a pooled buffer on the    *)
(*     COMMITTING thread (src/wal.rs:1858-1869) and hands a FramedEntry to  *)
(*     the sink; no sink calls frame_entry any more -- each one only moves  *)
(*     already-framed bytes (write_all / extend_from_slice). This does NOT  *)
(*     change append/sync VISIBILITY: bytes still reach the file only       *)
(*     through WalSink::append, and only a barrier makes them durable, so   *)
(*     Append keeping UNCHANGED <<walDurable, ...>> is still correct.       *)
(*     It DOES move serialization failure earlier: a framing error now      *)
(*     surfaces in WalHandle::write, on the committing thread, before       *)
(*     in_flight.fetch_add and before sender.send (src/wal.rs:1883-1887),   *)
(*     so a malformed entry aborts the commit with no WAL traffic at all.   *)
(*     The model has no serialization-failure action, so nothing it asserts *)
(*     changes -- but this removes a WAL-poison path that the model's       *)
(*     poison abstraction implicitly covers.                                *)
(*   - `capacity` / `writeHead` / `metaDurable` are PREALLOC-ONLY state.    *)
(*     FileSink and BufferedFileSink have no such fields (src/wal.rs:1001,  *)
(*     :1098); an O_APPEND file has no preallocated region. Under the other *)
(*     sinks they are pinned at their Init values and PreallocInvariant     *)
(*     checks that pinning, so the two Task 1-2 baselines keep their        *)
(*     original state counts exactly.                                       *)
(*   - ConsistentInline is NOT distinguished from Consistent here. It       *)
(*     changes WHO issues the fsync (the committing thread, off the store   *)
(*     lock) not WHAT the barrier covers (task38), and it is SingleWriter-  *)
(*     only, where at most one frame is ever buffered -- so the batch-      *)
(*     prefix nondeterminism it removes is already degenerate. Modelled as  *)
(*     equal, checked by running both.                                      *)
(*                                                                         *)
(* NOT YET modelled, arriving in later tasks of this plan:                  *)
(*   Fsync FAILURE (the "advance the gate without promoting" rule). Crash   *)
(*     did NOT give it a natural home: a crash removes a parked commit by   *)
(*     destroying the whole gate, whereas a failed fsync must remove ONE    *)
(*     ticket and let the rest of the FIFO proceed. That needs a per-ticket *)
(*     outcome on Fsync, not a crash. L1 (liveness) stays inexpressible     *)
(*     until then. S2 work; deliberately NOT added here.                    *)
(*   Checkpointing -- `checkpointVersion` is carried (Recover honours it as *)
(*     the replay floor, src/store.rs:1083-1086) but no action moves it off *)
(*     0, so no committed config exercises a non-zero floor. A Checkpoint   *)
(*     action also drags in WAL pruning (src/wal.rs:744 prune_wal), which   *)
(*     is where checkpoint/prune/crash interleavings would actually bite.   *)
(*     Out of scope for Task 2's brief; see the report.                     *)
(*   (MUTATION is no longer on this list: M1-M7 all exist and are gated in  *)
(*     mutations/. M1 = WriterSlotFree, M2 = GateApplies, M3 = the pre-fix  *)
(*     bump in Submit, M4 = ScanIsTolerant, M5 = SyncData's metaDurable       *)
(*     guard, M6 = ScanLen's stop-at-first-bad-frame, M7 = Replay's         *)
(*     per-position row identity. Each is ONE conjunct or one substitution, *)
(*     in the operator that carries the protection it removes.)             *)
(*   write_head RECONSTRUCTION on open (task37 §4 invariant 3,              *)
(*     src/wal.rs:1192-1193: PreallocFileSink::open runs a TOLERANT         *)
(*     scan_wal and takes `capacity` from metadata().len(); there is no     *)
(*     persisted head pointer). Crash sets writeHead to 0 -- the in-memory  *)
(*     cursor is gone -- and Recover leaves it there, because the bound is  *)
(*     "no operation after recovery", so nothing would consume a            *)
(*     reconstructed head. When S2 lifts that bound the reconstruction is   *)
(*     `writeHead' = ScanLen(walAfterCrash)` in Recover; it is deliberately *)
(*     absent rather than forgotten.                                       *)
(*   WAL PRUNE, including the preallocating prune (src/wal.rs:788-823,      *)
(*     task37 §6 strategy P2), which resets write_head/capacity from a      *)
(*     pre-sized tmp+rename. It is reachable only from Checkpoint, which    *)
(*     this model does not have (see above), so no action can move          *)
(*     writeHead backwards and `writeHead = Len(walDurable)` is currently a *)
(*     checked invariant rather than an assumption. A Checkpoint action     *)
(*     would break that clause, on purpose.                                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, TLC

CONSTANTS
    MaxCommits,     \* bound on commits per behaviour (<= 4)
    Tables,         \* set of table identities (<= 2)
    Durability,     \* "Consistent" | "ConsistentInline" | "Eventual"
    SinkKind,       \* "FsWrite" | "Coalesced" | "CoalescedPrealloc"
    WriterMode,     \* "SingleWriter" | "MultiWriter"
    ChunkSize,      \* prealloc grow quantum, in FRAME SLOTS (WAL_PREALLOC_CHUNK)
    MUTATION        \* "NONE" | "M1".."M7" -- calibration mutation selector

ASSUME MaxCommits \in Nat
ASSUME Durability \in {"Consistent", "ConsistentInline", "Eventual"}
ASSUME SinkKind   \in {"FsWrite", "Coalesced", "CoalescedPrealloc"}
ASSUME WriterMode \in {"SingleWriter", "MultiWriter"}
ASSUME ChunkSize  \in Nat /\ ChunkSize >= 1
ASSUME MUTATION   \in {"NONE", "M1", "M2", "M3", "M4", "M5", "M6", "M7"}

(* ConsistentInline is SingleWriter-ONLY: Store::new returns an error for   *)
(* the MultiWriter combination (task38, and CLAUDE.md §Persistence). This   *)
(* is mechanical rather than a comment because the combination is not       *)
(* merely uninteresting -- it is a store that cannot be constructed, so a   *)
(* config asserting it would model nothing and still come back exit 0,      *)
(* green over a configuration no user can reach.                            *)
ASSUME Durability = "ConsistentInline" => WriterMode = "SingleWriter"

Cids == 1..MaxCommits

(* Table identity is opaque -- no action or invariant inspects a table      *)
(* value, they are only compared for equality -- so TLC may quotient the    *)
(* state space by permutations of Tables. Used by the modes/ matrix; the    *)
(* Task 1-2 baselines deliberately do NOT declare it, so their committed    *)
(* state counts stay comparable across tasks.                               *)
TableSymmetry == Permutations(Tables)

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
    walAfterCrash,  \* the on-disk log the recovery scan reads
    \* ---- sink state (PreallocFileSink, src/wal.rs:1163-1170) -------------
    writeHead,      \* logical end of live records, in FRAME SLOTS
    capacity,       \* physically zero-filled file size, in frame slots
    syncedCapacity, \* the largest size a sync_all has covered -- the frontier
                    \* beyond which a crash may take bytes WHOLESALE
    metaDurable     \* is the CURRENT capacity sync_all-covered?

vars == <<walBuffered, walDurable, begun, submitted, parked, promoted,
          latestVersion, lastSubmitted, nextVersion, acked, crashed,
          recovered, recoverErr, checkpointVersion, walAfterCrash,
          writeHead, capacity, syncedCapacity, metaDurable>>

crashVars == <<crashed, recovered, recoverErr, checkpointVersion,
               walAfterCrash>>

sinkVars == <<writeHead, capacity, syncedCapacity, metaDurable>>

(* Frames are one SLOT each: the model has no byte-level framing (§4        *)
(* non-goal), so "offset" means "frame index" and ChunkSize is a count of   *)
(* frames rather than bytes. Nothing here depends on records being equal    *)
(* sized -- only on offsets being assigned in submission order and never    *)
(* reused, which is what src/wal.rs:1229-1246 does.                         *)
Prealloc == SinkKind = "CoalescedPrealloc"

(* src/wal.rs:1210 -- `need.div_ceil(chunk) * chunk`. *)
NewCap(need) == ((need + ChunkSize - 1) \div ChunkSize) * ChunkSize

(* `submitted` and `acked` are HISTORY variables: they record what happened *)
(* and what the caller was told, not implementation state, so they survive  *)
(* the crash. Everything else in `vars` is store state and is destroyed.    *)

Max2(a, b) == IF a > b THEN a ELSE b

(* Stores whose commits can never park skip the gate entirely and hold the  *)
(* write lock continuously from version assignment through promotion        *)
(* (StoreInner::commit_may_park, src/store.rs:437).                         *)
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
(* commit_multi_writer (src/store.rs:3945) has BOTH the version bump        *)
(* (:4130) and the PromoteGate FIFO (:4193 take, :4247/:4264 wait).         *)
(* commit_single_writer (src/store.rs:3838-3940) has NEITHER. Its only      *)
(* protection is holding the writer slot through the fsync wait             *)
(* (:3889-3905, and begin_write's active_writer_count check at              *)
(* src/store.rs:720).                                                       *)
(*                                                                         *)
(* Modelling the bump and the gate unconditionally would hand SingleWriter  *)
(* two protections the code does not have, and breaking its one real        *)
(* protection would then change nothing observable -- M1 would be masked.   *)
(***************************************************************************)
(* M2 re-creates the pre-fix code's SECOND failure mode: there was no       *)
(* PromoteGate at all, so a commit promoted whenever its own fsync wait     *)
(* finished -- COMPLETION order, not ticket order. Dropping GateApplies is  *)
(* exactly that: `Promote` may pick any parked k, not just the FIFO head.   *)
(* Per-table locks are why this was reachable in production and why it is   *)
(* modelled with no table reasoning at all: locks on DISJOINT tables never  *)
(* overlap, so writer B's phase 3 ran unimpeded while A sat parked. This    *)
(* model has no per-table locking, so every MultiWriter interleaving it     *)
(* explores is already the disjoint-table interleaving.                     *)
GateApplies == WriterMode = "MultiWriter" /\ MUTATION # "M2"
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
    \* A fresh wal.bin: PreallocFileSink::open scans an empty file, so
    \* write_head = 0, and capacity = metadata().len() = 0
    \* (src/wal.rs:1192-1193). Size 0 is trivially durable.
    /\ writeHead      = 0
    /\ capacity       = 0
    /\ syncedCapacity = 0
    /\ metaDurable    = TRUE

(* begin_write(None), src/store.rs:707: allocate the candidate commit       *)
(* version from next_version and keep next_version ahead of it.             *)
Begin(c, t) ==
    /\ ~crashed
    /\ c \notin UsedCids
    /\ WriterSlotFree
    /\ begun'       = begun \cup {[cid |-> c, ver |-> nextVersion, tbl |-> t]}
    /\ nextVersion' = nextVersion + 1
    /\ UNCHANGED <<walBuffered, walDurable, submitted, parked, promoted,
                   latestVersion, lastSubmitted, acked, crashVars, sinkVars>>

(* Phase 1 PREPARE (src/store.rs:4116 ff). Under the write lock: finalize   *)
(* the version against max(last_submitted, latest) allocating from          *)
(* next_version, submit the WAL entry (no fsync), take a ticket.            *)
(* Under Eventual / no-WAL the lock is never released, so phases 2-3        *)
(* collapse into this same step.                                            *)
Submit(r) ==
    /\ ~crashed
    /\ r \in begun
    \* M3 restores the pre-fix bump VERBATIM -- e60f8ce^ src/store.rs:2361-2366,
    \* inside commit_multi_writer (:2253); the fix is e60f8ce:

    \*     if !self.explicit_version && self.version <= inner.latest_version {
    \*         self.version = inner.latest_version + 1;
    \*         if self.version >= inner.next_version {
    \*             inner.next_version = self.version + 1;
    \*         }
    \*     }
    \* Both halves are the bug and both are needed to reproduce task15's
    \* failure mode 3. The COMPARISON against a lagging `latest_version` is
    \* what lets two writers bump at all; the ALLOCATION of `latest + 1` is
    \* what makes them collide, because `latest` did not move between the two
    \* submissions -- the earlier commit is parked in the fsync wait. Mutating
    \* only the comparison keeps allocation from `next_version`, which is
    \* unique by construction, so it breaks promotion ORDER but can never
    \* produce the documented duplicate `snapshots.insert(v, ..)`; measured,
    \* see the Task 4 report ("What M3-comparison-only does instead").
    /\ LET M3   == MUTATION = "M3"
           bump == /\ BumpApplies
                   /\ IF M3 THEN r.ver <= latestVersion
                            ELSE r.ver <= Max2(lastSubmitted, latestVersion)
           v    == IF ~bump THEN r.ver
                   ELSE IF M3 THEN latestVersion + 1
                   ELSE nextVersion
           e    == [cid |-> r.cid, ver |-> v, tbl |-> r.tbl]
       IN /\ nextVersion'   = IF ~bump THEN nextVersion
                              ELSE IF M3 THEN Max2(nextVersion, v + 1)
                              ELSE nextVersion + 1
          \* `last_submitted_version` is maintained only by commit_multi_writer
          \* (src/store.rs:4136). Kept unconditional here because it is read
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
    \* WalSink::append never advances the write head: FileSink write_all's the
    \* entry's already-framed bytes immediately (src/wal.rs:1074-1078) but has
    \* no head to advance; BufferedFileSink and PreallocFileSink only extend an
    \* in-memory Vec (src/wal.rs:1125, :1200). PreallocFileSink's write_head
    \* moves inside sync (src/wal.rs:1246), which is SyncData/SyncAll below.
    \* Since task57 the frame is built by WalHandle::write on the committing
    \* thread (src/wal.rs:1858-1869), so `append` no longer serializes anything
    \* -- but that changes only WHERE the bytes are produced, not when they
    \* become visible in the file, so this UNCHANGED clause is unaffected.
    /\ UNCHANGED <<walDurable, crashVars, sinkVars>>

----------------------------------------------------------------------------
(***************************************************************************)
(*                        THE SINK LAYER (Task 3)                           *)
(*                                                                         *)
(* The WAL background thread recv()s the first entry, drains the rest with  *)
(* try_recv(), then issues ONE sync for the whole batch (task15,            *)
(* "Background WAL writer"). A barrier therefore covers a non-empty PREFIX  *)
(* of the buffered log; the batch boundary is nondeterministic. Task 1-2    *)
(* had a single `Fsync` action for this; Task 3 splits it by WHAT the       *)
(* barrier covers, because that difference is the whole prealloc bug        *)
(* surface.                                                                 *)
(***************************************************************************)

(* The batch drain, common to both barriers. *)
FlushPrefix(n) ==
    /\ walDurable'  = walDurable \o SubSeq(walBuffered, 1, n)
    /\ walBuffered' = SubSeq(walBuffered, n + 1, Len(walBuffered))

FlushUnchanged ==
    UNCHANGED <<begun, submitted, parked, promoted, latestVersion,
                lastSubmitted, nextVersion, acked, crashVars>>

(***************************************************************************)
(* fdatasync -- a barrier over frame DATA within the already-durable file   *)
(* SIZE. src/wal.rs:1255-1256: "Steady-state barrier: size unchanged, so    *)
(* fdatasync suffices." Reached by PreallocFileSink and by nothing else in  *)
(* production: FileSink sync_all's (src/wal.rs:1079-1083) and the Coalesced *)
(* sink is BufferedFileSink with datasync = false (src/wal.rs:1133-1137;    *)
(* the datasync = true variant is WalSinkKind::BufferedFile, bench-only).   *)
(*                                                                         *)
(* metaDurable is the guard, and it is the ONE line that carries task37 §4  *)
(* invariant 2: the batch may only be written into a region whose SIZE is   *)
(* already sync_all-durable. In the Rust the ordering enforces it --        *)
(* preallocate_to (which ends in sync_all, src/wal.rs:665) runs at          *)
(* src/wal.rs:1211, strictly before the positioned write at :1245. Dropping *)
(* this conjunct is M5 -- an implementation that extends the file and then  *)
(* writes the batch into the new region under a bare sync_data, i.e. that   *)
(* skips preallocate_to's sync_all (or issues it after the write). The      *)
(* record is then "durable" by data barrier while the SIZE it needs is not, *)
(* which is task37 §4 invariant 2 ("new size must be durable before use")   *)
(* stated as its own failure.                                               *)
(***************************************************************************)
SyncData ==
    /\ ~crashed
    /\ Prealloc
    /\ (MUTATION # "M5") => metaDurable
    /\ walBuffered # <<>>
    /\ \E n \in 1..Len(walBuffered) :
         /\ writeHead + n <= capacity     \* the batch fits the zero region
         /\ FlushPrefix(n)
         /\ writeHead' = writeHead + n    \* src/wal.rs:1246
    /\ UNCHANGED <<capacity, syncedCapacity, metaDurable>>
    /\ FlushUnchanged

(***************************************************************************)
(* fsync -- a barrier over data AND metadata, so it makes the physical file *)
(* SIZE durable too. Two disjoint reasons a sink lands here:                *)
(*                                                                         *)
(*  - append-mode sinks (FsWrite, Coalesced) use it for EVERY batch,        *)
(*    because every append changes the size, so there is never a steady     *)
(*    state where sync_data would be sound;                                 *)
(*  - PreallocFileSink reaches it only via preallocate_to's sync_all        *)
(*    (src/wal.rs:665) -- i.e. only when an extend is outstanding. Hence    *)
(*    the `Prealloc => ~metaDurable` guard: a prealloc store that is        *)
(*    already meta-durable does NOT fsync, it fdatasyncs. Without that      *)
(*    guard the model would let prealloc buy sync_all's protection for      *)
(*    free and M5 would have nothing to attack.                             *)
(*                                                                         *)
(* Collapsing preallocate_to's sync_all and the batch's trailing sync_data  *)
(* into this one action is deliberate: their combined post-state is         *)
(* identical (batch durable, size durable), and the intermediate crash      *)
(* points are already covered, because a crash before SyncAll leaves the    *)
(* batch buffered and Crash gives every buffered frame an independent       *)
(* outcome.                                                                 *)
(***************************************************************************)
SyncAll ==
    /\ ~crashed
    /\ Prealloc => ~metaDurable
    /\ walBuffered # <<>>
    /\ \E n \in 1..Len(walBuffered) :
         /\ Prealloc => writeHead + n <= capacity
         /\ FlushPrefix(n)
         /\ writeHead' = IF Prealloc THEN writeHead + n ELSE writeHead
    /\ capacity'       = capacity
    /\ syncedCapacity' = capacity      \* the physical size is now durable
    /\ metaDurable'    = TRUE
    /\ FlushUnchanged

(***************************************************************************)
(* The batch overruns the zero-filled region, so grow by whole chunks       *)
(* (src/wal.rs:1207-1228). preallocate_to writes REAL zeros rather than a   *)
(* sparse set_len, so `capacity` is a physically written size, not just a   *)
(* stat() length -- which is why the model needs no separate physical_len   *)
(* variable and task37 §4's `capacity <= physical_len` is an equality here  *)
(* (src/wal.rs:619-623, 633-657).                                           *)
(*                                                                         *)
(* metaDurable goes FALSE, and the ONLY action enabled next for this batch  *)
(* is SyncAll -- which is exactly the Rust's ordering, expressed as a       *)
(* reachability fact rather than assumed. `n` ranges over batch sizes       *)
(* because the batch boundary is chosen by the drain, before the extend is  *)
(* sized to it.                                                             *)
(***************************************************************************)
Extend ==
    /\ ~crashed
    /\ Prealloc
    /\ walBuffered # <<>>
    /\ \E n \in 1..Len(walBuffered) :
         /\ writeHead + n > capacity
         /\ capacity' = NewCap(writeHead + n)
    /\ metaDurable' = FALSE
    /\ UNCHANGED <<writeHead, syncedCapacity>>
    /\ UNCHANGED <<walBuffered, walDurable>>
    /\ FlushUnchanged

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
                   nextVersion, crashVars, sinkVars>>

----------------------------------------------------------------------------
(***************************************************************************)
(*                       CRASH AND RECOVERY (Task 2)                        *)
(***************************************************************************)

(* A frame as the RECOVERY SCAN sees it after a crash. `st` is what became  *)
(* of the record at that OFFSET -- a flag, not bytes; the model has no      *)
(* byte-level framing (§4 non-goal).                                        *)
(*   "present" -- the whole record landed and its CRC verifies;             *)
(*   "torn"    -- the record is there but the CRC does not verify           *)
(*                (src/wal.rs:701);                                         *)
(*   "absent"  -- nothing landed at this offset: a zero len-prefix or a     *)
(*                short tail (src/wal.rs:692-697).                          *)
FrameSt   == {"present", "torn", "absent"}
CrashFrame == [cid : Cids, ver : Nat, tbl : Tables, st : FrameSt]

(***************************************************************************)
(* THE WHOLESALE-LOSS FRONTIER (Task 3).                                    *)
(*                                                                         *)
(* A frame occupies slot i-1, i.e. it lies wholly inside a file of size i.  *)
(* It can only survive a crash if the filesystem is guaranteed to still     *)
(* present a file at least that long -- which is what sync_all establishes  *)
(* and sync_data does NOT. So under the prealloc sink the surviving log is  *)
(* truncated at `syncedCapacity`.                                           *)
(*                                                                         *)
(* WHAT THE CUT ACTUALLY DOES AT MUTATION = "NONE", which is the only       *)
(* behaviour any committed config has. It removes exactly the merely-       *)
(* buffered tail after an extend whose sync_all has not landed -- and those *)
(* frames are still sitting in PreallocFileSink::buf (src/wal.rs:1200),     *)
(* never written to the file at all, so calling them absent is not a        *)
(* durability claim, it is arithmetic: a file of size `syncedCapacity` has  *)
(* no slot for them. It never removes a DURABLE frame, because              *)
(* PreallocInvariant proves writeHead <= syncedCapacity, so every flushed   *)
(* frame is inside the frontier.                                            *)
(*                                                                         *)
(* WHY IT IS STATED OVER walDurable ANYWAY, rather than only over the       *)
(* buffered tail: the interesting case is the one this model must be ABLE   *)
(* to express, not the one it currently reaches. A frame an fdatasync       *)
(* "covered" (src/wal.rs:1256) still vanishes if it sits past the last      *)
(* sync_all-covered size, because the size it needs was never made durable. *)
(* That is the M5 surface. It is unreachable here precisely because         *)
(* preallocate_to sync_all's before returning (src/wal.rs:665) and SyncData *)
(* is guarded on metaDurable -- drop that guard and this clause is what     *)
(* turns the bug into lost acked data rather than a silent no-op.           *)
(*                                                                         *)
(* Append-mode sinks have no frontier: sync_all is their only barrier, so   *)
(* every barriered frame's size is durable by construction.                 *)
(***************************************************************************)
SlotSafe(i) == ~Prealloc \/ i <= syncedCapacity

(* The on-disk log a crash leaves behind, position by position.             *)
(*                                                                         *)
(* POSITIONAL, deliberately: an absent frame is a HOLE at its offset, not a *)
(* removal that slides later frames forward. Every sink assigns a frame its *)
(* byte offset at append time, in submission order (PreallocFileSink seeks  *)
(* to its own write_head, src/wal.rs:1229), and scan_wal walks offsets      *)
(* strictly in order, breaking at the first record it cannot accept         *)
(* (src/wal.rs:690-723) -- there is no skip-and-continue branch. So a frame *)
(* that never landed makes every LATER frame unreachable to recovery,       *)
(* whether or not its own bytes reached the platter. Modelling absence as   *)
(* compaction instead (the shape Task 2's brief sketches) manufactures a    *)
(* log with a hole at offset 0, which no filesystem produces; it violates   *)
(* RecoverySound (a) under MultiWriter as a pure model artifact. See the    *)
(* Task 2 report, "Adjudication".                                           *)
(*                                                                         *)
(* Barriered frames survive intact; each unbarriered (write-back cached)    *)
(* frame independently landed whole, landed torn, or never landed. Both are *)
(* then cut at the frontier.                                                *)
CrashLog(dur, buf, outcome) ==
    [i \in 1..(Len(dur) + Len(buf)) |->
        LET fr == IF i <= Len(dur) THEN dur[i] ELSE buf[i - Len(dur)]
        IN [cid |-> fr.cid, ver |-> fr.ver, tbl |-> fr.tbl,
            st  |-> IF ~SlotSafe(i)      THEN "absent"
                    ELSE IF i <= Len(dur) THEN "present"
                    ELSE outcome[i - Len(dur)]]]

(* Store::recover passes tail_tolerant = TRUE exactly for CoalescedPrealloc *)
(* (src/store.rs:1073-1078).                                                *)
(*                                                                         *)
(* NAMED ScanIsTolerant, not TailTolerant, and the rename is defensive: the *)
(* PROPERTY below is TailTolerance, and a one-character difference between  *)
(* a model function and a checked property -- both in scope, both about the *)
(* same thing -- is an edit waiting to hit the wrong one. Editing this      *)
(* operator changes the M4 GATE; editing that one changes what is CHECKED.  *)
(*                                                                         *)
(* M4 is the pre-task37 rule applied to a prealloc store: scan STRICTLY,    *)
(* i.e. `CRC mismatch -> WalCorrupted` with no tail exemption. task37 §7    *)
(* states exactly why that is wrong here -- "preallocation means a          *)
(* partially-written record sits in front of durable zeros. It LOOKS        *)
(* complete (len + data + CRC bytes are all present, backed by the zero     *)
(* fill) but its CRC fails. Under the existing rule this would abort        *)
(* recovery for a torn tail that is actually harmless." The mutation is one *)
(* conjunct on the sink-selection predicate and touches nothing else: it is *)
(* the `wal_write == CoalescedPrealloc` arm of the tolerance selection at   *)
(* src/store.rs:1073-1078 going away, which is precisely the state the      *)
(* codebase was in before scan_wal grew its `tail_tolerant` parameter.      *)
ScanIsTolerant(sk) == sk = "CoalescedPrealloc" /\ MUTATION # "M4"

(* scan_wal walks frames at increasing offsets and STOPS at the first frame *)
(* it cannot accept (src/wal.rs:690-723). ScanLen is the length of the      *)
(* longest accepted prefix.                                                 *)
(*                                                                         *)
(* M6 -- the CALIBRATION for RecoverySound clause (c). The real scan's stop *)
(* at a CRC-bad frame is src/wal.rs:701-708: `break` when tail_tolerant,    *)
(* `return Err(WalCorrupted)` when not -- either way the frame and          *)
(* everything past it is NOT replayed, which is the whole of clause (c)'s   *)
(* content. M6 removes that stop and keeps ONLY the end-of-log stop (a zero *)
(* len-prefix or a short tail, src/wal.rs:692-697), so a torn frame is      *)
(* accepted into the prefix and replayed as though its CRC had matched --   *)
(* the corruption-passes-CRC failure the `break`/`return Err` exists to     *)
(* prevent. Note what this also does by construction: with no torn frame    *)
(* ever ending the scan, ScanFails below can no longer fire, so the strict  *)
(* abort disappears too. That is correct for the mutation -- it is the ONE  *)
(* stop being deleted, in both of its arms -- and it is why M6 is carried   *)
(* on a TOLERANT (prealloc) config, where that arm was never taken anyway   *)
(* and the only observable change is the replayed tear.                     *)
(*                                                                         *)
(* At every other MUTATION value the ELSE branch is the original expression *)
(* verbatim, so this is an identity there.                                  *)
ScanLen(log) ==
    IF MUTATION = "M6"
      THEN CHOOSE n \in 0..Len(log) :
               /\ \A i \in 1..n : log[i].st # "absent"
               /\ (n < Len(log)) => log[n+1].st = "absent"
      ELSE CHOOSE n \in 0..Len(log) :
               /\ \A i \in 1..n : log[i].st = "present"
               /\ (n < Len(log)) => log[n+1].st # "present"

(* WHICH stop it is decides whether recovery is silent or loud.             *)
(* An absent record -- zero len-prefix or short tail -- is a clean          *)
(* end-of-log in BOTH modes (src/wal.rs:692-697, unconditional `break`).    *)
(* A TORN record is end-of-log only when tail_tolerant (src/wal.rs:702-704);*)
(* strict mode returns Error::WalCorrupted (:705-707), which Store::recover *)
(* propagates with `?` (src/store.rs:1079) -- so NOTHING is replayed, not   *)
(* even the frames the scan had already accepted.                           *)
ScanFails(log, tolerant) ==
    /\ ~tolerant
    /\ ScanLen(log) < Len(log)
    /\ log[ScanLen(log) + 1].st = "torn"

(* The replay: the accepted prefix, filtered to entries whose version       *)
(* exceeds the checkpoint floor (src/store.rs:1083-1086), applied in order. *)
(* Each replayed entry is built on the state left by its predecessor, hence *)
(* the forkedFrom chain; `sub` is looked up in the submission history so    *)
(* that "replay order = submission order" stays a CHECKED property rather   *)
(* than one the construction assumes.                                       *)
(*                                                                         *)
(* M7 -- the CALIBRATION for RecoverySound clause (a). Clause (a) is the    *)
(* claim that the RECOVERED chain is the replay of a PREFIX OF SUBMISSION   *)
(* ORDER, matched position by position on cid, version AND table. The real  *)
(* replay gets that identity from the frame itself: scan_wal hands back     *)
(* records in offset order and Store::recover applies each one to the table *)
(* its own entry names (src/store.rs:1083-1086 -> the per-entry apply),     *)
(* so position i of the recovered chain carries submission i's row.         *)
(*                                                                         *)
(* M7 SWAPS the (cid, tbl) identity of chain positions 1 and 2 and CHANGES  *)
(* NOTHING ELSE: `ver`, `sub` and `forkedFrom` stay exactly as the correct  *)
(* Replay computed them, at the positions it computed them for. That is a   *)
(* replay that applies commit 2's row where commit 1's belongs -- the       *)
(* mis-ordered / mis-tabled apply clause (a) exists to forbid -- while the  *)
(* version sequence stays strictly monotone and the fork chain stays intact *)
(* underneath it. It is deliberately NOT an order reversal: reversing the   *)
(* replayed list drags the VERSIONS down with it, which reddens             *)
(* PromoteOrderIsSubmitOrder, ForkFromPromotePredecessor and clause (d) at  *)
(* the same depth, so the red would be as much about the neighbouring       *)
(* properties as about this clause. Permuting identity alone leaves every   *)
(* one of them green and puts clause (a) on its own.                        *)
(*                                                                         *)
(* Positions 1 and 2 rather than "some two": TLC evaluates this on every    *)
(* Recover, and the shallowest violating behaviour has a two-entry chain    *)
(* anyway. The guard is Len >= 2, so on a 0- or 1-entry chain M7 is the     *)
(* identity -- a permutation of one element is not a permutation.           *)
(*                                                                         *)
(* At every other MUTATION value the result is `chain` unchanged, which is  *)
(* the original expression verbatim, so this is an identity there.          *)
Replay(log, cpVer, tolerant) ==
    LET accepted == SubSeq(log, 1, ScanLen(log))
        live     == SelectSeq(accepted, LAMBDA fr : fr.ver > cpVer)
        chain    == [i \in 1..Len(live) |->
                        [cid        |-> live[i].cid,
                         ver        |-> live[i].ver,
                         tbl        |-> live[i].tbl,
                         sub        |-> SubIndex(live[i].cid),
                         forkedFrom |-> IF i = 1 THEN cpVer ELSE live[i-1].ver]]
    IN IF MUTATION = "M7" /\ Len(chain) >= 2
         THEN [i \in 1..Len(chain) |->
                  CASE i = 1 -> [chain[1] EXCEPT !.cid = chain[2].cid,
                                                 !.tbl = chain[2].tbl]
                    [] i = 2 -> [chain[2] EXCEPT !.cid = chain[1].cid,
                                                 !.tbl = chain[1].tbl]
                    [] OTHER -> chain[i]]
         ELSE chain

(* Every buffered frame independently survives, tears, or vanishes; durable *)
(* frames always survive. All volatile store state resets -- the process is *)
(* gone. `submitted` and `acked` are history and survive by construction:   *)
(* `acked` records what the CALLER was told, and RecoverySound exists to    *)
(* compare that against what actually came back.                            *)
Crash ==
    /\ ~crashed
    /\ \E outcome \in [1..Len(walBuffered) -> {"absent", "torn", "present"}] :
         walAfterCrash' = CrashLog(walDurable, walBuffered, outcome)
    /\ crashed'       = TRUE
    /\ walBuffered'   = <<>>    \* the write-back cache is gone
    /\ walDurable'    = <<>>    \* the log now lives only in walAfterCrash
    /\ begun'         = {}
    /\ parked'        = <<>>    \* parked commits are volatile -- their acks are not
    /\ promoted'      = <<>>    \* the snapshot chain was in memory
    /\ latestVersion' = 0
    /\ lastSubmitted' = 0
    /\ nextVersion'   = 1
    \* write_head is an in-memory cursor and dies with the process; it is
    \* rebuilt by scanning, never read from disk (task37 §4 invariant 3,
    \* src/wal.rs:1192). capacity / syncedCapacity / metaDurable describe
    \* the FILE, which survives, so they are carried -- that is what makes
    \* "was an extend un-synced at the moment of the crash?" observable
    \* after the fact. The model does not resolve what the post-crash
    \* physical size actually became (capacity or syncedCapacity): nothing
    \* reads it, because no operation follows recovery, and the only
    \* load-bearing quantity is the frontier, which SlotSafe already used.
    /\ writeHead'     = 0
    /\ UNCHANGED <<capacity, syncedCapacity, metaDurable>>
    /\ UNCHANGED <<submitted, acked, recovered, recoverErr, checkpointVersion>>

(* Store::recover (src/store.rs:1037): install the latest checkpoint, then  *)
(* in Standalone mode scan the WAL and replay past the checkpoint version.  *)
(* On a strict-mode scan error the checkpoint has ALREADY been installed    *)
(* (src/store.rs:1058-1062 runs before the scan at :1079) but recover()     *)
(* returns Err and no WAL entry is applied.                                 *)
Recover ==
    /\ crashed /\ ~recovered
    /\ recovered' = TRUE
    /\ LET tolerant == ScanIsTolerant(SinkKind)
           chain    == Replay(walAfterCrash, checkpointVersion, tolerant)
       IN IF ScanFails(walAfterCrash, tolerant)
            THEN /\ recoverErr'    = TRUE
                 /\ promoted'      = <<>>
                 /\ latestVersion' = checkpointVersion
            ELSE /\ recoverErr'    = FALSE
                 /\ promoted'      = chain
                 \* src/store.rs:1202-1213: latest_version is the version of
                 \* the last entry replayed, or the checkpoint's if none.
                 /\ latestVersion' = IF chain = <<>>
                                       THEN checkpointVersion
                                       ELSE chain[Len(chain)].ver
    /\ UNCHANGED <<walBuffered, walDurable, begun, submitted, parked,
                   lastSubmitted, nextVersion, acked, crashed,
                   checkpointVersion, walAfterCrash, sinkVars>>

----------------------------------------------------------------------------

Next ==
    \/ \E c \in Cids, t \in Tables : Begin(c, t)
    \/ \E r \in begun : Submit(r)
    \/ SyncData
    \/ SyncAll
    \/ Extend
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
    /\ writeHead      \in Nat
    /\ capacity       \in Nat
    /\ syncedCapacity \in Nat
    /\ metaDurable    \in BOOLEAN

(***************************************************************************)
(* PreallocInvariant -- task37 §4's invariants, stated separately so a      *)
(* model bug in the sink layer surfaces HERE rather than as a confusing     *)
(* downstream RecoverySound trace.                                          *)
(*                                                                         *)
(*  (1) write_head <= capacity <= physical_len. The upper half is an        *)
(*      EQUALITY in this model and needs no variable: preallocate_to writes *)
(*      real zeros rather than a sparse set_len (src/wal.rs:619-623), so    *)
(*      capacity IS the physically written length.                          *)
(*  (2) barrier discipline: a record is only written into a region whose    *)
(*      SIZE is already sync_all-durable. This is the clause M5 breaks.     *)
(*  (3) write_head is bookkeeping over the log, not an independent          *)
(*      pointer -- there is no persisted head to drift (§4 invariant 3).    *)
(*      Currently `= Len(walDurable)`; a Checkpoint/prune action would      *)
(*      break this clause on purpose (see the header).                      *)
(*  (4) the frontier is well-formed and metaDurable is not free-floating    *)
(*      bookkeeping but exactly "the current size is covered".              *)
(*  (5) the non-prealloc sinks own none of this state. FileSink and         *)
(*      BufferedFileSink have no write_head/capacity fields                 *)
(*      (src/wal.rs:1001-1004, :1098-1105); an O_APPEND file has no         *)
(*      preallocated region. Pinning them is what keeps the Task 1-2        *)
(*      baselines' state counts unchanged, so this clause is also the       *)
(*      regression guard for that.                                          *)
(*                                                                         *)
(* NOTE on the brief's canary shape. `NoExtendBeforeSync == ~(writeHead >   *)
(* capacity)` is clause (1); it HOLDS in any faithful encoding, because an  *)
(* extend runs before the write it is sized for. Asserting a real invariant *)
(* as a must-go-red canary inverts the signal -- it reports failure exactly *)
(* when the model is correct. It is an invariant here; the reachability     *)
(* canary is NoPreallocExtend below. See the Task 3 report, "Adjudication". *)
(***************************************************************************)
PreallocInvariant ==
    /\ syncedCapacity <= capacity                        \* (4)
    /\ metaDurable <=> (syncedCapacity = capacity)       \* (4)
    /\ Prealloc =>
         /\ writeHead <= capacity                        \* (1)
         /\ writeHead <= syncedCapacity                  \* (2)  <-- M5
         /\ writeHead = Len(walDurable)                  \* (3)
         /\ capacity % ChunkSize = 0                     \* grows by chunks
    /\ ~Prealloc =>
         /\ writeHead      = 0                           \* (5)
         /\ capacity       = 0
         /\ syncedCapacity = 0
         /\ metaDurable

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
(* The four clauses are named so a calibration mutation can be checked      *)
(* against ONE of them in isolation. TLC reports only "PromotionFaithful    *)
(* is violated" and stops at the SHALLOWEST counterexample, so a conjunction*)
(* hides which clause broke -- and the shallowest break is not necessarily  *)
(* the historical bug's documented symptom. M3 is exactly that case: it     *)
(* breaks LatestStrictlyAdvances at depth 7 but task15 mode 3's symptom is  *)
(* the duplicate insert, which needs a deeper trace (Task 4 report,         *)
(* "M3 and the MaxCommits bound"). Naming the clauses is a decomposition,   *)
(* not a change: PromotionFaithful is still their conjunction in the same   *)
(* order, so every committed config's result is bit-identical.              *)
PromoteOrderIsSubmitOrder ==
    \A i \in 1..Len(promoted) : promoted[i].sub = i

LatestStrictlyAdvances ==
    \A i \in 2..Len(promoted) : promoted[i].ver > promoted[i-1].ver

ForkFromPromotePredecessor ==
    \A i \in 1..Len(promoted) :
        promoted[i].forkedFrom = IF i = 1 THEN 0 ELSE promoted[i-1].ver

NoDuplicateVersion ==
    \A i, j \in 1..Len(promoted) : (i # j) => promoted[i].ver # promoted[j].ver

(* NoDuplicateVersion restricted to the LIVE snapshot chain. After a crash  *)
(* `promoted` is the REPLAY sequence, not the Rust's snapshot chain (see    *)
(* the header note on `promoted`), so a duplicate found there is a WAL      *)
(* duplicate -- real, but a different statement from task15 mode 3's        *)
(* "the second snapshots.insert(v, ..) silently replaced the first". This   *)
(* is the version that pins the documented symptom, and it is what          *)
(* mutations/M3Dup.cfg checks. `crashed` is monotone, so this switches off  *)
(* for the whole post-crash suffix rather than just the Recover step.       *)
NoDupLive == crashed \/ NoDuplicateVersion

PromotionFaithful ==
    /\ PromoteOrderIsSubmitOrder
    /\ LatestStrictlyAdvances
    /\ ForkFromPromotePredecessor
    /\ NoDuplicateVersion

(***************************************************************************)
(* CRASH-REACHABILITY CANARY (Task 2). Must be VIOLATED. If it HOLDS, no    *)
(* behaviour ever crashes and then recovers, and RecoverySound is being     *)
(* checked over zero crash behaviours -- a green with no content.           *)
(***************************************************************************)
NoCrashThenRecover == ~(crashed /\ recovered)

(***************************************************************************)
(* PREALLOC-EXTEND REACHABILITY CANARY (Task 3). Must be VIOLATED under any *)
(* CoalescedPrealloc config. If it HOLDS, no behaviour ever grows the file, *)
(* so Extend is dead, sync_all is never reached by the prealloc sink,       *)
(* metaDurable is constantly TRUE, and the whole barrier-discipline half of *)
(* PreallocInvariant is green over unreachable code -- which is precisely   *)
(* the state M5 needs to exist in order to break. Meaningless (trivially    *)
(* true) under the non-prealloc sinks; only ever paired with a prealloc     *)
(* config.                                                                  *)
(*                                                                         *)
(* capacity = 0 rather than the brief's ~(writeHead > capacity): see the    *)
(* note on PreallocInvariant.                                               *)
(***************************************************************************)
NoPreallocExtend == Prealloc => (capacity = 0)

(***************************************************************************)
(* EXTEND-FROM-A-LIVE-LOG CANARIES (Task 3, fix round 1). Must be VIOLATED  *)
(* -- but only at MaxCommits >= 3, and that is the whole point of them.     *)
(*                                                                         *)
(* NoPreallocExtend above proves an extend HAPPENS. It does not prove the   *)
(* PRODUCTION shape happens. At MaxCommits = 2 there is exactly one Extend  *)
(* per behaviour and it always fires from capacity = 0 on an empty log, so: *)
(*   - the steady-state -> extend transition (records already durable, the  *)
(*     next batch overruns the region) never occurs;                        *)
(*   - src/wal.rs:1210's need.div_ceil(chunk)*chunk is only ever evaluated  *)
(*     at need \in 1..2, never across a chunk boundary;                     *)
(*   - preallocate_to's `from != 0` case (src/wal.rs:628-667, zero-filling  *)
(*     a SUFFIX of an existing file rather than the whole of a new one) is  *)
(*     not modelled at all.                                                 *)
(* Both assertions come back exit 0 at MaxCommits = 2 and exit 12 at 3.     *)
(*                                                                         *)
(* This matters for M5 specifically, and Task 5 MEASURED it, which corrects *)
(* what this comment said when Task 3 wrote it. The claim was that at       *)
(* MaxCommits = 2 "the only frames the frontier can strand are the first    *)
(* batch's, which were never acked". That is wrong: at bound 2, M5 violates *)
(* RecoverySound at depth 8 with acked = {1} -- the stranded frame IS acked *)
(* (Consistent acks on promotion, and promotion only needs the frame to be  *)
(* in walDurable, which the mutated SyncData put it in). What bound 2 does  *)
(* lack is the LOG UNDERNEATH the loss: its one Extend fires from           *)
(* capacity = 0 on a fresh file, so the counterexample is the first batch   *)
(* after open, not the steady-state -> extend transition a running store    *)
(* lives on. Measured: NoAckLossAfterLiveExtend is exit 0 at MaxCommits = 2 *)
(* under M5 and exit 12 at 3 (mutations/M5Strand.cfg). So bound 3 is        *)
(* REQUIRED for that symptom, not a margin -- same relationship M3Dup.cfg   *)
(* has to its bound. modes/ConsistentPrealloc3.cfg is its clean control.    *)
(***************************************************************************)
NoExtendFromLiveLog == ~(Prealloc /\ ~metaDurable /\ writeHead > 0)
NoSecondChunk       == ~(Prealloc /\ capacity > ChunkSize)

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
(*      no reordering, and no hole (recovering commit 2 without commit 1),  *)
(*      matched position by position on cid, version AND table. Do not      *)
(*      assume M1/M2 cover it -- run alone this clause is clean under ALL   *)
(*      of M1-M6, because M1/M2's ordering break lands on                   *)
(*      PromotionFaithful, a claim about the LIVE promotion chain, not      *)
(*      about the recovered prefix. M7 closes it: mutations/M7.cfg swaps    *)
(*      the (cid, tbl) identity of two positions of the replayed chain      *)
(*      while leaving ver/sub/forkedFrom as Replay computed them, and this  *)
(*      clause -- and only this clause -- goes red at depth 9. See the      *)
(*      Task 5c report.                                                     *)
(*  (b) it contains every Consistent / ConsistentInline acked commit -- the *)
(*      failure class this scout exists for: the store said "durable" and   *)
(*      it was not;                                                         *)
(*  (c) no partial commit: at this altitude a commit is one WAL frame, so   *)
(*      "partial" is "torn", and replaying a torn frame is replaying half a *)
(*      commit record. (Task 3's header claimed M4 attacks this clause. It  *)
(*      does not: M4 is task37 §7's OTHER direction -- a strict scan that   *)
(*      refuses the whole log rather than one that replays past the tear,   *)
(*      and nothing in M1-M5 replayed a torn frame either, which left this  *)
(*      clause checked but UNCALIBRATED through Task 5. M6 closes it:       *)
(*      mutations/M6.cfg deletes ScanLen's stop at a CRC-bad frame and this *)
(*      clause -- and only this clause -- goes red at depth 9, with the     *)
(*      torn frame standing in the recovered `promoted` chain. See the      *)
(*      Task 5b report.)                                                    *)
(*  (d) recovered versions are strictly monotone.                           *)
(*                                                                         *)
(* Scoped to a SUCCESSFUL recovery: when recover() returns Err there is no  *)
(* recovered state to predicate over, and folding that case in would turn a *)
(* safety claim into an availability one. The exclusion is deliberate --    *)
(* but the excluded case is NOT benign, and it has its own named invariant  *)
(* below: StrictScanErrLosesDurableAck.                                     *)
(***************************************************************************)
(* Clauses (a), (c) and (d) -- everything that does NOT depend on what an   *)
(* ack promised. Factored out because it is exactly what Eventual still     *)
(* owes (EventualHonest below).                                             *)
RecoveredPrefixSound ==
    (recovered /\ ~recoverErr) =>
      /\ Len(promoted) <= Len(submitted)
      /\ \A i \in 1..Len(promoted) :
             /\ promoted[i].cid = submitted[i].cid
             /\ promoted[i].ver = submitted[i].ver
             \* The WAL entry names its table and Replay carries it through;
             \* without this a table mix-up during replay would pass.
             /\ promoted[i].tbl = submitted[i].tbl
      /\ \A i \in 1..Len(walAfterCrash) :
             (\E k \in 1..Len(promoted) : promoted[k].cid = walAfterCrash[i].cid)
                 => walAfterCrash[i].st = "present"
      /\ \A i \in 2..Len(promoted) : promoted[i].ver > promoted[i-1].ver

RecoverySound ==
    /\ RecoveredPrefixSound
    /\ (recovered /\ ~recoverErr /\ DurableAck) =>
           \A c \in acked : \E i \in 1..Len(promoted) : promoted[i].cid = c

(***************************************************************************)
(* S4 EventualHonest -- what Durability::Eventual still owes.               *)
(*                                                                         *)
(* Under Eventual, commit() returns as soon as the snapshot is promoted,    *)
(* with the WAL entry still only buffered, so LOSING AN ACKED COMMIT IS     *)
(* PERMITTED -- that is the documented bargain, not a bug. What is NOT      *)
(* permitted is the store lying about the SHAPE of what survived: the       *)
(* recovered state must still be a prefix of submission order, with no      *)
(* reordering, no hole, no replayed torn frame, no version regression. An   *)
(* Eventual store may lose your last commits; it may not resurrect them out *)
(* of order or half-applied.                                                *)
(*                                                                         *)
(* IT HAS NO INDEPENDENT DETECTION POWER, and that is worth saying out      *)
(* loud rather than leaving for someone to discover. RecoverySound is       *)
(* defined as RecoveredPrefixSound /\ (the acked clause), so RecoverySound  *)
(* IMPLIES EventualHonest unconditionally, and both run on the same config: *)
(* no state can ever violate this one without violating that one first.     *)
(* It is a READABILITY ALIAS. RecoverySound reads as "nothing acked is      *)
(* lost", which is FALSE under Eventual, and a reader who does not notice   *)
(* the DurableAck gate would take the Eventual config's green for a promise *)
(* the store does not make; this names the promise it does make.            *)
(*                                                                         *)
(* What actually gives the Eventual config teeth is the canary PAIR --      *)
(* modes/EventualFsWriteLossCanary.cfg (NoEventualAckLoss must go RED) and  *)
(* modes/ConsistentAckKeptCheck.cfg (the same invariant, one constant       *)
(* changed, must come back CLEAN). Do not mistake this alias for that pair. *)
(***************************************************************************)
EventualHonest == (Durability = "Eventual") => RecoveredPrefixSound

(***************************************************************************)
(* EVENTUAL-LOSS REACHABILITY CANARY (Task 3). Must be VIOLATED under the   *)
(* Eventual config -- and must HOLD under every Consistent /                *)
(* ConsistentInline one, where it is RecoverySound's acked-containment      *)
(* clause restated, so it doubles as a cross-check that the two durability  *)
(* tiers really do differ in this model.                                    *)
(*                                                                          *)
(* Without it, EventualHonest could be green over behaviours in which       *)
(* nothing acked was ever lost -- i.e. green without ever visiting the case *)
(* that distinguishes Eventual from Consistent at all.                      *)
(***************************************************************************)
NoEventualAckLoss ==
    ~(recovered /\ ~recoverErr
      /\ \E c \in acked : \A i \in 1..Len(promoted) : promoted[i].cid # c)

(***************************************************************************)
(* RecoverySound clause (b), restricted to a loss that happens with a LIVE  *)
(* LOG UNDERNEATH IT. This is the M5 counterpart of NoDupLive: the bare     *)
(* clause is violated at every bound, but its shallowest counterexample is  *)
(* the FRESH-FILE extend (capacity 0 -> one chunk, preallocate_to's from=0  *)
(* branch), which is the case a store meets once, on the first batch after  *)
(* open. task37 §4 invariant 2 governs every extend, and the one a running  *)
(* store actually spends its life on is the steady-state -> extend          *)
(* transition: records already durable, their size already sync_all-covered *)
(* (src/wal.rs:628-667 with from != 0). `syncedCapacity >= ChunkSize` is    *)
(* exactly that precondition -- a SyncAll has already covered at least one  *)
(* whole chunk, and SyncAll only fires on a non-empty batch, so the log     *)
(* under the loss is non-empty and its frames are inside the frontier.      *)
(*                                                                         *)
(* Implied by RecoverySound, so it is clean wherever RecoverySound is, and  *)
(* it therefore cannot fire independently on the control that carries it.   *)
(* modes/ConsistentPrealloc3.cfg lists it anyway, deliberately: the point   *)
(* of a control is that the SAME invariant name, at the SAME bound, comes   *)
(* back clean when the mutation is off, and a reader comparing that row     *)
(* with mutations/M5Strand.cfg's should not have to derive the implication  *)
(* to know the comparison is like-for-like. It adds no detection power to   *)
(* that config and is not claimed to.                                       *)
(***************************************************************************)
NoAckLossAfterLiveExtend ==
    ~(recovered /\ ~recoverErr /\ DurableAck
      /\ syncedCapacity >= ChunkSize
      /\ \E c \in acked : \A i \in 1..Len(promoted) : promoted[i].cid # c)

(***************************************************************************)
(* S5 TailTolerance -- task37 §7, "Tail-tolerant recovery: the prealloc     *)
(* subtlety". Under CoalescedPrealloc a torn TAIL is end-of-log, not an     *)
(* error: recovery stops at the last good frame and carries on. Two         *)
(* clauses, and they are different claims:                                  *)
(*                                                                         *)
(*  (1) it never ABORTS. src/wal.rs:702-704 breaks out of the scan on an    *)
(*      undecodable frame when tail_tolerant, instead of returning          *)
(*      Err(WalCorrupted) at :705-707. The store opens.                     *)
(*  (2) it stops at the LAST GOOD FRAME, not before it: every frame in the  *)
(*      maximal present-prefix of the on-disk log is replayed.              *)
(*                                                                         *)
(* CLAUSE (2) HAS NO INDEPENDENT DETECTION POWER TODAY, and that is worth   *)
(* saying rather than leaving for someone to discover -- the same courtesy  *)
(* EventualHonest gets. Recover sets promoted = Replay(walAfterCrash, ..),  *)
(* and Replay is BY CONSTRUCTION the accepted prefix (filtered by a floor   *)
(* no committed config moves off 0), so clause (2) holds in every state     *)
(* clause (1) admits. It cannot fail alone. An earlier draft of this        *)
(* comment justified it by pointing at the strict path throwing the whole   *)
(* log away -- that was WRONG as a justification: the strict path always    *)
(* sets recoverErr, which clause (1) already catches, so it is not an       *)
(* example only clause (2) sees.                                           *)
(*                                                                         *)
(* It is kept as a GUARD AGAINST A MUTATION NOBODY HAS WRITTEN YET (M6 is   *)
(* NOT it -- M6 replays MORE than the accepted prefix, which is             *)
(* RecoverySound clause (c), the opposite direction): any change that makes *)
(* recovery return successfully while replaying LESS than the accepted      *)
(* prefix -- a truncating replay, a floor bug, a Checkpoint                 *)
(* action that mis-filters -- lands here and nowhere else. Read it as a     *)
(* claim staked for the future, not as a check currently doing work. If a   *)
(* later task adds such a mutation, this is the clause that should go red   *)
(* for it, and that is the moment to delete this paragraph.                 *)
(*                                                                         *)
(* SCOPED TO Prealloc, and that scope is a FINDING, not a convenience.      *)
(* The brief asked what the model says about strict sinks; it says the      *)
(* unscoped property is FALSE for them, and the model is right. scan_wal's  *)
(* strict branch cannot distinguish a torn tail from mid-WAL corruption --  *)
(* task37 §7 says so in as many words ("the prealloc path cannot            *)
(* distinguish a torn tail from tail corruption") and the strict branch has *)
(* the same blindness pointing the other way: it treats BOTH as fatal. So   *)
(* under FsWrite/Coalesced a legal torn tail after an ordinary crash does   *)
(* abort recovery. That is not a spec bug to paper over and it is not new:  *)
(* it is exactly StrictScanErrLosesDurableAck, already committed as an owed *)
(* property and already red on the DEFAULT Standalone config. Asserting     *)
(* TailTolerance unscoped would simply re-report that gap under a name that *)
(* claims a promise no append-mode sink ever made. Restricting the scope    *)
(* to the sink whose documentation promises tolerance is what makes the     *)
(* property falsifiable by M4 rather than by the sink choice.               *)
(*                                                                         *)
(* Clause (2) reads "replayed" as "the cid appears in `promoted`", which is *)
(* sound only while checkpointVersion = 0 (no committed config moves it, so *)
(* Replay's version filter is the identity). A Checkpoint action would need *)
(* the floor added here; noted rather than pre-empted.                      *)
(***************************************************************************)
TailTolerance ==
    (Prealloc /\ recovered) =>
      /\ ~recoverErr
      /\ \A i \in 1..Len(walAfterCrash) :
           (\A j \in 1..i : walAfterCrash[j].st = "present")
             => \E k \in 1..Len(promoted) : promoted[k].cid = walAfterCrash[i].cid

(***************************************************************************)
(* TORN-TAIL REACHABILITY CANARY (Task 5). Must be VIOLATED under any       *)
(* CoalescedPrealloc config. It asserts that recovery never truncates at a  *)
(* TORN frame and carries on -- i.e. that the tolerant half of scan_wal     *)
(* (src/wal.rs:702-704) is dead code. If it HOLDS, TailTolerance is green   *)
(* over a state space containing no torn tail at all, and M4 would be       *)
(* attacking a branch nothing reaches.                                      *)
(*                                                                         *)
(* Task 2 measured this branch's reachability with a scratch assertion and  *)
(* recorded it in prose (WalCrashPrealloc.cfg's header, "assertion R5").    *)
(* Prose is not a gate; this is the same measurement as a committed config. *)
(* Note it demands specifically a TORN stop, not merely a short one: the    *)
(* absent case is a clean end-of-log in BOTH scan modes                     *)
(* (src/wal.rs:692-697) and reaching only that would prove nothing about    *)
(* tolerance.                                                               *)
(***************************************************************************)
NoTornTailTruncation ==
    ~(Prealloc /\ recovered /\ ~recoverErr
      /\ ScanLen(walAfterCrash) < Len(walAfterCrash)
      /\ walAfterCrash[ScanLen(walAfterCrash) + 1].st = "torn")

(***************************************************************************)
(* OWED PROPERTY -- expected RED, and checked so that it stays known.       *)
(*                                                                         *)
(* Under a STRICT scan (every sink except CoalescedPrealloc, which is every *)
(* sink except the opt-in one -- so this includes the DEFAULT Standalone    *)
(* configuration, Consistent + WalWrite::PerEntry), a single torn frame     *)
(* anywhere in the log costs the WHOLE log, including durable commits the   *)
(* scan had ALREADY ACCEPTED and whose commit() returned Ok under           *)
(* Consistent. src/wal.rs:705-707 returns Err(WalCorrupted); Store::recover *)
(* propagates it with `?` at src/store.rs:1079, before any entry is         *)
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
