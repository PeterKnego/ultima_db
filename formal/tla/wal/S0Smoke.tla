---------------------------- MODULE S0Smoke ----------------------------
(* S0 toolchain gate: a deliberately tiny durability-shaped spec.        *)
(* A frame is written, then either fsynced or lost to a crash. The       *)
(* invariant is the one F-DB-2 exists to check in earnest: anything      *)
(* acked durable survives a crash.                                       *)
EXTENDS Naturals

VARIABLES buffered, durable, acked, crashed

Init == buffered = 0 /\ durable = 0 /\ acked = 0 /\ crashed = FALSE

Write   == /\ ~crashed /\ buffered < 2
           /\ buffered' = buffered + 1
           /\ UNCHANGED <<durable, acked, crashed>>

Fsync   == /\ ~crashed /\ buffered > durable
           /\ durable' = buffered
           /\ acked'   = buffered
           /\ UNCHANGED <<buffered, crashed>>

Crash   == /\ ~crashed
           /\ crashed'  = TRUE
           /\ buffered' = durable          \* unsynced frames vanish
           /\ UNCHANGED <<durable, acked>>

Next == Write \/ Fsync \/ Crash
Spec == Init /\ [][Next]_<<buffered, durable, acked, crashed>>

(* Safety: an acked frame is always durable. *)
AckedIsDurable == acked <= durable
=======================================================================
