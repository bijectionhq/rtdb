--------------------------- MODULE ErasureRelease ---------------------------
EXTENDS Naturals

CONSTANTS AllowSupersededWrite, IgnoreReplica
VARIABLES term, pending, stored, cached, complete

vars == <<term, pending, stored, cached, complete>>

Init ==
  /\ term = 0
  /\ pending = FALSE
  /\ stored = TRUE
  /\ cached = FALSE
  /\ complete = FALSE

\* The writer has checked its original term and entered an asynchronous put.
AdmitWrite ==
  /\ term = 0
  /\ ~pending
  /\ pending' = TRUE
  /\ UNCHANGED <<term, stored, cached, complete>>

FinishWrite ==
  /\ pending
  /\ pending' = FALSE
  /\ stored' = IF term = 0 \/ AllowSupersededWrite THEN TRUE ELSE stored
  /\ UNCHANGED <<term, cached, complete>>

CacheRead ==
  /\ term = 0
  /\ stored
  /\ ~cached
  /\ cached' = TRUE
  /\ UNCHANGED <<term, pending, stored, complete>>

Fence ==
  /\ term = 0
  /\ term' = 1
  /\ UNCHANGED <<pending, stored, cached, complete>>

DeletePage ==
  /\ term = 1
  /\ stored
  /\ stored' = FALSE
  /\ UNCHANGED <<term, pending, cached, complete>>

EvictReplica ==
  /\ term = 1
  /\ cached
  /\ cached' = FALSE
  /\ UNCHANGED <<term, pending, stored, complete>>

Acknowledge ==
  /\ term = 1
  /\ ~complete
  /\ ~stored
  /\ IgnoreReplica \/ ~cached
  /\ complete' = TRUE
  /\ UNCHANGED <<term, pending, stored, cached>>

Finished == complete /\ ~pending /\ UNCHANGED vars

Next == AdmitWrite \/ FinishWrite \/ CacheRead \/ Fence \/ DeletePage
     \/ EvictReplica \/ Acknowledge \/ Finished
Spec == Init /\ [][Next]_vars

NoResurrectedObject == complete => ~stored
NoRetainedReplica == complete => ~cached

=============================================================================
