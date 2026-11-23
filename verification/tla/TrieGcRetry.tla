---------------------------- MODULE TrieGcRetry ----------------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES term, held, wake, queued, erased

vars == <<term, held, wake, queued, erased>>

Init ==
    /\ term = 0 /\ held = TRUE /\ wake = FALSE /\ queued = FALSE /\ erased = FALSE

\* An installed receipt already exists when the successor takes over.
StartSuccessor ==
    /\ term = 0 /\ term' = 1 /\ wake' = TRUE
    /\ UNCHANGED <<held, queued, erased>>

Drain ==
    /\ held /\ held' = FALSE
    /\ UNCHANGED <<term, wake, queued, erased>>

Retry ==
    /\ Mutation # "only_block_signals"
    /\ term = 1 /\ ~queued /\ ~wake /\ wake' = TRUE
    /\ UNCHANGED <<term, held, queued, erased>>

Collect ==
    /\ wake /\ wake' = FALSE
    /\ queued' = (queued \/ ~held \/ Mutation = "omit_hold")
    /\ UNCHANGED <<term, held, erased>>

Reap ==
    /\ queued /\ ~erased /\ erased' = TRUE
    /\ UNCHANGED <<term, held, wake, queued>>

Next == StartSuccessor \/ Drain \/ Retry \/ Collect \/ Reap
Spec == Init /\ [][Next]_vars /\ WF_vars(Retry) /\ WF_vars(Collect) /\ WF_vars(Reap)

RetainsWhileHeld == held => ~queued /\ ~erased
EventuallyReapsWhenDrained == (term = 1 /\ ~held) ~> erased
NeverReapsInSuccessor == ~(term = 1 /\ erased)
=============================================================================
