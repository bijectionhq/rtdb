---- MODULE ReplayedReset ----
CONSTANT Mutation
VARIABLES verified, synced, moved, published, reclaimed
vars == <<verified, synced, moved, published, reclaimed>>

Init ==
    /\ verified = FALSE
    /\ synced = FALSE
    /\ moved = FALSE
    /\ published = FALSE
    /\ reclaimed = FALSE

Replay ==
    /\ ~verified
    /\ verified' = TRUE
    /\ UNCHANGED <<synced, moved, published, reclaimed>>

Sync ==
    /\ verified
    /\ ~synced
    /\ synced' = TRUE
    /\ UNCHANGED <<verified, moved, published, reclaimed>>

AdvanceServingCut ==
    /\ ~moved
    /\ ~published
    /\ moved' = TRUE
    /\ UNCHANGED <<verified, synced, published, reclaimed>>

Publish ==
    /\ verified
    /\ (synced \/ Mutation = "skip_sync")
    /\ (~moved \/ Mutation = "allow_stale")
    /\ ~published
    /\ published' = TRUE
    /\ UNCHANGED <<verified, synced, moved, reclaimed>>

Reclaim ==
    /\ (published \/ Mutation = "early_reclaim")
    /\ ~reclaimed
    /\ reclaimed' = TRUE
    /\ UNCHANGED <<verified, synced, moved, published>>

Next == Replay \/ Sync \/ AdvanceServingCut \/ Publish \/ Reclaim
Spec == Init /\ [][Next]_vars
Safe == (published => synced /\ ~moved) /\ (reclaimed => published)
NeverCompletes == ~reclaimed
====
