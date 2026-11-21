------------------------- MODULE CacheGeneration -------------------------
EXTENDS Integers

CONSTANT LatePublish
VARIABLES current, canonical, oldPath, oldLoading, freshLease

Vars == <<current, canonical, oldPath, oldLoading, freshLease>>
Init ==
    /\ current = 0
    /\ canonical = -1
    /\ oldPath = "none"
    /\ oldLoading = FALSE
    /\ freshLease = FALSE

Start ==
    /\ current = 0
    /\ oldPath = "none"
    /\ ~oldLoading
    /\ oldLoading' = TRUE
    /\ UNCHANGED <<current, canonical, oldPath, freshLease>>

Invalidate ==
    /\ current = 0
    /\ current' = 1
    /\ canonical' = -1
    /\ oldPath' = IF oldPath = "canonical" THEN "private" ELSE oldPath
    /\ UNCHANGED <<oldLoading, freshLease>>

FinishOld ==
    /\ oldLoading
    /\ oldLoading' = FALSE
    /\ IF current = 0 \/ LatePublish
       THEN /\ oldPath' = "canonical"
            /\ canonical' = 0
       ELSE /\ oldPath' = "private"
            /\ UNCHANGED canonical
    /\ UNCHANGED <<current, freshLease>>

PublishFresh ==
    /\ current = 1
    /\ ~freshLease
    /\ freshLease' = TRUE
    /\ canonical' = 1
    /\ UNCHANGED <<current, oldPath, oldLoading>>

ReleaseOld ==
    /\ current = 1
    /\ oldPath # "none"
    /\ oldPath' = "none"
    /\ canonical' = IF oldPath = "canonical" THEN -1 ELSE canonical
    /\ UNCHANGED <<current, oldLoading, freshLease>>

Next == Start \/ Invalidate \/ FinishOld \/ PublishFresh \/ ReleaseOld
Spec == Init /\ [][Next]_Vars
FreshLeaseReachable == freshLease => canonical = 1

=============================================================================
