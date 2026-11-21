------------------------------ MODULE RTDBDeletion ---------------------------
EXTENDS Integers

CONSTANTS LeaseTtl, UploadLag, MaxTime, CoverUnrecordedLease, AwaitSecondInterval, AllowUndrainedAdministrator
VARIABLE state

None == -1
Max(a, b) == IF a >= b THEN a ELSE b

Init ==
    state = [phase |-> "live",
             now |-> 0,
             recordedExpiry |-> None,
             legacyExpiry |-> None,
             fenceUntil |-> None,
             uploadUntil |-> None,
             adminUpload |-> FALSE,
             objects |-> TRUE,
             firstEmpty |-> None,
             secondEmpty |-> None,
             keyPresent |-> TRUE]

Tick ==
    /\ state.now < MaxTime
    /\ state' = [state EXCEPT !.now = @ + 1]

IssueRecordedLease ==
    /\ state.phase = "live"
    /\ state.recordedExpiry < state.now + LeaseTtl
    /\ state' = [state EXCEPT !.recordedExpiry = state.now + LeaseTtl]

IssueLegacyLease ==
    /\ state.phase = "live"
    /\ state.legacyExpiry < state.now + LeaseTtl
    /\ state' = [state EXCEPT !.legacyExpiry = state.now + LeaseTtl]

StartUpload ==
    /\ state.now < Max(state.recordedExpiry, state.legacyExpiry)
    /\ state.uploadUntil < state.now
    /\ state' = [state EXCEPT !.uploadUntil = state.now + UploadLag]

CompleteUpload ==
    /\ state.uploadUntil >= state.now
    /\ state' = [state EXCEPT !.uploadUntil = None, !.objects = TRUE]

StartAdminUpload ==
    /\ AllowUndrainedAdministrator
    /\ state.phase = "live"
    /\ ~state.adminUpload
    /\ state' = [state EXCEPT !.adminUpload = TRUE]

CompleteAdminUpload ==
    /\ state.adminUpload
    /\ state' = [state EXCEPT !.adminUpload = FALSE, !.objects = TRUE]

Fence ==
    /\ state.phase = "live"
    /\ state' = [state EXCEPT !.phase = "fenced",
                    !.fenceUntil = IF CoverUnrecordedLease
                                   THEN Max(state.recordedExpiry, state.now + LeaseTtl)
                                   ELSE state.recordedExpiry]

PurgeAndObserve ==
    /\ state.phase = "fenced"
    /\ state.now >= state.fenceUntil
    /\ state.firstEmpty = None
    /\ state' = [state EXCEPT !.objects = FALSE, !.firstEmpty = state.now]

ObserveAgain ==
    /\ state.phase = "fenced"
    /\ state.firstEmpty # None
    /\ state.now >= state.firstEmpty + (IF AwaitSecondInterval THEN LeaseTtl ELSE 0)
    /\ state' = IF state.objects
               THEN [state EXCEPT !.firstEmpty = None]
               ELSE [state EXCEPT !.phase = "verified", !.secondEmpty = state.now]

PublishReceipt ==
    /\ state.phase = "verified"
    /\ state' = [state EXCEPT !.phase = "deleted", !.keyPresent = FALSE]

Next ==
    \/ Tick
    \/ IssueRecordedLease
    \/ IssueLegacyLease
    \/ StartUpload
    \/ CompleteUpload
    \/ StartAdminUpload
    \/ CompleteAdminUpload
    \/ Fence
    \/ PurgeAndObserve
    \/ ObserveAgain
    \/ PublishReceipt

Spec == Init /\ [][Next]_state

FirstInventoryFollowsExpiry ==
    state.firstEmpty # None => state.firstEmpty >= Max(state.recordedExpiry, state.legacyExpiry)

ReceiptFollowsSeparatedObservations ==
    state.phase = "deleted" =>
        /\ state.firstEmpty # None
        /\ state.secondEmpty >= state.firstEmpty + LeaseTtl
        /\ ~state.keyPresent

NoObjectAfterReceipt == state.phase = "deleted" => ~state.objects
=============================================================================
