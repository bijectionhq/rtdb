---------------------- MODULE OffloadedReplicaReclamation ----------------------
CONSTANT Mutation
VARIABLE custody

Init ==
    custody = [append |-> "pending", acknowledged |-> FALSE, bound |-> FALSE,
               legacyClosed |-> FALSE, legacyRead |-> FALSE, reaping |-> FALSE,
               liveRead |-> FALSE, authorized |-> FALSE, plan |-> FALSE,
               physicallyTrimmed |-> FALSE, trimObserved |-> FALSE,
               crashStage |-> "never", present |-> {"original", "replacement"},
               settled |-> FALSE]

Commit(acknowledged) ==
    /\ custody.append = "pending"
    /\ custody' = [custody EXCEPT !.append = "committed", !.acknowledged = acknowledged]

ResolveAbsent ==
    /\ custody.append = "pending"
    /\ custody' = [custody EXCEPT !.append = "sealed-absent"]

BindCertifiedPublication ==
    /\ ~custody.bound
    /\ custody.append = "committed"
        \/ (custody.append = "pending" /\ Mutation = "trust_pending_append")
    /\ custody' = [custody EXCEPT !.bound = TRUE]

AdmitLegacyRead ==
    /\ custody.append = "committed" /\ ~custody.legacyRead
    /\ ~custody.legacyClosed
        \/ (custody.authorized /\ Mutation = "legacy_reopens_after_closure")
    /\ custody' = [custody EXCEPT !.legacyRead = TRUE]

CloseLegacyAdmission ==
    /\ ~custody.legacyClosed
    /\ custody' = [custody EXCEPT !.legacyClosed = TRUE]

DrainLegacyRead ==
    /\ custody.legacyRead
    /\ custody' = [custody EXCEPT !.legacyRead = FALSE]

EnableReclamation ==
    /\ custody.legacyClosed /\ ~custody.reaping
    /\ ~custody.legacyRead \/ Mutation = "enable_before_legacy_drain"
    /\ custody' = [custody EXCEPT !.reaping = TRUE]

AdmitLiveRead ==
    /\ custody.append = "committed" /\ ~custody.liveRead
    /\ ~custody.authorized \/ Mutation = "live_reopens_after_anchor"
    /\ custody' = [custody EXCEPT !.liveRead = TRUE]

DrainLiveRead ==
    /\ custody.liveRead
    /\ custody' = [custody EXCEPT !.liveRead = FALSE]

Authorize ==
    /\ custody.reaping /\ custody.bound /\ ~custody.authorized
    /\ ~custody.liveRead \/ Mutation = "ignore_live_read"
    /\ custody' = [custody EXCEPT !.authorized = TRUE, !.plan = TRUE]

Trim ==
    /\ custody.authorized /\ ~custody.physicallyTrimmed
    /\ custody' = [custody EXCEPT !.physicallyTrimmed = TRUE]

ObserveTrim ==
    /\ custody.physicallyTrimmed /\ ~custody.trimObserved
    /\ custody' = [custody EXCEPT !.trimObserved = TRUE]

Crash ==
    /\ custody.authorized /\ custody.crashStage = "never" /\ ~custody.settled
    /\ LET stage == IF "original" \notin custody.present THEN "after-delete"
                    ELSE IF custody.physicallyTrimmed THEN "after-trim" ELSE "before-trim"
       IN custody' = [custody EXCEPT !.crashStage = stage, !.trimObserved = FALSE,
                        !.plan = IF Mutation = "lose_plan_on_crash" THEN FALSE ELSE @]

Erase ==
    /\ custody.plan /\ "original" \in custody.present
    /\ custody.trimObserved \/ Mutation = "delete_on_authorization"
    /\ custody' = [custody EXCEPT !.present = IF Mutation = "delete_by_key" THEN {} ELSE {"replacement"}]

Settle ==
    /\ custody.plan /\ custody.trimObserved /\ "original" \notin custody.present
    /\ custody' = [custody EXCEPT !.settled = TRUE, !.plan = FALSE]

Next ==
    \/ \E ack \in BOOLEAN : Commit(ack)
    \/ ResolveAbsent \/ BindCertifiedPublication
    \/ AdmitLegacyRead \/ CloseLegacyAdmission \/ DrainLegacyRead \/ EnableReclamation
    \/ AdmitLiveRead \/ DrainLiveRead \/ Authorize \/ Trim \/ ObserveTrim \/ Crash \/ Erase \/ Settle
Spec == Init /\ [][Next]_custody

BindingNamesCommittedRecord == custody.bound => custody.append = "committed"
AdmittedReaderCanResolve == custody.legacyRead \/ custody.liveRead => "original" \in custody.present
FutureAdmissionRespectsAnchor == custody.authorized => ~custody.liveRead
DeletionHasPhysicalTrim == "original" \notin custody.present =>
    custody.bound /\ custody.authorized /\ custody.physicallyTrimmed
SettlementHasExactPlan == custody.authorized /\ ~custody.settled => custody.plan
OtherVersionsSurvive == "replacement" \in custody.present

Safe ==
    /\ BindingNamesCommittedRecord /\ AdmittedReaderCanResolve /\ FutureAdmissionRespectsAnchor
    /\ DeletionHasPhysicalTrim /\ SettlementHasExactPlan /\ OtherVersionsSurvive

NeverSettlesLostReplyAfterCrash ==
    ~(custody.settled /\ ~custody.acknowledged /\ custody.crashStage = "after-trim")
NeverSettlesAfterDeleteCrash ==
    ~(custody.settled /\ custody.crashStage = "after-delete")
==============================================================================
