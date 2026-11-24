---------------------- MODULE OffloadedReplicaReclamation ----------------------
CONSTANT Mutation
VARIABLE custody

Init ==
    custody = [append |-> "pending", acknowledged |-> FALSE, bound |-> FALSE,
               successor |-> "unprepared", fenced |-> FALSE, activeRead |-> FALSE,
               reopened |-> FALSE, unrelatedHold |-> TRUE, historicalJournal |-> "absent",
               grant |-> FALSE, pending |-> FALSE, keyWithdrawn |-> FALSE,
               physicallyTrimmed |-> FALSE, crashStage |-> "never",
               present |-> {"original", "replacement"}, settled |-> FALSE]

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

AdmitRead ==
    /\ custody.append = "committed" /\ ~custody.activeRead
    /\ ~custody.fenced \/ Mutation = "reopen_fenced_route"
    /\ custody' = [custody EXCEPT !.activeRead = TRUE, !.reopened = custody.fenced]

DrainRead ==
    /\ custody.activeRead
    /\ custody' = [custody EXCEPT !.activeRead = FALSE]

PrepareSuccessor ==
    /\ custody.successor = "unprepared"
    /\ custody' = [custody EXCEPT !.successor =
                    IF Mutation = "omit_successor_member" THEN "incomplete" ELSE "complete"]

ExcludeOldDependencies ==
    /\ ~custody.fenced
    /\ custody.successor = "complete"
        \/ (custody.successor = "incomplete" /\ Mutation = "omit_successor_member")
    /\ custody' = [custody EXCEPT !.fenced = TRUE]

Authorize ==
    /\ custody.fenced /\ custody.bound /\ ~custody.grant
    /\ ~custody.activeRead \/ Mutation = "ignore_live_read"
    /\ ~custody.unrelatedHold \/ Mutation # "global_floor"
    /\ custody' = [custody EXCEPT !.grant = TRUE, !.pending = TRUE]

TrimActiveJournal ==
    /\ custody.fenced /\ ~custody.activeRead /\ ~custody.physicallyTrimmed
    /\ custody' = [custody EXCEPT !.physicallyTrimmed = TRUE]

WithdrawKey ==
    /\ custody.pending /\ ~custody.keyWithdrawn
    /\ custody' = [custody EXCEPT !.keyWithdrawn = TRUE]

Crash ==
    /\ custody.grant /\ custody.crashStage = "never" /\ ~custody.settled
    /\ LET stage == IF "original" \notin custody.present THEN "after-delete"
                    ELSE IF custody.keyWithdrawn THEN "after-key" ELSE "before-key"
       IN custody' = [custody EXCEPT !.crashStage = stage,
                        !.pending = IF Mutation = "lose_grant_on_crash" THEN FALSE ELSE @]

Erase ==
    /\ custody.pending /\ "original" \in custody.present
    /\ custody.keyWithdrawn \/ Mutation = "delete_without_key_withdrawal"
    /\ custody.historicalJournal # "absent" \/ Mutation # "wait_for_all_journals"
    /\ custody' = [custody EXCEPT !.present = IF Mutation = "delete_by_key" THEN {} ELSE {"replacement"}]

Settle ==
    /\ custody.pending /\ "original" \notin custody.present
    /\ custody' = [custody EXCEPT !.settled = TRUE, !.pending = FALSE]

Next ==
    \/ \E ack \in BOOLEAN : Commit(ack)
    \/ ResolveAbsent \/ BindCertifiedPublication \/ AdmitRead \/ DrainRead
    \/ PrepareSuccessor \/ ExcludeOldDependencies \/ Authorize \/ TrimActiveJournal
    \/ WithdrawKey \/ Crash \/ Erase \/ Settle

Spec == Init /\ [][Next]_custody
        /\ WF_custody(Authorize) /\ WF_custody(WithdrawKey)
        /\ WF_custody(Erase) /\ WF_custody(Settle)

BindingNamesCommittedRecord == custody.bound => custody.append = "committed"
AdmittedReaderCanResolve == custody.activeRead => "original" \in custody.present /\ ~custody.keyWithdrawn
ExcludedRouteStaysClosed == ~custody.reopened
FenceHasCompleteSuccessor == custody.fenced => custody.successor = "complete"
DeletionHasExactGrant == "original" \notin custody.present =>
    custody.bound /\ custody.fenced /\ custody.grant /\ custody.keyWithdrawn
SettlementHasExactPlan == custody.grant /\ ~custody.settled => custody.pending
OtherVersionsSurvive == "replacement" \in custody.present

Safe ==
    /\ BindingNamesCommittedRecord /\ AdmittedReaderCanResolve /\ ExcludedRouteStaysClosed
    /\ FenceHasCompleteSuccessor /\ DeletionHasExactGrant /\ SettlementHasExactPlan /\ OtherVersionsSurvive

ReclaimsExcludedPublication ==
    (custody.fenced /\ custody.bound /\ ~custody.activeRead) ~> custody.settled

NeverSettlesLostReplyAfterCrash ==
    ~(custody.settled /\ ~custody.acknowledged /\ custody.crashStage = "after-key")
NeverSettlesAfterDeleteCrash ==
    ~(custody.settled /\ custody.crashStage = "after-delete")
==============================================================================
