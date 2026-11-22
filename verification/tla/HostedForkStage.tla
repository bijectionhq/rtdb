---------------------------- MODULE HostedForkStage ----------------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES phase, version, observed, preparation, drained, name, payload

vars == <<phase, version, observed, preparation, drained, name, payload>>

Init ==
    /\ phase = "new" /\ version = 0 /\ observed = 0
    /\ preparation = FALSE /\ drained = FALSE /\ name = "free" /\ payload = FALSE

Acquire ==
    /\ phase = "new" /\ ~preparation
    /\ preparation' = TRUE
    /\ UNCHANGED <<phase, version, observed, drained, name, payload>>

Stage ==
    /\ phase = "new" /\ preparation
    /\ phase' = "staged" /\ version' = 1
    /\ name' = IF Mutation = "claim_name_during_stage" /\ name = "free" THEN "fork" ELSE name
    /\ UNCHANGED <<observed, preparation, drained, payload>>

Write ==
    /\ (phase = "staged" \/ (phase = "new" /\ Mutation = "write_before_stage"))
    /\ preparation /\ ~drained /\ ~payload
    /\ payload' = TRUE
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name>>

OccupyName ==
    /\ name = "free"
    /\ name' = "other"
    /\ UNCHANGED <<phase, version, observed, preparation, drained, payload>>

ObserveStage ==
    /\ phase = "staged" /\ observed # version
    /\ observed' = version
    /\ UNCHANGED <<phase, version, preparation, drained, name, payload>>

Drain ==
    /\ phase = "staged" /\ preparation /\ ~drained
    /\ drained' = TRUE
    /\ UNCHANGED <<phase, version, observed, preparation, name, payload>>

Publish ==
    /\ phase = "staged" /\ preparation /\ drained /\ name = "free" /\ payload
    /\ phase' = "published" /\ version' = version + 1
    /\ preparation' = FALSE /\ name' = "fork"
    /\ UNCHANGED <<observed, drained, payload>>

ReleaseAfterDrain ==
    /\ phase \in {"staged", "closed"} /\ preparation /\ drained
    /\ preparation' = FALSE
    /\ UNCHANGED <<phase, version, observed, drained, name, payload>>

Reject ==
    /\ phase = "staged" /\ preparation /\ drained /\ name = "other"
    /\ phase' = "closed" /\ version' = 2
    /\ UNCHANGED <<observed, preparation, drained, name, payload>>

Recover ==
    /\ observed # 0
    /\ (~preparation \/ Mutation = "ignore_preparation")
    /\ ((phase = "staged" /\ version = observed) \/ Mutation = "ignore_stage_version")
    /\ phase' = "closed" /\ version' = 2
    /\ UNCHANGED <<observed, preparation, drained, name, payload>>

Purge ==
    /\ phase = "closed" /\ ~preparation
    /\ phase' = "disposed" /\ version' = 3 /\ payload' = FALSE
    /\ UNCHANGED <<observed, preparation, drained, name>>

Next == Acquire \/ Stage \/ Write \/ OccupyName \/ ObserveStage \/ Drain \/ Publish \/ ReleaseAfterDrain \/ Reject \/ Recover \/ Purge
Spec == Init /\ [][Next]_vars

EffectsHaveStage == payload => phase # "new"
ServingNameHasPublication == name = "fork" => phase = "published" /\ payload
RecoveryAfterDrain == phase \in {"closed", "disposed"} => drained
NeverPublishesWithStaleObservation == ~(phase = "published" /\ observed # 0)
NeverDisposesRejectedCollision == ~(phase = "disposed" /\ name = "other")
=============================================================================
