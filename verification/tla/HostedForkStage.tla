---------------------------- MODULE HostedForkStage ----------------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES phase, version, observed, preparation, drained, name, payload, upload, journal

vars == <<phase, version, observed, preparation, drained, name, payload, upload, journal>>

Init ==
    /\ phase = "new" /\ version = 0 /\ observed = 0
    /\ preparation = FALSE /\ drained = FALSE /\ name = "free" /\ payload = FALSE
    /\ upload = "none" /\ journal = FALSE

Acquire ==
    /\ phase = "new" /\ ~preparation
    /\ preparation' = TRUE
    /\ UNCHANGED <<phase, version, observed, drained, name, payload, upload, journal>>

Stage ==
    /\ phase = "new" /\ preparation
    /\ phase' = "staged" /\ version' = 1
    /\ name' = IF Mutation = "claim_name_during_stage" /\ name = "free" THEN "fork" ELSE name
    /\ UNCHANGED <<observed, preparation, drained, payload, upload, journal>>

Write ==
    /\ (phase = "staged" \/ (phase = "new" /\ Mutation = "write_before_stage"))
    /\ preparation /\ ~drained /\ ~payload
    /\ payload' = TRUE
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name, upload, journal>>

OccupyName ==
    /\ name = "free"
    /\ name' = "other"
    /\ UNCHANGED <<phase, version, observed, preparation, drained, payload, upload, journal>>

ObserveStage ==
    /\ phase = "staged" /\ observed # version
    /\ observed' = version
    /\ UNCHANGED <<phase, version, preparation, drained, name, payload, upload, journal>>

Drain ==
    /\ phase = "staged" /\ preparation /\ ~drained
    /\ drained' = TRUE
    /\ UNCHANGED <<phase, version, observed, preparation, name, payload, upload, journal>>

Publish ==
    /\ phase = "staged" /\ preparation /\ drained /\ name = "free" /\ payload
    /\ ~journal
    /\ phase' = "published" /\ version' = version + 1
    /\ preparation' = FALSE /\ name' = "fork"
    /\ UNCHANGED <<observed, drained, payload, upload, journal>>

ReleaseAfterDrain ==
    /\ phase \in {"staged", "closed"} /\ preparation /\ drained
    /\ preparation' = FALSE
    /\ UNCHANGED <<phase, version, observed, drained, name, payload, upload, journal>>

Reject ==
    /\ phase = "staged" /\ preparation /\ drained /\ name = "other"
    /\ phase' = "closed" /\ version' = 2
    /\ UNCHANGED <<observed, preparation, drained, name, payload, upload, journal>>

Recover ==
    /\ observed # 0
    /\ (~preparation \/ Mutation = "ignore_preparation")
    /\ ((phase = "staged" /\ version = observed) \/ Mutation = "ignore_stage_version")
    /\ phase' = "closed" /\ version' = 2
    /\ UNCHANGED <<observed, preparation, drained, name, payload, upload, journal>>

ReserveUpload ==
    /\ phase = "staged" /\ preparation /\ ~drained /\ upload = "none"
    /\ upload' = "reserved"
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name, payload, journal>>

SubmitUpload ==
    /\ phase = "staged" /\ preparation /\ ~drained /\ upload = "reserved"
    /\ upload' = "sent"
    /\ journal' = (Mutation # "dispatch_without_journal")
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name, payload>>

CompleteUpload ==
    /\ upload = "sent"
    /\ upload' = "completed" /\ payload' = TRUE
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name, journal>>

AbortUpload ==
    /\ phase = "closed" /\ journal /\ upload = "sent"
    /\ upload' = "aborted"
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name, payload, journal>>

SettleUpload ==
    /\ journal /\ (upload \in {"aborted", "completed"} \/ Mutation = "forget_before_abort")
    /\ journal' = FALSE
    /\ UNCHANGED <<phase, version, observed, preparation, drained, name, payload, upload>>

Purge ==
    /\ phase = "closed" /\ ~preparation /\ ~journal
    /\ phase' = "disposed" /\ version' = 3 /\ payload' = FALSE
    /\ upload' = IF upload = "reserved" THEN "discarded" ELSE upload
    /\ UNCHANGED <<observed, preparation, drained, name, journal>>

Next == Acquire \/ Stage \/ Write \/ OccupyName \/ ObserveStage \/ Drain \/ Publish \/ ReleaseAfterDrain \/ Reject \/ Recover \/ Purge
        \/ ReserveUpload \/ SubmitUpload \/ CompleteUpload \/ AbortUpload \/ SettleUpload
Spec == Init /\ [][Next]_vars

EffectsHaveStage == payload => phase # "new"
ServingNameHasPublication == name = "fork" => phase = "published" /\ payload
RecoveryAfterDrain == phase \in {"closed", "disposed"} => drained
DisposedPayloadAbsent == phase = "disposed" => ~payload
NeverDisposesSubmittedUpload == ~(phase = "disposed" /\ upload = "aborted" /\ observed # 0)
NeverPublishesWithStaleObservation == ~(phase = "published" /\ observed # 0)
NeverDisposesRejectedCollision == ~(phase = "disposed" /\ name = "other")
=============================================================================
