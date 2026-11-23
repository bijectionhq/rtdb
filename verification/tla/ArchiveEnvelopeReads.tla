--------------------------- MODULE ArchiveEnvelopeReads ---------------------------
EXTENDS Integers, TLC

CONSTANT Mutation

VARIABLES envelope, legacyCache, currentCache, read, reserved, observed, served

Vars == <<envelope, legacyCache, currentCache, read, reserved, observed, served>>
WholeCharge == 9
RangeCharge == 3

Init ==
    /\ envelope \in {"legacy", "framed"}
    /\ legacyCache = TRUE /\ currentCache = FALSE
    /\ read = "idle" /\ reserved = 0 /\ observed = "none" /\ served = "none"

AdmitScan ==
    /\ read = "idle"
    /\ read' = IF envelope = "framed" \/ Mutation = "range_for_legacy" THEN "range" ELSE "whole"
    /\ reserved' = IF read' = "range" THEN RangeCharge ELSE WholeCharge
    /\ UNCHANGED <<envelope, legacyCache, currentCache, observed, served>>

AdmitCacheFill ==
    /\ read = "idle"
    /\ read' = "fill"
    /\ reserved' = IF Mutation = "budget_fill_as_range" THEN RangeCharge ELSE WholeCharge
    /\ UNCHANGED <<envelope, legacyCache, currentCache, observed, served>>

Observe ==
    /\ read # "idle" /\ observed = "none"
    /\ observed' \in {"accepted", "substituted"}
    /\ UNCHANGED <<envelope, legacyCache, currentCache, read, reserved, served>>

Finish ==
    /\ read # "idle" /\ observed # "none"
    /\ served' = IF observed = "accepted" \/ Mutation = "accept_substitution" THEN observed ELSE "none"
    /\ currentCache' = (currentCache \/ (read = "fill" /\ served' # "none"))
    /\ read' = "idle" /\ reserved' = 0 /\ observed' = "none"
    /\ UNCHANGED <<envelope, legacyCache>>

ReadCache ==
    /\ read = "idle"
    /\ currentCache \/ (Mutation = "reuse_legacy_cache" /\ legacyCache)
    /\ served' = IF currentCache THEN "accepted" ELSE "legacy-ciphertext"
    /\ UNCHANGED <<envelope, legacyCache, currentCache, read, reserved, observed>>

Next == AdmitScan \/ AdmitCacheFill \/ Observe \/ Finish \/ ReadCache
Spec == Init /\ [][Next]_Vars

AllowsOnlyAuthenticatedRanges == read = "range" => envelope = "framed"
AdmissionCoversAllocation ==
    /\ read = "range" => reserved >= RangeCharge
    /\ read \in {"whole", "fill"} => reserved >= WholeCharge
ExactAcceptedBytes == served \in {"none", "accepted"}
NeverReadsLegacy == envelope # "legacy" \/ served # "accepted"
===============================================================================
