--------------------- MODULE TerminalNamespaceEnrollment ---------------------
CONSTANT Mutation

VARIABLES versioned, attestation, residue, phase, serving, published

vars == <<versioned, attestation, residue, phase, serving, published>>

Init ==
    /\ versioned \in BOOLEAN
    /\ attestation \in {"absent", "other_root", "exact_root"}
    /\ residue \in {"empty", "object", "version", "upload"}
    /\ phase \in {"new", "evidence"}
    /\ phase = "evidence" => attestation # "absent"
    /\ serving = "closed"
    /\ published = FALSE

ExactEvidence == attestation = "exact_root"
Clean == residue = "empty"
CanEnroll ==
    /\ ~versioned
    /\ ExactEvidence \/ Mutation = "status_only"

StoreEvidence ==
    /\ phase = "new"
    /\ CanEnroll /\ Clean
    /\ phase' = "evidence"
    /\ UNCHANGED <<versioned, attestation, residue, serving, published>>

CheckBeforeActivation ==
    /\ phase = "evidence"
    /\ CanEnroll
    /\ Clean \/ Mutation = "trust_partial"
    /\ phase' = "checked"
    /\ UNCHANGED <<versioned, attestation, residue, serving, published>>

EarlyProviderWrite ==
    /\ Mutation = "allow_early_write" /\ phase = "checked" /\ Clean
    /\ residue' \in {"object", "upload"}
    /\ UNCHANGED <<versioned, attestation, phase, serving, published>>

Activate ==
    /\ phase = "checked"
    /\ phase' = "active"
    /\ UNCHANGED <<versioned, attestation, residue, serving, published>>

Open ==
    /\ phase = "active" /\ serving = "closed"
    /\ serving' = IF published THEN "reopened" ELSE "terminal"
    /\ UNCHANGED <<versioned, attestation, residue, phase, published>>

LegacyAlias ==
    /\ Mutation = "legacy_alias" /\ phase = "active" /\ serving = "closed"
    /\ serving' = "legacy"
    /\ UNCHANGED <<versioned, attestation, residue, phase, published>>

Publish ==
    /\ serving = "terminal" /\ ~published
    /\ published' = TRUE
    /\ UNCHANGED <<versioned, attestation, residue, phase, serving>>

Close ==
    /\ serving \in {"terminal", "reopened"}
    /\ serving' = "closed"
    /\ UNCHANGED <<versioned, attestation, residue, phase, published>>

Next == StoreEvidence \/ CheckBeforeActivation \/ EarlyProviderWrite \/ Activate
        \/ Open \/ LegacyAlias \/ Publish \/ Close
Spec == Init /\ [][Next]_vars

ActivatedWithExactEvidence == phase = "active" => ~versioned /\ ExactEvidence
ActivatedWithPristineInventory == phase = "active" => Clean
OnlyEnrolledOpens == serving # "closed" => serving \in {"terminal", "reopened"} /\ phase = "active"
NeverOpensPopulatedAllocation == serving # "reopened"
=============================================================================
