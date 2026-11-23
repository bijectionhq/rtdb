---------------------- MODULE OwnedRetirementAuthorization ----------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES phase, fence, revision, checked, authorizedWith

Vars == <<phase, fence, revision, checked, authorizedWith>>

Init ==
    /\ phase = "reserved" /\ fence = "none" /\ revision = 0
    /\ checked = 0 /\ authorizedWith = "none"

InstallFence ==
    /\ phase = "reserved" /\ revision < 3
    /\ fence' = "both" /\ revision' = revision + 1
    /\ checked' = revision'
    /\ UNCHANGED <<phase, authorizedWith>>

OwnerChanges ==
    /\ phase = "reserved" /\ revision < 3
    /\ fence' \in {"none", "data", "both"}
    /\ revision' = revision + 1
    /\ UNCHANGED <<phase, checked, authorizedWith>>

Authorize ==
    /\ phase = "reserved"
    /\ (checked = revision \/ Mutation = "skip_revision")
    /\ (fence = "both" \/ Mutation = "skip_fence")
    /\ phase' = "authorized"
    /\ authorizedWith' = [revision |-> revision, checked |-> checked, fence |-> fence]
    /\ UNCHANGED <<fence, revision, checked>>

Erase ==
    /\ phase = "authorized" /\ phase' = "erased"
    /\ UNCHANGED <<fence, revision, checked, authorizedWith>>

Settle ==
    /\ phase = "erased" /\ phase' = "settled" /\ fence' = "none"
    /\ UNCHANGED <<revision, checked, authorizedWith>>

Next == InstallFence \/ OwnerChanges \/ Authorize \/ Erase \/ Settle
Spec == Init /\ [][Next]_Vars

AuthorizationUsesCurrentOwner == phase # "reserved" => authorizedWith.checked = authorizedWith.revision
AuthorizationFencesBoth == phase # "reserved" => authorizedWith.fence = "both"
Safe == AuthorizationUsesCurrentOwner /\ AuthorizationFencesBoth
NeverSettlesAfterRecheck == ~(phase = "settled" /\ revision > 1)
=============================================================================
