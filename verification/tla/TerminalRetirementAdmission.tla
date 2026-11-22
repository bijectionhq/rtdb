--------------------- MODULE TerminalRetirementAdmission ---------------------
CONSTANT Mutation

VARIABLES qualified, installed, matchesPublication, held, recoveryCovered, phase, pending

vars == <<qualified, installed, matchesPublication, held, recoveryCovered, phase, pending>>

Init ==
    /\ qualified \in BOOLEAN
    /\ installed \in BOOLEAN
    /\ matchesPublication \in BOOLEAN
    /\ held \in BOOLEAN
    /\ recoveryCovered \in BOOLEAN
    /\ phase = "open" /\ pending = FALSE

Drain == held /\ held' = FALSE /\ UNCHANGED <<qualified, installed, matchesPublication, recoveryCovered, phase, pending>>

AdvanceRecovery ==
    /\ ~recoveryCovered /\ recoveryCovered' = TRUE
    /\ UNCHANGED <<qualified, installed, matchesPublication, held, phase, pending>>

Authorize ==
    /\ phase = "open"
    /\ (qualified \/ Mutation = "bypass_enrollment")
    /\ (installed \/ Mutation = "bypass_receipt")
    /\ (matchesPublication \/ Mutation = "trust_fresh_head")
    /\ (~held \/ Mutation = "ignore_hold")
    /\ (recoveryCovered \/ Mutation = "ignore_recovery")
    /\ phase' = "fenced"
    /\ UNCHANGED <<qualified, installed, matchesPublication, held, recoveryCovered, pending>>

Submit ==
    /\ phase = "fenced" /\ ~pending /\ pending' = TRUE
    /\ UNCHANGED <<qualified, installed, matchesPublication, held, recoveryCovered, phase>>

FinishUpload ==
    /\ phase = "fenced" /\ pending /\ pending' = FALSE
    /\ phase' = "observed"
    /\ UNCHANGED <<qualified, installed, matchesPublication, held, recoveryCovered>>

Settle ==
    /\ phase = "observed" \/ (phase = "fenced" /\ pending /\ Mutation = "forget_upload")
    /\ phase' = "settled"
    /\ UNCHANGED <<qualified, installed, matchesPublication, held, recoveryCovered, pending>>

Next == Drain \/ AdvanceRecovery \/ Authorize \/ Submit \/ FinishUpload \/ Settle
Spec == Init /\ [][Next]_vars

QualifiedAuthority == phase # "open" => qualified
InstalledAuthority == phase # "open" => installed
WriteWitnessMatches == phase # "open" => matchesPublication
OwnerDrained == phase # "open" => ~held
RecoveryCovered == phase # "open" => recoveryCovered
SettledUpload == phase = "settled" => ~pending
NeverSettles == phase # "settled"
=============================================================================
