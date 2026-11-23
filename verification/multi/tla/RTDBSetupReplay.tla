-------------------------- MODULE RTDBSetupReplay --------------------------
CONSTANT RecheckPending
VARIABLES phase, executePending

Vars == <<phase, executePending>>

Init == phase = "initial" /\ executePending = FALSE

ObserveRefusal ==
    /\ phase = "initial"
    /\ phase' = "draining"
    /\ UNCHANGED executePending

AppendExecute ==
    /\ phase = "draining"
    /\ ~executePending
    /\ executePending' = TRUE
    /\ UNCHANGED phase

ReadyForQuery ==
    /\ phase = "draining"
    /\ phase' = IF RecheckPending /\ executePending THEN "unknown" ELSE "resent"
    /\ UNCHANGED executePending

Spec == Init /\ [][ObserveRefusal \/ AppendExecute \/ ReadyForQuery]_Vars
SetupReplayHasNoExecute == phase = "resent" => ~executePending

============================================================================
