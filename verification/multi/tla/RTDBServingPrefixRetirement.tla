-------------------- MODULE RTDBServingPrefixRetirement --------------------
(* Grant 1 writes blocks and offloaded payloads below its own prefix; grant  *)
(* 2 replaces it. The custodian may remove prefix 1 only once the journal    *)
(* fences grant 1, an independent checkpoint covers and trims its records,   *)
(* nothing holds or prepares from it, and its writes have drained.          *)
(* Positions are record counts: a cut c covers log[1..c].                    *)
EXTENDS Naturals, Sequences

CONSTANTS Mutation, MaxRecords

VARIABLES grant, writing, log, checkpoint, floor, holds, preparing,
          prefix, successor, quiet

vars == <<grant, writing, log, checkpoint, floor, holds, preparing,
          prefix, successor, quiet>>

Positions(from) == {i \in 1..Len(log) : i > from}
HasGrant(from, e) == \E i \in Positions(from) : log[i] = e
PreparingFrom == IF preparing = 0 THEN 0 ELSE log[preparing]

Init ==
    /\ grant = 1
    /\ writing = TRUE
    /\ log = <<>>
    /\ checkpoint = 0
    /\ floor = 0
    /\ holds = {}
    /\ preparing = 0
    /\ prefix = "present"
    /\ successor = "absent"
    /\ quiet = FALSE

\* The journal appends only for the current grant.
AppendRecord ==
    /\ Len(log) < MaxRecords
    /\ (grant = 1 => writing)
    /\ log' = Append(log, grant)
    /\ successor' = IF grant = 2 THEN "present" ELSE successor
    /\ UNCHANGED <<grant, writing, checkpoint, floor, holds, preparing, prefix, quiet>>

\* A zombie of grant 1 can still write objects until its lease or process ends.
LateWrite ==
    /\ writing
    /\ prefix' = IF prefix = "present" THEN "present" ELSE "late"
    /\ UNCHANGED <<grant, writing, log, checkpoint, floor, holds, preparing, successor, quiet>>

Fence ==
    /\ grant = 1
    /\ grant' = 2
    /\ UNCHANGED <<writing, log, checkpoint, floor, holds, preparing, prefix, successor, quiet>>

\* A current grant may go quiet and later obtain new credentials; only the
\* fence stops it from doing so.
Pause ==
    /\ writing
    /\ writing' = FALSE
    /\ UNCHANGED <<grant, log, checkpoint, floor, holds, preparing, prefix, successor, quiet>>

\* A replay hold pins the retained suffix from its start.
Resume ==
    /\ grant = 1
    /\ ~writing
    /\ writing' = TRUE
    /\ UNCHANGED <<grant, log, checkpoint, floor, holds, preparing, prefix, successor, quiet>>

\* Overlapping readers eventually finish; afterwards new holds start at the checkpoint.
Quiesce ==
    /\ ~quiet
    /\ quiet' = TRUE
    /\ UNCHANGED <<grant, writing, log, checkpoint, floor, holds, preparing, prefix, successor>>

Hold(s) ==
    /\ s >= floor /\ s < Len(log)
    /\ (~quiet \/ s >= checkpoint)
    /\ s \notin holds
    /\ holds' = holds \cup {s}
    /\ UNCHANGED <<grant, writing, log, checkpoint, floor, preparing, prefix, successor, quiet>>

Release(s) ==
    /\ s \in holds
    /\ holds' = holds \ {s}
    /\ UNCHANGED <<grant, writing, log, checkpoint, floor, preparing, prefix, successor, quiet>>

\* Preparation copies from the prefix of the grant whose upload it chose.
Prepare(p) ==
    /\ preparing = 0
    /\ p > checkpoint /\ p <= Len(log)
    /\ preparing' = p
    /\ UNCHANGED <<grant, writing, log, checkpoint, floor, holds, prefix, successor, quiet>>

Publish ==
    /\ preparing # 0
    /\ checkpoint' = preparing
    /\ preparing' = 0
    /\ UNCHANGED <<grant, writing, log, floor, holds, prefix, successor, quiet>>

Trim ==
    /\ floor < checkpoint
    /\ \A s \in holds : s >= checkpoint
    /\ floor' = checkpoint
    /\ UNCHANGED <<grant, writing, log, checkpoint, holds, preparing, prefix, successor, quiet>>

Retire ==
    /\ prefix = "present"
    /\ (grant > 1 \/ Mutation = "ignore_fence")
    /\ checkpoint > 0
    /\ (floor = checkpoint \/ Mutation = "ignore_trim")
    \* Inspection reaches only the records at or after the selected checkpoint.
    /\ (~HasGrant(checkpoint, 1) \/ Mutation = "ignore_retained")
    /\ (PreparingFrom # 1 \/ Mutation = "ignore_preparation")
    /\ (~writing \/ Mutation = "ignore_drain")
    /\ prefix' = "removed"
    /\ successor' = IF Mutation = "shared_prefix" /\ successor = "present" THEN "removed" ELSE successor
    /\ UNCHANGED <<grant, writing, log, checkpoint, floor, holds, preparing, quiet>>

Next ==
    \/ AppendRecord \/ LateWrite \/ Fence \/ Pause \/ Resume \/ Quiesce \/ Publish \/ Trim \/ Retire
    \/ \E s \in 0..MaxRecords : Hold(s) \/ Release(s)
    \/ \E p \in 1..MaxRecords : Prepare(p)

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(Fence) /\ WF_vars(Pause) /\ WF_vars(Quiesce) /\ WF_vars(Publish) /\ SF_vars(Trim) /\ WF_vars(Retire)
    /\ WF_vars(Prepare(MaxRecords))
    /\ \A s \in 0..MaxRecords : WF_vars(Release(s))

\* Whatever can still read grant 1's prefix finds it.
NeededPrefixPresent ==
    (grant = 1 \/ HasGrant(floor, 1) \/ PreparingFrom = 1) => prefix = "present"
NoLateWrite == prefix # "late"
SuccessorPreserved == successor # "removed"
\* Once grant 2 has appended the final record, prefix 1 is eventually retired.
EventuallyRetired == (Len(log) = MaxRecords /\ log[MaxRecords] = 2) ~> (prefix = "removed")
=============================================================================
