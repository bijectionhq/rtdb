------------------------- MODULE ReplicaTrimAdmission -------------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES checkpoint, floor, authorized, pending, holds

Vars == <<checkpoint, floor, authorized, pending, holds>>
Offsets == 0..2
Readers == 1..2
Idle == -1

Init ==
    /\ checkpoint = 0
    /\ floor = 0
    /\ authorized = 0
    /\ pending = {}
    /\ holds = [reader \in Readers |-> Idle]

Publish ==
    /\ checkpoint < 2
    /\ checkpoint' = checkpoint + 1
    /\ UNCHANGED <<floor, authorized, pending, holds>>

Acquire(reader, first) ==
    /\ holds[reader] = Idle
    /\ first <= checkpoint
    /\ first >= (IF Mutation = "trust_physical_floor" THEN floor ELSE authorized)
    /\ holds' = [holds EXCEPT ![reader] = first]
    /\ UNCHANGED <<checkpoint, floor, authorized, pending>>

Release(reader) ==
    /\ holds[reader] # Idle
    /\ holds' = [holds EXCEPT ![reader] = Idle]
    /\ UNCHANGED <<checkpoint, floor, authorized, pending>>

AuthorizeTrim(first) ==
    /\ authorized < first
    /\ first <= checkpoint
    /\ Mutation = "ignore_recovery"
          \/ \A reader \in Readers : holds[reader] = Idle \/ first <= holds[reader]
    /\ authorized' = first
    /\ pending' = pending \cup {first}
    /\ UNCHANGED <<checkpoint, floor, holds>>

CompleteTrim(first) ==
    /\ first \in pending
    /\ floor' = IF first > floor THEN first ELSE floor
    /\ pending' = pending \ {first}
    /\ UNCHANGED <<checkpoint, authorized, holds>>

Next ==
    \/ Publish
    \/ \E reader \in Readers : Release(reader) \/ (\E first \in Offsets : Acquire(reader, first))
    \/ \E first \in Offsets : AuthorizeTrim(first) \/ CompleteTrim(first)

Spec == Init /\ [][Next]_Vars

PublishedCheckpointCovered == floor <= authorized /\ authorized <= checkpoint
ActiveRecoveryCovered == \A reader \in Readers : holds[reader] = Idle \/ holds[reader] >= floor
NeverRecoveredAfterTrim == ~(floor = 2 /\ \E reader \in Readers : holds[reader] = 2)

=============================================================================
