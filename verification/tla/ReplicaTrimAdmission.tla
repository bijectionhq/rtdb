------------------------- MODULE ReplicaTrimAdmission -------------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES checkpoint, observed, receipted, floor, authorized, pending, holds

Vars == <<checkpoint, observed, receipted, floor, authorized, pending, holds>>
Offsets == 0..2
Readers == 1..2
Idle == -1

Init ==
    /\ checkpoint = 0
    /\ observed = {}
    /\ receipted = FALSE
    /\ floor = 0
    /\ authorized = 0
    /\ pending = {}
    /\ holds = [reader \in Readers |-> Idle]

Observe(first) ==
    /\ first \notin observed
    /\ observed' = observed \cup {first}
    /\ UNCHANGED <<checkpoint, receipted, floor, authorized, pending, holds>>

Publish(first) ==
    /\ first >= checkpoint
    /\ first > checkpoint \/ ~receipted
    /\ first \in observed \/ Mutation = "publish_incomplete"
    /\ checkpoint' = first
    /\ receipted' = TRUE
    /\ UNCHANGED <<observed, floor, authorized, pending, holds>>

LoadLegacy ==
    /\ checkpoint = 0
    /\ ~receipted
    /\ checkpoint' = 1
    /\ UNCHANGED <<observed, receipted, floor, authorized, pending, holds>>

Acquire(reader, first) ==
    /\ holds[reader] = Idle
    /\ first <= checkpoint
    /\ first >= (IF Mutation = "trust_physical_floor" THEN floor ELSE authorized)
    /\ holds' = [holds EXCEPT ![reader] = first]
    /\ UNCHANGED <<checkpoint, observed, receipted, floor, authorized, pending>>

Release(reader) ==
    /\ holds[reader] # Idle
    /\ holds' = [holds EXCEPT ![reader] = Idle]
    /\ UNCHANGED <<checkpoint, observed, receipted, floor, authorized, pending>>

AuthorizeTrim(first) ==
    /\ authorized < first
    /\ first <= checkpoint
    /\ receipted \/ Mutation = "trim_uncertified"
    /\ Mutation = "ignore_recovery"
          \/ \A reader \in Readers : holds[reader] = Idle \/ first <= holds[reader]
    /\ authorized' = first
    /\ pending' = pending \cup {first}
    /\ UNCHANGED <<checkpoint, observed, receipted, floor, holds>>

CompleteTrim(first) ==
    /\ first \in pending
    /\ floor' = IF first > floor THEN first ELSE floor
    /\ pending' = pending \ {first}
    /\ UNCHANGED <<checkpoint, observed, receipted, authorized, holds>>

Next ==
    \/ LoadLegacy
    \/ \E first \in Offsets : Observe(first) \/ Publish(first)
    \/ \E reader \in Readers : Release(reader) \/ (\E first \in Offsets : Acquire(reader, first))
    \/ \E first \in Offsets : AuthorizeTrim(first) \/ CompleteTrim(first)

Spec == Init /\ [][Next]_Vars

PublishedCheckpointCovered == floor <= authorized /\ authorized <= checkpoint
ActiveRecoveryCovered == \A reader \in Readers : holds[reader] = Idle \/ holds[reader] >= floor
PublishedManifestComplete == receipted => checkpoint \in observed
AuthorizedTrimHasManifest == authorized > 0 => receipted
NeverRecoveredAfterTrim == ~(floor = 2 /\ \E reader \in Readers : holds[reader] = 2)

=============================================================================
