-------------------- MODULE CheckpointMetadataRetention ---------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES published, holds, planned, deleted

Vars == <<published, holds, planned, deleted>>
Blocks == 0..2
Readers == {1, 2}
None == -1

Init ==
    /\ published = 0
    /\ holds = [reader \in Readers |-> None]
    /\ planned = {}
    /\ deleted = {}

Publish(block) ==
    /\ block # published
    /\ block > published \/ Mutation = "publish_backwards"
    /\ published' = block
    /\ UNCHANGED <<holds, planned, deleted>>

Admit(reader, first) ==
    /\ holds[reader] = None
    /\ first = published \/ Mutation = "allow_stale"
    /\ holds' = [holds EXCEPT ![reader] = first]
    /\ UNCHANGED <<published, planned, deleted>>

Release(reader) ==
    /\ holds[reader] # None
    /\ holds' = [holds EXCEPT ![reader] = None]
    /\ UNCHANGED <<published, planned, deleted>>

Plan(block) ==
    /\ block < published
    /\ block \notin planned
    /\ Mutation = "ignore_existing_holds" \/ \A reader \in Readers : holds[reader] = None \/ block < holds[reader]
    /\ planned' = planned \cup {block}
    /\ UNCHANGED <<published, holds, deleted>>

Delete(block) ==
    /\ block \in planned \ deleted
    /\ deleted' = deleted \cup {block}
    /\ UNCHANGED <<published, holds, planned>>

Next ==
    \/ \E block \in Blocks : Publish(block) \/ Plan(block) \/ Delete(block)
    \/ \E reader \in Readers : Release(reader) \/ (\E first \in Blocks : Admit(reader, first))

Spec == Init /\ [][Next]_Vars

PublishedMetadataPresent == \A block \in deleted : block < published
AdmittedMetadataPresent == \A reader \in Readers : holds[reader] = None \/
    \A block \in deleted : block < holds[reader]
NeverCollectsSupersededMetadata == ~(deleted = {0, 1} /\ \E reader \in Readers : holds[reader] = 2)

=============================================================================
