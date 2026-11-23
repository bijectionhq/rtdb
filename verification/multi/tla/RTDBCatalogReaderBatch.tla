---------------------------- MODULE RTDBCatalogReaderBatch ----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Readers, Cuts, MaxVersion, RequireVersionMatch, FenceAbsentRelease
VARIABLES version, floor, readers, released, pending

Vars == <<version, floor, readers, released, pending>>
Empty == [version |-> 0, readers |-> {}]

Init ==
    /\ version = 1
    /\ floor = 0
    /\ readers = {}
    /\ released = {}
    /\ pending = Empty

PrepareBatch ==
    /\ pending = Empty
    /\ \E batch \in SUBSET ((Readers \ released) \X Cuts) :
        /\ batch # {}
        /\ \A entry \in batch : entry[2] >= floor
        /\ pending' = [version |-> version, readers |-> batch]
    /\ UNCHANGED <<version, floor, readers, released>>

CommitBatch ==
    /\ pending # Empty
    /\ version < MaxVersion
    /\ LET accepted == pending.version = version \/ ~RequireVersionMatch
       IN /\ version' = IF accepted THEN version + 1 ELSE version
          /\ readers' = IF accepted THEN readers \cup pending.readers ELSE readers
    /\ pending' = Empty
    /\ UNCHANGED <<floor, released>>

Advance(cut) ==
    /\ version < MaxVersion
    /\ cut > floor
    /\ \A entry \in readers : entry[2] >= cut
    /\ floor' = cut
    /\ version' = version + 1
    /\ UNCHANGED <<readers, released, pending>>

Release(reader) ==
    /\ reader \notin released
    /\ version < MaxVersion
    /\ readers' = {entry \in readers : entry[1] # reader}
    /\ released' = released \cup {reader}
    /\ version' = IF FenceAbsentRelease \/ readers' # readers THEN version + 1 ELSE version
    /\ UNCHANGED <<floor, pending>>

Next ==
    \/ PrepareBatch
    \/ CommitBatch
    \/ \E cut \in Cuts : Advance(cut)
    \/ \E reader \in Readers : Release(reader)

Spec == Init /\ [][Next]_Vars
AdmissionsRespectFloor == \A entry \in readers : entry[2] >= floor
ReleaseFencesAdmission == \A entry \in readers : entry[1] \notin released

=======================================================================================
