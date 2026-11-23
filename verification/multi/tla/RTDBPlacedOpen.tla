------------------------------- MODULE RTDBPlacedOpen -------------------------------
EXTENDS FiniteSets, Naturals

CONSTANTS Slots, PublishAfterStop
VARIABLES opening, prepared, visible, stopped, latePublication

Vars == <<opening, prepared, visible, stopped, latePublication>>
Stalled == "stalled"
Independent == "independent"

Init ==
    /\ opening = {Stalled}
    /\ prepared = {}
    /\ visible = {}
    /\ stopped = FALSE
    /\ latePublication = FALSE

Begin ==
    /\ ~stopped
    /\ Independent \notin opening \cup visible
    /\ Cardinality(opening) < Slots
    /\ opening' = opening \cup {Independent}
    /\ UNCHANGED <<prepared, visible, stopped, latePublication>>

Backed ==
    /\ ~stopped
    /\ Independent \in opening \ prepared
    /\ prepared' = prepared \cup {Independent}
    /\ UNCHANGED <<opening, visible, stopped, latePublication>>

Publish ==
    /\ Independent \in prepared
    /\ (~stopped \/ PublishAfterStop)
    /\ visible' = visible \cup {Independent}
    /\ opening' = opening \ {Independent}
    /\ prepared' = prepared \ {Independent}
    /\ latePublication' = stopped
    /\ UNCHANGED stopped

Stop ==
    /\ ~stopped
    /\ stopped' = TRUE
    /\ UNCHANGED <<opening, prepared, visible, latePublication>>

Spec == Init /\ [][Begin \/ Backed \/ Publish \/ Stop]_Vars
             /\ WF_Vars(Begin) /\ WF_Vars(Backed) /\ WF_Vars(Publish)
OneAttachmentPerName == opening \cap visible = {}
ShutdownFencesPublication == ~latePublication
IndependentEventuallyOpens == <>(stopped \/ Independent \in visible)

=======================================================================================
