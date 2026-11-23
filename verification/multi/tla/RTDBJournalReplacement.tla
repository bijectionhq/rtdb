------------------------- MODULE RTDBJournalReplacement -------------------------
EXTENDS Naturals

CONSTANTS MaxEntries, RequireCompleteCopy, RequirePublishedCheckpoint
VARIABLES state

Init ==
    state = [ensemble |-> "old", oldTip |-> 0, newTip |-> 0,
             staged |-> 0, checkpoint |-> 0, anchor |-> 0,
             oldFloor |-> 0, newFloor |-> 0, ack |-> 0,
             phase |-> "serving"]

Tip == IF state.ensemble = "old" THEN state.oldTip ELSE state.newTip
Floor == IF state.ensemble = "old" THEN state.oldFloor ELSE state.newFloor

Append ==
    /\ state.phase = "serving"
    /\ Tip < MaxEntries
    /\ state' = IF state.ensemble = "old"
                THEN [state EXCEPT !.oldTip = @ + 1, !.ack = @ + 1]
                ELSE [state EXCEPT !.newTip = @ + 1, !.ack = @ + 1]

Upload(n) ==
    /\ state.phase = "serving"
    /\ n \in (state.staged + 1)..Tip
    /\ state' = [state EXCEPT !.staged = n]

Publish ==
    /\ state.phase = "serving"
    /\ state.checkpoint < state.staged
    /\ state' = [state EXCEPT !.checkpoint = state.staged]

RememberTrim(n) ==
    /\ state.phase = "serving"
    /\ n \in (state.anchor + 1)..(IF RequirePublishedCheckpoint THEN state.checkpoint ELSE state.staged)
    /\ state' = [state EXCEPT !.anchor = n]

Trim ==
    /\ Floor < state.anchor
    /\ state' = IF state.ensemble = "old"
                THEN [state EXCEPT !.oldFloor = state.anchor]
                ELSE [state EXCEPT !.newFloor = state.anchor]

Crash ==
    /\ state.phase # "down"
    /\ state' = [state EXCEPT !.phase = "down"]

Restart ==
    /\ state.phase = "down"
    /\ state' = [state EXCEPT !.phase = IF state.ensemble = "old" THEN "copying" ELSE "serving"]

Copy(n) ==
    /\ state.phase = "copying"
    /\ n \in state.checkpoint..state.oldTip
    /\ state' = [state EXCEPT !.newFloor = state.checkpoint, !.newTip = n]

PublishReplacement ==
    /\ state.phase = "copying"
    /\ state.newFloor <= state.checkpoint
    /\ (~RequireCompleteCopy \/ state.newTip = state.oldTip)
    /\ state' = [state EXCEPT !.ensemble = "new", !.phase = "serving"]

Next ==
    \/ Append \/ Publish \/ Trim \/ Crash \/ Restart \/ PublishReplacement
    \/ \E n \in 1..MaxEntries : Upload(n) \/ RememberTrim(n)
    \/ \E n \in 0..MaxEntries : Copy(n)

Spec == Init /\ [][Next]_state
AcknowledgedPrefixRecoverable == Floor <= state.checkpoint /\ state.ack <= Tip

=======================================================================================
