--------------------------- MODULE RecoveryReplay --------------------------
EXTENDS Integers

CONSTANTS
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Int;
    head,
    \* @type: Int;
    floor,
    \* @type: Int;
    selected,
    \* @type: Int;
    previous,
    \* @type: Str;
    phase,
    \* @type: Int;
    observedFloor,
    \* @type: Int;
    tailFloor,
    \* @type: Bool;
    refreshed

Vars == <<head, floor, selected, previous, phase, observedFloor, tailFloor, refreshed>>
NextOffset(block) == 2 * block + 1

Init ==
    /\ head = 0
    /\ floor = 0
    /\ selected = -1
    /\ previous = -1
    /\ phase = "list"
    /\ observedFloor = -1
    /\ tailFloor = -1
    /\ refreshed = FALSE

Publish ==
    /\ head < 2
    /\ head' = head + 1
    /\ UNCHANGED <<floor, selected, previous, phase, observedFloor, tailFloor, refreshed>>

TrimPrefix ==
    /\ floor < NextOffset(head)
    /\ floor' = NextOffset(head)
    /\ UNCHANGED <<head, selected, previous, phase, observedFloor, tailFloor, refreshed>>

Discover ==
    /\ phase = "list"
    /\ selected' \in 0..head
    /\ phase' = "probe"
    /\ UNCHANGED <<head, floor, previous, observedFloor, tailFloor, refreshed>>

Probe ==
    /\ phase = "probe"
    /\ observedFloor' = floor
    /\ phase' = IF NextOffset(selected) >= floor \/ Mutation = "skip_probe"
                THEN "ready"
                ELSE IF Mutation = "fail_fast" THEN "refused" ELSE "refresh"
    /\ refreshed' = FALSE
    /\ UNCHANGED <<head, floor, selected, previous, tailFloor>>

Refresh ==
    /\ phase = "refresh"
    /\ \E listed \in 0..head:
        IF listed > selected \/ (Mutation = "retry_same" /\ listed = selected)
        THEN /\ selected' = listed
             /\ previous' = selected
             /\ phase' = "probe"
        ELSE /\ phase' = "refused"
             /\ UNCHANGED <<selected, previous>>
    /\ refreshed' = TRUE
    /\ UNCHANGED <<head, floor, observedFloor, tailFloor>>

OpenTail ==
    /\ phase = "ready"
    /\ phase' = IF NextOffset(selected) >= floor \/ Mutation = "trust_probe" THEN "opened" ELSE "lost"
    /\ tailFloor' = floor
    /\ UNCHANGED <<head, floor, selected, previous, observedFloor, refreshed>>

Next == Publish \/ TrimPrefix \/ Discover \/ Probe \/ Refresh \/ OpenTail
Spec == Init /\ [][Next]_Vars

ObservedAdmission == phase \in {"ready", "opened", "lost"} => NextOffset(selected) >= observedFloor
OnlyNewerCheckpointsRetried == previous < selected \/ phase = "list"
RefreshBeforeRefusing == phase = "refused" => refreshed
TailStartCovered == phase = "opened" => NextOffset(selected) >= tailFloor

ReplaySafe == ObservedAdmission /\ OnlyNewerCheckpointsRetried /\ RefreshBeforeRefusing /\ TailStartCovered

NeverOpenedReplacement == ~(phase = "opened" /\ previous >= 0)
NeverLostAfterProbe == phase # "lost"

=============================================================================
