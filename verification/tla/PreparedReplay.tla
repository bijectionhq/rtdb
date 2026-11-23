---------------------------- MODULE PreparedReplay ----------------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES head, floor, selected, previous, phase, admittedFloor, cursorUsed, refreshed

Vars == <<head, floor, selected, previous, phase, admittedFloor, cursorUsed, refreshed>>
FirstRequired(block) == 2 * block + 1

Init ==
    /\ head = 0
    /\ floor = 0
    /\ selected = -1
    /\ previous = -1
    /\ phase = "discover"
    /\ admittedFloor = -1
    /\ cursorUsed = 0
    /\ refreshed = FALSE

Publish ==
    /\ head < 2
    /\ head' = head + 1
    /\ UNCHANGED <<floor, selected, previous, phase, admittedFloor, cursorUsed, refreshed>>

Trim ==
    /\ floor < FirstRequired(head)
    /\ floor' = FirstRequired(head)
    /\ UNCHANGED <<head, selected, previous, phase, admittedFloor, cursorUsed, refreshed>>

Discover ==
    /\ phase = "discover"
    /\ selected' \in 0..head
    /\ phase' = "admit"
    /\ UNCHANGED <<head, floor, previous, admittedFloor, cursorUsed, refreshed>>

Admit ==
    /\ phase = "admit"
    /\ IF Mutation = "install_without_admission"
          THEN /\ phase' = "prepared"
               /\ UNCHANGED admittedFloor
          ELSE IF FirstRequired(selected) >= floor
               THEN /\ phase' = "prepared"
                    /\ admittedFloor' = floor
               ELSE /\ phase' = IF Mutation = "refuse_without_refresh" THEN "refused" ELSE "refresh"
                    /\ UNCHANGED admittedFloor
    /\ refreshed' = FALSE
    /\ UNCHANGED <<head, floor, selected, previous, cursorUsed>>

Refresh ==
    /\ phase = "refresh"
    /\ \E listed \in 0..head:
         IF listed > selected
           THEN /\ selected' = listed
                /\ previous' = selected
                /\ phase' = "admit"
           ELSE /\ phase' = "refused"
                /\ UNCHANGED <<selected, previous>>
    /\ refreshed' = TRUE
    /\ UNCHANGED <<head, floor, admittedFloor, cursorUsed>>

Install ==
    /\ phase = "prepared"
    /\ phase' = "installed"
    /\ UNCHANGED <<head, floor, selected, previous, admittedFloor, cursorUsed, refreshed>>

RunDriver ==
    /\ phase = "installed"
    /\ cursorUsed' = IF Mutation = "reopen" THEN 2 ELSE 1
    /\ phase' = "replaying"
    /\ UNCHANGED <<head, floor, selected, previous, admittedFloor, refreshed>>

Next == Publish \/ Trim \/ Discover \/ Admit \/ Refresh \/ Install \/ RunDriver
Spec == Init /\ [][Next]_Vars

InstallHasAdmission ==
    phase \in {"installed", "replaying"} => admittedFloor \in 0..FirstRequired(selected)
DriverUsesPrepared == phase = "replaying" => cursorUsed = 1
OnlyNewerCheckpointsRetried == phase = "discover" \/ previous < selected
RefreshBeforeRefusing == phase = "refused" => refreshed

NeverReplaysReplacement == ~(phase = "replaying" /\ previous >= 0)
NeverTrimmedAfterAdmission == ~(phase = "installed" /\ FirstRequired(selected) < floor)

=============================================================================
