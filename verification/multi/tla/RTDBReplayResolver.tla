------------------------- MODULE RTDBReplayResolver -------------------------
EXTENDS Naturals

CONSTANTS LegacyAssertion, MaxAttempts
VARIABLES appended, resolved, marker, effects

Vars == <<appended, resolved, marker, effects>>

Init ==
    /\ appended = 0
    /\ resolved = 0
    /\ marker = FALSE
    /\ effects = 0

Append ==
    /\ appended < MaxAttempts
    /\ appended' = appended + 1
    /\ UNCHANGED <<resolved, marker, effects>>

Resolve ==
    /\ resolved < appended
    /\ resolved' = resolved + 1
    /\ \E supportsAssertion \in BOOLEAN:
        IF supportsAssertion
        THEN /\ marker' = TRUE
             /\ effects' = IF marker THEN effects ELSE effects + 1
        ELSE /\ UNCHANGED marker
             /\ effects' = IF LegacyAssertion THEN effects ELSE effects + 1
    /\ UNCHANGED appended

Spec == Init /\ [][Append \/ Resolve]_Vars
OneEffect == effects <= 1

=============================================================================
