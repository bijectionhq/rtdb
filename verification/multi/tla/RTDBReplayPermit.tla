-------------------------- MODULE RTDBReplayPermit ---------------------------
EXTENDS Integers

CONSTANTS Cutoff, LastClock, CheckOriginal, CheckCapability, CheckCommand, RetainMarker, ProtectMarker
VARIABLES originalSupported, replacementSupported, single, original, replacement, lost, expiry, mutations

Vars == <<originalSupported, replacementSupported, single, original, replacement, lost, expiry, mutations>>
Keys == {"command", "original-app-key", "replacement-app-key"}

Init ==
    /\ originalSupported \in BOOLEAN
    /\ replacementSupported \in BOOLEAN
    /\ single \in BOOLEAN
    /\ original = "running"
    /\ replacement = "absent"
    /\ lost = FALSE
    /\ expiry = [k \in Keys |-> -1]
    /\ mutations = 0

Apply(key, clock, bound) ==
    LET older == clock < expiry[key]
        newExpiry == IF bound /\ RetainMarker /\ clock + 1 < Cutoff THEN Cutoff ELSE clock + 1
    IN /\ expiry' = IF older THEN expiry ELSE [expiry EXCEPT ![key] = newExpiry]
       /\ mutations' = IF older THEN mutations ELSE mutations + 1

FinishOriginal ==
    /\ original = "running"
    /\ \E clock \in 0..LastClock:
       LET bound == originalSupported /\ single
           key == IF single THEN "command" ELSE "original-app-key"
       IN IF bound /\ CheckOriginal /\ clock >= Cutoff
          THEN UNCHANGED <<expiry, mutations>>
          ELSE Apply(key, clock, bound)
    /\ original' = "done"
    /\ UNCHANGED <<originalSupported, replacementSupported, single, replacement, lost>>

LoseOriginal ==
    /\ ~lost
    /\ lost' = TRUE
    /\ UNCHANGED <<originalSupported, replacementSupported, single, original, replacement, expiry, mutations>>

SendReplacement ==
    /\ lost
    /\ replacement = "absent"
    /\ replacement' = IF ((originalSupported /\ replacementSupported) \/ ~CheckCapability) /\ (single \/ ~CheckCommand)
                      THEN "running" ELSE "unknown"
    /\ UNCHANGED <<originalSupported, replacementSupported, single, original, lost, expiry, mutations>>

FinishReplacement ==
    /\ replacement = "running"
    /\ \E clock \in 0..LastClock:
       LET key == IF single THEN "command" ELSE "replacement-app-key"
       IN IF replacementSupported /\ clock >= Cutoff
          THEN UNCHANGED <<expiry, mutations>> ELSE Apply(key, clock, replacementSupported)
    /\ replacement' = "done"
    /\ UNCHANGED <<originalSupported, replacementSupported, single, original, lost>>

EraseMarker ==
    /\ ~ProtectMarker
    /\ expiry["command"] # -1
    /\ expiry' = [expiry EXCEPT !["command"] = -1]
    /\ UNCHANGED <<originalSupported, replacementSupported, single, original, replacement, lost, mutations>>

Next == FinishOriginal \/ LoseOriginal \/ SendReplacement \/ FinishReplacement \/ EraseMarker
Spec == Init /\ [][Next]_Vars
OneMutation == mutations <= 1

=============================================================================
