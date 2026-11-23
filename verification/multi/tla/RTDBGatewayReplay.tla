------------------------ MODULE RTDBGatewayReplay ------------------------
EXTENDS Integers, TLC

CONSTANTS Window, LastTick, PrivateKey, UseFallback, RefreshDeadline
VARIABLES phase, now, firstLoss, cutoff, installed, malformed, recorded, effects

Vars == <<phase, now, firstLoss, cutoff, installed, malformed, recorded, effects>>

Init ==
    /\ phase = "executing"
    /\ now = 0
    /\ firstLoss = -1
    /\ cutoff = -1
    /\ installed = TRUE
    /\ malformed \in BOOLEAN
    /\ recorded = FALSE
    /\ effects = 0

ResetSettings ==
    /\ phase = "executing"
    /\ installed
    /\ ~PrivateKey
    /\ installed' = FALSE
    /\ UNCHANGED <<phase, now, firstLoss, cutoff, malformed, recorded, effects>>

Commit ==
    LET keyed == installed /\ (~malformed \/ UseFallback)
    IN /\ phase = "executing"
       /\ phase' = "answer"
       /\ effects' = IF keyed /\ recorded THEN effects ELSE effects + 1
       /\ recorded' = (keyed \/ recorded)
       /\ UNCHANGED <<now, firstLoss, cutoff, installed, malformed>>

LoseReplyOrWriter ==
    /\ phase \in {"executing", "answer"}
    /\ phase' = "replacing"
    /\ firstLoss' = IF firstLoss = -1 THEN now ELSE firstLoss
    /\ cutoff' = IF firstLoss = -1 \/ RefreshDeadline THEN now + Window ELSE cutoff
    /\ installed' = FALSE
    /\ UNCHANGED <<now, malformed, recorded, effects>>

Replace ==
    /\ phase = "replacing"
    /\ now < cutoff
    /\ phase' = "executing"
    /\ installed' = TRUE
    /\ UNCHANGED <<now, firstLoss, cutoff, malformed, recorded, effects>>

Answer ==
    /\ phase = "answer"
    /\ phase' = "done"
    /\ UNCHANGED <<now, firstLoss, cutoff, installed, malformed, recorded, effects>>

Tick ==
    /\ now < LastTick
    /\ now' = now + 1
    /\ phase' = IF phase \in {"replacing", "executing", "answer"}
                    /\ firstLoss # -1 /\ now' >= cutoff
                 THEN "unknown"
                 ELSE phase
    /\ UNCHANGED <<firstLoss, cutoff, installed, malformed, recorded, effects>>

Next == ResetSettings \/ Commit \/ LoseReplyOrWriter \/ Replace \/ Answer \/ Tick
Spec == Init /\ [][Next]_Vars

OneEffect == effects <= 1
FixedCutoff == firstLoss = -1 \/ cutoff = firstLoss + Window
StopsWaitingAtCutoff ==
    firstLoss # -1 /\ now >= cutoff => phase \in {"done", "unknown"}

=============================================================================
