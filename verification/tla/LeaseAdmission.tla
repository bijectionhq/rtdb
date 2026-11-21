-------------------------- MODULE LeaseAdmission ---------------------------
EXTENDS Naturals, Integers

CONSTANTS Mode, Lease, MaxTime
VARIABLES now, current, seen, retired, oldPresent, readGeneration, expires

Vars == <<now, current, seen, retired, oldPresent, readGeneration, expires>>

Init ==
    /\ now = 0
    /\ current = 0
    /\ seen = 0
    /\ retired = -1
    /\ oldPresent = TRUE
    /\ readGeneration = -1
    /\ expires = 0

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<current, seen, retired, oldPresent, readGeneration, expires>>

Replace ==
    /\ current = 0
    /\ current' = 1
    /\ retired' = now
    /\ UNCHANGED <<now, seen, oldPresent, readGeneration, expires>>

Refresh ==
    /\ seen < current
    /\ seen' = current
    /\ UNCHANGED <<now, current, retired, oldPresent, readGeneration, expires>>

Admit ==
    /\ readGeneration = -1
    /\ seen = current \/ Mode = "stale_follower" \/ Mode = "age_only"
    /\ readGeneration' = seen
    /\ expires' = now + Lease
    /\ UNCHANGED <<now, current, seen, retired, oldPresent>>

Release ==
    /\ readGeneration # -1
    /\ readGeneration' = -1
    /\ UNCHANGED <<now, current, seen, retired, oldPresent, expires>>

Reclaim ==
    /\ current = 1
    /\ oldPresent
    /\ IF Mode = "pin" THEN readGeneration # 0 ELSE retired + Lease <= now
    /\ oldPresent' = FALSE
    /\ UNCHANGED <<now, current, seen, retired, readGeneration, expires>>

Next == Tick \/ Replace \/ Refresh \/ Admit \/ Release \/ Reclaim
Spec == Init /\ [][Next]_Vars

CanStillRead == readGeneration # -1 /\
    (Mode = "pin" \/ Mode = "age_only" \/ now < expires)

LiveReaderHasObject == (CanStillRead /\ readGeneration = 0) => oldPresent

=============================================================================
