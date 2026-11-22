------------------------- MODULE CollectorTurnover -------------------------
EXTENDS Integers

CONSTANTS Horizon, Lease, Margin, IOBound, Origins, Mutation
VARIABLES now, term, origins, retired, anchor, ioUntil, observation, ownFirst, collected

vars == <<now, term, origins, retired, anchor, ioUntil, observation, ownFirst, collected>>
NoObservation == [term |-> -1, stamp |-> -1]
LocalTime == origins[term] + now

Init ==
    /\ origins \in [0..1 -> Origins]
    /\ now = 0 /\ term = 0
    /\ retired = FALSE /\ collected = FALSE
    /\ anchor = -1 /\ ioUntil = -1
    /\ observation = NoObservation /\ ownFirst = -1

Tick ==
    /\ now < Horizon /\ now' = now + 1
    /\ UNCHANGED <<term, origins, retired, anchor, ioUntil, observation, ownFirst, collected>>

Capture ==
    /\ ~retired /\ anchor = -1 /\ anchor' = now
    /\ UNCHANGED <<now, term, origins, retired, ioUntil, observation, ownFirst, collected>>

Retire ==
    /\ ~retired /\ retired' = TRUE
    /\ UNCHANGED <<now, term, origins, anchor, ioUntil, observation, ownFirst, collected>>

\* The confirmed prefix retains the file. A later prefix excludes it after retirement.
\* Reader elapsed time is real elapsed time in this study. I/O completes by ioUntil.
AdmitIO ==
    /\ anchor >= 0 /\ now < anchor + Lease - Margin
    /\ ioUntil <= now /\ ioUntil' = now + IOBound
    /\ UNCHANGED <<now, term, origins, retired, anchor, observation, ownFirst, collected>>

Turnover ==
    /\ term = 0 /\ ~collected /\ term' = 1
    /\ ownFirst' = -1
    /\ observation' = IF Mutation = "reuse_observation" THEN observation ELSE NoObservation
    /\ UNCHANGED <<now, origins, retired, anchor, ioUntil, collected>>

Observe ==
    /\ retired /\ ownFirst = -1
    /\ ownFirst' = now
    /\ observation' = IF observation = NoObservation
                      THEN [term |-> term, stamp |-> LocalTime]
                      ELSE observation
    /\ UNCHANGED <<now, term, origins, retired, anchor, ioUntil, collected>>

Collect ==
    /\ ownFirst >= 0 /\ ~collected
    /\ LET stamp == IF Mutation = "restart_window" THEN LocalTime ELSE observation.stamp
       IN LocalTime >= stamp + Lease
    /\ collected' = TRUE
    /\ UNCHANGED <<now, term, origins, retired, anchor, ioUntil, observation, ownFirst>>

Next == Tick \/ Capture \/ Retire \/ AdmitIO \/ Turnover \/ Observe \/ Collect
Spec == Init /\ [][Next]_vars /\ WF_vars(Collect)

RetainsAdmittedIO == ioUntil > now => ~collected
WaitsForOwnObservation == collected => (observation.term = term /\ now >= ownFirst + Lease)
ReadyInSuccessor == term = 1 /\ ownFirst >= 0 /\ now >= ownFirst + Lease
ExpiryProgress == ReadyInSuccessor ~> collected
NeverReclaimsWithChangedClockOrigin == ~(collected /\ term = 1 /\ origins[0] # origins[1])
=============================================================================
