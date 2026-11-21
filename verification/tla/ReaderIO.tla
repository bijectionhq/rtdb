------------------------------- MODULE ReaderIO -------------------------------
EXTENDS Integers

CONSTANTS Mode, Lifetime, MaxSkew, IOBound, MaxTime

VARIABLES now, skew, retired, present, opened, expires, pendingSince, pinned

Vars == <<now, skew, retired, present, opened, expires, pendingSince, pinned>>

Init ==
    /\ now = 0
    /\ skew \in 0..MaxSkew
    /\ retired = -1
    /\ present = TRUE
    /\ opened = FALSE
    /\ expires = 0
    /\ pendingSince = -1
    /\ pinned = FALSE

PinMode == Mode \in {"pin", "release_during_io", "forget_pin_on_failover"}

CanAdvance ==
    \/ PinMode
    \/ Mode = "unbounded_io"
    \/ pendingSince = -1
    \/ now < pendingSince + IOBound

Tick ==
    /\ now < MaxTime
    /\ CanAdvance
    /\ now' = now + 1
    /\ UNCHANGED <<skew, retired, present, opened, expires, pendingSince, pinned>>

Admit ==
    /\ retired = -1
    /\ ~opened
    /\ pendingSince = -1
    /\ opened' = TRUE
    /\ expires' = now + Lifetime
    /\ pinned' = PinMode
    /\ UNCHANGED <<now, skew, retired, present, pendingSince>>

Replace ==
    /\ retired = -1
    /\ retired' = now
    /\ UNCHANGED <<now, skew, present, opened, expires, pendingSince, pinned>>

ReaderUpper == now - skew + (IF Mode = "raw_clock" THEN 0 ELSE MaxSkew)

CanStart ==
    /\ opened
    /\ pendingSince = -1
    /\ IF PinMode THEN pinned
       ELSE IF Mode = "inclusive_deadline" THEN ReaderUpper + IOBound <= expires
       ELSE ReaderUpper + IOBound < expires

Start ==
    /\ CanStart
    /\ pendingSince' = now
    /\ UNCHANGED <<now, skew, retired, present, opened, expires, pinned>>

Finish ==
    /\ pendingSince >= 0
    /\ pendingSince' = -1
    /\ pinned' = (pinned /\ opened)
    /\ UNCHANGED <<now, skew, retired, present, opened, expires>>

Close ==
    /\ opened
    /\ opened' = FALSE
    /\ pinned' = (pinned /\ pendingSince >= 0 /\ Mode # "release_during_io")
    /\ UNCHANGED <<now, skew, retired, present, expires, pendingSince>>

LoseAuthority ==
    /\ Mode = "forget_pin_on_failover"
    /\ pinned
    /\ pinned' = FALSE
    /\ UNCHANGED <<now, skew, retired, present, opened, expires, pendingSince>>

Reclaim ==
    /\ present
    /\ retired >= 0
    /\ IF PinMode THEN ~pinned ELSE retired + Lifetime <= now
    /\ present' = FALSE
    /\ UNCHANGED <<now, skew, retired, opened, expires, pendingSince, pinned>>

Next == Tick \/ Admit \/ Replace \/ Start \/ Finish \/ Close \/ LoseAuthority \/ Reclaim

Spec == Init /\ [][Next]_Vars

InflightHasObject == pendingSince >= 0 => present

=============================================================================
