-------------------------- MODULE FragmentResidence --------------------------
EXTENDS Naturals

CONSTANT Mutation

Origin == [storage |-> 0, epoch |-> 0]
Moved == [storage |-> 1, epoch |-> 0]
NewJournal == [storage |-> 0, epoch |-> 1]
None == [storage |-> 2, epoch |-> 2]

VARIABLES route, opened, requested, captured, phase, answer
vars == <<route, opened, requested, captured, phase, answer>>

Init ==
    /\ route = Origin
    /\ opened = Origin
    /\ requested = None
    /\ captured = None
    /\ phase = "planning"
    /\ answer = None

Replace ==
    /\ route = Origin
    /\ route' \in {Moved, NewJournal}
    /\ UNCHANGED <<opened, requested, captured, phase, answer>>

OpenReplacement ==
    /\ opened # route
    /\ opened' = route
    /\ UNCHANGED <<route, requested, captured, phase, answer>>

Plan ==
    /\ phase = "planning"
    /\ requested' = IF Mutation = "rebind_to_route" THEN route ELSE Origin
    /\ phase' = IF requested' = route THEN "sent" ELSE "fallback"
    /\ answer' = IF phase' = "fallback" THEN Origin ELSE None
    /\ UNCHANGED <<route, opened, captured>>

Matches ==
    CASE Mutation = "trust_plan" -> TRUE
      [] Mutation = "ignore_epoch" -> opened.storage = requested.storage
      [] OTHER -> opened = requested

Receive ==
    /\ phase = "sent"
    /\ phase' = IF Matches THEN "reading" ELSE "fallback"
    /\ captured' = opened
    /\ answer' = IF phase' = "fallback" THEN Origin ELSE None
    /\ UNCHANGED <<route, opened, requested>>

Complete ==
    /\ phase = "reading"
    /\ phase' = "answered"
    /\ answer' = captured
    /\ UNCHANGED <<route, opened, requested, captured>>

Next == Replace \/ OpenReplacement \/ Plan \/ Receive \/ Complete
Spec == Init /\ [][Next]_vars

SameHistory == phase \in {"answered", "fallback"} => answer = Origin
NoMatchingResponse == phase # "answered"
NoReceiverFallback == phase # "fallback" \/ captured \notin {Moved, NewJournal}
=============================================================================
