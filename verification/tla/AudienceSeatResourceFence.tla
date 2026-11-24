---- MODULE AudienceSeatResourceFence ----
EXTENDS Naturals, FiniteSets

CONSTANTS Generations, Mutation
VARIABLES durable, requested, observed, owners, offered, last, stopped

vars == <<durable, requested, observed, owners, offered, last, stopped>>
Operations == {"read", "write", "continue"}

Init ==
    /\ durable = 1
    /\ requested = 1
    /\ observed = 1
    /\ owners = {1}
    /\ offered = {}
    /\ last = <<1, "none", 1>>
    /\ stopped = FALSE

RequestReplacement ==
    /\ requested = observed
    /\ requested < Generations
    /\ requested' = requested + 1
    /\ UNCHANGED <<durable, observed, owners, offered, last, stopped>>

Fence ==
    /\ durable < requested
    /\ Mutation # "wait_for_stop" \/ stopped
    /\ durable' = requested
    /\ owners' = CASE Mutation = "retain_old_owner" -> owners \cup {requested}
                   [] Mutation = "omit_reclaim" -> {}
                   [] OTHER -> {requested}
    /\ UNCHANGED <<requested, observed, offered, last, stopped>>

Recall ==
    /\ observed < durable
    /\ observed' = durable
    /\ UNCHANGED <<durable, requested, owners, offered, last, stopped>>

Offer(op) ==
    /\ observed \in owners
    /\ offered' = offered \cup {<<observed, op>>}
    /\ UNCHANGED <<durable, requested, observed, owners, last, stopped>>

Handle(g, op) ==
    /\ <<g, op>> \in offered
    /\ offered' = offered \ {<<g, op>>}
    /\ last' = IF g = durable \/ Mutation = op
               THEN <<g, op, durable>> ELSE last
    /\ UNCHANGED <<durable, requested, observed, owners, stopped>>

ConfirmStop ==
    /\ ~stopped
    /\ stopped' = TRUE
    /\ UNCHANGED <<durable, requested, observed, owners, offered, last>>

Next ==
    \/ RequestReplacement \/ Fence \/ Recall \/ ConfirmStop
    \/ \E op \in Operations : Offer(op)
    \/ \E g \in 1..Generations, op \in Operations : Handle(g, op)

Spec == Init /\ [][Next]_vars /\ WF_vars(Fence) /\ WF_vars(Recall)

NoSupersededIO == last[1] = last[3]
SingleSeat == Cardinality(owners) <= 1
ReclaimedAtFence == durable \in owners
ReclaimWithoutStop == \A g \in 2..Generations : (requested >= g) ~> (observed >= g)

\* A held request can still carry a physical charge after logical replacement.
NoResidualCharges == \A request \in offered : request[1] \in owners
NeverServedWithResidualCharge ==
    (last[2] # "none" /\ last[3] = durable) => NoResidualCharges

====
