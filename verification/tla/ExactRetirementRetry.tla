----------------------------- MODULE ExactRetirementRetry -----------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Set(Int);
    versions,
    \* @type: Set(Int);
    published,
    \* @type: Set(Int);
    issued,
    \* @type: Set(Int);
    pending,
    \* @type: Bool;
    acknowledged,
    \* @type: Bool;
    observedAbsent,
    \* @type: Bool;
    settled,
    \* @type: Bool;
    completedAfterSettlement

\* @type: <<Set(Int), Set(Int), Set(Int), Set(Int), Bool, Bool, Bool, Bool>>;
Vars == <<versions, published, issued, pending, acknowledged, observedAbsent, settled, completedAfterSettlement>>

\* Version 0 is older data and can never be published again.
\* The durable authority already closed admission to version 1. Versions 2 and 3
\* can still be published at its key. Each request has the same durable intent.
Init ==
    /\ versions \in {{1}, {0, 1}} /\ published = {} /\ issued = {} /\ pending = {}
    /\ acknowledged = FALSE /\ observedAbsent = FALSE /\ settled = FALSE
    /\ completedAfterSettlement = FALSE

Submit ==
    /\ ~settled
    /\ (0 \notin versions \/ published # {} \/ Mutation = "skip_current_version_guard")
    /\ \E request \in {1, 2} \ issued:
        /\ issued' = issued \cup {request}
        /\ pending' = pending \cup {request}
    /\ UNCHANGED <<versions, published, acknowledged, observedAbsent, settled, completedAfterSettlement>>

Publish ==
    /\ \E replacement \in {2, 3} \ published:
        /\ versions' = versions \cup {replacement}
        /\ published' = published \cup {replacement}
    /\ UNCHANGED <<issued, pending, acknowledged, observedAbsent, settled, completedAfterSettlement>>

Reply ==
    /\ pending # {} /\ ~acknowledged /\ acknowledged' = TRUE
    /\ UNCHANGED <<versions, published, issued, pending, observedAbsent, settled, completedAfterSettlement>>

Remove ==
    /\ \E request \in pending: pending' = pending \ {request}
    /\ versions' = IF Mutation = "remove_path" THEN {} ELSE versions \ {1}
    /\ completedAfterSettlement' = (completedAfterSettlement \/ (settled /\ 2 \in versions))
    /\ UNCHANGED <<published, issued, acknowledged, observedAbsent, settled>>

Observe ==
    /\ issued # {} /\ ~observedAbsent
    /\ (1 \notin versions \/ Mutation = "accept_unfinished_inventory")
    /\ observedAbsent' = TRUE
    /\ UNCHANGED <<versions, published, issued, pending, acknowledged, settled, completedAfterSettlement>>

Settle ==
    /\ ~settled
    /\ observedAbsent \/ (acknowledged /\ Mutation = "accept_mutation_reply")
    /\ settled' = TRUE
    /\ UNCHANGED <<versions, published, issued, pending, acknowledged, observedAbsent, completedAfterSettlement>>

Next == Submit \/ Publish \/ Reply \/ Remove \/ Observe \/ Settle
Spec == Init /\ [][Next]_Vars

SettlementRequiresExactAbsence == settled => 1 \notin versions
ReplacementSurvivesRemoval == published \subseteq versions
OlderDataStaysHidden == versions # {0}
Safety == SettlementRequiresExactAbsence /\ ReplacementSurvivesRemoval /\ OlderDataStaysHidden
NeverCompletesAfterSettlement == ~completedAfterSettlement
=============================================================================
