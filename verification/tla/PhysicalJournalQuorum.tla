--------------------------- MODULE PhysicalJournalQuorum ---------------------------
EXTENDS Integers, FiniteSets

CONSTANT Mutation

Slots == 1..3
Declared == 1..3
Journals == 1..5
WriteQuorums == {subset \in SUBSET Declared : Cardinality(subset) = 2}

VARIABLES routes, rebound, priorWrite, votes, decision
vars == <<routes, rebound, priorWrite, votes, decision>>

PhysicalVotes == {journal \in Journals : \E slot \in Slots : votes[slot] = journal}

Init ==
    /\ routes = [slot \in Slots |-> slot]
    /\ rebound = {}
    /\ priorWrite \in WriteQuorums
    /\ votes = [slot \in Slots |-> 0]
    /\ decision = "waiting"

Rebind(slot, journal) ==
    /\ decision = "waiting" /\ slot \notin rebound /\ routes[slot] # journal
    /\ rebound' = rebound \cup {slot}
    /\ routes' = [routes EXCEPT ![slot] = journal]
    /\ UNCHANGED <<priorWrite, votes, decision>>

Vote(slot) ==
    /\ decision = "waiting" /\ votes[slot] = 0
    /\ routes[slot] = slot \/ Mutation \in {"trust_open_route", "count_endpoints"}
    /\ votes' = [votes EXCEPT ![slot] = routes[slot]]
    /\ UNCHANGED <<routes, rebound, priorWrite, decision>>

Confirm ==
    /\ decision = "waiting"
    /\ IF Mutation = "count_endpoints"
       THEN Cardinality({slot \in Slots : votes[slot] # 0}) >= 2
       ELSE Cardinality(PhysicalVotes) >= 2
    /\ decision' = "confirmed"
    /\ UNCHANGED <<routes, rebound, priorWrite, votes>>

Next ==
    \/ \E slot \in Slots, journal \in Journals : Rebind(slot, journal)
    \/ \E slot \in Slots : Vote(slot)
    \/ Confirm

Spec == Init /\ [][Next]_vars

DistinctVoteIdentities == decision = "confirmed" => Cardinality(PhysicalVotes) >= 2
OnlyAdmittedJournalsVote == decision = "confirmed" => \A slot \in Slots : votes[slot] \in {0, slot}
IntersectsPriorWrite == decision = "confirmed" => PhysicalVotes \cap priorWrite # {}
NeverConfirmsWithUnaffectedQuorum ==
    ~(decision = "confirmed" /\ rebound # {} /\ \A slot \in rebound : votes[slot] = 0)

===============================================================================
