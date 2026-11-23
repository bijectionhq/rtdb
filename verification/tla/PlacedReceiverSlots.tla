--------------------------- MODULE PlacedReceiverSlots ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANT Defect

Slots == 1..3
Lane(a) == IF a = 3 THEN {3} ELSE {1, 2}

VARIABLES intake, online, writer, admitted, open, debt, expired, applied, journal, held
Vars == <<intake, online, writer, admitted, open, debt, expired, applied, journal, held>>

Init ==
    /\ intake = {} /\ online = TRUE /\ writer = 1
    /\ admitted = {} /\ open = {} /\ debt = {} /\ expired = {} /\ applied = {}
    /\ journal = [a \in Slots |-> "none"] /\ held = journal

Offer(a) ==
    /\ a \notin intake
    /\ intake' = intake \cup {a}
    /\ UNCHANGED <<online, writer, admitted, open, debt, expired, applied, journal, held>>

Reserve(a) ==
    /\ online /\ a \in intake \ admitted /\ Lane(a) \cap debt = {}
    /\ admitted' = admitted \cup {a} /\ open' = open \cup {a} /\ debt' = debt \cup {a}
    /\ UNCHANGED <<intake, online, writer, expired, applied, journal, held>>

Apply(a) ==
    /\ online
    /\ (a \in open \/ (Defect = "volatile_apply" /\ a \in intake \ admitted)
        \/ (Defect = "late_copy" /\ journal[a] = "fenced"))
    /\ applied' = applied \cup {a}
    /\ open' = open \ {a} /\ debt' = debt \ {a}
    /\ journal' = IF Defect = "split_witness" \/ journal[a] = "fenced" THEN journal
                  ELSE [journal EXCEPT ![a] = "applied"]
    /\ UNCHANGED <<intake, online, writer, admitted, expired, held>>

Fence(a) ==
    /\ online /\ a \in open
    /\ journal' = [journal EXCEPT ![a] = "fenced"]
    /\ open' = open \ {a} /\ debt' = debt \ {a}
    /\ UNCHANGED <<intake, online, writer, admitted, expired, applied, held>>

Expire(a) ==
    /\ a \in admitted \ expired
    /\ expired' = expired \cup {a}
    /\ debt' = IF Defect = "expire_credit" THEN debt \ {a} ELSE debt
    /\ UNCHANGED <<intake, online, writer, admitted, open, applied, journal, held>>

Hold ==
    /\ held # journal /\ held' = journal
    /\ UNCHANGED <<intake, online, writer, admitted, open, debt, expired, applied, journal>>

Crash ==
    /\ online /\ writer = 1
    /\ online' = FALSE /\ intake' = {}
    /\ UNCHANGED <<writer, admitted, open, debt, expired, applied, journal, held>>

Successor ==
    /\ ~online /\ writer = 1
    /\ online' = TRUE /\ writer' = 2
    /\ journal' = IF Defect = "stale_cut" THEN held ELSE journal
    /\ debt' = IF Defect = "stale_cut" THEN {} ELSE debt
    /\ UNCHANGED <<intake, admitted, open, expired, applied, held>>

Next ==
    \/ Hold \/ Crash \/ Successor
    \/ \E a \in Slots : Offer(a) \/ Reserve(a) \/ Apply(a) \/ Fence(a) \/ Expire(a)

Spec == Init /\ [][Next]_Vars
Fair == Spec /\ WF_Vars(Successor) /\ \A a \in Slots : WF_Vars(Fence(a) \/ Apply(a))

DurableAdmission == applied \subseteq admitted
AtomicWitness == applied = {a \in Slots : journal[a] = "applied"}
FundedOpenSlots == open = debt /\ open = {a \in admitted : journal[a] = "none"}
ScopedCapacity == \A a \in Slots : Cardinality(Lane(a) \cap debt) <= 1
AfterFence == \A a \in applied : journal[a] # "fenced"
Drain == \A a \in Slots : (a \in open) ~> (journal[a] # "none")
NoExpiredPending == expired \cap open = {}

===============================================================================
