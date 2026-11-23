----------------------------- MODULE FrozenReceiptCut -----------------------------
EXTENDS Integers

CONSTANT Mutation

Selected == [publication |-> 1, journal |-> 1, cut |-> 1, witness |-> 1]
Replacement == [publication |-> 2, journal |-> 1, cut |-> 1, witness |-> 3]
Roots == {Selected, Replacement}
Empty == [publication |-> 0, journal |-> 0, cut |-> 0, witness |-> 0]

Pending == {[kind |-> "pending", journal |-> 1, cut |-> 0, attempt |-> id] : id \in 1..3}
Applied == {[kind |-> "applied", journal |-> journal, cut |-> cut, attempt |-> 0] :
            journal \in 1..2, cut \in 1..2}
Tokens == Pending \cup Applied
NoToken == [kind |-> "", journal |-> 0, cut |-> 0, attempt |-> 0]

VARIABLES loaded, request, reply, served
vars == <<loaded, request, reply, served>>

Init == /\ loaded = Empty /\ request = NoToken /\ reply = "" /\ served = 0

Install(root) ==
    /\ loaded = Empty
    /\ root = Selected \/ Mutation = "trust_same_position"
    /\ loaded' = root
    /\ UNCHANGED <<request, reply, served>>

Covered(root, token) ==
    IF token.kind = "applied"
    THEN token.journal = root.journal /\ token.cut <= root.cut
    ELSE token.attempt = root.witness

Confirms(token) ==
    IF token.kind = "applied"
    THEN (token.journal = loaded.journal \/ Mutation = "ignore_journal")
         /\ (token.cut <= loaded.cut \/ Mutation = "ignore_bound")
    ELSE token.attempt = loaded.witness

Redeem(token) ==
    /\ loaded # Empty /\ request = NoToken
    /\ request' = token
    /\ reply' = IF Confirms(token) THEN "confirmed"
                ELSE IF token.kind = "pending" /\ Mutation = "absence_is_final" THEN "unresolved"
                ELSE "behind"
    /\ served' = IF reply' = "confirmed" THEN loaded.cut ELSE 0
    /\ UNCHANGED loaded

Next == (\E root \in Roots : Install(root)) \/ (\E token \in Tokens : Redeem(token))

Spec == Init /\ [][Next]_vars

ConfirmedAgainstSelectedRoot == reply = "confirmed" => loaded = Selected
ConfirmedWithinCut == reply = "confirmed" => Covered(loaded, request) /\ served = loaded.cut
FrozenAbsenceIsRetryable == reply # "unresolved"
NeverConfirms == reply # "confirmed"

===============================================================================
