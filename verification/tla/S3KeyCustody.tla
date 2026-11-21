----------------------------- MODULE S3KeyCustody -----------------------------

CONSTANT Mutation
VARIABLES current, historical, observed, conflict, confirmed, enrolled

Live == {"A", "B"}
Absent == "absent"
Withdrawn == "withdrawn"
Unobserved == "unobserved"
Versioned == Mutation = "ignore_versioning"
Vars == <<current, historical, observed, conflict, confirmed, enrolled>>

Init ==
    /\ current = Absent
    /\ historical = {}
    /\ observed = Unobserved
    /\ conflict = FALSE
    /\ confirmed = FALSE
    /\ enrolled = FALSE

Provision(key) ==
    /\ current = Absent \/ (Mutation = "reinstall" /\ current = Withdrawn)
    /\ current' = key
    /\ enrolled' = TRUE
    /\ UNCHANGED <<historical, observed, conflict, confirmed>>

Observe ==
    /\ observed' = current
    /\ conflict' = FALSE
    /\ UNCHANGED <<current, historical, confirmed, enrolled>>

TryWithdraw ==
    /\ observed \in Live \cup {Absent}
    /\ IF current = observed
          THEN /\ current' = Withdrawn
               /\ historical' = IF Versioned /\ current \in Live THEN historical \cup {current} ELSE historical
               /\ conflict' = FALSE
          ELSE /\ UNCHANGED <<current, historical>>
               /\ conflict' = TRUE
    /\ observed' = Unobserved
    /\ UNCHANGED <<confirmed, enrolled>>

Confirm ==
    /\ ~confirmed
    /\ current = Withdrawn \/ (Mutation = "ack_conflict" /\ conflict)
    /\ confirmed' = TRUE
    /\ UNCHANGED <<current, historical, observed, conflict, enrolled>>

Next ==
    (\E key \in Live : Provision(key)) \/ Observe \/ TryWithdraw \/ Confirm
Spec == Init /\ [][Next]_Vars

WithdrawalIsTerminal == confirmed => current = Withdrawn
NoProviderKeyAfterWithdrawal == confirmed => historical = {}
NeverWithdrawsAnInstalledKey == ~(confirmed /\ enrolled)

=============================================================================
