---- MODULE ResetRetirement ----
CONSTANT Mutation
VARIABLES prepared, published, adopted, receipt, floor, reader, deleted, restarted
vars == <<prepared, published, adopted, receipt, floor, reader, deleted, restarted>>

Init ==
    /\ prepared = FALSE
    /\ published = FALSE
    /\ adopted = FALSE
    /\ receipt = FALSE
    /\ floor = FALSE
    /\ reader = FALSE
    /\ deleted = FALSE
    /\ restarted = FALSE

Stage ==
    /\ ~prepared
    /\ prepared' = TRUE
    /\ UNCHANGED <<published, adopted, receipt, floor, reader, deleted, restarted>>

Publish ==
    /\ prepared
    /\ ~published
    /\ published' = TRUE
    /\ UNCHANGED <<prepared, adopted, receipt, floor, reader, deleted, restarted>>

Adopt ==
    /\ published
    /\ ~adopted
    /\ adopted' = TRUE
    /\ UNCHANGED <<prepared, published, receipt, floor, reader, deleted, restarted>>

Restart ==
    /\ ~restarted
    /\ ~deleted
    /\ restarted' = TRUE
    /\ adopted' = FALSE
    /\ UNCHANGED <<prepared, published, receipt, floor, reader, deleted>>

AdmitOldReader ==
    /\ ~reader
    /\ ~floor
    /\ ~adopted
    /\ reader' = TRUE
    /\ UNCHANGED <<prepared, published, adopted, receipt, floor, deleted, restarted>>

Drain ==
    /\ reader
    /\ reader' = FALSE
    /\ UNCHANGED <<prepared, published, adopted, receipt, floor, deleted, restarted>>

InstallReceipt ==
    /\ prepared
    /\ (adopted \/ Mutation = "skip_adoption")
    /\ ~receipt
    /\ receipt' = TRUE
    /\ UNCHANGED <<prepared, published, adopted, floor, reader, deleted, restarted>>

AdvanceFloor ==
    /\ receipt
    /\ ~floor
    /\ (~reader \/ Mutation = "ignore_reader")
    /\ floor' = TRUE
    /\ UNCHANGED <<prepared, published, adopted, receipt, reader, deleted, restarted>>

Release ==
    /\ receipt
    /\ adopted
    /\ (floor \/ Mutation = "skip_floor")
    /\ ~deleted
    /\ deleted' = TRUE
    /\ UNCHANGED <<prepared, published, adopted, receipt, floor, reader, restarted>>

Next == Stage \/ Publish \/ Adopt \/ Restart \/ AdmitOldReader \/ Drain \/ InstallReceipt \/ AdvanceFloor \/ Release

Spec == Init /\ [][Next]_vars
Safe == (receipt => published) /\ (deleted => receipt /\ floor /\ ~reader)
NeverSettlesAfterRestart == ~(restarted /\ deleted)
====
