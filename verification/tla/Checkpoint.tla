----------------------------- MODULE Checkpoint -----------------------------
EXTENDS FiniteSets

CONSTANT Mutation
VARIABLES durable, received, online, logRetained, recovered

Vars == <<durable, received, online, logRetained, recovered>>
Children == {"data", "metadata", "table"}
Root == "root"

Init ==
    /\ durable = {}
    /\ received = {}
    /\ online = TRUE
    /\ logRetained = TRUE
    /\ recovered = "not-opened"

PutChild(child) ==
    /\ online
    /\ child \notin durable
    /\ durable' = durable \cup {child}
    /\ UNCHANGED <<received, online, logRetained, recovered>>

Receive(child) ==
    /\ online
    /\ child \in durable \ received
    /\ received' = received \cup {child}
    /\ UNCHANGED <<durable, online, logRetained, recovered>>

AcceptCopy ==
    /\ Mutation \in {"check_copy_completion", "trust_copy_acceptance"}
    /\ online
    /\ "data" \notin durable
    /\ received \cap {"data", "pending-copy"} = {}
    /\ received' = received \cup {IF Mutation = "trust_copy_acceptance" THEN "data" ELSE "pending-copy"}
    /\ UNCHANGED <<durable, online, logRetained, recovered>>

PutRoot ==
    /\ online
    /\ Root \notin durable
    /\ (IF Mutation = "root_after_data" THEN "data" \in received ELSE Children \subseteq received)
    /\ durable' = durable \cup {Root}
    /\ UNCHANGED <<received, online, logRetained, recovered>>

Crash ==
    /\ online
    /\ online' = FALSE
    /\ received' = {}
    /\ recovered' = "not-opened"
    /\ UNCHANGED <<durable, logRetained>>

Recover(listedRoot) ==
    /\ ~online
    /\ listedRoot => Root \in durable
    /\ online' = TRUE
    /\ recovered' = IF listedRoot THEN "checkpoint" ELSE IF logRetained THEN "log" ELSE "unavailable"
    /\ UNCHANGED <<durable, received, logRetained>>

LoseChild(child) ==
    /\ Mutation = "durability_lie"
    /\ Root \in durable
    /\ child \in durable
    /\ durable' = durable \ {child}
    /\ UNCHANGED <<received, online, logRetained, recovered>>

TrimLog ==
    /\ Mutation = "trim_with_unvalidated_listing"
    /\ logRetained
    /\ Root \in received
    /\ logRetained' = FALSE
    /\ UNCHANGED <<durable, received, online, recovered>>

Next ==
    \/ \E child \in Children : PutChild(child) \/ LoseChild(child)
    \/ \E object \in Children \cup {Root} : Receive(object)
    \/ AcceptCopy \/ PutRoot \/ Crash \/ TrimLog
    \/ \E listedRoot \in BOOLEAN : Recover(listedRoot)

Spec == Init /\ [][Next]_Vars

CompletePublishedRoot == Root \in durable => Children \subseteq durable
RecoveryHasRepresentation == recovered # "unavailable"

=============================================================================
