--------------------------- MODULE ReplicaTrimArchive ---------------------------
CONSTANT Mutation
VARIABLES pin, observed, authorized, removed

Vars == <<pin, observed, authorized, removed>>

Init ==
    /\ pin = "absent"
    /\ observed = FALSE
    /\ authorized = FALSE
    /\ removed = FALSE

Pin ==
    /\ pin = "absent"
    /\ ~authorized
    /\ pin' = "journal"
    /\ UNCHANGED <<observed, authorized, removed>>

ObserveArchive ==
    /\ pin = "journal"
    /\ ~observed
    /\ observed' = TRUE
    /\ UNCHANGED <<pin, authorized, removed>>

PublishArchive ==
    /\ pin = "journal"
    /\ observed \/ Mutation = "publish_unverified"
    /\ pin' = "archive"
    /\ UNCHANGED <<observed, authorized, removed>>

Expire ==
    /\ pin \in {"journal", "archive"}
    /\ pin' = "expired"
    /\ UNCHANGED <<observed, authorized, removed>>

Authorize ==
    /\ ~authorized
    /\ pin # "journal" \/ Mutation = "ignore_pending_archive"
    /\ authorized' = TRUE
    /\ UNCHANGED <<pin, observed, removed>>

Trim ==
    /\ authorized
    /\ ~removed
    /\ removed' = TRUE
    /\ UNCHANGED <<pin, observed, authorized>>

Next == Pin \/ ObserveArchive \/ PublishArchive \/ Expire \/ Authorize \/ Trim
Spec == Init /\ [][Next]_Vars

PendingPinRetainsJournal == pin = "journal" => ~authorized
PublishedArchiveVerified == pin = "archive" => observed
JournalReleasedDuringCapture == ~(pin = "archive" /\ removed)
=============================================================================
