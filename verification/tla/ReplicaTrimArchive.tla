--------------------------- MODULE ReplicaTrimArchive ---------------------------
CONSTANT Mutation
VARIABLES pin, copying, observed, authorized, removed

Vars == <<pin, copying, observed, authorized, removed>>

Init ==
    /\ pin = "absent"
    /\ copying = FALSE
    /\ observed = FALSE
    /\ authorized = FALSE
    /\ removed = FALSE

Pin ==
    /\ pin = "absent"
    /\ ~authorized
    /\ pin' = "journal"
    /\ UNCHANGED <<copying, observed, authorized, removed>>

StartCopy ==
    /\ pin = "journal"
    /\ ~copying
    /\ ~authorized
    /\ copying' = TRUE
    /\ UNCHANGED <<pin, observed, authorized, removed>>

FinishCopy ==
    /\ copying
    /\ copying' = FALSE
    /\ UNCHANGED <<pin, observed, authorized, removed>>

ObserveArchive ==
    /\ pin = "journal"
    /\ copying
    /\ ~observed
    /\ observed' = TRUE
    /\ UNCHANGED <<pin, copying, authorized, removed>>

PublishArchive ==
    /\ pin = "journal"
    /\ observed \/ Mutation = "publish_unverified"
    /\ pin' = "archive"
    /\ UNCHANGED <<copying, observed, authorized, removed>>

Expire ==
    /\ pin \in {"journal", "archive"}
    /\ pin' = "expired"
    /\ UNCHANGED <<copying, observed, authorized, removed>>

Authorize ==
    /\ ~authorized
    /\ pin # "journal" \/ Mutation = "ignore_pending_archive"
    /\ ~copying \/ Mutation = "ignore_copy_drain"
    /\ authorized' = TRUE
    /\ UNCHANGED <<pin, copying, observed, removed>>

Trim ==
    /\ authorized
    /\ ~removed
    /\ removed' = TRUE
    /\ UNCHANGED <<pin, copying, observed, authorized>>

Next == Pin \/ StartCopy \/ FinishCopy \/ ObserveArchive \/ PublishArchive \/ Expire \/ Authorize \/ Trim
Spec == Init /\ [][Next]_Vars

PendingPinRetainsJournal == pin = "journal" => ~authorized
PublishedArchiveVerified == pin = "archive" => observed
ActiveCopyRetainsJournal == copying => ~authorized
JournalReleasedDuringCapture == ~(pin = "archive" /\ removed)
=============================================================================
