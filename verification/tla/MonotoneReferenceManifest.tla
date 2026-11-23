--------------------------- MODULE MonotoneReferenceManifest ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

Names == {1, 2, 3}

VARIABLES
    \* @type: Int;
    head,
    \* @type: Set(Int);
    current,
    \* @type: Set(Int);
    withdrawn,
    \* @type: Str;
    writer,
    \* @type: Int;
    plannedAt,
    \* @type: Set(Int);
    planned,
    \* @type: Int;
    backfilled,
    \* @type: Int;
    listed,
    \* @type: Bool;
    inconsistentRead,
    \* @type: Bool;
    localCopy,
    \* @type: Bool;
    exposedFallback,
    \* @type: Bool;
    advancedAfterBackfill

\* @type: <<Int, Set(Int), Set(Int), Str, Int, Set(Int), Int, Int, Bool, Bool, Bool, Bool>>;
Vars == <<head, current, withdrawn, writer, plannedAt, planned, backfilled, listed,
          inconsistentRead, localCopy, exposedFallback, advancedAfterBackfill>>

Init ==
    /\ head = 0 /\ current = Names /\ withdrawn = {}
    /\ writer = "idle" /\ plannedAt = 0 /\ planned = {} /\ backfilled = -1 /\ listed = -1
    /\ inconsistentRead = FALSE /\ localCopy = TRUE /\ exposedFallback = FALSE
    /\ advancedAfterBackfill = FALSE

\* One delayed request withdraws name 1. Foreground requests withdraw 2 and 3.
Prepare ==
    /\ writer = "idle" /\ 1 \in current
    /\ writer' = "prepared" /\ plannedAt' = head /\ planned' = current \ {1}
    /\ UNCHANGED <<head, current, withdrawn, backfilled, listed, inconsistentRead,
                   localCopy, exposedFallback, advancedAfterBackfill>>

OtherWithdrawal ==
    /\ \E name \in current \ {1}:
        /\ head' = head + 1 /\ current' = current \ {name}
        /\ withdrawn' = withdrawn \cup {name}
    /\ backfilled' = -1
    /\ UNCHANGED <<writer, plannedAt, planned, listed, inconsistentRead,
                   localCopy, exposedFallback, advancedAfterBackfill>>

Write ==
    /\ writer = "prepared" /\ writer' = "submitted"
    /\ LET sequence == IF Mutation = "append_stale_whole_manifest" THEN head + 1 ELSE plannedAt + 1
       IN /\ head' = IF sequence > head THEN sequence ELSE head
          /\ current' = IF sequence > head THEN planned ELSE current
          /\ backfilled' = IF sequence < head THEN sequence ELSE -1
    /\ withdrawn' = withdrawn \cup (current \ current')
    /\ advancedAfterBackfill' = (advancedAfterBackfill \/ (backfilled # -1 /\ head' > head))
    /\ UNCHANGED <<plannedAt, planned, listed, inconsistentRead, localCopy, exposedFallback>>

Rebase ==
    /\ writer = "submitted" /\ 1 \in current
    /\ writer' = "prepared" /\ plannedAt' = head /\ planned' = current \ {1}
    /\ UNCHANGED <<head, current, withdrawn, backfilled, listed, inconsistentRead,
                   localCopy, exposedFallback, advancedAfterBackfill>>

Confirm ==
    /\ writer = "submitted" /\ (1 \notin current \/ Mutation = "confirm_put_reply")
    /\ writer' = "confirmed"
    /\ UNCHANGED <<head, current, withdrawn, plannedAt, planned, backfilled, listed,
                   inconsistentRead, localCopy, exposedFallback, advancedAfterBackfill>>

ListForRead ==
    /\ listed = -1 /\ listed' = head
    /\ UNCHANGED <<head, current, withdrawn, writer, plannedAt, planned, backfilled,
                   inconsistentRead, localCopy, exposedFallback, advancedAfterBackfill>>

Read ==
    /\ listed # -1 /\ listed' = -1
    /\ (listed = head \/ (listed = backfilled /\ Mutation = "trust_backfilled_read"))
    /\ inconsistentRead' = (inconsistentRead \/ listed # head)
    /\ UNCHANGED <<head, current, withdrawn, writer, plannedAt, planned, backfilled,
                   localCopy, exposedFallback, advancedAfterBackfill>>

Refresh ==
    /\ listed # -1 /\ listed # head /\ listed' = head
    /\ UNCHANGED <<head, current, withdrawn, writer, plannedAt, planned, backfilled,
                   inconsistentRead, localCopy, exposedFallback, advancedAfterBackfill>>

RemoveLocalCopy ==
    /\ localCopy /\ (1 \notin current \/ Mutation = "remove_before_unlink")
    /\ localCopy' = FALSE /\ exposedFallback' = (1 \in current)
    /\ UNCHANGED <<head, current, withdrawn, writer, plannedAt, planned, backfilled,
                   listed, inconsistentRead, advancedAfterBackfill>>

Next == Prepare \/ OtherWithdrawal \/ Write \/ Rebase \/ Confirm \/ ListForRead \/ Read \/ Refresh \/ RemoveLocalCopy
Spec == Init /\ [][Next]_Vars

NoWithdrawnReferenceReturns == withdrawn \cap current = {}
ConfirmedWithdrawalIsCurrent == writer = "confirmed" => 1 \notin current
ReadUsesThePublishedHead == ~inconsistentRead
NoFallbackExposed == ~exposedFallback
Safe == NoWithdrawnReferenceReturns /\ ConfirmedWithdrawalIsCurrent /\ ReadUsesThePublishedHead /\ NoFallbackExposed
NeverConfirmsAfterBackfill == ~(advancedAfterBackfill /\ writer = "confirmed")
NeverRemovesAfterUnlink == localCopy
=============================================================================
