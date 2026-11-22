---------------------------- MODULE HostedPathRelease ----------------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES kind, owned, inherited, marker, oldSlot, backfilled, swept, authorized, settled

vars == <<kind, owned, inherited, marker, oldSlot, backfilled, swept, authorized, settled>>

Init ==
    /\ kind \in {"owned", "shared"}
    /\ owned = (kind = "owned") /\ inherited = TRUE /\ marker = TRUE /\ oldSlot = TRUE
    /\ backfilled = FALSE /\ swept = FALSE /\ authorized = FALSE /\ settled = FALSE

Sweep ==
    /\ ~swept
    /\ swept' = TRUE
    /\ inherited' = (inherited /\ owned)
    /\ owned' = (owned /\ Mutation # "delete_owned_by_name")
    /\ marker' = (marker /\ Mutation # "drop_release_marker")
    /\ oldSlot' = (oldSlot /\ Mutation # "prune_hosted_slot")
    /\ UNCHANGED <<kind, backfilled, authorized, settled>>

AuthorizeExact ==
    /\ owned /\ marker /\ ~authorized
    /\ authorized' = TRUE /\ inherited' = FALSE
    /\ UNCHANGED <<kind, owned, marker, oldSlot, backfilled, swept, settled>>

SettleExact ==
    /\ authorized /\ ~settled
    /\ settled' = TRUE /\ owned' = FALSE /\ marker' = FALSE
    /\ UNCHANGED <<kind, inherited, oldSlot, backfilled, swept, authorized>>

DelayedWrite ==
    /\ ~oldSlot
    /\ oldSlot' = TRUE /\ backfilled' = TRUE
    /\ UNCHANGED <<kind, owned, inherited, marker, swept, authorized, settled>>

Next == Sweep \/ AuthorizeExact \/ SettleExact \/ DelayedWrite
Spec == Init /\ [][Next]_vars

OwnedRemovalHasAuthority == (kind = "owned" /\ ~owned) => settled
ReopenHidesReleasedBytes == owned => marker
ManifestSlotsNeverReused == ~backfilled
NeverWithdrawsInherited == ~(kind = "shared" /\ swept /\ ~inherited /\ marker)
NeverSettlesExact == ~settled
=============================================================================
