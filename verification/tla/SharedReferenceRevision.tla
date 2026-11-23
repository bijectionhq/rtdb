----------------------------- MODULE SharedReferenceRevision -----------------------------
EXTENDS Integers, FiniteSets

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Set(Int);
    referrers,
    \* @type: Int;
    revision,
    \* @type: Bool;
    independent,
    \* @type: Str;
    scan,
    \* @type: Int;
    startRevision,
    \* @type: Set(Int);
    inventory,
    \* @type: Bool;
    authorizedIncomplete

\* @type: <<Set(Int), Int, Bool, Str, Int, Set(Int), Bool>>;
vars == <<referrers, revision, independent, scan, startRevision, inventory, authorizedIncomplete>>

Init ==
    /\ referrers = {} /\ revision = 0 /\ independent = FALSE
    /\ scan = "idle" /\ startRevision = 0 /\ inventory = {}
    /\ authorizedIncomplete = FALSE

\* The first fork shares from the anchor. A grandchild shares through that
\* fork and must invalidate the same inventory of the physical lineage.
PublishShared ==
    /\ \E child \in {1, 2} \ referrers:
        /\ (child = 1 \/ 1 \in referrers)
        /\ referrers' = referrers \cup {child}
        /\ revision' = IF Mutation = "parent_only" /\ child = 2 THEN revision ELSE revision + 1
    /\ UNCHANGED <<independent, scan, startRevision, inventory, authorizedIncomplete>>

PublishIndependent ==
    /\ ~independent /\ independent' = TRUE
    /\ revision' = IF Mutation = "count_independent" THEN revision + 1 ELSE revision
    /\ UNCHANGED <<referrers, scan, startRevision, inventory, authorizedIncomplete>>

Refuse ==
    /\ revision' = IF Mutation = "count_refused" /\ revision < 2 THEN revision + 1 ELSE revision
    /\ UNCHANGED <<referrers, independent, scan, startRevision, inventory, authorizedIncomplete>>

BeginScan ==
    /\ scan = "idle" /\ scan' = "reading" /\ startRevision' = revision
    /\ UNCHANGED <<referrers, revision, independent, inventory, authorizedIncomplete>>

TakeInventory ==
    /\ scan = "reading" /\ scan' = "ready" /\ inventory' = referrers
    /\ UNCHANGED <<referrers, revision, independent, startRevision, authorizedIncomplete>>

Authorize ==
    /\ scan = "ready" /\ (startRevision = revision \/ Mutation = "skip_recheck")
    /\ scan' = "accepted"
    /\ authorizedIncomplete' = (inventory # referrers)
    /\ UNCHANGED <<referrers, revision, independent, startRevision, inventory>>

Retry ==
    /\ scan = "ready" /\ startRevision # revision /\ scan' = "idle"
    /\ UNCHANGED <<referrers, revision, independent, startRevision, inventory, authorizedIncomplete>>

Next == PublishShared \/ PublishIndependent \/ Refuse \/ BeginScan \/ TakeInventory \/ Authorize \/ Retry
Spec == Init /\ [][Next]_vars

RevisionCountsSharedPublications == revision = Cardinality(referrers)
AuthorizationUsesCompleteInventory == ~authorizedIncomplete
Safe == RevisionCountsSharedPublications /\ AuthorizationUsesCompleteInventory
NeverAcceptsGrandchildInventory == ~(scan = "accepted" /\ 2 \in inventory)
NeverSeesObsoleteInventory == ~(scan = "ready" /\ inventory = {1} /\ referrers = {1, 2} /\ startRevision # revision)
=============================================================================
