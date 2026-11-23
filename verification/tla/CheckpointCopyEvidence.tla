--------------------------- MODULE CheckpointCopyEvidence ----------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    source,
    \* @type: Int;
    metadata,
    \* @type: Bool;
    changed,
    \* @type: Int;
    planned,
    \* @type: Int;
    supplied,
    \* @type: Int;
    copied,
    \* @type: Int;
    copiedMetadata,
    \* @type: Bool;
    published

\* @type: <<Int, Int, Bool, Int, Int, Int, Int, Bool>>;
Vars == <<source, metadata, changed, planned, supplied, copied, copiedMetadata, published>>

\* 0 is a response with no identity. 1 and 2 are two versions at the same
\* inherited path. 3 is a materialized copy and 4 belongs to an archive.
PhysicalIds == 0..4
OpaqueVersion(identity) == IF identity \in {1, 3, 4} THEN 1 ELSE identity

Init ==
    /\ source \in {0, 1, 3, 4} /\ metadata = 1 /\ changed = FALSE
    /\ planned = -1 /\ supplied = -1 /\ copied = -1
    /\ copiedMetadata = 0 /\ published = FALSE

Change ==
    /\ ~changed /\ changed' = TRUE
    /\ source' \in PhysicalIds /\ metadata' \in {1, 2}
    /\ UNCHANGED <<planned, supplied, copied, copiedMetadata, published>>

Plan ==
    /\ planned = -1 /\ planned' = source
    /\ UNCHANGED <<source, metadata, changed, supplied, copied, copiedMetadata, published>>

Copy ==
    /\ planned # -1 /\ copied = -1
    /\ \/ planned = source
       \/ Mutation = "trust_version_token" /\ OpaqueVersion(planned) = OpaqueVersion(source)
       \/ Mutation = "accept_lost_evidence" /\ source = 0
       \/ Mutation = "fabricate_receipt"
    /\ metadata = 1 \/ Mutation = "ignore_metadata"
    /\ supplied' = source
    /\ copied' = IF Mutation = "fabricate_receipt" THEN planned ELSE source
    /\ copiedMetadata' = metadata
    /\ UNCHANGED <<source, metadata, changed, planned, published>>

Publish ==
    /\ ~published /\ copied # -1 /\ published' = TRUE
    /\ UNCHANGED <<source, metadata, changed, planned, supplied, copied, copiedMetadata>>

Next == Change \/ Plan \/ Copy \/ Publish
Spec == Init /\ [][Next]_Vars

PlanMatchesBody == copied # -1 => planned = supplied
ReceiptNamesBody == copied # -1 => copied = supplied
AuthenticatedMetadata == copied # -1 => copiedMetadata = 1
Safe == PlanMatchesBody /\ ReceiptNamesBody /\ AuthenticatedMetadata

NeverPublishesInherited == ~(published /\ copied = 1)
NeverPublishesArchived == ~(published /\ copied = 4)
NeverPublishesUnversioned == ~(published /\ copied = 0)
======================================================================================
