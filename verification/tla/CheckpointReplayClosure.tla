--------------------------- MODULE CheckpointReplayClosure ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Bool;
    tableChanged,
    \* @type: Bool;
    artefactPresent,
    \* @type: Int;
    plannedTable,
    \* @type: Int;
    copiedTable,
    \* @type: Set(Str);
    copied,
    \* @type: Bool;
    published

\* @type: <<Bool, Bool, Int, Int, Set(Str), Bool>>;
Vars == <<tableChanged, artefactPresent, plannedTable, copiedTable, copied, published>>

Primary == {"data", "metadata"}
Source == {"l0_data", "l0_metadata"}
Artefact == {"shared_search"}
Required == Primary \cup Source \cup Artefact

Init ==
    /\ tableChanged = FALSE /\ artefactPresent = TRUE
    /\ plannedTable = 0 /\ copiedTable = 0 /\ copied = {} /\ published = FALSE

ReplaceTable ==
    /\ ~tableChanged /\ tableChanged' = TRUE
    /\ UNCHANGED <<artefactPresent, plannedTable, copiedTable, copied, published>>

LoseArtefact ==
    /\ artefactPresent /\ artefactPresent' = FALSE
    /\ UNCHANGED <<tableChanged, plannedTable, copiedTable, copied, published>>

ObserveTable ==
    /\ plannedTable = 0
    /\ ~tableChanged \/ Mutation = "ignore_root_digest"
    /\ plannedTable' = IF tableChanged THEN 2 ELSE 1
    /\ UNCHANGED <<tableChanged, artefactPresent, copiedTable, copied, published>>

CopyTable ==
    /\ plannedTable # 0 /\ copiedTable = 0
    /\ (IF tableChanged THEN 2 ELSE 1) = plannedTable \/ Mutation = "ignore_copy_digest"
    /\ copiedTable' = IF tableChanged THEN 2 ELSE 1
    /\ UNCHANGED <<tableChanged, artefactPresent, plannedTable, copied, published>>

CopyMember ==
    /\ plannedTable # 0
    /\ \E file \in Required \ copied :
        /\ file \notin Artefact \/ artefactPresent
        /\ copied' = copied \cup {file}
    /\ UNCHANGED <<tableChanged, artefactPresent, plannedTable, copiedTable, published>>

Expected ==
    Primary \cup (IF Mutation = "omit_retained_source" THEN {} ELSE Source)
            \cup (IF Mutation = "omit_artefact" THEN {} ELSE Artefact)

Publish ==
    /\ ~published /\ plannedTable # 0
    /\ (copiedTable # 0 /\ Expected \subseteq copied) \/ Mutation = "publish_early"
    /\ published' = TRUE
    /\ UNCHANGED <<tableChanged, artefactPresent, plannedTable, copiedTable, copied>>

Next == ReplaceTable \/ LoseArtefact \/ ObserveTable \/ CopyTable \/ CopyMember \/ Publish
Spec == Init /\ [][Next]_Vars

DiscoveryIsBound == plannedTable # 0 => plannedTable = 1
CopyIsBound == copiedTable # 0 => copiedTable = plannedTable
PublicationIsComplete == published => copiedTable = 1 /\ Required \subseteq copied
NeverPublishesAfterSourceLoss == ~(published /\ ~artefactPresent)
Safe == DiscoveryIsBound /\ CopyIsBound /\ PublicationIsComplete
=========================================================================================
