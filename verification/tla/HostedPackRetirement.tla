---------------------------- MODULE HostedPackRetirement ----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Str;
    Mutation,
    \* @type: Set(Int);
    Members

VARIABLES
    \* @type: Set(Int);
    issued,
    \* @type: Set(Int);
    due,
    \* @type: Set(Int);
    stale,
    \* @type: Bool;
    newerRoot,
    \* @type: Bool;
    held,
    \* @type: Bool;
    referenced,
    \* @type: Bool;
    oldPresent,
    \* @type: Bool;
    replacement,
    \* @type: Bool;
    replacementPublished,
    \* @type: Str;
    phase,
    \* @type: Int;
    markers

vars == <<issued, due, stale, newerRoot, held, referenced, oldPresent,
          replacement, replacementPublished, phase, markers>>

Init ==
    /\ issued = {} /\ due = {} /\ stale = {}
    /\ newerRoot = FALSE /\ held = FALSE /\ referenced = FALSE /\ oldPresent = TRUE
    /\ replacement = FALSE /\ replacementPublished = FALSE
    /\ phase = "open" /\ markers = Cardinality(Members)

Issue(member) ==
    /\ member \in Members \ issued
    /\ issued' = issued \cup {member}
    /\ UNCHANGED <<due, stale, newerRoot, held, referenced, oldPresent, replacement, replacementPublished, phase, markers>>

Mature(member) ==
    /\ member \in issued \ due
    /\ due' = due \cup {member}
    /\ UNCHANGED <<issued, stale, newerRoot, held, referenced, oldPresent, replacement, replacementPublished, phase, markers>>

StaleEvidence(member) ==
    /\ member \in Members \ stale
    /\ stale' = stale \cup {member}
    /\ UNCHANGED <<issued, due, newerRoot, held, referenced, oldPresent, replacement, replacementPublished, phase, markers>>

PublishSuccessor ==
    /\ ~newerRoot /\ newerRoot' = TRUE
    /\ UNCHANGED <<issued, due, stale, held, referenced, oldPresent, replacement, replacementPublished, phase, markers>>

Hold ==
    /\ ~newerRoot /\ ~held /\ held' = TRUE
    /\ UNCHANGED <<issued, due, stale, newerRoot, referenced, oldPresent, replacement, replacementPublished, phase, markers>>

ReleaseHold ==
    /\ held /\ held' = FALSE
    /\ UNCHANGED <<issued, due, stale, newerRoot, referenced, oldPresent, replacement, replacementPublished, phase, markers>>

AddReference ==
    /\ phase = "open" /\ oldPresent /\ ~referenced /\ referenced' = TRUE
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, oldPresent, replacement, replacementPublished, phase, markers>>

RemoveReference ==
    /\ referenced /\ referenced' = FALSE
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, oldPresent, replacement, replacementPublished, phase, markers>>

AuthorizePack ==
    /\ phase = "open" /\ markers > 0 /\ oldPresent /\ ~replacement
    /\ (newerRoot \/ Mutation = "ignore_current_root")
    /\ (~held \/ Mutation = "ignore_hold")
    /\ (~referenced \/ Mutation = "ignore_reference")
    /\ (due = Members
        \/ (Mutation = "count_stale_evidence" /\ due \cup stale = Members)
        \/ (Mutation = "ignore_grace" /\ issued = Members)
        \/ (Mutation = "allow_partial_coverage" /\ due # {}))
    /\ phase' = "pack"
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, referenced, oldPresent, replacement, replacementPublished, markers>>

PublishReplacement ==
    /\ ~replacementPublished /\ replacement' = TRUE /\ replacementPublished' = TRUE
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, referenced, oldPresent, phase, markers>>

RemovePack ==
    /\ phase = "pack" /\ oldPresent
    /\ oldPresent' = FALSE
    /\ replacement' = IF Mutation = "erase_path" THEN FALSE ELSE replacement
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, referenced, replacementPublished, phase, markers>>

AuthorizeRemainingMarker ==
    /\ phase = "open" /\ markers > 0 /\ ~oldPresent
    /\ phase' = "marker"
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, referenced, oldPresent, replacement, replacementPublished, markers>>

Settle ==
    /\ phase # "open" /\ ~oldPresent /\ markers > 0
    /\ phase' = "open" /\ markers' = markers - 1
    /\ UNCHANGED <<issued, due, stale, newerRoot, held, referenced, oldPresent, replacement, replacementPublished>>

Next ==
    \/ \E m \in Members : (Issue(m) \/ Mature(m) \/ StaleEvidence(m))
    \/ PublishSuccessor \/ Hold \/ ReleaseHold \/ AddReference \/ RemoveReference
    \/ AuthorizePack \/ PublishReplacement \/ RemovePack \/ AuthorizeRemainingMarker \/ Settle

Spec == Init /\ [][Next]_vars

CompleteCoverage == phase = "pack" => due = Members
SuccessorAvailable == phase = "pack" => newerRoot
HoldsRespected == phase = "pack" => ~held /\ ~referenced
ReplacementSurvives == replacementPublished => replacement
Safe == CompleteCoverage /\ SuccessorAvailable /\ HoldsRespected /\ ReplacementSurvives
NeverSettlesAllMarkersAfterReplacement == ~(markers = 0 /\ replacement)
=============================================================================
