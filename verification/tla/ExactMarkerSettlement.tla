---------------------------- MODULE ExactMarkerSettlement ----------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Bool;
    active,
    \* @type: Bool;
    fence,
    \* @type: Bool;
    previousData,
    \* @type: Bool;
    previousMarker,
    \* @type: Bool;
    replacement,
    \* @type: Bool;
    replacementPublished,
    \* @type: Bool;
    markerInFlight,
    \* @type: Bool;
    confirmedData,
    \* @type: Bool;
    confirmedMarker

\* @type: <<Bool, Bool, Bool, Bool, Bool, Bool, Bool, Bool, Bool>>;
vars == <<active, fence, previousData, previousMarker, replacement,
          replacementPublished, markerInFlight, confirmedData, confirmedMarker>>

Init ==
    /\ active = TRUE /\ fence = TRUE /\ previousData = TRUE /\ previousMarker = TRUE
    /\ replacement = FALSE /\ replacementPublished = FALSE /\ markerInFlight = FALSE
    /\ confirmedData = FALSE /\ confirmedMarker = FALSE

EraseData ==
    /\ active /\ previousData /\ previousData' = FALSE
    /\ UNCHANGED <<active, fence, previousMarker, replacement,
                   replacementPublished, markerInFlight, confirmedData, confirmedMarker>>

ConfirmData ==
    /\ active /\ ~previousData /\ ~confirmedData /\ confirmedData' = TRUE
    /\ UNCHANGED <<active, fence, previousData, previousMarker, replacement,
                   replacementPublished, markerInFlight, confirmedMarker>>

PublishReplacement ==
    /\ ~replacementPublished /\ replacement' = TRUE /\ replacementPublished' = TRUE
    /\ UNCHANGED <<active, fence, previousData, previousMarker, markerInFlight, confirmedData, confirmedMarker>>

SubmitMarker ==
    /\ active /\ confirmedData /\ ~markerInFlight /\ markerInFlight' = TRUE
    /\ UNCHANGED <<active, fence, previousData, previousMarker, replacement,
                   replacementPublished, confirmedData, confirmedMarker>>

EraseMarker ==
    /\ markerInFlight /\ markerInFlight' = FALSE
    /\ previousMarker' = IF Mutation = "delete_marker_by_name" /\ replacement THEN previousMarker ELSE FALSE
    /\ replacement' = IF Mutation = "delete_marker_by_name" THEN FALSE ELSE replacement
    /\ UNCHANGED <<active, fence, previousData, replacementPublished, confirmedData, confirmedMarker>>

ConfirmMarker ==
    /\ active /\ confirmedData /\ ~confirmedMarker
    /\ (~previousMarker \/ (Mutation = "confirm_on_reply" /\ markerInFlight))
    /\ confirmedMarker' = TRUE
    /\ UNCHANGED <<active, fence, previousData, previousMarker, replacement,
                   replacementPublished, markerInFlight, confirmedData>>

Settle ==
    /\ active /\ confirmedData /\ (confirmedMarker \/ Mutation = "skip_marker")
    /\ active' = FALSE /\ fence' = (Mutation = "leave_owner_closed")
    /\ UNCHANGED <<previousData, previousMarker, replacement, replacementPublished,
                   markerInFlight, confirmedData, confirmedMarker>>

Next == EraseData \/ ConfirmData \/ PublishReplacement \/ SubmitMarker \/ EraseMarker \/ ConfirmMarker \/ Settle
Spec == Init /\ [][Next]_vars

NoReplacementLost == replacementPublished => replacement
SettlementFollowsEvidence == ~active => ~previousData /\ ~previousMarker
AtomicReopen == active = fence
Safe == NoReplacementLost /\ SettlementFollowsEvidence /\ AtomicReopen
NeverSettlesWithLateMarkerEffect == ~(~active /\ replacement /\ markerInFlight)
=============================================================================
