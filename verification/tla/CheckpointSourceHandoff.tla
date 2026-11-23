-------------------------- MODULE CheckpointSourceHandoff ---------------------------
EXTENDS Integers

CONSTANTS
    \* @type: Str;
    Design,
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    latest,
    \* @type: Set(Str);
    resident,
    \* @type: Set(Str);
    authorized,
    \* @type: Str;
    phase,
    \* @type: Int;
    selected,
    \* @type: Int;
    hold,
    \* @type: Bool;
    reading,
    \* @type: Bool;
    copied,
    \* @type: Bool;
    durable,
    \* @type: Bool;
    cancelledWithRead,
    \* @type: Bool;
    orphanRemovedDuringRead

\* @type: <<Int, Set(Str), Set(Str), Str, Int, Int, Bool, Bool, Bool, Bool, Bool>>;
Vars == <<latest, resident, authorized, phase, selected, hold, reading, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

Roots == {1, 2}

\* Exact publication identities under one source custody.
\* 'inherited-pack' contains an object read through a reference to another store.
\* The new checkpoint reads an independent copy of that member.
\* 'shared' is named by both cuts. 'orphan' is required by neither.
Required(cut) ==
    CASE cut = 1 -> {"pack-1", "inherited-pack", "shared"}
      [] cut = 2 -> {"pack-2", "relocated-pack", "shared"}
      [] OTHER -> {}

AllPublications == Required(1) \cup Required(2) \cup {"orphan"}

AdmittedCover(cut) == IF Design = "residence" THEN AllPublications ELSE Required(cut)
RetirementCover(cut) ==
    IF cut = 0 THEN {}
    ELSE IF Mutation = "ignore_inherited_pack" THEN AdmittedCover(cut) \ {"inherited-pack"}
    ELSE AdmittedCover(cut)

Init ==
    /\ latest = 1 /\ resident = AllPublications /\ authorized = {}
    /\ phase = "idle" /\ selected = 0 /\ hold = 0
    /\ reading = FALSE /\ copied = FALSE /\ durable = FALSE /\ cancelledWithRead = FALSE
    /\ orphanRemovedDuringRead = FALSE

Adopt ==
    /\ latest = 1 /\ Required(2) \subseteq resident /\ latest' = 2
    /\ UNCHANGED <<resident, authorized, phase, selected, hold, reading, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

Eligible ==
    {"orphan"} \cup (IF latest = 2 THEN Required(1) \ Required(2) ELSE {})
               \cup (IF Mutation = "authorize_current_root" THEN Required(latest) ELSE {})

Authorize ==
    /\ \E publication \in (Eligible \cap resident) \ authorized :
        /\ publication \notin RetirementCover(hold)
        /\ authorized' = authorized \cup {publication}
    /\ UNCHANGED <<latest, resident, phase, selected, hold, reading, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

Remove ==
    /\ \E publication \in authorized \cap resident :
        /\ resident' = resident \ {publication}
        /\ orphanRemovedDuringRead' = (orphanRemovedDuringRead \/ (reading /\ publication = "orphan"))
    /\ UNCHANGED <<latest, authorized, phase, selected, hold, reading, copied, durable, cancelledWithRead>>

Admit ==
    /\ phase = "idle"
    /\ \E cut \in Roots :
        /\ cut <= latest
        /\ Required(cut) \subseteq resident
        /\ (AdmittedCover(cut) \cap authorized \cap resident = {}) \/ Mutation = "ignore_authorized_removal"
        /\ selected' = cut
        /\ hold' = IF Mutation = "hold_wrong_cut" THEN 3 - cut ELSE cut
    /\ phase' = "copying"
    /\ UNCHANGED <<latest, resident, authorized, reading, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

BeginRead ==
    /\ phase = "copying" /\ ~reading /\ ~copied
    /\ reading' = TRUE
    /\ UNCHANGED <<latest, resident, authorized, phase, selected, hold, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

EndRead ==
    /\ reading /\ Required(selected) \subseteq resident
    /\ reading' = FALSE /\ copied' = (phase = "copying")
    /\ UNCHANGED <<latest, resident, authorized, phase, selected, hold, durable, cancelledWithRead, orphanRemovedDuringRead>>

AcceptDestination ==
    /\ phase = "copying" /\ copied /\ ~durable
    /\ durable' = TRUE
    /\ UNCHANGED <<latest, resident, authorized, phase, selected, hold, reading, copied, cancelledWithRead, orphanRemovedDuringRead>>

PublishDestination ==
    /\ phase = "copying" /\ ~reading
    /\ Mutation # "omit_handoff"
    /\ durable \/ (copied /\ Mutation = "publish_staged")
    /\ phase' = "published" /\ hold' = 0
    /\ UNCHANGED <<latest, resident, authorized, selected, reading, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

Cancel ==
    /\ phase = "copying" /\ phase' = "closing"
    /\ hold' = IF Mutation = "release_before_drain" THEN 0 ELSE hold
    /\ cancelledWithRead' = reading
    /\ UNCHANGED <<latest, resident, authorized, selected, reading, copied, durable, orphanRemovedDuringRead>>

Drain ==
    /\ phase = "closing" /\ ~reading
    /\ phase' = "cancelled" /\ hold' = 0
    /\ UNCHANGED <<latest, resident, authorized, selected, reading, copied, durable, cancelledWithRead, orphanRemovedDuringRead>>

Next == Adopt \/ Authorize \/ Remove \/ Admit \/ BeginRead \/ EndRead \/ AcceptDestination \/ PublishDestination \/ Cancel \/ Drain
Spec == Init /\ [][Next]_Vars
FairSpec ==
    /\ Spec
    /\ WF_Vars(BeginRead) /\ WF_Vars(EndRead) /\ WF_Vars(AcceptDestination)
    /\ WF_Vars(PublishDestination) /\ WF_Vars(Drain)

CopyEventuallyFinishes == (phase = "copying" \/ phase = "closing") ~> (phase = "published" \/ phase = "cancelled")

AdmissionIsBound == hold # 0 => hold = selected /\ selected <= latest
ProviderRetainsClaim == (phase = "copying" \/ reading) => hold = selected /\ hold # 0
SourceIsRetained == hold # 0 => Required(hold) \subseteq resident \ authorized
CurrentIsRetained == Required(latest) \subseteq resident \ authorized
PublicationIsDurable == phase = "published" => copied /\ durable /\ ~reading /\ hold = 0
Safe == AdmissionIsBound /\ ProviderRetainsClaim /\ SourceIsRetained /\ CurrentIsRetained /\ PublicationIsDurable

NeverPublishes == phase # "published"
NeverReclaimsWhileHeld == ~orphanRemovedDuringRead
NeverDrainsCancelledRead == ~(phase = "cancelled" /\ cancelledWithRead)
======================================================================================
