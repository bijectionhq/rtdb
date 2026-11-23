--------------------------- MODULE ResidenceComposition ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    head,
    \* @type: Int;
    checkpoint,
    \* @type: Int;
    floor,
    \* @type: Int;
    query,
    \* @type: Int;
    providerRead,
    \* @type: Int;
    recovery,
    \* @type: Int;
    firstRecord,
    \* @type: Str;
    phase,
    \* @type: Set(Str);
    source,
    \* @type: Set(Str);
    forkCopy,
    \* @type: Int;
    prepared,
    \* @type: Int;
    fork,
    \* @type: Bool;
    stalePublication

vars == <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
          phase, source, forkCopy, prepared, fork, stalePublication>>

None == -1
Needs(cut) == IF cut = 0 THEN {"old", "shared"} ELSE {"new", "shared"}
Publications == {"old", "shared", "new"}

Open == phase \in {"serving", "retiring"}
Protected == {query, providerRead, recovery, checkpoint, fork} \ {None}
RetirementCuts == Protected \ (IF Mutation = "ignore_recovery" THEN {recovery} \ {query, providerRead, checkpoint, fork} ELSE {})
ServingDependencies == IF Open THEN {head, checkpoint, query, providerRead, recovery} \ {None} ELSE {}

Init ==
    /\ head = 0 /\ checkpoint = 0 /\ floor = 0
    /\ query = None /\ providerRead = None /\ recovery = None /\ firstRecord = 1
    /\ phase = "serving" /\ source = Needs(0) /\ forkCopy = {}
    /\ prepared = None /\ fork = None /\ stalePublication = FALSE

Install ==
    /\ phase = "serving" /\ head = 0
    /\ head' = 1 /\ source' = source \cup Needs(1)
    /\ UNCHANGED <<checkpoint, floor, query, providerRead, recovery, firstRecord,
                   phase, forkCopy, prepared, fork, stalePublication>>

PublishCheckpoint ==
    /\ phase = "serving" /\ checkpoint < head
    /\ checkpoint' = head
    /\ UNCHANGED <<head, floor, query, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

OpenQuery ==
    /\ Open /\ query = None
    /\ \E cut \in 0..head :
        /\ (cut >= floor \/ Mutation = "admit_below_floor")
        /\ query' = cut
    /\ UNCHANGED <<head, checkpoint, floor, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

CloseQuery ==
    /\ query # None /\ query' = None
    /\ UNCHANGED <<head, checkpoint, floor, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

BeginProviderRead ==
    /\ Open /\ query # None /\ providerRead = None
    /\ providerRead' = query
    /\ UNCHANGED <<head, checkpoint, floor, query, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

DrainProviderRead ==
    /\ providerRead # None /\ providerRead' = None
    /\ UNCHANGED <<head, checkpoint, floor, query, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

AdmitRecovery ==
    /\ Open /\ recovery = None /\ recovery' = checkpoint
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

FinishRecovery ==
    /\ recovery # None /\ recovery' = None
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

Trim ==
    /\ Open /\ firstRecord = 1 /\ checkpoint = 1
    /\ (recovery # 0 \/ Mutation = "trim_ignores_recovery")
    /\ firstRecord' = 2
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

AdvanceFloor ==
    /\ phase = "serving" /\ floor < head
    /\ floor' = head
    /\ UNCHANGED <<head, checkpoint, query, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

PrepareFork ==
    /\ phase = "serving" /\ fork = None /\ prepared = None
    /\ prepared' = head
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, fork, stalePublication>>

PublishFork ==
    /\ prepared # None /\ fork = None
    /\ (phase = "serving" \/ (Mutation = "publish_after_close" /\ phase = "disposing"))
    /\ fork' = prepared /\ prepared' = None /\ stalePublication' = (phase # "serving")
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord, phase, source, forkCopy>>

CancelPreparation ==
    /\ prepared # None /\ prepared' = None
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, fork, stalePublication>>

AuthorizeVersion ==
    /\ phase = "serving" /\ prepared = None /\ floor = 1 /\ "old" \in source
    /\ 0 \notin RetirementCuts
    /\ phase' = "retiring"
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   source, forkCopy, prepared, fork, stalePublication>>

ApplyVersionRemoval ==
    /\ phase = "retiring" /\ "old" \in source
    /\ source' = source \ (IF Mutation = "remove_by_path" THEN {"old", "new"} ELSE {"old"})
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   phase, forkCopy, prepared, fork, stalePublication>>

SettleVersion ==
    /\ phase = "retiring" /\ "old" \notin source /\ phase' = "serving"
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   source, forkCopy, prepared, fork, stalePublication>>

AuthorizeDisposal ==
    /\ phase = "serving" /\ query = None /\ recovery = None
    /\ (providerRead = None \/ Mutation = "dispose_before_drain")
    /\ (prepared = None \/ Mutation = "publish_after_close")
    /\ phase' = "disposing"
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   source, forkCopy, prepared, fork, stalePublication>>

CopyForkPublication(publication) ==
    /\ phase = "disposing" /\ fork # None
    /\ publication \in (Needs(fork) \cap source) \ forkCopy
    /\ LET copied == IF Mutation = "copy_wrong_identity"
                     THEN IF fork = 0 THEN "new" ELSE "old"
                     ELSE publication
       IN forkCopy' = forkCopy \cup {copied}
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   phase, source, prepared, fork, stalePublication>>

EraseResidencePublication(publication) ==
    /\ phase = "disposing" /\ publication \in source
    /\ (fork = None \/ publication \notin Needs(fork) \/ publication \in forkCopy
        \/ Mutation = "erase_before_copy")
    /\ source' = source \ {publication}
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   phase, forkCopy, prepared, fork, stalePublication>>

SettleDisposal ==
    /\ phase = "disposing" /\ (source = {} \/ Mutation = "settle_before_absence")
    /\ phase' = "disposed"
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   source, forkCopy, prepared, fork, stalePublication>>

Next == Install \/ PublishCheckpoint \/ OpenQuery \/ CloseQuery \/ BeginProviderRead \/ DrainProviderRead
        \/ AdmitRecovery \/ FinishRecovery \/ Trim \/ AdvanceFloor \/ PrepareFork \/ PublishFork
        \/ CancelPreparation \/ AuthorizeVersion \/ ApplyVersionRemoval \/ SettleVersion
        \/ AuthorizeDisposal \/ SettleDisposal
        \/ \E publication \in Publications : CopyForkPublication(publication) \/ EraseResidencePublication(publication)
Spec == Init /\ [][Next]_vars

RetainedCutsReadable == \A cut \in ServingDependencies : Needs(cut) \subseteq source
ReplaySurvivesTrim == Open => firstRecord <= checkpoint + 1 /\ (recovery = None \/ firstRecord <= recovery + 1)
ForkReadable == fork = None \/ Needs(fork) \subseteq (source \cup forkCopy)
ForkCopyMatches == fork # None => forkCopy \subseteq Needs(fork)
ClosedAfterDrain == ~Open => query = None /\ providerRead = None /\ recovery = None
ReceiptConfirmsAbsence == phase = "disposed" => source = {}
PublicationRespectsClose == ~stalePublication
Safe == RetainedCutsReadable /\ ReplaySurvivesTrim /\ ForkReadable
        /\ ForkCopyMatches /\ ClosedAfterDrain /\ ReceiptConfirmsAbsence /\ PublicationRespectsClose

NeverDisposesAfterTrimAndForkCopy ==
    ~(phase = "disposed" /\ head = 1 /\ firstRecord = 2 /\ fork = 1 /\ forkCopy = Needs(1))
NeverReclaimsWhileCurrentQueryOpen == ~(phase = "serving" /\ query = 1 /\ "old" \notin source)
=============================================================================
