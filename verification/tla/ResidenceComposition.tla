--------------------------- MODULE ResidenceComposition ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation,
    \* @type: Bool;
    ExtendAuthorities

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
    stalePublication,
    \* @type: {systemTime: Int, rawTime: Int, receiptClock: Int, clockRegressed: Bool, mode: Str, requestedTime: Int, decision: Str, retained: Bool, redemption: Str, redemptionBody: Str, redemptionExpired: Bool, namingEpoch: Int, loglet: Int, indexFor: Int, indexThrough: Int, captured: Int, captureLoglet: Int, confirmed: Int, erasure: Str, cacheResult: Str, certified: Int, publications: Set(Int), acknowledged: Int, activationFloor: Int, recoveryFence: Int, deletion: Str, delivery: {state: Str, time: Int, deadline: Int, lastWrite: Int}};
    authority

vars == <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
          phase, source, forkCopy, prepared, fork, stalePublication>>

allVars == <<vars, authority>>

None == -1
Needs(cut) == IF cut = 0 THEN {"old", "shared"} ELSE {"new", "shared"}
Publications == {"old", "shared", "new"}

Open == phase \in {"serving", "retiring"}
Protected == {query, providerRead, recovery, checkpoint, fork} \ {None}
RetirementCuts == Protected \ (IF Mutation = "ignore_recovery" THEN {recovery} \ {query, providerRead, checkpoint, fork} ELSE {})
ServingDependencies == IF Open THEN {head, checkpoint, query, providerRead, recovery} \ {None} ELSE {}

AuthorityInit ==
    [systemTime |-> 0, rawTime |-> 0, receiptClock |-> 0, clockRegressed |-> FALSE, mode |-> "", requestedTime |-> 0,
     decision |-> "", retained |-> FALSE, redemption |-> "", redemptionBody |-> "", redemptionExpired |-> FALSE,
     namingEpoch |-> 0, loglet |-> 0, indexFor |-> None, indexThrough |-> None,
     captured |-> None, captureLoglet |-> None, confirmed |-> None, erasure |-> "open", cacheResult |-> "",
     certified |-> None, publications |-> {}, acknowledged |-> None, activationFloor |-> 0,
     recoveryFence |-> 0, deletion |-> "",
     delivery |-> [state |-> "", time |-> 0, deadline |-> None, lastWrite |-> None]]

Init ==
    /\ authority = AuthorityInit
    /\ head = 0 /\ checkpoint = 0 /\ floor = 0
    /\ query = None /\ providerRead = None /\ recovery = None /\ firstRecord = 1
    /\ phase = "serving" /\ source = Needs(0) /\ forkCopy = {}
    /\ prepared = None /\ fork = None /\ stalePublication = FALSE

Install ==
    /\ phase = "serving" /\ head = 0
    /\ head' = 1 /\ source' = source \cup Needs(1)
    /\ authority' = [authority EXCEPT !.namingEpoch = IF Mutation = "reuse_names" THEN 0 ELSE 1]
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
    /\ (~ExtendAuthorities \/ authority.recoveryFence = 1 \/ Mutation = "trim_without_fence")
    /\ firstRecord' = 2
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

AdvanceFloor ==
    /\ phase = "serving" /\ floor < head
    /\ (~ExtendAuthorities \/ Mutation = "ignore_floor_readers" \/ \A cut \in {query, providerRead} \ {None} : cut >= head)
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
    /\ (~ExtendAuthorities \/ authority.recoveryFence = 1 \/ Mutation = "grant_without_fence")
    /\ authority' = [authority EXCEPT !.deletion = IF ExtendAuthorities THEN "old" ELSE ""]
    /\ phase' = "retiring"
    /\ UNCHANGED <<head, checkpoint, floor, query, providerRead, recovery, firstRecord,
                   source, forkCopy, prepared, fork, stalePublication>>

ApplyVersionRemoval ==
    /\ phase = "retiring" /\ "old" \in source
    /\ (~ExtendAuthorities \/ authority.deletion = "old")
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

StorageNext == PublishCheckpoint \/ OpenQuery \/ CloseQuery \/ BeginProviderRead \/ DrainProviderRead
        \/ AdmitRecovery \/ FinishRecovery \/ Trim \/ AdvanceFloor \/ PrepareFork \/ PublishFork
        \/ CancelPreparation \/ ApplyVersionRemoval \/ SettleVersion
        \/ AuthorizeDisposal \/ SettleDisposal
        \/ \E publication \in Publications : CopyForkPublication(publication) \/ EraseResidencePublication(publication)

\* The modeled receipt identity has one original body, with exclusive expiry 3.
AppendAttempt(mode, requested, observed, decision) ==
    /\ phase = "serving" /\ head = 0 /\ authority.decision = ""
    /\ mode \in {"ordinary", "external"} /\ requested \in {1, 3} /\ observed = 2
    /\ decision \in {"accepted", "refused"}
    /\ (mode = "external" \/ requested <= observed \/ Mutation = "accept_future_time")
    /\ authority' = [authority EXCEPT
                      !.systemTime = IF mode = "external" /\ Mutation # "unclamped_external"
                                     THEN IF requested > observed THEN observed ELSE requested
                                     ELSE requested,
                      !.rawTime = observed, !.receiptClock = observed, !.mode = mode,
                      !.requestedTime = requested, !.decision = decision, !.retained = TRUE]
    /\ UNCHANGED vars

ObserveReceiptClock(sample) ==
    /\ Open /\ authority.decision # ""
    /\ ((sample = 3 /\ authority.receiptClock < 3)
        \/ (sample = 0 /\ authority.receiptClock = 3 /\ ~authority.clockRegressed))
    /\ authority' = [authority EXCEPT !.receiptClock = IF sample > @ \/ Mutation = "rewind_receipt_clock" THEN sample ELSE @,
                      !.clockRegressed = sample = 0]
    /\ UNCHANGED vars

CollectReceipt ==
    /\ Open /\ authority.retained
    /\ (authority.receiptClock >= 3 \/ Mutation = "collect_live_receipt")
    /\ authority' = [authority EXCEPT !.retained = FALSE]
    /\ UNCHANGED vars

ReceiptAnswer(body, expired, decision) ==
    IF expired THEN "expired" ELSE IF body = "original" THEN decision ELSE "conflict"

RedeemReceipt(body, answer) ==
    /\ Open /\ authority.decision # "" /\ authority.redemption = ""
    /\ body \in {"original", "other"}
    /\ LET expired == authority.receiptClock >= 3
       IN /\ (answer = ReceiptAnswer(body, expired, authority.decision)
              \/ (Mutation = "replace_receipt_outcome" /\ answer = "accepted"))
          /\ authority' = [authority EXCEPT !.redemption = answer, !.redemptionBody = body, !.redemptionExpired = expired]
    /\ UNCHANGED vars

DecisionVisible == authority.publications \in {{1, 2}, {1, 3}, {2, 3}, {1, 2, 3}}

CertifyDecision(cut) ==
    /\ phase = "serving" /\ cut = 1 /\ head = cut /\ authority.certified = None
    /\ authority' = [authority EXCEPT !.certified = cut]
    /\ UNCHANGED vars

PublishDecision(peer, cut) ==
    /\ Open /\ cut = authority.certified /\ cut = 1 /\ peer \notin authority.publications
    /\ authority' = [authority EXCEPT !.publications = @ \cup {peer},
            !.activationFloor = IF authority.loglet = 1 /\ authority.publications # {}
                                 THEN cut ELSE @]
    /\ UNCHANGED vars

AcknowledgeCommit(cut) ==
    /\ Open /\ head = cut /\ cut = authority.certified /\ cut = 1
    /\ authority.acknowledged = None
    /\ (DecisionVisible \/ Mutation = "ack_payload_quorum")
    /\ authority' = [authority EXCEPT !.acknowledged = cut]
    /\ UNCHANGED vars

ChangeLoglet ==
    /\ phase = "serving" /\ authority.loglet = 0
    /\ authority' = [authority EXCEPT !.loglet = 1,
            !.activationFloor = IF DecisionVisible /\ Mutation # "lose_decision_on_activation" THEN authority.certified ELSE 0]
    /\ UNCHANGED vars

ConfirmReadIndex(epoch, through) ==
    /\ Open /\ epoch = authority.loglet /\ through = head /\ authority.indexFor # epoch
    /\ (through = 0 \/ (DecisionVisible /\ (epoch = 0 \/ authority.activationFloor >= through))
        \/ Mutation = "read_unpublished_decision")
    /\ authority' = [authority EXCEPT !.indexFor = epoch, !.indexThrough = through]
    /\ UNCHANGED vars

CaptureFresh(cut) ==
    /\ Open /\ query = None /\ authority.captured = None
    /\ authority.indexFor # None
    /\ (authority.indexFor = authority.loglet \/ Mutation = "use_stale_index")
    /\ cut = head /\ cut >= floor /\ cut >= authority.indexThrough
    /\ query' = cut
    /\ authority' = [authority EXCEPT !.captured = authority.indexFor, !.captureLoglet = authority.loglet]
    /\ UNCHANGED <<head, checkpoint, floor, providerRead, recovery, firstRecord,
                   phase, source, forkCopy, prepared, fork, stalePublication>>

Acknowledge(cut) ==
    /\ Open /\ cut = 1 /\ head = cut /\ authority.confirmed = None
    /\ ((checkpoint >= cut /\ Needs(checkpoint) \subseteq source) \/ Mutation = "ack_before_closure")
    /\ authority' = [authority EXCEPT !.confirmed = cut]
    /\ UNCHANGED vars

InstallRecoveryFence(cut) ==
    /\ phase = "serving" /\ cut = 1 /\ authority.recoveryFence < cut
    /\ checkpoint = cut /\ Needs(cut) \subseteq source /\ recovery # 0
    /\ authority' = [authority EXCEPT !.recoveryFence = cut]
    /\ UNCHANGED vars

PrepareErasure ==
    /\ phase = "serving" /\ head = 1 /\ authority.erasure = "open"
    /\ authority' = [authority EXCEPT !.erasure = "prepared"]
    /\ UNCHANGED vars

ArmErasure ==
    /\ phase = "serving" /\ authority.erasure = "prepared"
    /\ ((checkpoint = 1 /\ Needs(checkpoint) \subseteq source) \/ Mutation = "arm_before_successor")
    /\ authority' = [authority EXCEPT !.erasure = "armed"]
    /\ UNCHANGED vars

AdmitTransportAnswer ==
    /\ Open /\ query = 0 /\ authority.delivery.state = ""
    /\ authority.erasure \in {"open", "prepared"}
    /\ authority' = [authority EXCEPT !.delivery.state = "admitted", !.delivery.deadline = 1]
    /\ UNCHANGED vars

AdvanceTransportTime ==
    /\ authority.delivery.state \in {"admitted", "writing"} /\ authority.delivery.time = 0
    /\ authority' = [authority EXCEPT !.delivery.time = 1]
    /\ UNCHANGED vars

HandoffTransportAnswer(finished) ==
    /\ authority.delivery.state \in {"admitted", "writing"}
    /\ (authority.delivery.time < authority.delivery.deadline \/ Mutation = "skip_transport_deadline")
    /\ authority' = [authority EXCEPT !.delivery.state = IF finished THEN "delivered" ELSE "writing",
                     !.delivery.lastWrite = authority.delivery.time]
    /\ UNCHANGED vars

RefuseTransportAnswer ==
    /\ authority.delivery.state \in {"admitted", "writing"}
    /\ authority.delivery.time >= authority.delivery.deadline
    /\ authority' = [authority EXCEPT !.delivery.state = "refused"]
    /\ UNCHANGED vars

RevokeKey ==
    /\ Open /\ authority.erasure = "armed"
    /\ (authority.delivery.state \notin {"admitted", "writing"}
        \/ authority.delivery.time >= authority.delivery.deadline)
    /\ authority' = [authority EXCEPT !.erasure = "revoked"]
    /\ UNCHANGED vars

ReadOldCachedPage(answer) ==
    /\ Open /\ query = 0 /\ authority.erasure = "revoked" /\ authority.cacheResult = ""
    /\ (answer = "erased-under-view" \/ Mutation = "use_revoked_cache" /\ answer = "page")
    /\ authority' = [authority EXCEPT !.cacheResult = answer]
    /\ UNCHANGED vars

AuthorityNext ==
    \/ \E mode \in {"ordinary", "external"}, requested \in {1, 3}, observed \in {2},
          decision \in {"accepted", "refused"} : AppendAttempt(mode, requested, observed, decision)
    \/ \E sample \in {0, 3} : ObserveReceiptClock(sample)
    \/ CollectReceipt
    \/ \E body \in {"original", "other"}, answer \in {"accepted", "refused", "conflict", "expired"} : RedeemReceipt(body, answer)
    \/ \E cut \in {1} : CertifyDecision(cut) \/ AcknowledgeCommit(cut) \/ InstallRecoveryFence(cut)
    \/ \E peer \in 1..3, cut \in {1} : PublishDecision(peer, cut)
    \/ ChangeLoglet
    \/ \E epoch \in 0..1, through \in 0..1 : ConfirmReadIndex(epoch, through)
    \/ \E cut \in 0..1 : CaptureFresh(cut) \/ Acknowledge(cut)
    \/ PrepareErasure \/ ArmErasure \/ RevokeKey
    \/ AdmitTransportAnswer \/ AdvanceTransportTime \/ RefuseTransportAnswer
    \/ \E finished \in BOOLEAN : HandoffTransportAnswer(finished)
    \/ \E answer \in {"page", "erased-under-view"} : ReadOldCachedPage(answer)

Next == Install \/ AuthorizeVersion \/ (StorageNext /\ UNCHANGED authority) \/ (ExtendAuthorities /\ AuthorityNext)
Spec == Init /\ [][Next]_allVars

AcknowledgedDecision == authority.acknowledged # None => DecisionVisible
FreshIndexHasPublication == authority.indexThrough = 1 => DecisionVisible
DecisionSurvivesActivation == authority.loglet = 1 /\ authority.acknowledged # None =>
    authority.activationFloor >= authority.acknowledged
RecoveryFenceProtectsDeletion == ExtendAuthorities /\ (authority.deletion # "" \/ firstRecord = 2) =>
    authority.recoveryFence = 1
BoundedAppend == authority.decision # "" => authority.systemTime > 0 /\ authority.systemTime <= authority.rawTime
ExternalClamp == authority.mode = "external" =>
    authority.systemTime = IF authority.requestedTime > authority.rawTime THEN authority.rawTime ELSE authority.requestedTime
DistinctNamingEpoch == authority.namingEpoch = head
ReceiptExpiry == (authority.clockRegressed \/ (authority.decision # "" /\ ~authority.retained)) =>
    authority.receiptClock >= 3
ReceiptReplay == authority.redemption # "" =>
    authority.redemption = ReceiptAnswer(authority.redemptionBody, authority.redemptionExpired, authority.decision)
FreshCapture == authority.captured = authority.captureLoglet
AcknowledgedClosure == Open /\ authority.confirmed # None =>
    checkpoint >= authority.confirmed /\ Needs(checkpoint) \subseteq source /\ firstRecord <= checkpoint + 1
ErasureHasSuccessor == Open /\ authority.erasure \in {"armed", "revoked"} =>
    checkpoint = 1 /\ Needs(checkpoint) \subseteq source
RevokedCacheClosed == authority.cacheResult # "page"
OnlyLiveTransportWrites == authority.delivery.lastWrite # None =>
    authority.delivery.lastWrite < authority.delivery.deadline

FloorKeepsHeldCatalogs == ExtendAuthorities => \A cut \in {query, providerRead} \ {None} : cut >= floor
RetainedCutsReadable == \A cut \in ServingDependencies : Needs(cut) \subseteq source
ReplaySurvivesTrim == Open => firstRecord <= checkpoint + 1 /\ (recovery = None \/ firstRecord <= recovery + 1)
ForkReadable == fork = None \/ Needs(fork) \subseteq (source \cup forkCopy)
ForkCopyMatches == fork # None => forkCopy \subseteq Needs(fork)
ClosedAfterDrain == ~Open => query = None /\ providerRead = None /\ recovery = None
ReceiptConfirmsAbsence == phase = "disposed" => source = {}
PublicationRespectsClose == ~stalePublication
Safe == FloorKeepsHeldCatalogs /\ RetainedCutsReadable /\ ReplaySurvivesTrim /\ ForkReadable
        /\ ForkCopyMatches /\ ClosedAfterDrain /\ ReceiptConfirmsAbsence /\ PublicationRespectsClose
        /\ BoundedAppend /\ ExternalClamp /\ DistinctNamingEpoch /\ ReceiptExpiry /\ ReceiptReplay
        /\ FreshCapture /\ AcknowledgedClosure /\ ErasureHasSuccessor /\ RevokedCacheClosed
        /\ AcknowledgedDecision /\ FreshIndexHasPublication /\ DecisionSurvivesActivation
        /\ RecoveryFenceProtectsDeletion /\ OnlyLiveTransportWrites

NeverDisposesAfterTrimAndForkCopy ==
    ~(phase = "disposed" /\ head = 1 /\ firstRecord = 2 /\ fork = 1 /\ forkCopy = Needs(1))
NeverReclaimsWhileCurrentQueryOpen == ~(phase = "serving" /\ query = 1 /\ "old" \notin source)
=============================================================================
