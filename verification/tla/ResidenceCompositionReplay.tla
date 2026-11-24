--------------------------- MODULE ResidenceCompositionReplay ---------------------------
EXTENDS ResidenceComposition, ResidenceCompositionEvents, Sequences

VARIABLE cursor

traceVars == <<allVars, cursor>>
TraceInit ==
    /\ cursor = 0
    /\ IF DisposalEffectsOnly
        THEN /\ authority = AuthorityInit
             /\ head = 0 /\ checkpoint = 0 /\ floor = 0
             /\ query = None /\ providerRead = None /\ recovery = None /\ firstRecord = 1
             /\ phase = "disposing" /\ source = Needs(0) /\ forkCopy = {}
             /\ prepared = None /\ fork = 0 /\ stalePublication = FALSE
        ELSE Init

\* A replay projects observed cuts, provider publications and directory phases
\* into the bounded model rather than choosing them nondeterministically.
ObservedStep(event) ==
    LET kind == event[1]
        cut == event[2]
        publication == event[3]
        epoch == event[4]
        outcome == event[5]
        observedTime == event[6]
    IN
    CASE kind = "open-query" -> OpenQuery /\ UNCHANGED authority /\ query' = cut
      [] kind = "begin-provider-read" -> BeginProviderRead /\ providerRead' = cut
      [] kind = "close-query" -> CloseQuery /\ query = cut
      [] kind = "drain-provider-read" -> DrainProviderRead /\ providerRead = cut
      [] kind = "install" -> Install /\ head' = cut
            /\ (~ExtendAuthorities \/ (authority'.namingEpoch = epoch /\ publication = "new"))
      [] kind = "publish-checkpoint" -> PublishCheckpoint /\ checkpoint' = cut
      [] kind = "admit-recovery" -> AdmitRecovery /\ recovery' = cut
      [] kind = "finish-recovery" -> FinishRecovery /\ recovery = cut
      [] kind = "advance-floor" -> AdvanceFloor /\ floor' = cut
      [] kind = "authorize-version" -> AuthorizeVersion /\ publication = "old"
      [] kind = "remove-version" ->
            ApplyVersionRemoval /\ source \ source' = {publication}
      [] kind = "settle-version" -> SettleVersion /\ publication = "old"
      [] kind = "trim" -> Trim /\ firstRecord' = cut
      [] kind = "prepare-fork" -> PrepareFork /\ prepared' = cut
      [] kind = "publish-fork" -> PublishFork /\ fork' = cut
      [] kind = "authorize-disposal" -> AuthorizeDisposal
      [] kind = "copy-fork" -> CopyForkPublication(publication) /\ fork = cut
      [] kind = "erase-residence" -> EraseResidencePublication(publication)
      [] kind = "settle-disposal" -> SettleDisposal
      [] kind = "attempt-outcome" -> AppendAttempt(publication, cut, epoch, outcome) /\ authority'.systemTime = observedTime
      [] kind = "receipt-clock" -> ObserveReceiptClock(cut) /\ authority'.receiptClock = epoch
      [] kind = "collect-receipt" -> CollectReceipt /\ authority.receiptClock = cut
      [] kind = "redeem-receipt" -> RedeemReceipt(publication, outcome)
      [] kind = "certify-decision" -> CertifyDecision(cut)
      [] kind = "publish-decision" -> PublishDecision(epoch, cut)
      [] kind = "ack-commit" -> AcknowledgeCommit(cut)
      [] kind = "install-recovery-fence" -> InstallRecoveryFence(cut)
      [] kind = "change-loglet" -> ChangeLoglet /\ authority'.loglet = epoch
      [] kind = "read-index" -> ConfirmReadIndex(epoch, cut)
      [] kind = "capture-fresh" -> CaptureFresh(cut) /\ authority.indexFor = epoch
      [] kind = "acknowledge" -> Acknowledge(cut)
      [] kind = "prepare-erasure" -> PrepareErasure /\ publication = "old"
      [] kind = "arm-erasure" -> ArmErasure /\ checkpoint = cut /\ publication = "old"
      [] kind = "admit-transport-answer" -> AdmitTransportAnswer /\ authority'.delivery.deadline = cut /\ publication = "old"
      [] kind = "handoff-transport-answer" -> HandoffTransportAnswer(outcome = "complete") /\ authority.delivery.time = cut /\ publication = "old"
      [] kind = "advance-transport-time" -> AdvanceTransportTime /\ authority'.delivery.time = cut /\ publication = "old"
      [] kind = "refuse-transport-answer" -> RefuseTransportAnswer /\ authority.delivery.time = cut /\ publication = "old"
      [] kind = "revoke-key" -> RevokeKey /\ publication = "old"
      [] kind = "read-old-cache" -> ReadOldCachedPage(outcome) /\ query = cut /\ publication = "old"
      [] kind = "hold-replica-position" -> HoldReplica(cut)
      [] kind = "release-replica-position" -> ReleaseReplica(cut)
      [] kind = "lose-historical-journal" -> LoseHistoricalJournal
      [] kind = "install-body-census" -> InstallBodyCensus /\ cut = BodyPosition
      [] kind = "redeem-body-hold" -> RedeemBodyHold /\ cut = BodyPosition
      [] kind = "drain-body-read" -> DrainBodyRead
      [] kind = "begin-body-upload" -> BeginBodyUpload
      [] kind = "finish-body-upload" -> FinishBodyUpload
      [] kind = "grant-detached-body" -> GrantBody(publication) /\ cut = BodyPosition
      [] kind = "withdraw-body-key" -> DetachedEffect(publication, "granted", "key-withdrawn")
      [] kind = "remove-detached-body" -> DetachedEffect(publication, "key-withdrawn", "removed")
      [] kind = "settle-detached-body" -> DetachedEffect(publication, "removed", "settled")
      [] kind = "select-fork-inventory" -> SelectForkInventory(cut)
      [] kind = "verify-fork-inventory" -> VerifyForkInventory(cut, publication)
      [] kind = "verify-fork-replacement" -> VerifyForkReplacement(publication)
      [] kind = "withdraw-fork-edge" -> WithdrawForkEdge(publication)
      [] kind = "enroll-repair-inventory" -> EnrollRepairInventory
      [] kind = "verify-repair-inventory" -> VerifyRepairInventory(publication)
      [] kind = "replay-repair-inventory" -> ReplayRepairInventory
      [] OTHER -> FALSE

Advance ==
    /\ cursor < Len(Events)
    /\ ObservedStep(Events[cursor + 1])
    /\ (Next \/ (ExtendAuthorities /\ ReachabilityNext))
    /\ cursor' = cursor + 1

Complete ==
    /\ cursor = Len(Events)
    /\ source = {} /\ forkCopy = Needs(fork)
    /\ (~ExtendAuthorities \/
        /\ authority.confirmed = 1 /\ ~authority.retained /\ authority.clockRegressed
        /\ authority.acknowledged = 1 /\ authority.activationFloor = 1
        /\ authority.recoveryFence = 1 /\ authority.deletion = "old"
        /\ authority.redemption = "expired" /\ authority.captured = 1
        /\ authority.cacheResult = "erased-under-view" /\ authority.delivery.state = "refused"
        /\ Reach.body = "settled" /\ ~Reach.historicalJournal
        /\ Reach.forkInventory = "withdrawn" /\ Reach.repair = "replayed")
    /\ IF DisposalEffectsOnly
        THEN phase = "disposing" /\ fork = 0
        ELSE phase = "disposed" /\ firstRecord = 2 /\ fork = 1

Accept == Complete /\ UNCHANGED traceVars
TraceNext == Advance \/ Accept
TraceSpec == TraceInit /\ [][TraceNext]_traceVars /\ WF_traceVars(Advance)
ReplayTerminates == <>Complete
=============================================================================
