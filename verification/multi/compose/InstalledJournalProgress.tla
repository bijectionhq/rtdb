----------------------- MODULE InstalledJournalProgress -----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Mutation, Focus

Zones == 1..3
Slots == 0..1
OldCut == 1
ClockError == 1
Drift == 1
LastLeaseEnd == 2
Horizon == LastLeaseEnd + Drift + ClockError
LeaseTicks == 2
ReceiverBudget == 2
Reserve == 3 + ReceiverBudget - (IF Mutation = "unfunded-lane" THEN 1 ELSE 0)
NoCut == 0
NoCommit == [cut |-> NoCut, due |-> 0]

VARIABLES lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd,
          time, read, erasure

vars == <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd,
          time, read, erasure>>

Init ==
    /\ lostAZ \in Zones
    /\ reachable \in IF Mutation = "no-majority"
                    THEN SUBSET (Zones \ {lostAZ}) ELSE {Zones \ {lostAZ}}
    /\ custodian = TRUE
    /\ writer = "vacant"
    /\ \E settled \in 0..1, inherited \in BOOLEAN :
        checkpoint = [phase |-> "pending", used |-> 1 + settled, floor |-> NoCut,
                       settled |-> settled, inherited |-> inherited]
    /\ pins = {"withdrawn", "overlap"}
    /\ leaseEnd \in {0, LastLeaseEnd}
    /\ time = [real |-> 0, journal |-> 0]
    /\ read = [remaining |-> 0, cut |-> NoCut]
    /\ erasure = [reservedBy |-> 0, clean |-> {}, decision |-> NoCommit]

HasMajority == Cardinality(reachable) * 2 > Cardinality(Zones)
CustodyAvailable == custodian /\ HasMajority
ExactProviderAvailable == Mutation # "no-provider"
WriterGranted == writer # "vacant"
WriterReady == writer = "ready"

GrantWriter ==
    /\ ~WriterGranted /\ HasMajority
    /\ Mutation # "no-candidate" /\ Mutation # "no-activation-slot"
    /\ writer' = "granted"
    /\ UNCHANGED <<lostAZ, reachable, custodian, checkpoint, pins, leaseEnd, time, read, erasure>>

ReplayGrant ==
    /\ writer = "granted" /\ ExactProviderAvailable
    /\ writer' = "replayed"
    /\ UNCHANGED <<lostAZ, reachable, custodian, checkpoint, pins, leaseEnd, time, read, erasure>>

ConfirmWriter ==
    /\ writer = "replayed" /\ HasMajority
    /\ writer' = "ready"
    /\ UNCHANGED <<lostAZ, reachable, custodian, checkpoint, pins, leaseEnd, time, read, erasure>>

AwaitingBoundary == checkpoint.phase = "pending"
AwaitingUpload == checkpoint.phase = "bounded"
Published == checkpoint.phase \in {"published", "trimmed"}
BoundaryCut == OldCut + checkpoint.settled + 1
UploadCut == BoundaryCut + 1
Trimmed == checkpoint.floor = BoundaryCut
Room == checkpoint.used + 1 <= Reserve
Head == CASE AwaitingBoundary -> BoundaryCut - 1
          [] AwaitingUpload -> BoundaryCut
          [] OTHER -> UploadCut

SettleInherited ==
    /\ AwaitingBoundary /\ checkpoint.inherited /\ WriterReady /\ HasMajority /\ Room
    /\ checkpoint' = [checkpoint EXCEPT !.inherited = FALSE, !.settled = @ + 1, !.used = @ + 1]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

AppendBoundary ==
    /\ AwaitingBoundary /\ ~checkpoint.inherited /\ WriterReady /\ HasMajority /\ Room
    /\ checkpoint' = [checkpoint EXCEPT !.phase = "bounded", !.used = @ + 1]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

AppendUpload ==
    /\ AwaitingUpload /\ WriterReady /\ HasMajority /\ Room
    /\ checkpoint' = [checkpoint EXCEPT !.phase = "uploaded", !.used = @ + 1]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

PrepareCheckpoint ==
    /\ checkpoint.phase = "uploaded" /\ CustodyAvailable
    /\ checkpoint' = [checkpoint EXCEPT !.phase = "prepared"]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

CopyCheckpoint ==
    /\ checkpoint.phase = "prepared" /\ custodian /\ ExactProviderAvailable
    /\ Mutation # "unfunded-checkpoint-copy"
    /\ checkpoint' = [checkpoint EXCEPT !.phase = "copied"]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

PublishCheckpoint ==
    /\ checkpoint.phase = "copied" /\ CustodyAvailable
    /\ checkpoint' = [checkpoint EXCEPT !.phase = "published"]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

TickJournal ==
    /\ time.real < Horizon
    /\ LET now == time.real + 1 IN
       \E journal \in time.journal..(now + ClockError) :
           /\ Mutation # "unbounded-clock" => now <= journal + ClockError
           /\ time' = [real |-> now, journal |-> journal]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd, read, erasure>>

MayDispose == leaseEnd = 0 \/ leaseEnd + Drift <= time.journal

DisposeExpired ==
    /\ "withdrawn" \in pins /\ MayDispose /\ CustodyAvailable
    /\ pins' = pins \ {"withdrawn"}
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, leaseEnd, time, read, erasure>>

ReleaseHolds ==
    /\ pins \ {"withdrawn"} # {}
    /\ pins' = pins \cap {"withdrawn"}
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, leaseEnd, time, read, erasure>>

Trim ==
    /\ checkpoint.phase = "published" /\ pins = {} /\ CustodyAvailable
    /\ checkpoint' = [checkpoint EXCEPT !.phase = "trimmed", !.used = 1, !.floor = BoundaryCut]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, pins, leaseEnd, time, read, erasure>>

FreshReadAdmitted == read.cut # NoCut

RenewLease ==
    /\ ~FreshReadAdmitted /\ WriterReady /\ HasMajority /\ read.remaining = 0
    /\ Mutation # "no-usable-renewal"
    /\ read' = [read EXCEPT !.remaining = LeaseTicks]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd, time, erasure>>

AgeLease ==
    /\ ~FreshReadAdmitted /\ read.remaining > 0
    /\ read' = [read EXCEPT !.remaining = @ - 1]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd, time, erasure>>

AdmitFreshRead ==
    /\ ~FreshReadAdmitted /\ WriterReady /\ HasMajority /\ read.remaining > 0
    /\ Mutation # "no-reader-slot"
    /\ read' = [read EXCEPT !.cut = Head]
    /\ pins' = IF Published THEN pins ELSE pins \cup {"fresh"}
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, leaseEnd, time, erasure>>

ErasureRequested == erasure.reservedBy # 0
ErasureCommitted == erasure.decision # NoCommit
BothSlotsClean == HasMajority /\ erasure.clean = reachable \X Slots
DeadlineUsable == time.real + ClockError < erasure.reservedBy

\* The request begins at a quiesced head with a prepared clean root and an authenticated reservation.
StartErasure ==
    /\ Trimmed /\ FreshReadAdmitted /\ WriterReady /\ CustodyAvailable /\ ~ErasureRequested
    /\ erasure' = [erasure EXCEPT !.reservedBy =
        IF Mutation = "short-deadline" THEN time.real + ClockError + 1
        ELSE Horizon + ClockError + 1]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd, time, read>>

MirrorCleanSlot(voter, slot) ==
    /\ ErasureRequested /\ CustodyAvailable /\ <<voter, slot>> \notin erasure.clean
    /\ Mutation # "stale-slot" \/ slot = 0
    /\ erasure' = [erasure EXCEPT !.clean = @ \cup {<<voter, slot>>}]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd, time, read>>

MirrorSome == \E voter \in reachable, slot \in Slots : MirrorCleanSlot(voter, slot)

CommitErasure ==
    /\ BothSlotsClean /\ ~ErasureCommitted /\ custodian
    /\ Mutation # "unqualified-authority"
    /\ DeadlineUsable
    /\ erasure' = [erasure EXCEPT !.decision = [cut |-> Head, due |-> erasure.reservedBy]]
    /\ UNCHANGED <<lostAZ, reachable, custodian, writer, checkpoint, pins, leaseEnd, time, read>>

LoseCustodian ==
    /\ Mutation = "no-custodian" /\ custodian
    /\ custodian' = FALSE
    /\ UNCHANGED <<lostAZ, reachable, writer, checkpoint, pins, leaseEnd, time, read, erasure>>

DisplaceWriter ==
    /\ Mutation = "writer-displaced" /\ WriterReady
    /\ writer' = "displaced"
    /\ UNCHANGED <<lostAZ, reachable, custodian, checkpoint, pins, leaseEnd, time, read, erasure>>

Next ==
    \/ GrantWriter \/ ReplayGrant \/ ConfirmWriter
    \/ SettleInherited
    \/ AppendBoundary \/ AppendUpload \/ PrepareCheckpoint \/ CopyCheckpoint \/ PublishCheckpoint
    \/ TickJournal \/ DisposeExpired \/ ReleaseHolds \/ Trim
    \/ RenewLease \/ AgeLease \/ AdmitFreshRead
    \/ StartErasure \/ MirrorSome \/ CommitErasure \/ LoseCustodian \/ DisplaceWriter

GrantFairness == {"grant"}
RecoveryFairness == GrantFairness \cup {"replay", "confirm"}
PublicationFairness == RecoveryFairness \cup {"settle", "boundary", "upload", "prepare", "copy", "publish"}
DisposalFairness == {"tick", "dispose"}
TrimFairness == DisposalFairness \cup {"release", "trim"}
ReadFairness == {"renew", "read"}
CleanSlotFairness == {"commit"}
ErasureFairness == CleanSlotFairness \cup {"mirror"}

FairActions ==
    CASE Focus = "grant" -> GrantFairness
      [] Focus = "recovery" -> RecoveryFairness
      [] Focus = "publication" -> PublicationFairness
      [] Focus = "disposal" -> DisposalFairness
      [] Focus = "trim" -> TrimFairness
      [] Focus = "read" -> ReadFairness
      [] Focus = "clean-slots" -> CleanSlotFairness
      [] Focus = "erasure" -> ErasureFairness
      [] Focus = "composed" -> PublicationFairness \cup TrimFairness \cup ReadFairness \cup ErasureFairness

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars("grant" \in FairActions /\ Mutation # "unfair-grant" /\ GrantWriter)
    /\ WF_vars("replay" \in FairActions /\ Mutation # "unfair-replay" /\ ReplayGrant)
    /\ WF_vars("confirm" \in FairActions /\ Mutation # "unfair-confirm" /\ ConfirmWriter)
    /\ WF_vars("settle" \in FairActions /\ Mutation # "unfair-settlement" /\ SettleInherited)
    /\ WF_vars("boundary" \in FairActions /\ Mutation # "unfair-boundary" /\ AppendBoundary)
    /\ WF_vars("upload" \in FairActions /\ Mutation # "unfair-upload" /\ AppendUpload)
    /\ WF_vars("prepare" \in FairActions /\ Mutation # "unfair-prepare" /\ PrepareCheckpoint)
    /\ WF_vars("copy" \in FairActions /\ Mutation # "unfair-copy" /\ CopyCheckpoint)
    /\ WF_vars("publish" \in FairActions /\ Mutation # "unfair-publish" /\ PublishCheckpoint)
    /\ WF_vars("tick" \in FairActions /\ Mutation # "unfair-tick" /\ TickJournal)
    /\ WF_vars("dispose" \in FairActions /\ Mutation # "unfair-dispose" /\ DisposeExpired)
    /\ WF_vars("release" \in FairActions /\ Mutation # "holds-never-release" /\ ReleaseHolds)
    /\ WF_vars("trim" \in FairActions /\ Mutation # "unfair-trim" /\ Trim)
    /\ WF_vars("renew" \in FairActions /\ Mutation # "unfair-renew" /\ RenewLease)
    /\ SF_vars("read" \in FairActions /\ Mutation # "weak-read-fairness" /\ AdmitFreshRead)
    /\ WF_vars("read" \in FairActions /\ Mutation = "weak-read-fairness" /\ AdmitFreshRead)
    /\ WF_vars("mirror" \in FairActions /\ Mutation # "unfair-mirror" /\ MirrorSome)
    /\ WF_vars("commit" \in FairActions /\ Mutation # "unfair-commit" /\ CommitErasure)

MaintenanceAlwaysAdmitsNext == (AwaitingBoundary \/ AwaitingUpload) => Room
SettlementAccounting ==
    /\ checkpoint.settled + (IF checkpoint.inherited THEN 1 ELSE 0) <= ReceiverBudget
    /\ checkpoint.inherited => AwaitingBoundary
    /\ checkpoint.used = (CASE AwaitingBoundary -> 1 + checkpoint.settled
                           [] AwaitingUpload -> 2 + checkpoint.settled
                           [] Trimmed -> 1
                           [] OTHER -> 3 + checkpoint.settled)
TrimRequiresPublication == Trimmed => Published /\ pins = {}
DisposalRequiresElapsedLease == "withdrawn" \notin pins => MayDispose
CommitBindsCutAndDeadline == ErasureCommitted =>
    BothSlotsClean /\ erasure.decision = [cut |-> Head, due |-> erasure.reservedBy]

CheckpointEventuallyPublishes == <>Published
PublishedCheckpointEventuallyTrims == Published ~> Trimmed
WithdrawnHoldsEventuallyDisposed == ("withdrawn" \in pins) ~> ("withdrawn" \notin pins)
FreshReadEventuallyAdmitted == WriterReady ~> FreshReadAdmitted
RequestedErasureEventuallyCommits == ErasureRequested ~> ErasureCommitted
CleanSlotsEventuallyCommitErasure == BothSlotsClean ~> ErasureCommitted
OpenReservationEventuallyCommitsErasure == (BothSlotsClean /\ DeadlineUsable) ~> ErasureCommitted
WriterEventuallyGranted == <>WriterGranted
WriterEventuallyReady == <>WriterReady

NoCompletedScenario ==
    ~(Trimmed /\ checkpoint.settled = ReceiverBudget /\ read.cut = BoundaryCut
      /\ ErasureCommitted /\ WriterReady /\ leaseEnd = LastLeaseEnd)

=============================================================================
