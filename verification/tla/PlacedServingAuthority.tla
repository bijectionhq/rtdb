------------------------ MODULE PlacedServingAuthority ------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANT Mutation

Epochs == 1..2
Slots == 1..2
Attempts == Epochs \X Slots
None == <<0, 0>>

VARIABLES epoch, directory, intake, committed, sealedAt, applied, report,
          basis, read, outageCommit

Vars == <<epoch, directory, intake, committed, sealedAt, applied, report,
          basis, read, outageCommit>>

Init ==
    /\ epoch = 1 /\ directory = TRUE /\ intake = {}
    /\ committed = <<>> /\ sealedAt = 0 /\ applied = 0
    /\ report = <<None, None>> /\ basis = <<None, 0>>
    /\ read = [phase |-> "idle", epoch |-> 1, minimum |-> 0, tip |-> 0, served |-> 0]
    /\ outageCommit = FALSE

Range(sequence) == {sequence[idx] : idx \in DOMAIN sequence}

IndexOf(attempt) ==
    CHOOSE idx \in 0..Len(committed) :
        IF attempt \in Range(committed)
        THEN idx > 0 /\ committed[idx] = attempt
        ELSE idx = 0

Submit(attempt) ==
    /\ attempt[1] = epoch /\ attempt \notin intake
    /\ intake' = intake \cup {attempt}
    /\ UNCHANGED <<epoch, directory, committed, sealedAt, applied, report, basis, read, outageCommit>>

Commit(attempt) ==
    /\ attempt[1] = epoch /\ attempt \in intake \ Range(committed)
    /\ committed' = Append(committed, attempt)
    /\ outageCommit' = (outageCommit \/ ~directory)
    /\ UNCHANGED <<epoch, directory, intake, sealedAt, applied, report, basis, read>>

Move ==
    /\ directory /\ epoch = 1
    /\ epoch' = 2 /\ sealedAt' = Len(committed)
    /\ UNCHANGED <<directory, intake, committed, applied, report, basis, read, outageCommit>>

ToggleDirectory ==
    /\ directory' = ~directory
    /\ UNCHANGED <<epoch, intake, committed, sealedAt, applied, report, basis, read, outageCommit>>

Apply ==
    /\ applied < Len(committed) /\ applied' = applied + 1
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, report, basis, read, outageCommit>>

Answer(attempt, returned) ==
    /\ report = <<None, None>> /\ attempt \in intake
    /\ returned \in Range(committed) /\ IndexOf(returned) <= applied
    /\ IF Mutation = "reuse_slot" THEN attempt[2] = returned[2] ELSE attempt = returned
    /\ report' = <<attempt, returned>>
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, applied, basis, read, outageCommit>>

Unresolved(attempt) ==
    /\ report = <<None, None>> /\ attempt \in intake /\ attempt[1] < epoch
    /\ IF Mutation = "close_before_replay"
       THEN IndexOf(attempt) = 0 \/ IndexOf(attempt) > applied
       ELSE applied >= sealedAt /\ IndexOf(attempt) = 0
    /\ report' = <<attempt, None>>
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, applied, basis, read, outageCommit>>

IssueBasis(attempt) ==
    LET idx == IndexOf(attempt) IN
    /\ basis = <<None, 0>> /\ attempt \in intake
    /\ idx > 0 \/ Mutation = "basis_at_intake"
    /\ idx <= applied \/ Mutation \in {"basis_at_intake", "basis_before_apply"}
    /\ basis' = <<attempt, IF idx = 0 THEN attempt[2] ELSE idx>>
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, applied, report, read, outageCommit>>

StartRead(knownEpoch) ==
    /\ read.phase = "idle" /\ knownEpoch <= epoch
    /\ read' = [phase |-> "waiting", epoch |-> knownEpoch, minimum |-> Len(committed),
                 tip |-> 0, served |-> 0]
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, applied, report, basis, outageCommit>>

ConfirmRead ==
    /\ read.phase = "waiting"
    /\ read.epoch = epoch \/ Mutation = "accept_sealed_loglet"
    /\ read' = [read EXCEPT !.phase = "confirmed",
                   !.tip = IF read.epoch = epoch THEN Len(committed) ELSE sealedAt]
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, applied, report, basis, outageCommit>>

ReplyRead ==
    /\ read.phase = "confirmed" \/ (read.phase = "waiting" /\ Mutation = "skip_quorum")
    /\ applied >= read.tip \/ Mutation = "skip_apply"
    /\ read' = [read EXCEPT !.phase = "answered", !.served = applied]
    /\ UNCHANGED <<epoch, directory, intake, committed, sealedAt, applied, report, basis, outageCommit>>

Next ==
    \/ \E attempt \in Attempts : Submit(attempt) \/ Commit(attempt) \/ IssueBasis(attempt) \/ Unresolved(attempt)
    \/ \E attempt, returned \in Attempts : Answer(attempt, returned)
    \/ \E knownEpoch \in Epochs : StartRead(knownEpoch)
    \/ Move \/ ToggleDirectory \/ Apply \/ ConfirmRead \/ ReplyRead

Spec == Init /\ [][Next]_Vars

ReceiptNamesTheAttempt == report[2] = None \/ report[1] = report[2]
BasisNamesCommittedAttempt == basis[1] = None \/ IndexOf(basis[1]) = basis[2]
BasisAppearsAfterApply == basis[1] = None \/ basis[2] <= applied
ClosedReceiptIsAbsent == (report[1] # None /\ report[2] = None) => IndexOf(report[1]) = 0
FreshReadIncludesPriorCommits == read.phase = "answered" => read.served >= read.minimum
NeverServesDuringOutage == ~(~directory /\ outageCommit /\ read.phase = "answered" /\ read.served > 0)

===============================================================================
