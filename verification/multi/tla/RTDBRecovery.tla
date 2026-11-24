---------------------------- MODULE RTDBRecovery ----------------------------
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Keepers, Payloads, MaxEpoch, MaxWrites, MaxLost, AllowEarlyTrim, AllowPartialSync
VARIABLE state

Epochs == 1..MaxEpoch
Positions == 1..MaxWrites
Quorum == (Cardinality(Keepers) \div 2) + 1
KeeperSymmetry == Permutations(Keepers)
Writes == [position: Positions, epoch: Epochs, payload: Payloads]
NoWrite == [position |-> 0, epoch |-> 0, payload |-> 0]
EmptyJournal == [promise |-> 1, synced |-> 1, trimmed |-> 0, tail |-> <<>>]
Last(j) == j.trimmed + Len(j.tail)
Available == Keepers \ state.lost

HasRecord(j, w) ==
    IF w.position \in (j.trimmed + 1)..Last(j)
    THEN j.tail[w.position - j.trimmed] = w
    ELSE FALSE

HasEntry(log, w) ==
    IF w.position \in 1..Len(log) THEN log[w.position] = w ELSE FALSE

Entries(log) == {log[i] : i \in 1..Len(log)}
Suffix(log, through) == SubSeq(log, through + 1, Len(log))

Restore(checkpoint, journal) ==
    checkpoint \o Suffix(journal.tail, Len(checkpoint) - journal.trimmed)

Copies(journals, w) == {k \in Available : HasRecord(journals[k], w)}

Init ==
    state = [epoch |-> 1,
             phase |-> "ready",
             journals |-> [k \in Keepers |-> EmptyJournal],
             lost |-> {},
             sealed |-> {},
             visible |-> <<>>,
             pending |-> NoWrite,
             sent |-> {},
             uploads |-> {},
             checkpoint |-> <<>>,
             committed |-> {},
             acknowledged |-> {},
             readAfterRestart |-> FALSE]

Send(payload) ==
    /\ state.phase = "ready"
    /\ state.pending = NoWrite
    /\ Len(state.visible) < MaxWrites
    /\ LET w == [position |-> Len(state.visible) + 1,
                  epoch |-> state.epoch, payload |-> payload]
       IN state' = [state EXCEPT !.pending = w, !.sent = @ \cup {w}]

DeliverAppend(k, w) ==
    LET journal == state.journals[k]
        updated == [state.journals EXCEPT ![k].tail = Append(@, w)]
    IN /\ k \in Available
       /\ journal.promise = w.epoch
       /\ journal.synced = w.epoch
       /\ w.position = Last(journal) + 1
       /\ state' = [state EXCEPT
              !.journals = updated,
              !.committed = IF Cardinality(Copies(updated, w)) >= Quorum
                            THEN @ \cup {w} ELSE @]

Reply ==
    LET w == state.pending
    IN /\ state.phase = "ready"
       /\ w # NoWrite
       /\ Cardinality(Copies(state.journals, w)) >= Quorum
       /\ state' = [state EXCEPT !.pending = NoWrite,
                    !.visible = Append(@, w), !.acknowledged = @ \cup {w}]

Upload ==
    LET pack == [epoch |-> state.epoch, entries |-> state.visible]
    IN /\ state.phase = "ready"
       /\ Len(pack.entries) > Len(state.checkpoint)
       /\ pack \notin state.uploads
       /\ state' = [state EXCEPT !.uploads = @ \cup {pack}]

Publish(pack) ==
    /\ state.phase = "ready"
    /\ pack.epoch = state.epoch
    /\ Len(pack.entries) > Len(state.checkpoint)
    /\ state' = [state EXCEPT !.checkpoint = pack.entries]

Trim(k, through) ==
    LET journal == state.journals[k]
    IN /\ k \in Available
       /\ journal.trimmed < through
       /\ through <= Last(journal)
       /\ (through <= Len(state.checkpoint)
           \/ (AllowEarlyTrim /\ \E pack \in state.uploads : through <= Len(pack.entries)))
       /\ state' = [state EXCEPT !.journals[k] =
                    [journal EXCEPT !.trimmed = through,
                     !.tail = Suffix(@, through - journal.trimmed)]]

Crash ==
    /\ state.phase # "down"
    /\ state.epoch < MaxEpoch
    /\ state' = [state EXCEPT !.phase = "down", !.sealed = {},
                 !.visible = <<>>, !.pending = NoWrite]

Restart ==
    /\ state.phase = "down"
    /\ state' = [state EXCEPT !.epoch = @ + 1, !.phase = "sealing"]

LoseJournal(k) ==
    /\ k \in Available
    /\ Cardinality(state.lost) < MaxLost
    /\ state' = [state EXCEPT !.lost = @ \cup {k}, !.journals[k] = EmptyJournal]

Seal(k) ==
    /\ state.phase \in {"sealing", "repairing"}
    /\ k \in Available \ state.sealed
    /\ state' = [state EXCEPT !.journals[k].promise = state.epoch, !.sealed = @ \cup {k}]

AtLeastAsGood(left, right) ==
    \/ left.synced > right.synced
    \/ /\ left.synced = right.synced
       /\ Last(left) >= Last(right)

ChooseHistory(k) ==
    LET replies == state.sealed \cap Available
        winner == state.journals[k]
    IN /\ state.phase = "sealing"
       /\ k \in replies
       /\ Cardinality(replies) >= Quorum
       /\ \A other \in replies : AtLeastAsGood(winner, state.journals[other])
       /\ winner.trimmed <= Len(state.checkpoint)
       /\ state' = [state EXCEPT !.phase = "repairing",
                    !.visible = Restore(state.checkpoint, winner)]

Repair(k, through) ==
    LET journal == state.journals[k]
        boundary == Len(state.checkpoint)
        last == Len(state.visible)
    IN /\ state.phase = "repairing"
       /\ k \in state.sealed \cap Available
       /\ journal.synced < state.epoch
       /\ through \in boundary..last
       /\ (through = last \/ through > Last(journal))
       /\ LET repaired ==
                  [journal EXCEPT !.trimmed = boundary,
                   !.tail = SubSeq(state.visible, boundary + 1, through),
                   !.synced = IF through = last \/ AllowPartialSync THEN state.epoch ELSE @]
          IN /\ repaired # journal
             /\ state' = [state EXCEPT !.journals[k] = repaired]

Confirm ==
    /\ state.phase = "repairing"
    /\ Cardinality({k \in Available : state.journals[k].synced = state.epoch}) >= Quorum
    /\ state' = [state EXCEPT !.phase = "ready", !.committed = @ \cup Entries(state.visible)]

ProbeRecoveredRead ==
    /\ state.phase = "ready"
    /\ state.epoch > 1
    /\ ~state.readAfterRestart
    /\ \E w \in state.acknowledged : w.epoch < state.epoch /\ HasEntry(state.visible, w)
    /\ state' = [state EXCEPT !.readAfterRestart = TRUE]

SealSome == \E k \in Keepers : Seal(k)
ChooseSome == \E k \in Keepers : ChooseHistory(k)
RepairSome == \E k \in Keepers, through \in 0..MaxWrites : Repair(k, through)

Next ==
    \/ \E payload \in Payloads : Send(payload)
    \/ \E k \in Keepers, w \in state.sent : DeliverAppend(k, w)
    \/ Reply
    \/ Upload
    \/ \E pack \in state.uploads : Publish(pack)
    \/ \E k \in Keepers, through \in Positions : Trim(k, through)
    \/ Crash
    \/ Restart
    \/ \E k \in Keepers : LoseJournal(k)
    \/ SealSome
    \/ ChooseSome
    \/ RepairSome
    \/ Confirm
    \/ ProbeRecoveredRead

Spec == Init /\ [][Next]_state
FairSpec == Spec /\ WF_state(Restart) /\ WF_state(SealSome) /\ WF_state(ChooseSome)
                 /\ WF_state(RepairSome) /\ WF_state(Confirm)

OneHistory ==
    \A a, b \in state.committed : a.position = b.position => a = b

TrimFollowsPublication ==
    \A k \in Available : state.journals[k].trimmed <= Len(state.checkpoint)

CheckpointIsCommitted == Entries(state.checkpoint) \subseteq state.committed

RecoveredHistoryContainsCommits ==
    state.phase = "repairing" => state.committed \subseteq Entries(state.visible)

SyncedHistoryIsComplete ==
    state.phase = "repairing" =>
        \A k \in Available :
            state.journals[k].synced = state.epoch =>
                Restore(state.checkpoint, state.journals[k]) = state.visible

CommitSurvives(w) ==
    HasEntry(state.checkpoint, w) \/ Copies(state.journals, w) # {}

CommitsSurvive == \A w \in state.committed : CommitSurvives(w)
AcknowledgedWritesSurvive == \A w \in state.acknowledged : CommitSurvives(w)
EachQuorumCoversUncheckpointedCommits ==
    \A w \in state.committed :
        HasEntry(state.checkpoint, w) \/
            Cardinality(Available \ Copies(state.journals, w)) < Quorum

ReadyImpliesCommittedView ==
    state.phase = "ready" => Entries(state.visible) \subseteq state.committed

AcknowledgmentsAreCommitted == state.acknowledged \subseteq state.committed

EventuallyReady == (state.phase # "ready") ~> (state.phase = "ready")
NoPostRestartReadAndWrite ==
    ~(state.readAfterRestart /\ \E w \in state.acknowledged : w.epoch > 1)
=============================================================================
