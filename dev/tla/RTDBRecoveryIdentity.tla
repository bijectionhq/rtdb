------------------------- MODULE RTDBRecoveryIdentity ------------------------
EXTENDS Integers, FiniteSets

CONSTANTS Addresses, FreshJournal, CheckJournalIdentity, AdvertiseBeforeOpen
VARIABLE state

Quorum == (Cardinality(Addresses) \div 2) + 1
Unbound == "unbound"
EmptyVote == "no vote"

Init ==
    state = [copies |-> {},
             replaced |-> Unbound,
             journalAt |-> [address \in Addresses |-> address],
             process |-> [address \in Addresses |-> "ready"],
             eligible |-> Addresses,
             votes |-> [address \in Addresses |-> EmptyVote],
             result |-> "waiting"]

Acknowledge(copies) ==
    /\ state.copies = {}
    /\ Cardinality(copies) = Quorum
    /\ state' = [state EXCEPT !.copies = copies]

Replace(address) ==
    /\ state.copies # {}
    /\ state.replaced = Unbound
    /\ state' = [state EXCEPT
                 !.replaced = address,
                 !.journalAt[address] = FreshJournal,
                 !.process[address] = "opening",
                 !.eligible = IF AdvertiseBeforeOpen THEN @ ELSE @ \ {address}]

OpenJournal(succeeds) ==
    LET address == state.replaced
    IN /\ address \in Addresses
       /\ state.process[address] = "opening"
       /\ state' = [state EXCEPT
                    !.process[address] = IF succeeds THEN "ready" ELSE "failed",
                    !.eligible = IF succeeds THEN @ \cup {address} ELSE @]

AcceptSeal(address) ==
    /\ state.replaced # Unbound
    /\ state.result = "waiting"
    /\ state.process[address] = "ready"
    /\ state.votes[address] = EmptyVote
    /\ (~CheckJournalIdentity \/ state.journalAt[address] = address)
    /\ state' = [state EXCEPT !.votes[address] = state.journalAt[address]]

Sealed == {address \in Addresses : state.votes[address] # EmptyVote}

Recover ==
    /\ state.result = "waiting"
    /\ Cardinality(Sealed) >= Quorum
    /\ state' = [state EXCEPT
                 !.result = IF \E address \in Sealed : state.votes[address] \in state.copies
                            THEN "found" ELSE "missing acknowledged write"]

Next ==
    \/ \E copies \in SUBSET Addresses : Acknowledge(copies)
    \/ \E address \in Addresses : Replace(address)
    \/ \E succeeds \in BOOLEAN : OpenJournal(succeeds)
    \/ \E address \in Addresses : AcceptSeal(address)
    \/ Recover

Spec == Init /\ [][Next]_state

FailedJournalsAreIneligible ==
    \A address \in state.eligible : state.process[address] # "failed"

CountedVotesMatchExpectedJournal ==
    \A address \in Sealed : state.votes[address] = address

RecoveredHistoryPreservesAcknowledgedWrite ==
    state.result # "missing acknowledged write"
=============================================================================
