--------------------------- MODULE PlacedAttemptOutcome ---------------------------
EXTENDS Integers, Sequences

CONSTANT Mutation

(* Two histories, writer epochs and principals share one local position. *)
Attempts == (1..2) \X (1..2) \X (1..2)
None == <<0, 0, 0>>

VARIABLES staged, journal, applied, witnesses, checkpoint, restored, lookup
Vars == <<staged, journal, applied, witnesses, checkpoint, restored, lookup>>

Prefix(length) == {journal[i] : i \in 1..length}

Init ==
    /\ staged = None /\ journal = <<>> /\ applied = 0 /\ witnesses = {}
    /\ checkpoint = [position |-> 0, witnesses |-> {}] /\ restored = FALSE
    /\ lookup = <<None, None>>

Stage(attempt) ==
    /\ staged = None /\ Len(journal) < 2
    /\ \A prior \in Prefix(Len(journal)) : <<prior[1], prior[2]>> # <<attempt[1], attempt[2]>>
    /\ staged' = attempt
    /\ witnesses' = IF Mutation = "visible_before_commit" THEN witnesses \cup {attempt} ELSE witnesses
    /\ UNCHANGED <<journal, applied, checkpoint, restored, lookup>>

Commit ==
    /\ staged # None
    /\ journal' = Append(journal, staged) /\ staged' = None
    /\ UNCHANGED <<applied, witnesses, checkpoint, restored, lookup>>

Apply ==
    /\ applied < Len(journal)
    /\ applied' = applied + 1 /\ witnesses' = witnesses \cup {journal[applied']}
    /\ UNCHANGED <<staged, journal, checkpoint, restored, lookup>>

PublishCheckpoint ==
    /\ applied > checkpoint.position
    /\ checkpoint' = [position |-> applied, witnesses |-> witnesses]
    /\ UNCHANGED <<staged, journal, applied, witnesses, restored, lookup>>

Recover ==
    /\ checkpoint.position > 0 /\ ~restored
    /\ staged' = None /\ applied' = checkpoint.position
    /\ witnesses' = IF Mutation = "forget_witness_at_restore" THEN {} ELSE checkpoint.witnesses
    /\ restored' = TRUE
    /\ UNCHANGED <<journal, checkpoint, lookup>>

Match(requested, found) ==
    /\ (requested[1] = found[1] \/ Mutation = "ignore_history")
    /\ (requested[2] = found[2] \/ Mutation = "ignore_epoch")
    /\ (requested[3] = found[3] \/ Mutation = "ignore_principal")

Lookup(requested, found) ==
    /\ lookup = <<None, None>> /\ found \in witnesses /\ Match(requested, found)
    /\ lookup' = <<requested, found>>
    /\ UNCHANGED <<staged, journal, applied, witnesses, checkpoint, restored>>

Next ==
    \/ \E attempt \in Attempts : Stage(attempt)
    \/ \E requested, found \in Attempts : Lookup(requested, found)
    \/ Commit \/ Apply \/ PublishCheckpoint \/ Recover

Spec == Init /\ [][Next]_Vars

WitnessesCoverInstalledPrefix == witnesses = Prefix(applied)
LookupNamesExactAttempt == lookup[1] = lookup[2]
NeverRecoversWitness == ~(restored /\ witnesses # {})

\* PlacedReceiverSlots models reservation, fencing and succession.

===============================================================================
