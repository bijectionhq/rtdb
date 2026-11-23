------------------------------ MODULE TombstoneDisposal ------------------------------
EXTENDS FiniteSets

CONSTANT Mutation
VARIABLES phase, upload, sent, terminal

Attempts == {1, 2}
Vars == <<phase, upload, sent, terminal>>

Init ==
    /\ phase = "retiring" /\ upload = [id \in Attempts |-> "unused"]
    /\ sent = {} /\ terminal = FALSE

Submit(id) ==
    /\ phase = "retiring" /\ upload[id] = "unused" /\ id \notin sent
    /\ upload' = IF Mutation = "untracked_retry" /\ id = 2
                 THEN upload ELSE [upload EXCEPT ![id] = "open"]
    /\ sent' = sent \cup {id}
    /\ UNCHANGED <<phase, terminal>>

Complete(id) ==
    /\ id \in sent /\ sent' = sent \ {id}
    /\ LET canApply == upload[id] = "open" \/ (Mutation = "untracked_retry" /\ id = 2) IN
           /\ terminal' = (terminal \/ canApply)
           /\ upload' = IF canApply THEN [upload EXCEPT ![id] = "closed"] ELSE upload
    /\ UNCHANGED phase

Abort(id) ==
    /\ upload[id] = "open" /\ upload' = [upload EXCEPT ![id] = "closed"]
    /\ UNCHANGED <<phase, sent, terminal>>

Settle ==
    /\ phase = "retiring" /\ terminal
    /\ (\A id \in Attempts : upload[id] # "open") \/ Mutation = "settle_before_drain"
    /\ phase' = "settled"
    /\ UNCHANGED <<upload, sent, terminal>>

Purge ==
    /\ phase = "settled" /\ phase' = "disposed" /\ terminal' = FALSE
    /\ UNCHANGED <<upload, sent>>

Next == Settle \/ Purge \/ (\E id \in Attempts : Submit(id) \/ Complete(id) \/ Abort(id))
Spec == Init /\ [][Next]_Vars

ReceiptMeansEmpty == phase = "disposed" => ~terminal
NeverPurgesWithLateProviderWork == ~(phase = "disposed" /\ sent # {})
=============================================================================
