---------------------- MODULE ExternalConfirmation ----------------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES synced, applied, checkpoint, received, confirmed, retained

Vars == <<synced, applied, checkpoint, received, confirmed, retained>>
Tip == 2

Init ==
    /\ synced = 0
    /\ applied = 0
    /\ checkpoint = 0
    /\ received = 0
    /\ confirmed = 0
    /\ retained \in BOOLEAN

Receive ==
    /\ received < Tip
    /\ received' = received + 1
    /\ UNCHANGED <<synced, applied, checkpoint, confirmed, retained>>

Sync ==
    /\ synced < received
    /\ synced' = synced + 1
    /\ UNCHANGED <<applied, checkpoint, received, confirmed, retained>>

Apply ==
    /\ applied < synced
    /\ applied' = applied + 1
    /\ UNCHANGED <<synced, checkpoint, received, confirmed, retained>>

Adopt ==
    /\ checkpoint < applied
    /\ checkpoint' = applied
    /\ UNCHANGED <<synced, applied, received, confirmed, retained>>

Recoverable == IF retained THEN synced ELSE checkpoint
Bound == IF retained THEN applied ELSE checkpoint

Confirm ==
    LET offered ==
        CASE Mutation = "trust_applied" -> applied
          [] Mutation = "first_block_exception" /\ checkpoint = 0 -> received
          [] Mutation = "trust_received" /\ retained -> received
          [] OTHER -> Bound
    IN
        /\ offered > confirmed
        /\ confirmed' = offered
        /\ UNCHANGED <<synced, applied, checkpoint, received, retained>>

Restart ==
    /\ synced' = Recoverable
    /\ applied' = Recoverable
    /\ received' = Recoverable
    /\ UNCHANGED <<checkpoint, confirmed, retained>>

Spec == Init /\ [][Receive \/ Sync \/ Apply \/ Adopt \/ Confirm \/ Restart]_Vars

ConfirmedRecoverable == confirmed <= Recoverable
NeverConfirmsBeforeFirstBlock == checkpoint = 0 => confirmed = 0

=============================================================================
