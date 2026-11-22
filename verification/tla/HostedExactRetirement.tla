----------------------- MODULE HostedExactRetirement -----------------------
EXTENDS Integers

CONSTANT Mutation

VARIABLES phase, preparing, reference, read, fence, oldVersion, replacement,
          pending, sent, erasedAfterSettlement

Vars == <<phase, preparing, reference, read, fence, oldVersion, replacement,
          pending, sent, erasedAfterSettlement>>

Init ==
    /\ phase = "open" /\ preparing = FALSE /\ reference = FALSE
    /\ read = FALSE /\ fence = FALSE /\ oldVersion = TRUE /\ replacement = FALSE
    /\ pending = 0 /\ sent = 0 /\ erasedAfterSettlement = FALSE

PrepareFork ==
    /\ phase = "open" /\ ~preparing /\ oldVersion
    /\ preparing' = TRUE
    /\ UNCHANGED <<phase, reference, read, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

PublishFork ==
    /\ preparing /\ phase = "open"
    /\ preparing' = FALSE /\ reference' = TRUE
    /\ UNCHANGED <<phase, read, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Withdraw ==
    /\ reference /\ reference' = FALSE
    /\ UNCHANGED <<phase, preparing, read, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

AdmitPhysicalRead ==
    /\ ~read /\ oldVersion /\ (~fence \/ Mutation = "ignore_fence")
    /\ read' = TRUE
    /\ UNCHANGED <<phase, preparing, reference, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Drain ==
    /\ read /\ read' = FALSE
    /\ UNCHANGED <<phase, preparing, reference, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Reserve ==
    /\ phase = "open" /\ ~preparing /\ oldVersion
    /\ phase' = "reserved"
    /\ UNCHANGED <<preparing, reference, read, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

FenceOwner ==
    /\ phase = "reserved" /\ ~fence /\ ~read
    /\ fence' = TRUE
    /\ UNCHANGED <<phase, preparing, reference, read, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Authorize ==
    /\ phase = "reserved" /\ fence
    /\ (~reference \/ Mutation = "ignore_reference")
    /\ phase' = "erasing"
    /\ UNCHANGED <<preparing, reference, read, fence, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Abandon ==
    /\ phase = "reserved" \/ (phase = "erasing" /\ Mutation = "abandon_authorized")
    /\ phase' = "open" /\ fence' = FALSE
    /\ UNCHANGED <<preparing, reference, read, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Submit ==
    /\ phase = "erasing" /\ sent < 2
    /\ sent' = sent + 1 /\ pending' = pending + 1
    /\ UNCHANGED <<phase, preparing, reference, read, fence, oldVersion, replacement, erasedAfterSettlement>>

PublishReplacement ==
    /\ ~replacement /\ replacement' = TRUE
    /\ UNCHANGED <<phase, preparing, reference, read, fence, oldVersion, pending, sent, erasedAfterSettlement>>

ProviderEffect ==
    /\ pending > 0
    /\ pending' = pending - 1 /\ oldVersion' = FALSE
    /\ replacement' = IF Mutation = "delete_current_name" THEN FALSE ELSE replacement
    /\ erasedAfterSettlement' = (erasedAfterSettlement \/ (phase = "settled" /\ replacement))
    /\ UNCHANGED <<phase, preparing, reference, read, fence, sent>>

Settle ==
    /\ phase = "erasing" /\ ~oldVersion
    /\ phase' = "settled" /\ fence' = FALSE
    /\ UNCHANGED <<preparing, reference, read, oldVersion, replacement, pending, sent, erasedAfterSettlement>>

Next == PrepareFork \/ PublishFork \/ Withdraw \/ AdmitPhysicalRead \/ Drain
        \/ Reserve \/ FenceOwner \/ Authorize \/ Abandon \/ Submit
        \/ PublishReplacement \/ ProviderEffect \/ Settle
Spec == Init /\ [][Next]_Vars

PreservesDependencies == (reference \/ read \/ preparing) => oldVersion
UnsettledEffectsKeepFence == pending > 0 /\ oldVersion => fence
ReplacementSurvives == [][replacement => replacement']_Vars
Safe == PreservesDependencies /\ UnsettledEffectsKeepFence
NeverRetriesAfterSettlement == ~erasedAfterSettlement
=============================================================================
