-------------------------- MODULE LineageRetirementAdmission --------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    generation,
    \* @type: Str;
    intent,
    \* @type: Bool;
    source,
    \* @type: Bool;
    reader,
    \* @type: Str;
    fork,
    \* @type: Int;
    preparedAt,
    \* @type: Bool;
    childOwns,
    \* @type: Bool;
    transferredAndSettled

\* @type: <<Int, Str, Bool, Bool, Str, Int, Bool, Bool>>;
vars == <<generation, intent, source, reader, fork, preparedAt, childOwns, transferredAndSettled>>

Init ==
    /\ generation = 0 /\ intent = "idle" /\ source = TRUE /\ reader = FALSE
    /\ fork = "waiting" /\ preparedAt = 0 /\ childOwns = FALSE /\ transferredAndSettled = FALSE

AdmitReader ==
    /\ source /\ ~reader /\ (intent = "idle" \/ Mutation = "admit_during_retirement")
    /\ reader' = TRUE
    /\ UNCHANGED <<generation, intent, source, fork, preparedAt, childOwns, transferredAndSettled>>

DrainReader ==
    /\ reader /\ reader' = FALSE
    /\ UNCHANGED <<generation, intent, source, fork, preparedAt, childOwns, transferredAndSettled>>

BeginFork ==
    /\ source /\ intent = "idle" /\ fork = "waiting"
    /\ fork' = "reading" /\ preparedAt' = generation
    /\ UNCHANGED <<generation, intent, source, reader, childOwns, transferredAndSettled>>

FinishReading ==
    /\ fork = "reading" /\ source /\ fork' = "prepared"
    /\ UNCHANGED <<generation, intent, source, reader, preparedAt, childOwns, transferredAndSettled>>

Publish ==
    /\ fork = "prepared" /\ intent = "idle"
    /\ (preparedAt = generation \/ Mutation = "check_only_open_gate")
    /\ fork' = "published"
    /\ UNCHANGED <<generation, intent, source, reader, preparedAt, childOwns, transferredAndSettled>>

Refuse ==
    /\ fork = "prepared" /\ intent = "idle" /\ preparedAt # generation /\ fork' = "refused"
    /\ UNCHANGED <<generation, intent, source, reader, preparedAt, childOwns, transferredAndSettled>>

Authorize ==
    /\ source /\ intent = "idle" /\ generation = 0
    /\ (~reader \/ Mutation = "ignore_active_reader") /\ fork # "reading"
    /\ generation' = generation + 1 /\ intent' = "retiring"
    /\ UNCHANGED <<source, reader, fork, preparedAt, childOwns, transferredAndSettled>>

TransferReference ==
    /\ intent = "retiring" /\ source /\ fork = "published" /\ ~childOwns
    /\ childOwns' = TRUE
    /\ UNCHANGED <<generation, intent, source, reader, fork, preparedAt, transferredAndSettled>>

Remove ==
    /\ intent = "retiring" /\ source
    /\ (fork # "published" \/ childOwns \/ Mutation = "skip_transfer")
    /\ source' = FALSE
    /\ UNCHANGED <<generation, intent, reader, fork, preparedAt, childOwns, transferredAndSettled>>

Settle ==
    /\ intent = "retiring" /\ ~source
    /\ intent' = "idle" /\ transferredAndSettled' = childOwns
    /\ UNCHANGED <<generation, source, reader, fork, preparedAt, childOwns>>

Next == AdmitReader \/ DrainReader \/ BeginFork \/ FinishReading \/ Publish \/ Refuse
        \/ Authorize \/ TransferReference \/ Remove \/ Settle
Spec == Init /\ [][Next]_vars

ReadersKeepTheirSource == (reader \/ fork = "reading") => source
PublishedForkHasBytes == fork = "published" => source \/ childOwns
Safe == ReadersKeepTheirSource /\ PublishedForkHasBytes
NeverRefusesOldPreparationAfterSettlement == ~(fork = "refused" /\ ~source /\ intent = "idle")
NeverTransfersAndSettles == ~transferredAndSettled
=============================================================================
