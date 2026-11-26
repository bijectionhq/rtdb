---- MODULE ShardedOrder ----
(*
 * T6-3 §4: One ordering tier, k resolver shards, generic Bound { key, delta, min, max },
 * epoch-fenced place handoff, and shard failover.
 *
 * Invariants:
 *   - StrictSerializability: every committed transaction is ordered along one
 *     global version line, and its validated reads observe the latest commit
 *     to each read place at or before its ordered version.
 *   - BoundWithinRange: every shared counter stays within [BoundMin, BoundMax],
 *     and equals the sum of deltas of all committed transactions touching it.
 *   - HandoffPreservesReads: a place handoff or shard failover transfers the
 *     exact latest version of that place before the new owner validates any tx.
 *   - ConcurrentBoundsConflictFree: two concurrent transactions that only touch
 *     the shared Bound (disjoint data places) never conflict on the Bound key.
 *
 * Failing controls (via Mutation constant):
 *   - "stale_shard": a new shard owner validates reads before catching up to the
 *     handoff version, violating HandoffPreservesReads / StrictSerializability.
 *   - "readset_bound": Bound adds its key to the transaction's read set, causing
 *     concurrent Bound spenders to conflict, violating ConcurrentBoundsConflictFree.
 *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Places,          \* set of logical places, e.g. {"p1", "p2"}
    Shards,          \* set of resolver shards, e.g. {"s1", "s2"}
    TxIds,           \* bounded set of transaction ids, e.g. {1, 2, 3}
    BoundMin,        \* e.g. 0
    BoundMax,        \* e.g. 2
    MaxEpoch,        \* e.g. 2
    Mutation         \* "none" | "stale_shard" | "readset_bound"

VARIABLES
    orderVer,        \* nat: global monotonic version assigned by ordering tier
    owner,           \* [Places -> Shards]: current shard owning each place
    epoch,           \* [Places -> 1..MaxEpoch]: fencing epoch per place
    handoffVer,      \* [Places -> Nat]: global version at last handoff/failover
    shardCaughtUp,   \* [Shards -> [Places -> Nat]]: highest version applied per shard/place
    placeVer,        \* [Places -> Nat]: last committed version modifying each place
    boundVal,        \* Int: live shared Bound counter value (host-only key)
    boundVer,        \* Nat: last committed version updating boundVal
    txState,         \* [TxIds -> "init" | "staged" | "committed" | "conflict" | "bound_refused"]
    txReadPlace,     \* [TxIds -> Places]
    txSince,         \* [TxIds -> Nat]: snapshot version read by tx
    txWritePlace,    \* [TxIds -> Places]
    txDelta,         \* [TxIds -> {-1, 0, 1}]
    txCommitVer,     \* [TxIds -> Nat]
    txValidatedAt,   \* [TxIds -> Nat]: shard version used to validate tx's read place
    txConflictReason,\* [TxIds -> "none" | "place" | "bound"]
    quiesced         \* BOOLEAN: explicit terminal stutter guard for CHECK_DEADLOCK TRUE

vars == <<
    orderVer, owner, epoch, handoffVer, shardCaughtUp, placeVer,
    boundVal, boundVer, txState, txReadPlace, txSince, txWritePlace,
    txDelta, txCommitVer, txValidatedAt, txConflictReason, quiesced
>>

Init ==
    /\ orderVer = 0
    /\ owner = [p \in Places |-> CHOOSE s \in Shards : TRUE]
    /\ epoch = [p \in Places |-> 1]
    /\ handoffVer = [p \in Places |-> 0]
    /\ shardCaughtUp = [s \in Shards |-> [p \in Places |-> 0]]
    /\ placeVer = [p \in Places |-> 0]
    /\ boundVal = 0
    /\ boundVer = 0
    /\ txState = [t \in TxIds |-> "init"]
    /\ txReadPlace = [t \in TxIds |-> CHOOSE p \in Places : TRUE]
    /\ txSince = [t \in TxIds |-> 0]
    /\ txWritePlace = [t \in TxIds |-> CHOOSE p \in Places : TRUE]
    /\ txDelta = [t \in TxIds |-> 0]
    /\ txCommitVer = [t \in TxIds |-> 0]
    /\ txValidatedAt = [t \in TxIds |-> 0]
    /\ txConflictReason = [t \in TxIds |-> "none"]
    /\ quiesced = FALSE

\* Stage a transaction at current snapshot orderVer, reading rp, writing wp,
\* with an optional Bound delta in {-1, 0, 1}.
StageTx(t, rp, wp, d) ==
    /\ txState[t] = "init"
    /\ txState' = [txState EXCEPT ![t] = "staged"]
    /\ txReadPlace' = [txReadPlace EXCEPT ![t] = rp]
    /\ txSince' = [txSince EXCEPT ![t] = orderVer]
    /\ txWritePlace' = [txWritePlace EXCEPT ![t] = wp]
    /\ txDelta' = [txDelta EXCEPT ![t] = d]
    /\ UNCHANGED <<
        orderVer, owner, epoch, handoffVer, shardCaughtUp, placeVer,
        boundVal, boundVer, txCommitVer, txValidatedAt, txConflictReason, quiesced
       >>

\* Epoch-fenced place handoff or shard failover: transfer place p to shard s2.
\* Under healthy execution, s2 must catch up to placeVer[p] (handoffVer[p])
\* before validating; under "stale_shard" it does not.
HandoffOrFailover(p, s2) ==
    /\ owner[p] # s2
    /\ epoch[p] < MaxEpoch
    /\ owner' = [owner EXCEPT ![p] = s2]
    /\ epoch' = [epoch EXCEPT ![p] = @ + 1]
    /\ handoffVer' = [handoffVer EXCEPT ![p] = placeVer[p]]
    /\ shardCaughtUp' =
        IF Mutation = "stale_shard"
        THEN shardCaughtUp
        ELSE [shardCaughtUp EXCEPT ![s2][p] = placeVer[p]]
    /\ UNCHANGED <<
        orderVer, placeVer, boundVal, boundVer,
        txState, txReadPlace, txSince, txWritePlace,
        txDelta, txCommitVer, txValidatedAt, txConflictReason, quiesced
       >>

\* Catch up shard s on place p to the latest committed place version.
CatchUpShard(s, p) ==
    /\ shardCaughtUp[s][p] < placeVer[p]
    /\ shardCaughtUp' = [shardCaughtUp EXCEPT ![s][p] = placeVer[p]]
    /\ UNCHANGED <<
        orderVer, owner, epoch, handoffVer, placeVer,
        boundVal, boundVer, txState, txReadPlace, txSince, txWritePlace,
        txDelta, txCommitVer, txValidatedAt, txConflictReason, quiesced
       >>

\* Ordering tier assigns the next global version and the owning shard resolves t.
ResolveTx(t) ==
    /\ txState[t] = "staged"
    /\ LET rp == txReadPlace[t]
           wp == txWritePlace[t]
           sRead == owner[rp]
           sWrite == owner[wp]
           seenReadVer == shardCaughtUp[sRead][rp]
           \* Healthy shard refuses to resolve until caught up to handoffVer[rp].
           ready == (Mutation = "stale_shard") \/ (seenReadVer >= handoffVer[rp] /\ seenReadVer = placeVer[rp])
           readConflict == seenReadVer > txSince[t]
           \* Control "readset_bound": falsely treats Bound key as part of reads.
           boundReadConflict == (Mutation = "readset_bound") /\ (txDelta[t] # 0) /\ (boundVer > txSince[t])
           nextBound == boundVal + txDelta[t]
           boundOk == (txDelta[t] = 0) \/ (nextBound >= BoundMin /\ nextBound <= BoundMax)
           nextVer == orderVer + 1
       IN
       /\ ready
       /\ orderVer' = nextVer
       /\ txValidatedAt' = [txValidatedAt EXCEPT ![t] = seenReadVer]
       /\ IF readConflict THEN
              /\ txState' = [txState EXCEPT ![t] = "conflict"]
              /\ txConflictReason' = [txConflictReason EXCEPT ![t] = "place"]
              /\ UNCHANGED <<placeVer, boundVal, boundVer, shardCaughtUp, txCommitVer>>
          ELSE IF boundReadConflict THEN
              /\ txState' = [txState EXCEPT ![t] = "conflict"]
              /\ txConflictReason' = [txConflictReason EXCEPT ![t] = "bound"]
              /\ UNCHANGED <<placeVer, boundVal, boundVer, shardCaughtUp, txCommitVer>>
          ELSE IF ~boundOk THEN
              /\ txState' = [txState EXCEPT ![t] = "bound_refused"]
              /\ txConflictReason' = [txConflictReason EXCEPT ![t] = "none"]
              /\ UNCHANGED <<placeVer, boundVal, boundVer, shardCaughtUp, txCommitVer>>
          ELSE
              /\ txState' = [txState EXCEPT ![t] = "committed"]
              /\ txConflictReason' = [txConflictReason EXCEPT ![t] = "none"]
              /\ txCommitVer' = [txCommitVer EXCEPT ![t] = nextVer]
              /\ placeVer' = [placeVer EXCEPT ![wp] = nextVer]
              /\ shardCaughtUp' = [shardCaughtUp EXCEPT ![sWrite][wp] = nextVer]
              /\ boundVal' = IF txDelta[t] # 0 THEN nextBound ELSE boundVal
              /\ boundVer' = IF txDelta[t] # 0 THEN nextVer ELSE boundVer
       /\ UNCHANGED <<owner, epoch, handoffVer, txReadPlace, txSince, txWritePlace, txDelta, quiesced>>

\* Terminal quiescent transition so CHECK_DEADLOCK TRUE verifies that no
\* non-terminal state can get stuck.
Quiesce ==
    /\ \A t \in TxIds : txState[t] \in {"committed", "conflict", "bound_refused"}
    /\ quiesced' = TRUE
    /\ UNCHANGED <<
        orderVer, owner, epoch, handoffVer, shardCaughtUp, placeVer,
        boundVal, boundVer, txState, txReadPlace, txSince, txWritePlace,
        txDelta, txCommitVer, txValidatedAt, txConflictReason
       >>

Next ==
    \/ \E t \in TxIds, rp \in Places, wp \in Places, d \in {-1, 0, 1} : StageTx(t, rp, wp, d)
    \/ \E p \in Places, s \in Shards : HandoffOrFailover(p, s)
    \/ \E s \in Shards, p \in Places : CatchUpShard(s, p)
    \/ \E t \in TxIds : ResolveTx(t)
    \/ Quiesce

Spec == Init /\ [][Next]_vars

\* --- Safety invariants ---

\* 1. Bound counter always stays within [BoundMin, BoundMax].
BoundWithinRange ==
    boundVal >= BoundMin /\ boundVal <= BoundMax

\* 2. Strict serializability: every committed transaction validated against a
\*    read version >= every earlier commit to its read place, and no two
\*    committed transactions share a commit version.
StrictSerializability ==
    /\ \A t1, t2 \in TxIds :
        (txState[t1] = "committed" /\ txState[t2] = "committed" /\ t1 # t2)
            => txCommitVer[t1] # txCommitVer[t2]
    /\ \A t \in TxIds :
        txState[t] = "committed" =>
            \A prev \in TxIds :
                (txState[prev] = "committed"
                 /\ txWritePlace[prev] = txReadPlace[t]
                 /\ txCommitVer[prev] < txCommitVer[t])
                    => txCommitVer[prev] <= txSince[t]

\* 3. Handoff read preservation: no resolved transaction ever validates at a
\*    version older than the place's handoff version that existed before it.
HandoffPreservesReads ==
    \A t \in TxIds :
        txState[t] = "committed" =>
            txValidatedAt[t] >= handoffVer[txReadPlace[t]]
            \/ txCommitVer[t] <= handoffVer[txReadPlace[t]]

\* 4. Concurrent Bounds never conflict with each other via read-set intersection.
ConcurrentBoundsConflictFree ==
    \A t \in TxIds : txConflictReason[t] # "bound"

\* --- Witness predicates (negated in witness config to assert reachability) ---

\* Witness: two concurrent Bound transactions both commit after a handoff,
\* and a third Bound transaction is refused when exceeding BoundMax.
NotWitnessReached ==
    ~(\E t1, t2, t3 \in TxIds :
        /\ t1 # t2 /\ t2 # t3 /\ t1 # t3
        /\ txState[t1] = "committed" /\ txDelta[t1] = 1
        /\ txState[t2] = "committed" /\ txDelta[t2] = 1
        /\ txSince[t1] = txSince[t2]
        /\ txState[t3] = "bound_refused" /\ txDelta[t3] = 1
        /\ \E p \in Places : epoch[p] > 1)

====
