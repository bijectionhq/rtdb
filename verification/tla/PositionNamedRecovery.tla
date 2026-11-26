---- MODULE PositionNamedRecovery ----
(*
 * T7 §3: positions name transactions. The writer's clock may repeat micros
 * (ties are admitted), so recovery must group a stage's events into
 * transactions by each event's position, never by its system time.
 *
 * Model: one writer appends transactions at consecutive positions, each
 * stamped with the writer's monotone clock (which may stand still between
 * commits) and carrying 1..MaxEvents events. Every staged event carries its
 * transaction's position and its system time. Recovery groups the stage.
 *
 * Invariants:
 *   - TxAtomic: after recovery every recovered group is exactly one committed
 *     transaction's events, and every committed transaction is one group.
 *   - StampMonotone: system time never decreases along positions.
 *
 * Failing control (Mutation = "group_by_time", PositionNamedRecovery_control_group_by_time.cfg):
 *   recovery groups by system time, as Default's stage did before T7; two
 *   transactions tied on one microsecond merge, violating TxAtomic.
 *
 * Witness (PositionNamedRecovery_witness.cfg): NotWitnessReached is violated,
 * so a recovered history with two tied transactions is reachable.
 *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    MaxTx,       \* bound on committed transactions, e.g. 3
    MaxTime,     \* bound on the writer's clock, e.g. 1 (forces ties)
    MaxEvents,   \* bound on events per transaction, e.g. 2
    Mutation     \* "none" | "group_by_time"

VARIABLES
    clock,       \* the writer's clock: never decreases, may stand still
    txs,         \* Seq of [time, events]: committed transactions by position
    stage,       \* set of staged events [pos, ord, time]
    recovered,   \* set of recovered groups (sets of staged events)
    done         \* recovery ran (terminal stutter guard for CHECK_DEADLOCK TRUE)

vars == <<clock, txs, stage, recovered, done>>

Init ==
    /\ clock = 0
    /\ txs = <<>>
    /\ stage = {}
    /\ recovered = {}
    /\ done = FALSE

Tick ==
    /\ ~done
    /\ clock < MaxTime
    /\ clock' = clock + 1
    /\ UNCHANGED <<txs, stage, recovered, done>>

Commit(n) ==
    /\ ~done
    /\ Len(txs) < MaxTx
    /\ LET pos == Len(txs) + 1 IN
       /\ txs' = Append(txs, [time |-> clock, events |-> n])
       /\ stage' = stage \cup { [pos |-> pos, ord |-> o, time |-> clock] : o \in 1..n }
    /\ UNCHANGED <<clock, recovered, done>>

\* What recovery names a transaction by.
Key(e) == IF Mutation = "group_by_time" THEN e.time ELSE e.pos

Group(k) == { e \in stage : Key(e) = k }

Recover ==
    /\ ~done
    /\ recovered' = { Group(k) : k \in { Key(e) : e \in stage } }
    /\ done' = TRUE
    /\ UNCHANGED <<clock, txs, stage>>

Quiesce ==
    /\ done
    /\ UNCHANGED vars

Next ==
    \/ Tick
    \/ \E n \in 1..MaxEvents : Commit(n)
    \/ Recover
    \/ Quiesce

Spec == Init /\ [][Next]_vars

TxEvents(p) == { e \in stage : e.pos = p }

TxAtomic ==
    done =>
        /\ \A g \in recovered : \E p \in 1..Len(txs) : g = TxEvents(p)
        /\ \A p \in 1..Len(txs) : TxEvents(p) \in recovered

StampMonotone ==
    \A i, j \in 1..Len(txs) : i < j => txs[i].time <= txs[j].time

\* Violated exactly when a recovered history holds two tied transactions.
NotWitnessReached ==
    ~(done /\ \E i, j \in 1..Len(txs) : i < j /\ txs[i].time = txs[j].time)
====
