---- MODULE WriterClockLease ----
(*
 * T7 §3: a writer clock lease. A writer samples only while its clock plus the
 * drift bound is before its durable lease end `Until` (Multi's
 * `Record::Lease { until }`, renewed on the clock's own schedule); a
 * successor writer, or a writer restored from the lineage's lease, starts
 * only once its clock passes `Until` plus the drift bound. It waits once, at
 * takeover, and never per append.
 *
 * Paired histories over one real-time tape and one pair of clock skews:
 * history A carries hidden predecessor commits that history B lacks; both
 * carry the same public ones. The successor's start must not tell them apart.
 *
 * Invariants:
 *   - SamplesMonotone: in each history every successor sample is later than
 *     every predecessor sample.
 *   - StartIndependent: the successor starts at the same real time in both
 *     histories, so the takeover wait carries nothing about hidden commits.
 *
 * Failing control (Mutation = "fence_last_stamp",
 * WriterClockLease_control_fence_last_stamp.cfg): the successor starts once
 * its clock passes its history's last stamp. Monotonicity still holds, but a
 * hidden tail commit delays the start in A only, violating StartIndependent.
 *
 * Witness (WriterClockLease_witness.cfg): NotWitnessReached is violated, so a
 * takeover after a hidden predecessor commit is reachable.
 *)
EXTENDS Integers, FiniteSets

CONSTANTS
    MaxTime,     \* real-time bound, e.g. 6
    Drift,       \* clock drift bound, e.g. 1
    Until,       \* the predecessor's durable lease end, e.g. 3
    Mutation     \* "none" | "fence_last_stamp"

Writers == {"pred", "succ"}
Skews == -Drift..Drift
NoStart == -1

VARIABLES
    rt,          \* real time
    skew,        \* [Writers -> Skews], fixed at Init
    predA,       \* predecessor samples in history A (public and hidden)
    predB,       \* predecessor samples in history B (public only)
    succA,       \* successor samples in history A
    succB,       \* successor samples in history B
    startA,      \* real time the successor started in A, or NoStart
    startB       \* real time the successor started in B, or NoStart

vars == <<rt, skew, predA, predB, succA, succB, startA, startB>>

Local(w) == rt + skew[w]

Init ==
    /\ rt = 0
    /\ skew \in [Writers -> Skews]
    /\ predA = {}
    /\ predB = {}
    /\ succA = {}
    /\ succB = {}
    /\ startA = NoStart
    /\ startB = NoStart

\* The predecessor samples only under its lease, and never once fenced by a
\* successor in either history.
PredCanSample ==
    /\ Local("pred") + Drift < Until
    /\ startA = NoStart
    /\ startB = NoStart

PublicPred ==
    /\ PredCanSample
    /\ predA' = predA \cup {Local("pred")}
    /\ predB' = predB \cup {Local("pred")}
    /\ UNCHANGED <<rt, skew, succA, succB, startA, startB>>

HiddenPred ==
    /\ PredCanSample
    /\ predA' = predA \cup {Local("pred")}
    /\ UNCHANGED <<rt, skew, predB, succA, succB, startA, startB>>

Max(S) == CHOOSE m \in S : \A x \in S : x <= m
LastStamp(P) == IF P = {} THEN -(MaxTime + Drift + 1) ELSE Max(P)

\* Whether the successor's fence lets it start, given its history's
\* predecessor samples.
MayStart(P) ==
    IF Mutation = "fence_last_stamp"
    THEN Local("succ") > LastStamp(P)
    ELSE Local("succ") > Until + Drift

StartAEnabled == startA = NoStart /\ MayStart(predA)
StartBEnabled == startB = NoStart /\ MayStart(predB)

StartA ==
    /\ StartAEnabled
    /\ startA' = rt
    /\ UNCHANGED <<rt, skew, predA, predB, succA, succB, startB>>

StartB ==
    /\ StartBEnabled
    /\ startB' = rt
    /\ UNCHANGED <<rt, skew, predA, predB, succA, succB, startA>>

SuccSample ==
    /\ startA # NoStart \/ startB # NoStart
    /\ succA' = IF startA # NoStart THEN succA \cup {Local("succ")} ELSE succA
    /\ succB' = IF startB # NoStart THEN succB \cup {Local("succ")} ELSE succB
    /\ UNCHANGED <<rt, skew, predA, predB, startA, startB>>

\* Time advances only once every enabled start has happened, so each history
\* starts at the first instant its fence allows.
Tick ==
    /\ rt < MaxTime
    /\ ~StartAEnabled
    /\ ~StartBEnabled
    /\ rt' = rt + 1
    /\ UNCHANGED <<skew, predA, predB, succA, succB, startA, startB>>

Quiesce ==
    /\ rt = MaxTime
    /\ ~StartAEnabled
    /\ ~StartBEnabled
    /\ UNCHANGED vars

Next ==
    \/ PublicPred
    \/ HiddenPred
    \/ StartA
    \/ StartB
    \/ SuccSample
    \/ Tick
    \/ Quiesce

Spec == Init /\ [][Next]_vars

SamplesMonotone ==
    /\ \A p \in predA, s \in succA : p < s
    /\ \A p \in predB, s \in succB : p < s

StartIndependent ==
    (startA # NoStart /\ startB # NoStart) => startA = startB

\* Violated exactly when the successor started after a hidden commit.
NotWitnessReached ==
    ~(startA # NoStart /\ startB # NoStart /\ predA # predB)
====
