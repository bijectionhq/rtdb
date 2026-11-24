-------------------------- MODULE ExactTemporalCut --------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANT Defect
VARIABLES history, cut, erased, reopened, seen
vars == <<history, cut, erased, reopened, seen>>
Cap == 20
None == 0
Loss == -1

Init ==
    /\ history = <<[sql |-> 10, append |-> 10]>>
    /\ cut = 0
    /\ erased = {}
    /\ reopened = FALSE
    /\ seen = None

AppendWrite ==
    /\ Len(history) < 3
    /\ \E sql \in {10, 15, 20, 40}, at \in {10, 40}:
        /\ sql >= history[Len(history)].sql
        /\ sql <= at \/ Defect = "lead"
        /\ history' = Append(history, [sql |-> sql, append |-> at])
    /\ UNCHANGED <<cut, erased, reopened, seen>>

Capture ==
    /\ cut = 0
    /\ Len(history) = 1
    /\ cut' = Len(history)
    /\ UNCHANGED <<history, erased, reopened, seen>>

Erase ==
    /\ cut # 0
    /\ \E p \in 1..Len(history) \ erased: erased' = erased \cup {p}
    /\ reopened' = FALSE
    /\ UNCHANGED <<history, cut, seen>>

Winner(h, limit, hidden) ==
    LET eligible == {p \in (1..limit) \ hidden: h[p].sql <= Cap}
    IN IF eligible = {} THEN None
       ELSE CHOOSE p \in eligible :
         \A q \in eligible :
           \/ h[p].sql > h[q].sql
           \/ /\ h[p].sql = h[q].sql
              /\ IF Defect = "tie" THEN p <= q ELSE p >= q

Deliver(p, masks) == IF p \in masks THEN Loss ELSE p

Reopen ==
    /\ cut # 0
    /\ seen' = Deliver(
        Winner(history, IF Defect = "cap" THEN Len(history) ELSE cut,
            IF Defect = "survivor" THEN erased ELSE {}),
        erased)
    /\ reopened' = TRUE
    /\ UNCHANGED <<history, cut, erased>>

Next == AppendWrite \/ Capture \/ Erase \/ Reopen
Spec == Init /\ [][Next]_vars

NoLead == \A p \in 1..Len(history): history[p].sql <= history[p].append
ExactResult == reopened => seen = (IF 1 \in erased THEN Loss ELSE 1)
TiePrecedence ==
    \A n \in 2..Len(history):
        (history[n].sql = history[n-1].sql /\ history[n].sql <= Cap)
            => Winner(history, n, {}) = n

NoBackdatedSuffix ==
    ~(reopened /\ Len(history) >= 2 /\ history[2].sql = 15 /\ history[2].append = 40)
NoTiedCut == ~(reopened /\ Len(history) >= 2 /\ history[2].sql = 10)
NoErasedWinner == ~(reopened /\ seen = Loss)
=============================================================================
