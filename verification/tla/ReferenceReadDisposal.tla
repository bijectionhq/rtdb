--------------------------- MODULE ReferenceReadDisposal ---------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    Mutation
Readers == {1, 2}

VARIABLES
    \* @type: Str;
    phase,
    \* @type: Set(Int);
    held,
    \* @type: Set(Int);
    pending,
    \* @type: Set(Int);
    cancelled,
    \* @type: Set(Int);
    finished,
    \* @type: Bool;
    copy,
    \* @type: Bool;
    late

\* @type: <<Str, Set(Int), Set(Int), Set(Int), Set(Int), Bool, Bool>>;
vars == <<phase, held, pending, cancelled, finished, copy, late>>

Init ==
    /\ phase = "open" /\ copy = FALSE /\ late = FALSE
    /\ held = {} /\ pending = {} /\ cancelled = {} /\ finished = {}

Admit(r) ==
    /\ (phase = "open" \/ (phase = "closing" /\ Mutation = "late_admission"))
    /\ r \notin (pending \cup finished)
    /\ pending' = pending \cup {r}
    /\ held' = IF Mutation = "wrong_owner" THEN held ELSE held \cup {r}
    /\ late' = (phase # "open")
    /\ UNCHANGED <<phase, cancelled, finished, copy>>

Cancel(r) ==
    /\ r \in pending /\ r \notin cancelled
    /\ cancelled' = cancelled \cup {r}
    /\ held' = IF Mutation = "drop_on_cancel" THEN held \ {r} ELSE held
    /\ UNCHANGED <<phase, pending, finished, copy, late>>

Drain(r) ==
    /\ r \in pending
    /\ pending' = pending \ {r}
    /\ held' = held \ {r}
    /\ finished' = finished \cup {r}
    /\ UNCHANGED <<phase, cancelled, copy, late>>

Fence ==
    /\ phase = "open" /\ phase' = "closing"
    /\ UNCHANGED <<held, pending, cancelled, finished, copy, late>>

Transfer ==
    /\ phase = "closing" /\ ~copy
    /\ copy' = TRUE
    /\ UNCHANGED <<phase, held, pending, cancelled, finished, late>>

Erase ==
    /\ phase = "closing"
    /\ (held = {} \/ Mutation = "skip_drain")
    /\ (copy \/ Mutation = "skip_transfer")
    /\ phase' = "erased"
    /\ UNCHANGED <<held, pending, cancelled, finished, copy, late>>

Next ==
    (\E r \in Readers : Admit(r) \/ Cancel(r) \/ Drain(r)) \/ Fence \/ Transfer \/ Erase
Spec == Init /\ [][Next]_vars

PendingIsOwned == pending \subseteq held
ClosedDoesNotAdmit == ~late
EraseAfterDrain == phase = "erased" => pending = {}
ForkStillHasBytes == phase = "erased" => copy
Safe == PendingIsOwned /\ ClosedDoesNotAdmit /\ EraseAfterDrain /\ ForkStillHasBytes
NeverDisposesAfterCancelledRead == ~(phase = "erased" /\ cancelled # {} /\ copy)
=============================================================================
