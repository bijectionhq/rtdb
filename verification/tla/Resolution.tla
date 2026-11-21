--------------------------- MODULE Resolution ----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Set(Str);
    Clients,
    \* @type: Str;
    Mode,
    \* @type: Int;
    Limit
VARIABLES
    \* @type: Int;
    revision,
    \* @type: Int;
    value,
    \* @type: Str -> {base: Int, result: Int, staged: Bool};
    held

Vars == <<revision, value, held>>
Ready == [base |-> 0, result |-> 0, staged |-> FALSE]
Init == revision = 0 /\ value = 0 /\ held = [c \in Clients |-> Ready]

Resolve(c) ==
    /\ ~held[c].staged
    /\ Mode # "serialized" \/ \A d \in Clients : ~held[d].staged
    /\ held' = [held EXCEPT ![c] = [base |-> revision, result |-> value + 1, staged |-> TRUE]]
    /\ UNCHANGED <<revision, value>>

Commit(c) ==
    /\ held[c].staged
    /\ revision < Limit
    /\ Mode = "blind" \/ held[c].base = revision
    /\ revision' = revision + 1
    /\ value' = held[c].result
    /\ held' = [held EXCEPT ![c] = Ready]

Retry(c) ==
    /\ held[c].staged
    /\ held[c].base < revision
    /\ held' = [held EXCEPT ![c] = Ready]
    /\ UNCHANGED <<revision, value>>

Next == \E c \in Clients : Resolve(c) \/ Commit(c) \/ Retry(c)
Spec == Init /\ [][Next]_Vars
NoLostUpdates == revision = value

=============================================================================
