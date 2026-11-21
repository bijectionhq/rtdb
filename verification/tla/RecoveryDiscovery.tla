------------------------- MODULE RecoveryDiscovery --------------------------
EXTENDS Integers

CONSTANTS
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Int;
    head,
    \* @type: Set(Int);
    roots,
    \* @type: Set(Int);
    tables,
    \* @type: Str;
    phase,
    \* @type: Int;
    target,
    \* @type: Int;
    lastMissing,
    \* @type: Int;
    observed,
    \* @type: Bool;
    badPayload

Vars == <<head, roots, tables, phase, target, lastMissing, observed, badPayload>>
None == -1
NoObservation == -2

Init ==
    /\ head = 0
    /\ roots = {0}
    /\ tables = {0}
    /\ phase = "initial"
    /\ target = None
    /\ lastMissing = None
    /\ observed = NoObservation
    /\ badPayload = FALSE

TypeOK ==
    /\ head \in 0..2
    /\ roots \subseteq 0..head
    /\ tables \subseteq 0..head
    /\ head \in roots \cap tables
    /\ phase \in {"initial", "root", "table", "refresh", "ready", "empty", "missing", "corrupt"}
    /\ {target, lastMissing} \subseteq None..head
    /\ observed \in NoObservation..head
    /\ badPayload \in BOOLEAN

Publish ==
    /\ head < 2
    /\ head' = head + 1
    /\ roots' = roots \cup {head'}
    /\ tables' = tables \cup {head'}
    /\ UNCHANGED <<phase, target, lastMissing, observed, badPayload>>

CollectRoot(g) ==
    /\ g \in roots
    /\ g < head
    /\ roots' = roots \ {g}
    /\ UNCHANGED <<head, tables, phase, target, lastMissing, observed, badPayload>>

CollectTable(g) ==
    /\ g \in tables
    /\ g < head
    /\ tables' = tables \ {g}
    /\ UNCHANGED <<head, roots, phase, target, lastMissing, observed, badPayload>>

(* A listing can be stale or incomplete. It cannot report an unpublished generation. *)
Discover(g) ==
    /\ phase = "initial"
    /\ phase' = IF g = None THEN "empty" ELSE "root"
    /\ target' = g
    /\ UNCHANGED <<head, roots, tables, lastMissing, observed, badPayload>>

MissingPhase == IF Mutation = "fail_fast" THEN "missing" ELSE "refresh"

ReadRoot ==
    /\ phase = "root"
    /\ phase' = IF target \in roots THEN "table" ELSE MissingPhase
    /\ lastMissing' = IF target \in roots THEN lastMissing ELSE target
    /\ observed' = IF target \in roots THEN observed ELSE NoObservation
    /\ UNCHANGED <<head, roots, tables, target, badPayload>>

ReadTable ==
    /\ phase = "table"
    /\ phase' = IF target \in tables THEN "ready" ELSE MissingPhase
    /\ lastMissing' = IF target \in tables THEN lastMissing ELSE target
    /\ observed' = IF target \in tables THEN observed ELSE NoObservation
    /\ UNCHANGED <<head, roots, tables, target, badPayload>>

InvalidPayload ==
    /\ phase \in {"root", "table"}
    /\ IF Mutation = "retry_corrupt"
          THEN /\ phase' = "refresh"
               /\ lastMissing' = target
          ELSE /\ phase' = "corrupt"
               /\ UNCHANGED lastMissing
    /\ badPayload' = TRUE
    /\ UNCHANGED <<head, roots, tables, target, observed>>

CanAdvance(g) ==
    IF Mutation = "ignore_newer" THEN FALSE
    ELSE IF Mutation = "allow_same" THEN g >= lastMissing
    ELSE g > lastMissing

TryReplacement(g) ==
    /\ phase = "refresh"
    /\ observed' = g
    /\ IF CanAdvance(g)
          THEN /\ phase' = "root"
               /\ target' = g
          ELSE /\ phase' = IF Mutation = "empty_after_missing" /\ g = None
                                THEN "empty" ELSE "missing"
               /\ UNCHANGED target
    /\ UNCHANGED <<head, roots, tables, lastMissing, badPayload>>

Next ==
    \/ Publish
    \/ \E g \in 0..head : CollectRoot(g) \/ CollectTable(g)
    \/ \E g \in None..head : Discover(g) \/ TryReplacement(g)
    \/ ReadRoot \/ ReadTable \/ InvalidPayload

Spec == Init /\ [][Next]_Vars

AttemptsAdvance == phase \in {"root", "table"} => lastMissing < target
NoEmptyAfterMissing == phase = "empty" => lastMissing = None
UsesNewerListing == phase = "missing" => observed <= lastMissing
CorruptionIsTerminal == badPayload => phase = "corrupt"
MissingFailureHasRefreshed == phase = "missing" => observed # NoObservation
DiscoverySafe ==
    /\ AttemptsAdvance /\ NoEmptyAfterMissing /\ UsesNewerListing
    /\ CorruptionIsTerminal /\ MissingFailureHasRefreshed
NeverRecoveredAfterRefresh == phase # "ready" \/ lastMissing = None

=============================================================================
