---------------------------- MODULE PublishedCheckpoint ----------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: $checkpoint;
    selected,
    \* @type: $checkpoint;
    previous,
    \* @type: Str;
    phase,
    \* @type: Bool;
    retried

Vars == <<selected, previous, phase, retried>>

\* @typeAlias: checkpoint = {block: Int, journal: Int, boundary: Int};
Published == [block |-> 0, journal |-> 0, boundary |-> 1]
Candidates == {Published,
    [block |-> 0, journal |-> 0, boundary |-> 2],
    [block |-> 1, journal |-> 0, boundary |-> 3],
    [block |-> 2, journal |-> 0, boundary |-> 2],
    [block |-> 2, journal |-> 0, boundary |-> 4],
    [block |-> 2, journal |-> 1, boundary |-> 4]}

Init ==
    /\ selected = Published /\ previous = Published
    /\ phase = "initial" /\ retried = FALSE

\* @type: ($checkpoint, $checkpoint) => Bool;
AgreesOrAdvances(anchor, candidate) ==
    /\ candidate.journal = anchor.journal
    /\ \/ candidate = anchor
       \/ candidate.block > anchor.block /\ candidate.boundary > anchor.boundary

\* @type: ($checkpoint) => Bool;
Allowed(candidate) ==
    LET anchor == IF Mutation = "forget_retry_anchor" THEN Published ELSE previous
    IN /\ (~retried \/ candidate.block > previous.block)
       /\ IF Mutation = "trust_block"
          THEN candidate.block >= anchor.block
          ELSE /\ (candidate.journal = anchor.journal \/ Mutation = "ignore_journal")
               /\ \/ candidate.block = anchor.block /\ candidate.boundary = anchor.boundary
                  \/ candidate.block > anchor.block /\ candidate.boundary > anchor.boundary

\* @type: ($checkpoint) => Bool;
Select(candidate) ==
    /\ phase \in {"initial", "retry"}
    /\ Allowed(candidate)
    /\ selected' = candidate
    /\ phase' = "selected"
    /\ UNCHANGED <<previous, retried>>

LosePrefix ==
    /\ phase = "selected" /\ ~retried
    /\ previous' = selected
    /\ phase' = "retry"
    /\ retried' = TRUE
    /\ UNCHANGED selected

Open(log) ==
    /\ phase = "selected"
    /\ (log = selected.journal \/ Mutation = "open_wrong_journal")
    /\ phase' = IF log = Published.journal THEN "open" ELSE "wrong_log"
    /\ UNCHANGED <<selected, previous, retried>>

Next == (\E candidate \in Candidates: Select(candidate)) \/ LosePrefix \/ (\E log \in 0..1: Open(log))
Spec == Init /\ [][Next]_Vars

MatchesAdmittedBoundary == phase \in {"selected", "open", "wrong_log"} =>
    AgreesOrAdvances(previous, selected)
OpensAdmittedJournal == phase # "wrong_log"
Safe == MatchesAdmittedBoundary /\ OpensAdmittedJournal
NeverOpensAfterAdvancing == ~(phase = "open" /\ retried /\ selected.block > previous.block)
=============================================================================
