---------------------------- MODULE DelegatedScan ----------------------------
EXTENDS Naturals

CONSTANT Mutation

VARIABLES head, probed, captured, phase, pinned, available, answer
vars == <<head, probed, captured, phase, pinned, available, answer>>
NoHead == 3
Basis == 1
Erase == 2

Init ==
    /\ head = 0
    /\ probed = NoHead
    /\ captured = NoHead
    /\ phase = "waiting"
    /\ pinned = FALSE
    /\ available = TRUE
    /\ answer = "none"

Advance ==
    /\ head < Erase
    /\ head' = head + 1
    /\ UNCHANGED <<probed, captured, phase, pinned, available, answer>>

Probe ==
    /\ phase = "waiting"
    /\ probed' = head
    /\ UNCHANGED <<head, captured, phase, pinned, available, answer>>

MayRead ==
    CASE Mutation = "admit_ahead" -> head >= Basis
      [] Mutation = "trust_probe" -> probed = Basis
      [] OTHER -> head = Basis

Capture ==
    /\ phase = "waiting"
    /\ MayRead
    /\ captured' = head
    /\ phase' = "reading"
    /\ pinned' = (head = Basis)
    /\ UNCHANGED <<head, probed, available, answer>>

Refuse ==
    /\ phase = "waiting"
    /\ ~MayRead
    /\ phase' = "fallback"
    /\ captured' = head
    /\ answer' = "row"
    /\ UNCHANGED <<head, probed, pinned, available>>

Cancel ==
    /\ phase = "reading"
    /\ phase' = "draining"
    /\ pinned' = (pinned /\ Mutation # "forget_drain")
    /\ UNCHANGED <<head, probed, captured, available, answer>>

Finish ==
    /\ phase \in {"reading", "draining"}
    /\ phase' = IF phase = "draining" THEN "closed" ELSE "replied"
    /\ answer' = CASE phase = "draining" -> "none"
                    [] captured = Basis -> "row"
                    [] OTHER -> "empty"
    /\ pinned' = FALSE
    /\ UNCHANGED <<head, probed, captured, available>>

Collect ==
    /\ head = Erase
    /\ available
    /\ ~pinned
    /\ available' = FALSE
    /\ UNCHANGED <<head, probed, captured, phase, pinned, answer>>

Next == Advance \/ Probe \/ Capture \/ Refuse \/ Cancel \/ Finish \/ Collect
Spec == Init /\ [][Next]_vars

ExactAnswer == phase \in {"replied", "fallback"} => answer = "row"
InFlightReadable == phase \in {"reading", "draining"} /\ captured = Basis => available
NoMatchingReply == phase # "replied"
NoFallbackAfterErase == captured # Erase \/ phase # "fallback"
NoCollectionAfterDrain == phase # "closed" \/ available
=============================================================================
