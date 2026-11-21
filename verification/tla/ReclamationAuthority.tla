------------------------ MODULE ReclamationAuthority ------------------------
EXTENDS Naturals

CONSTANT Mode
Readers == 1..2
Generations == 0..1
Idle == 2

VARIABLES current, held, opened, inFlight, present, pending
vars == <<current, held, opened, inFlight, present, pending>>

Init ==
    /\ current = 0
    /\ held = [reader \in Readers |-> Idle]
    /\ opened = [reader \in Readers |-> FALSE]
    /\ inFlight = [reader \in Readers |-> FALSE]
    /\ present = {0}
    /\ pending = FALSE

Admit(reader, selected) ==
    /\ held[reader] = Idle
    /\ (selected = current \/ (Mode = "allow_stale" /\ selected \in present))
    /\ held' = [held EXCEPT ![reader] = selected]
    /\ opened' = [opened EXCEPT ![reader] = TRUE]
    /\ UNCHANGED <<current, inFlight, present, pending>>

BeginRead(reader) ==
    /\ opened[reader]
    /\ ~inFlight[reader]
    /\ inFlight' = [inFlight EXCEPT ![reader] = TRUE]
    /\ UNCHANGED <<current, held, opened, present, pending>>

Close(reader) ==
    /\ opened[reader]
    /\ opened' = [opened EXCEPT ![reader] = FALSE]
    /\ held' = [held EXCEPT ![reader] = IF inFlight[reader] THEN @ ELSE Idle]
    /\ UNCHANGED <<current, inFlight, present, pending>>

FinishRead(reader) ==
    /\ inFlight[reader]
    /\ inFlight' = [inFlight EXCEPT ![reader] = FALSE]
    /\ held' = [held EXCEPT ![reader] = IF opened[reader] THEN @ ELSE Idle]
    /\ UNCHANGED <<current, opened, present, pending>>

PublishNext ==
    /\ current = 0
    /\ current' = 1
    /\ present' = present \cup {1}
    /\ UNCHANGED <<held, opened, inFlight, pending>>

AuthorizeOldDeletion ==
    /\ current = 1
    /\ 0 \in present
    /\ ~pending
    /\ IF Mode = "one_reader"
          THEN held[1] # 0
          ELSE \A reader \in Readers: held[reader] # 0
    /\ pending' = TRUE
    /\ UNCHANGED <<current, held, opened, inFlight, present>>

Delete ==
    /\ pending
    /\ pending' = FALSE
    /\ present' = IF Mode = "reuse_path" THEN {} ELSE present \ {0}
    /\ UNCHANGED <<current, held, opened, inFlight>>

Next ==
    \/ \E reader \in Readers:
        \/ \E selected \in Generations: Admit(reader, selected)
        \/ BeginRead(reader)
        \/ Close(reader)
        \/ FinishRead(reader)
    \/ PublishNext
    \/ AuthorizeOldDeletion
    \/ Delete

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ current \in Generations
    /\ held \in [Readers -> Generations \cup {Idle}]
    /\ opened \in [Readers -> BOOLEAN]
    /\ inFlight \in [Readers -> BOOLEAN]
    /\ present \subseteq Generations
    /\ pending \in BOOLEAN

ReaderDependenciesAvailable ==
    \A reader \in Readers: (opened[reader] \/ inFlight[reader]) => held[reader] \in present

CurrentGenerationAvailable == current \in present
OldGenerationNeverDeleted == 0 \in present
=============================================================================
