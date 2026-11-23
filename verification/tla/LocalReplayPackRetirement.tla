------------------------ MODULE LocalReplayPackRetirement ------------------------
EXTENDS TLC

CONSTANT Mutation

VARIABLES original, source, reader, lineage, descendant, journal, grant, readerOpenedAfterErasure

vars == <<original, source, reader, lineage, descendant, journal, grant, readerOpenedAfterErasure>>

Init ==
    /\ original = TRUE /\ source = "original" /\ reader = "closed"
    /\ lineage = "open" /\ descendant = FALSE /\ journal = "required" /\ grant = FALSE
    /\ readerOpenedAfterErasure = FALSE

Install ==
    /\ source = "original"
    /\ source' = "exact-replay"
    /\ UNCHANGED <<original, reader, lineage, descendant, journal, grant, readerOpenedAfterErasure>>

Restore ==
    /\ source = "exact-replay"
    /\ source' = IF Mutation = "primary_restore" THEN "primary-only" ELSE "exact-replay"
    /\ UNCHANGED <<original, reader, lineage, descendant, journal, grant, readerOpenedAfterErasure>>

AdmitReader ==
    /\ reader = "closed"
    /\ reader' = source
    /\ readerOpenedAfterErasure' = (readerOpenedAfterErasure \/ ~original)
    /\ UNCHANGED <<original, source, lineage, descendant, journal, grant>>

DrainReader ==
    /\ reader # "closed"
    /\ reader' = "closed"
    /\ UNCHANGED <<original, source, lineage, descendant, journal, grant, readerOpenedAfterErasure>>

PublishDescendant ==
    /\ lineage = "open" /\ source = "original" /\ ~descendant
    /\ descendant' = TRUE
    /\ UNCHANGED <<original, source, reader, lineage, journal, grant, readerOpenedAfterErasure>>

DrainDescendant ==
    /\ lineage = "open" /\ descendant
    /\ descendant' = FALSE
    /\ UNCHANGED <<original, source, reader, lineage, journal, grant, readerOpenedAfterErasure>>

BeginRawCopy ==
    /\ lineage = "open" /\ original
    /\ lineage' = "copy"
    /\ UNCHANGED <<original, source, reader, descendant, journal, grant, readerOpenedAfterErasure>>

DrainRawCopy ==
    /\ lineage = "copy"
    /\ lineage' = "open"
    /\ UNCHANGED <<original, source, reader, descendant, journal, grant, readerOpenedAfterErasure>>

CertifyPrefix ==
    /\ journal = "required"
    /\ journal' = "covered"
    /\ UNCHANGED <<original, source, reader, lineage, descendant, grant, readerOpenedAfterErasure>>

Authorize ==
    /\ original /\ source = "exact-replay" /\ ~grant
    /\ lineage = "open" \/ (lineage = "copy" /\ Mutation = "ignore_copy")
    /\ reader = "closed" \/ Mutation = "ignore_reader"
    /\ ~descendant \/ Mutation = "ignore_descendant"
    /\ journal = "covered" \/ Mutation = "ignore_journal"
    /\ lineage' = IF lineage = "copy" THEN "copy" ELSE "reserved"
    /\ grant' = TRUE
    /\ UNCHANGED <<original, source, reader, descendant, journal, readerOpenedAfterErasure>>

Erase ==
    /\ original /\ grant
    /\ original' = FALSE
    /\ UNCHANGED <<source, reader, lineage, descendant, journal, grant, readerOpenedAfterErasure>>

Settle ==
    /\ grant
    /\ grant' = FALSE
    /\ lineage' = IF lineage = "reserved" THEN "open" ELSE lineage
    /\ UNCHANGED <<original, source, reader, descendant, journal, readerOpenedAfterErasure>>

Next == Install \/ Restore \/ AdmitReader \/ DrainReader \/ PublishDescendant \/ DrainDescendant
    \/ BeginRawCopy \/ DrainRawCopy \/ CertifyPrefix \/ Authorize \/ Erase \/ Settle

Spec == Init /\ [][Next]_vars

ServingCovered == ~original => source = "exact-replay" /\ reader \in {"closed", "exact-replay"}
ReferencesCovered == ~original => ~descendant /\ lineage # "copy"
BoundaryCovered == ~original => journal = "covered"
NeverOpensAfterErasure == ~readerOpenedAfterErasure
===============================================================================
