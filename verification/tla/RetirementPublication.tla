----------------------- MODULE RetirementPublication -----------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES queued, installed, certified, floor, catalog, physical, reader, authorization

Vars == <<queued, installed, certified, floor, catalog, physical, reader, authorization>>
NoReader == [cut |-> -2, object |-> 0]
Opaque == -1

Init ==
    /\ queued = FALSE
    /\ installed = 0
    /\ certified = FALSE
    /\ floor = 0
    /\ catalog = 1
    /\ physical = 1
    /\ reader = NoReader
    /\ authorization = FALSE

Queue ==
    /\ ~queued
    /\ queued' = TRUE
    /\ UNCHANGED <<installed, certified, floor, catalog, physical, reader, authorization>>

Install ==
    /\ queued /\ installed = 0
    /\ installed' = 1
    /\ catalog' = 0
    /\ UNCHANGED <<queued, certified, floor, physical, reader, authorization>>

Publish ==
    /\ queued /\ ~certified
    /\ installed >= 1 \/ Mutation = "certify_before_install"
    /\ certified' = TRUE
    /\ floor' = IF Mutation = "no_admission_floor" THEN floor ELSE 1
    /\ UNCHANGED <<queued, installed, catalog, physical, reader, authorization>>

Acquire ==
    /\ reader = NoReader
    /\ \E cut \in Opaque..installed :
       /\ cut >= floor \/ (cut = Opaque /\ floor = 0)
       /\ reader' = [cut |-> cut, object |-> IF cut <= 0 THEN 1 ELSE IF cut = 2 THEN 2 ELSE 0]
    /\ UNCHANGED <<queued, installed, certified, floor, catalog, physical, authorization>>

Release ==
    /\ reader # NoReader
    /\ reader' = NoReader
    /\ UNCHANGED <<queued, installed, certified, floor, catalog, physical, authorization>>

Authorize ==
    /\ certified /\ ~authorization
    /\ reader = NoReader \/ reader.cut >= 1
    /\ authorization' = TRUE
    /\ UNCHANGED <<queued, installed, certified, floor, catalog, physical, reader>>

Erase ==
    /\ authorization /\ physical # 0
    /\ physical = 1 \/ Mutation = "erase_by_key"
    /\ physical' = 0
    /\ UNCHANGED <<queued, installed, certified, floor, catalog, reader, authorization>>

Reuse ==
    /\ installed = 1 /\ physical = 0
    /\ installed' = 2
    /\ physical' = 2
    /\ catalog' = 2
    /\ UNCHANGED <<queued, certified, floor, reader, authorization>>

Next == Queue \/ Install \/ Publish \/ Acquire \/ Release \/ Authorize \/ Erase \/ Reuse
Spec == Init /\ [][Next]_Vars

CertificateFollowsInstall == certified => installed >= 1
CurrentCatalogReadable == catalog = 0 \/ catalog = physical
AdmittedReaderReadable == reader.object = 0 \/ reader.object = physical
NeverCollectsAndReuses == installed # 2

=============================================================================
