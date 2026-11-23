-------------------------- MODULE CatalogRetirement --------------------------
EXTENDS Naturals

CONSTANTS Readers, InspectVersions, RetainCatalogFence
VARIABLES cut, readers, recorded, versions, discovered, finished
Vars == <<cut, readers, recorded, versions, discovered, finished>>
Old == 0
Replacement == 1

Init ==
  /\ cut = 0
  /\ readers = {}
  /\ recorded = {}
  /\ versions = {Old, Replacement}
  /\ discovered = FALSE
  /\ finished = FALSE

PublishDrop ==
  /\ cut = 0
  /\ cut' = 1
  /\ UNCHANGED <<readers, recorded, versions, discovered, finished>>

Admit(reader) ==
  /\ reader \notin readers
  /\ cut = 0 \/ (~RetainCatalogFence /\ finished /\ Old \notin recorded)
  /\ readers' = readers \cup {reader}
  /\ recorded' = recorded \cup {Old}
  /\ UNCHANGED <<cut, versions, discovered, finished>>

Release(reader) ==
  /\ reader \in readers
  /\ readers' = readers \ {reader}
  /\ UNCHANGED <<cut, recorded, versions, discovered, finished>>

Discover ==
  /\ cut = 1 /\ ~discovered
  /\ recorded' = recorded \cup (IF InspectVersions THEN versions \cap {Old} ELSE {})
  /\ discovered' = TRUE
  /\ UNCHANGED <<cut, readers, versions, finished>>

Erase ==
  /\ cut = 1 /\ discovered
  /\ Old \in recorded
  /\ readers = {}
  /\ versions' = versions \ {Old}
  /\ recorded' = recorded \ {Old}
  /\ UNCHANGED <<cut, readers, discovered, finished>>

Acknowledge ==
  /\ cut = 1 /\ discovered
  /\ Old \notin recorded
  /\ readers = {}
  /\ ~finished
  /\ finished' = TRUE
  /\ UNCHANGED <<cut, readers, recorded, versions, discovered>>

Next ==
  \/ PublishDrop \/ Discover \/ Erase \/ Acknowledge
  \/ \E reader \in Readers : Admit(reader) \/ Release(reader)
  \/ finished /\ UNCHANGED Vars

Spec == Init /\ [][Next]_Vars
PreservesReplacement == Replacement \in versions
NoRetainedVersion == finished => Old \notin versions
NoReopenedDefinition == finished => readers = {}

FairSpec ==
  /\ Spec
  /\ WF_Vars(PublishDrop)
  /\ WF_Vars(Discover)
  /\ \A reader \in Readers : WF_Vars(Release(reader))
  /\ WF_Vars(Erase)
  /\ WF_Vars(Acknowledge)

Completes == <>finished

=============================================================================
