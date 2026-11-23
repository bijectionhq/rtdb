---------------------------- MODULE HostedTrieReclamation ----------------------------
EXTENDS FiniteSets

CONSTANT Mutation

Residences == {"original", "replacement"}
Kinds == {"legacy", "receipt"}
Markers == [kind : Kinds, residence : Residences]

VARIABLES receipts, floors, holds, markers, observed, erased
vars == <<receipts, floors, holds, markers, observed, erased>>

Init ==
    /\ receipts = {}
    /\ floors = {}
    /\ holds = {}
    /\ markers = {}
    /\ observed = {}
    /\ erased = {}

Admit(residence) ==
    /\ residence \notin floors
    /\ holds' = holds \cup {residence}
    /\ UNCHANGED <<receipts, floors, markers, observed, erased>>

Drain(residence) ==
    /\ residence \in holds
    /\ holds' = holds \ {residence}
    /\ UNCHANGED <<receipts, floors, markers, observed, erased>>

Install(residence) ==
    /\ receipts' = receipts \cup {residence}
    /\ UNCHANGED <<floors, holds, markers, observed, erased>>

Advance(residence) ==
    /\ residence \in receipts
    /\ residence \notin holds
    /\ floors' = floors \cup {residence}
    /\ UNCHANGED <<receipts, holds, markers, observed, erased>>

Queue(marker) ==
    /\ marker.kind = "legacy" \/ marker.residence \in floors
    /\ markers' = markers \cup {marker}
    /\ UNCHANGED <<receipts, floors, holds, observed, erased>>

Observe ==
    /\ observed' = floors
    /\ UNCHANGED <<receipts, floors, holds, markers, erased>>

Reap(marker, residence) ==
    /\ marker \in markers
    /\ CASE Mutation = "allow_legacy" -> marker.residence = residence
         [] Mutation = "ignore_scope" -> marker.kind = "receipt" /\ marker.residence \in observed
         [] Mutation = "ignore_observation" -> marker.kind = "receipt" /\ marker.residence = residence
         [] OTHER -> marker.kind = "receipt" /\ marker.residence = residence /\ residence \in observed
    /\ erased' = erased \cup {residence}
    /\ UNCHANGED <<receipts, floors, holds, markers, observed>>

Next ==
    \/ \E residence \in Residences : Admit(residence) \/ Drain(residence) \/ Install(residence) \/ Advance(residence)
    \/ \E marker \in Markers : Queue(marker)
    \/ Observe
    \/ \E marker \in Markers, residence \in Residences : Reap(marker, residence)

Spec == Init /\ [][Next]_vars

ErasureHasReceipt == erased \subseteq receipts
ErasureUsesObservedFloor == erased \subseteq observed
HeldFilesSurvive == erased \cap holds = {}
NeverErasesWithOtherHold == ~(erased # {} /\ holds # {})

=============================================================================
