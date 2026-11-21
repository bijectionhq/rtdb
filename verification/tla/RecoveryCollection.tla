------------------------- MODULE RecoveryCollection -------------------------
EXTENDS Naturals

CONSTANTS
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Set(Str);
    durable,
    \* @type: Set(Int);
    advertised,
    \* @type: Int;
    head,
    \* @type: Int;
    logFloor,
    \* @type: Int;
    selected,
    \* @type: Set(Str);
    deletions,
    \* @type: Bool;
    openedAfterCollection

Vars == <<durable, advertised, head, logFloor, selected, deletions, openedAfterCollection>>
Generations == {0, 1}
Idle == 2
Shared == "shared-data"
Contents == [g \in Generations |->
    IF g = 0 THEN {"old-root", "old-trie", Shared}
    ELSE {"new-root", "new-trie", Shared}]
Objects == UNION {Contents[g] : g \in Generations}

Init ==
    /\ durable = Contents[0]
    /\ advertised = {0}
    /\ head = 0
    /\ logFloor = 0
    /\ selected = Idle
    /\ deletions = {}
    /\ openedAfterCollection = FALSE

TypeOK ==
    /\ durable \subseteq Objects
    /\ advertised \subseteq Generations
    /\ head \in Generations
    /\ logFloor \in Generations
    /\ selected \in Generations \cup {Idle}
    /\ deletions \subseteq durable
    /\ openedAfterCollection \in BOOLEAN

(* The published head may advance only after all replacement dependencies are durable. *)
WriteReplacementObject(object) ==
    /\ head = 0
    /\ object \in Contents[1] \ durable
    /\ durable' = durable \cup {object}
    /\ UNCHANGED <<advertised, head, logFloor, selected, deletions, openedAfterCollection>>

PublishReplacement ==
    /\ head = 0
    /\ (IF Mutation = "publish_incomplete"
          THEN "new-root" \in durable
          ELSE Contents[1] \subseteq durable)
    /\ head' = 1
    /\ advertised' = advertised \cup {1}
    /\ UNCHANGED <<durable, logFloor, selected, deletions, openedAfterCollection>>

RetireOldRoot ==
    /\ head = 1
    /\ 0 \in advertised
    /\ advertised' = advertised \ {0}
    /\ UNCHANGED <<durable, head, logFloor, selected, deletions, openedAfterCollection>>

(* Selection owns a recovery reference through successful open or confirmed abandonment. *)
SelectRoot(generation) ==
    /\ selected = Idle
    /\ generation \in advertised
    /\ (generation >= logFloor \/ Mutation = "select_below_floor")
    /\ selected' = generation
    /\ openedAfterCollection' = FALSE
    /\ UNCHANGED <<durable, advertised, head, logFloor, deletions>>

OpenRoot ==
    /\ selected \in Generations
    /\ Contents[selected] \subseteq durable
    /\ selected >= logFloor
    /\ openedAfterCollection' = (selected = 1 /\ "old-trie" \notin durable /\ logFloor = 1)
    /\ selected' = Idle
    /\ UNCHANGED <<durable, advertised, head, logFloor, deletions>>

AbandonRecovery ==
    /\ selected \in Generations
    /\ selected' = Idle
    /\ UNCHANGED <<durable, advertised, head, logFloor, deletions, openedAfterCollection>>

TrimLog ==
    /\ logFloor < head
    /\ (selected \in {Idle, head} \/ Mutation = "trim_for_head_only")
    /\ logFloor' = head
    /\ UNCHANGED <<durable, advertised, head, selected, deletions, openedAfterCollection>>

RetainedGenerations ==
    advertised \cup (IF selected = Idle \/ Mutation = "ignore_selected_root" THEN {} ELSE {selected})

Eligible(object) ==
    \/ object \notin UNION {Contents[g] : g \in RetainedGenerations}
    \/ Mutation = "forget_shared_dependency" /\ object = Shared /\ 0 \notin advertised

(* Selection and authorization use the same view. A later delete uses the immutable identity. *)
AuthorizeDeletion(object) ==
    /\ head = 1
    /\ object \in durable \ deletions
    /\ Eligible(object)
    /\ deletions' = deletions \cup {object}
    /\ UNCHANGED <<durable, advertised, head, logFloor, selected, openedAfterCollection>>

FinishDeletion(object) ==
    /\ object \in deletions
    /\ durable' = durable \ {object}
    /\ deletions' = deletions \ {object}
    /\ UNCHANGED <<advertised, head, logFloor, selected, openedAfterCollection>>

Next ==
    \/ \E object \in Objects : WriteReplacementObject(object)
    \/ \E object \in Objects : AuthorizeDeletion(object) \/ FinishDeletion(object)
    \/ \E generation \in Generations : SelectRoot(generation)
    \/ PublishReplacement \/ RetireOldRoot \/ OpenRoot \/ AbandonRecovery \/ TrimLog

Spec == Init /\ [][Next]_Vars

AdvertisedRootsReadable == \A g \in advertised : Contents[g] \subseteq durable
RecoveryReadSetAvailable == selected \in Generations => Contents[selected] \subseteq durable
RecoverySuffixAvailable == selected \in Generations => selected >= logFloor
UsableHead == head \in advertised /\ head >= logFloor
RecoverySafe == AdvertisedRootsReadable /\ RecoveryReadSetAvailable /\ RecoverySuffixAvailable /\ UsableHead
NoReplacementOpenAfterCollection == ~openedAfterCollection

=============================================================================
