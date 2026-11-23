----------------------- MODULE RepairAnchorRetention -----------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES head, visible, durable, acknowledged, present, selected, crashed, advanced

Vars == <<head, visible, durable, acknowledged, present, selected, crashed, advanced>>
Blocks == 0..2
Kinds == {"catalog", "table-block", "offload"}
Files == Blocks \X Kinds
NoAnchor == 3
NoSelection == <<3, "none">>

AtOrThrough(cut) == {f \in Files : f[1] <= cut}
Required(anchor) ==
    IF anchor = NoAnchor THEN {}
    ELSE {f \in AtOrThrough(head) : anchor <= f[1]}

Init ==
    /\ head = 0
    /\ visible = NoAnchor /\ durable = NoAnchor /\ acknowledged = NoAnchor
    /\ present = AtOrThrough(0) /\ selected = NoSelection
    /\ crashed = FALSE /\ advanced = FALSE

Publish ==
    /\ head < 2
    /\ head' = head + 1
    /\ present' = present \cup (AtOrThrough(head') \ AtOrThrough(head))
    /\ UNCHANGED <<visible, durable, acknowledged, selected, crashed, advanced>>

StageAnchor ==
    /\ \E cut \in Blocks :
        /\ (cut = head \/ (Mutation = "stale_pin" /\ cut < head))
        /\ visible # cut /\ visible' = cut
        /\ advanced' = (advanced \/ visible # NoAnchor)
    /\ UNCHANGED <<head, durable, acknowledged, present, selected, crashed>>

ConfirmAnchor ==
    /\ visible # NoAnchor /\ acknowledged # visible
    /\ acknowledged' = visible /\ durable' = visible
    /\ UNCHANGED <<head, visible, present, selected, crashed, advanced>>

Select ==
    /\ selected = NoSelection
    /\ \E f \in present : f[1] < head /\ selected' = f
    /\ UNCHANGED <<head, visible, durable, acknowledged, present, crashed, advanced>>

Remove ==
    /\ selected # NoSelection
    /\ (selected \notin Required(visible) \/ Mutation = "ignore_anchor")
    /\ durable' = IF Mutation = "skip_sync" THEN durable ELSE visible
    /\ present' = present \ {selected}
    /\ selected' = NoSelection
    /\ UNCHANGED <<head, visible, acknowledged, crashed, advanced>>

Restart ==
    /\ ~crashed
    /\ crashed' = TRUE /\ selected' = NoSelection
    /\ durable' = IF Mutation = "forget_anchor" THEN NoAnchor ELSE durable
    /\ visible' = durable'
    /\ UNCHANGED <<head, acknowledged, present, advanced>>

Next == Publish \/ StageAnchor \/ ConfirmAnchor \/ Select \/ Remove \/ Restart

HeadReadable == {f \in Files : f[1] = head} \subseteq present
SuffixReadable == (Required(visible) \cup Required(durable)) \subseteq present
DurableAnchor ==
    acknowledged = NoAnchor \/ (durable # NoAnchor /\ acknowledged <= durable)
Safe == HeadReadable /\ SuffixReadable /\ DurableAnchor
NeverReclaimsAfterAdvance ==
    ~(durable = 2 /\ advanced /\ crashed /\ <<0, "offload">> \notin present /\ SuffixReadable)

Spec == Init /\ [][Next]_Vars

=============================================================================
