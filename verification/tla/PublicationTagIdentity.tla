------------------------ MODULE PublicationTagIdentity ------------------------
CONSTANT Policy

VARIABLES oldKey, freshKey, request

vars == <<oldKey, freshKey, request>>

Init ==
    /\ oldKey = "original"
    /\ freshKey = FALSE
    /\ request = "open"

Submit ==
    /\ request = "open" /\ oldKey = "original"
    /\ request' = "pending"
    /\ UNCHANGED <<oldKey, freshKey>>

ReuseKey ==
    /\ Policy # "terminal_key"
    /\ oldKey = "original"
    /\ oldKey' \in {"new_equal_tag", "new_distinct_tag"}
    /\ UNCHANGED <<freshKey, request>>

PublishFreshKey ==
    /\ ~freshKey /\ freshKey' = TRUE
    /\ UNCHANGED <<oldKey, request>>

Apply ==
    /\ request = "pending" /\ request' = "applied"
    /\ LET matches == oldKey = "original" \/ (Policy # "atomic_identity" /\ oldKey = "new_equal_tag")
       IN oldKey' = IF matches THEN "retired" ELSE oldKey
    /\ UNCHANGED freshKey

Next == Submit \/ ReuseKey \/ PublishFreshKey \/ Apply
Spec == Init /\ [][Next]_vars

SameKeyReplacementSurvives ==
    oldKey \in {"new_equal_tag", "new_distinct_tag"} => oldKey' = oldKey
Safe == [][SameKeyReplacementSurvives]_vars
NeverRetiresAndPublishes == ~(oldKey = "retired" /\ freshKey)
NeverFinishesAfterSameTagReplacement == ~(oldKey = "new_equal_tag" /\ request = "applied")
=============================================================================
