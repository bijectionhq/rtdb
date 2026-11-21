--------------------------- MODULE CompactionGroup ---------------------------
EXTENDS FiniteSets, Naturals

CONSTANT Mutation

Buckets == 0..3
VARIABLES written, announced, live, parent, cursor
vars == <<written, announced, live, parent, cursor>>

Init ==
    /\ written = {}
    /\ announced = {}
    /\ live = {}
    /\ parent = TRUE
    /\ cursor = "closed"

Write(b) ==
    /\ b \notin written
    /\ written' = written \cup {b}
    /\ UNCHANGED <<announced, live, parent, cursor>>

Announce(b) ==
    /\ b \notin announced
    /\ (b \in written \/ Mutation = "pending_write")
    /\ announced' = announced \cup {b}
    /\ IF announced' = Buckets
          THEN /\ live' = Buckets
               /\ parent' = FALSE
          ELSE /\ live' = IF Mutation = "early_child" THEN announced' ELSE live
               /\ parent' = IF Mutation = "early_parent" THEN FALSE ELSE parent
    /\ UNCHANGED <<written, cursor>>

Capture ==
    /\ cursor = "closed"
    /\ cursor' = IF parent THEN "parent" ELSE "children"
    /\ UNCHANGED <<written, announced, live, parent>>

Release ==
    /\ cursor # "closed"
    /\ cursor' = "closed"
    /\ UNCHANGED <<written, announced, live, parent>>

Next ==
    \/ \E b \in Buckets: Write(b) \/ Announce(b)
    \/ Capture
    \/ Release

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ written \subseteq Buckets
    /\ announced \subseteq Buckets
    /\ live \subseteq Buckets
    /\ parent \in BOOLEAN
    /\ cursor \in {"closed", "parent", "children"}

CopiesFor(b) == (IF parent THEN 1 ELSE 0) + (IF b \in live THEN 1 ELSE 0)
ExactCoverage == \A b \in Buckets: CopiesFor(b) = 1
PublishedOutputReadable == live \subseteq written
OnlyParentsCaptured == cursor # "children"
=============================================================================
