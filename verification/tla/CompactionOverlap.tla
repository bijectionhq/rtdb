-------------------------- MODULE CompactionOverlap --------------------------
EXTENDS FiniteSets, Naturals

CONSTANT Mode
Buckets == 0..3
Generations == 1..2
Output == Buckets \X Generations

VARIABLES admitted, parent, active, generation
vars == <<admitted, parent, active, generation>>

Init ==
    /\ admitted = {}
    /\ parent = TRUE
    /\ active = {}
    /\ generation = 0

Tip(bucket, items) ==
    IF <<bucket, 2>> \in items THEN 2
    ELSE IF <<bucket, 1>> \in items THEN 1 ELSE 0

Notice(output) ==
    /\ output[2] > Tip(output[1], admitted)
    /\ admitted' = admitted \cup {output}
    /\ UNCHANGED <<parent, active, generation>>

Complete(target, items) ==
    \A bucket \in Buckets: <<bucket, target>> \in items

Promote(g) ==
    /\ g > generation
    /\ IF Mode = "watermark"
          THEN \A bucket \in Buckets: Tip(bucket, admitted) >= g
          ELSE Complete(g, admitted)
    /\ generation' = g
    /\ parent' = FALSE
    /\ active' = {output \in admitted: output[2] = g}
    /\ UNCHANGED admitted

Next ==
    \/ \E output \in Output: Notice(output)
    \/ \E g \in Generations: Promote(g)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ admitted \subseteq Output
    /\ parent \in BOOLEAN
    /\ active \subseteq Output
    /\ generation \in {0, 1, 2}

CompleteCoverage ==
    IF parent THEN active = {}
    ELSE active = Buckets \X {generation}

NoReplacementActivated == generation = 0
=============================================================================
