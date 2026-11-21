----------------------------- MODULE ReadBack -----------------------------
EXTENDS Naturals

CONSTANTS Limit, Mutation
VARIABLES staged, durable, delivered, applied, observed, acknowledged, online

Vars == <<staged, durable, delivered, applied, observed, acknowledged, online>>

Init ==
    /\ staged = 0
    /\ durable = 0
    /\ delivered = 0
    /\ applied = 0
    /\ observed = 0
    /\ acknowledged = 0
    /\ online = TRUE

Stage ==
    /\ online
    /\ staged < Limit
    /\ staged' = staged + 1
    /\ UNCHANGED <<durable, delivered, applied, observed, acknowledged, online>>

AppendDurable ==
    /\ online
    /\ durable < staged
    /\ durable' = durable + 1
    /\ UNCHANGED <<staged, delivered, applied, observed, acknowledged, online>>

Deliver ==
    /\ online
    /\ delivered < durable
    /\ delivered' = delivered + 1
    /\ UNCHANGED <<staged, durable, applied, observed, acknowledged, online>>

Apply ==
    /\ online
    /\ applied < (IF Mutation = "skip_readback" THEN durable ELSE delivered)
    /\ applied' = applied + 1
    /\ observed' = IF observed < applied' THEN applied' ELSE observed
    /\ UNCHANGED <<staged, durable, delivered, acknowledged, online>>

Ack ==
    /\ online
    /\ acknowledged < (IF Mutation = "ack_on_append" THEN durable ELSE applied)
    /\ acknowledged' = acknowledged + 1
    /\ UNCHANGED <<staged, durable, delivered, applied, observed, online>>

Crash ==
    /\ online
    /\ online' = FALSE
    /\ staged' = durable
    /\ delivered' = 0
    /\ applied' = 0
    /\ UNCHANGED <<durable, observed, acknowledged>>

Restart ==
    /\ ~online
    /\ online' = TRUE
    /\ UNCHANGED <<staged, durable, delivered, applied, observed, acknowledged>>

Next == Stage \/ AppendDurable \/ Deliver \/ Apply \/ Ack \/ Crash \/ Restart
Spec == Init /\ [][Next]_Vars

AppliedAfterReadBack == applied <= delivered
AckAfterApply == acknowledged <= observed
AckSurvives == acknowledged <= durable

=============================================================================
