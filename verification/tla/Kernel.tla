----------------------------- MODULE Kernel -----------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANTS Requests, MaxTerm, Mutation, CopyOnRead

VARIABLES log, applied, acked, term, pending, up, liveRoot, objects,
          checkpoints, readBasis, readRoot, readHistory

NoRequest == 0
Vars == <<log, applied, acked, term, pending, up, liveRoot, objects,
          checkpoints, readBasis, readRoot, readHistory>>
Prefix(n) == SubSeq(log, 1, n)
Ids(prefix) == {prefix[i].id : i \in 1..Len(prefix)}
Batch(id) == <<[key |-> "debit", value |-> id],
                [key |-> "credit", value |-> id]>>

Init ==
    /\ log = <<>>
    /\ applied = 0
    /\ acked = {}
    /\ term = 1
    /\ pending = [id |-> NoRequest, epoch |-> 0]
    /\ up = TRUE
    /\ liveRoot = 0
    /\ objects = {0}
    /\ checkpoints = [root \in 0..1 |-> 0]
    /\ readBasis = -1
    /\ readRoot = -1
    /\ readHistory = <<>>

Resolve(id) ==
    /\ up
    /\ pending.id = NoRequest
    /\ id \notin Ids(log) \/ Mutation = "duplicate_retry"
    /\ pending' = [id |-> id, epoch |-> term]
    /\ UNCHANGED <<log, applied, acked, term, up, liveRoot, objects,
                    checkpoints, readBasis, readRoot, readHistory>>

DurableAppend ==
    /\ up
    /\ pending.id \in Requests
    /\ Len(log) < Cardinality(Requests)
    /\ pending.id \notin Ids(log) \/ Mutation = "duplicate_retry"
    /\ pending.epoch = term \/ Mutation = "stale_term"
    /\ log' = Append(log, [id |-> pending.id,
                            epoch |-> pending.epoch, authority |-> term,
                            facts |-> IF Mutation = "torn_batch"
                                      THEN SubSeq(Batch(pending.id), 1, 1)
                                      ELSE Batch(pending.id)])
    /\ pending' = [id |-> NoRequest, epoch |-> 0]
    /\ UNCHANGED <<applied, acked, term, up, liveRoot, objects, checkpoints,
                    readBasis, readRoot, readHistory>>

Apply ==
    /\ up
    /\ applied < Len(log)
    /\ applied' = applied + 1
    /\ readHistory' = IF Mutation = "move_pin" /\ readBasis # -1
                       THEN Prefix(applied + 1) ELSE readHistory
    /\ UNCHANGED <<log, acked, term, pending, up, liveRoot, objects,
                    checkpoints, readBasis, readRoot>>

Ack(id) ==
    /\ up
    /\ id \notin acked
    /\ (id \in Ids(Prefix(applied))
        \/ (Mutation = "early_ack" /\ pending.id = id))
    /\ acked' = acked \cup {id}
    /\ UNCHANGED <<log, applied, term, pending, up, liveRoot, objects,
                    checkpoints, readBasis, readRoot, readHistory>>

Handover ==
    /\ up
    /\ term < MaxTerm
    /\ term' = term + 1
    /\ UNCHANGED <<log, applied, acked, pending, up, liveRoot, objects,
                    checkpoints, readBasis, readRoot, readHistory>>

DiscardStaged ==
    /\ pending.id \in Requests
    /\ pending.epoch < term
    /\ pending' = [id |-> NoRequest, epoch |-> 0]
    /\ UNCHANGED <<log, applied, acked, term, up, liveRoot, objects,
                    checkpoints, readBasis, readRoot, readHistory>>

Crash ==
    /\ up
    /\ up' = FALSE
    /\ applied' = checkpoints[liveRoot]
    /\ UNCHANGED <<log, acked, term, pending, liveRoot, objects,
                    checkpoints, readBasis, readRoot, readHistory>>

Recover ==
    /\ ~up
    /\ liveRoot \in objects
    /\ up' = TRUE
    /\ UNCHANGED <<log, applied, acked, term, pending, liveRoot, objects,
                    checkpoints, readBasis, readRoot, readHistory>>

WriteReplacement ==
    /\ up
    /\ liveRoot = 0
    /\ 1 \notin objects
    /\ objects' = objects \cup {1}
    /\ checkpoints' = [checkpoints EXCEPT ![1] = applied]
    /\ UNCHANGED <<log, applied, acked, term, pending, up, liveRoot,
                    readBasis, readRoot, readHistory>>

PublishReplacement ==
    /\ up
    /\ liveRoot = 0
    /\ 1 \in objects \/ Mutation = "undurable_publish"
    /\ liveRoot' = 1
    /\ UNCHANGED <<log, applied, acked, term, pending, up, objects, checkpoints,
                    readBasis, readRoot, readHistory>>

Pin ==
    /\ up
    /\ readBasis = -1
    /\ liveRoot \in objects
    /\ readBasis' = applied
    /\ readHistory' = Prefix(applied)
    /\ readRoot' = IF CopyOnRead THEN -1 ELSE liveRoot
    /\ UNCHANGED <<log, applied, acked, term, pending, up, liveRoot, objects,
                    checkpoints>>

Release ==
    /\ readBasis # -1
    /\ readBasis' = -1
    /\ readRoot' = -1
    /\ readHistory' = <<>>
    /\ UNCHANGED <<log, applied, acked, term, pending, up, liveRoot, objects,
                    checkpoints>>

Collect ==
    /\ up
    /\ liveRoot = 1
    /\ 0 \in objects
    /\ readRoot # 0 \/ Mutation = "collect_pinned"
    /\ objects' = objects \ {0}
    /\ UNCHANGED <<log, applied, acked, term, pending, up, liveRoot, checkpoints,
                    readBasis, readRoot, readHistory>>

Next ==
    \/ \E id \in Requests : Resolve(id) \/ Ack(id)
    \/ DurableAppend \/ Apply \/ Handover \/ DiscardStaged \/ Crash \/ Recover
    \/ WriteReplacement \/ PublishReplacement \/ Pin \/ Release \/ Collect

Spec == Init /\ [][Next]_Vars
FairSpec ==
    /\ Spec
    /\ WF_Vars(Apply)
    /\ \A id \in Requests : WF_Vars(Ack(id))

EventuallyAcknowledged ==
    (<>[]up) => \A id \in Requests : ((id \in Ids(log)) ~> (id \in acked))

Bounds ==
    /\ applied \in 0..Len(log)
    /\ readBasis \in {-1} \cup (0..Len(log))
    /\ liveRoot \in 0..1
    /\ objects \subseteq (0..1)

AckHasDurableBatch == acked \subseteq Ids(log)
UniqueRequest == Cardinality(Ids(log)) = Len(log)
WholeBatches == \A i \in 1..Len(log) : log[i].facts = Batch(log[i].id)
Fenced == \A i \in 1..Len(log) : log[i].epoch = log[i].authority
RootDurable == liveRoot \in objects
PinReachable == readRoot = -1 \/ readRoot \in objects
PinStable == readBasis = -1 \/ readHistory = Prefix(readBasis)

=============================================================================
