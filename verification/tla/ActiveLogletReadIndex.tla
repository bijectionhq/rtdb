--------------------------- MODULE ActiveLogletReadIndex ---------------------------
EXTENDS Integers, FiniteSets

CONSTANT Mutation

Old == {"a", "b", "c"}
New == {"b", "c", "d"}
Servers == Old \cup New
Quorums(members) == {votes \in SUBSET members : Cardinality(votes) = 2}
Routes == {"old", "new"}

VARIABLES phase, prepared, active, held, published, acknowledged, offered, applied, read

Vars == <<phase, prepared, active, held, published, acknowledged, offered, applied, read>>

Init ==
    /\ phase = "old" /\ prepared = {} /\ active = Old
    /\ held = [server \in Servers |-> 0]
    /\ published = held
    /\ acknowledged = 0 /\ offered = 0 /\ applied = 0
    /\ read = [phase |-> "idle", route |-> "old", epoch |-> 0,
                minimum |-> 0, before_confirmation |-> 0, bound |-> 0, served |-> 0]

Write(votes) ==
    /\ phase \in {"old", "new"} /\ votes \subseteq active /\ votes # {}
    /\ offered < 2
    /\ \A server \in votes : held[server] = offered
    /\ offered' = offered + 1
    /\ held' = [server \in Servers |-> IF server \in votes THEN offered' ELSE held[server]]
    /\ acknowledged' = IF Cardinality(votes) >= 2 THEN offered' ELSE acknowledged
    /\ UNCHANGED <<phase, prepared, active, published, applied, read>>

Publish(server) ==
    /\ server \in active
    /\ published[server] < acknowledged /\ held[server] >= acknowledged
    /\ published' = [published EXCEPT ![server] = acknowledged]
    /\ UNCHANGED <<phase, prepared, active, held, acknowledged, offered, applied, read>>

Prepare(votes) ==
    /\ phase = "old" /\ votes \in Quorums(Old)
    /\ phase' = "preparing" /\ prepared' = votes /\ active' = {}
    /\ held' = [server \in Servers |-> IF server \in votes THEN acknowledged ELSE held[server]]
    /\ UNCHANGED <<published, acknowledged, offered, applied, read>>

Activate(votes) ==
    /\ phase = "preparing" /\ votes \in Quorums(New)
    /\ phase' = "new" /\ active' = votes /\ offered' = acknowledged
    /\ held' = [server \in Servers |-> IF server \in votes THEN acknowledged ELSE held[server]]
    /\ published' = [server \in Servers |-> IF server \in votes THEN acknowledged ELSE published[server]]
    /\ UNCHANGED <<prepared, acknowledged, applied, read>>

Begin(route, epoch) ==
    /\ read.phase = "idle"
    /\ read' = [phase |-> "waiting", route |-> route, epoch |-> epoch,
                 minimum |-> acknowledged, before_confirmation |-> applied, bound |-> 0, served |-> 0]
    /\ UNCHANGED <<phase, prepared, active, held, published, acknowledged, offered, applied>>

VotingJournals ==
    CASE read.route = "old" /\ read.epoch = 1 /\ phase = "old" -> active
      [] read.route = "new" /\ read.epoch = 2 /\ phase = "new" -> active
      [] read.route = "old" /\ read.epoch = 2 /\ Mutation = "synced_is_active" -> prepared
      [] OTHER -> {}

Sample(votes) ==
    /\ read.phase = "waiting"
    /\ votes \in Quorums(IF read.route = "old" THEN Old ELSE New)
    /\ votes \subseteq VotingJournals
    /\ LET position == IF Mutation = "published_only" THEN published ELSE held
           bound == CHOOSE x \in {position[server] : server \in votes} :
                            \A server \in votes : position[server] <= x
       IN read' = [read EXCEPT !.phase = "confirmed", !.bound = bound]
    /\ UNCHANGED <<phase, prepared, active, held, published, acknowledged, offered, applied>>

Apply(server) ==
    /\ published[server] > applied /\ applied' = published[server]
    /\ UNCHANGED <<phase, prepared, active, held, published, acknowledged, offered, read>>

Serve ==
    /\ read.phase = "confirmed" /\ (applied >= read.bound \/ Mutation = "skip_apply_wait")
    /\ LET basis == IF Mutation = "basis_before_fence" THEN read.before_confirmation ELSE applied
       IN read' = [read EXCEPT !.phase = "served", !.served = basis]
    /\ UNCHANGED <<phase, prepared, active, held, published, acknowledged, offered, applied>>

Next ==
    \/ \E votes \in SUBSET Servers : Write(votes) \/ Prepare(votes) \/ Activate(votes) \/ Sample(votes)
    \/ \E server \in Servers : Publish(server) \/ Apply(server)
    \/ \E route \in Routes, epoch \in 1..2 : Begin(route, epoch)
    \/ Serve

Spec == Init /\ [][Next]_Vars

BoundCoversAcknowledgements == read.phase \in {"confirmed", "served"} => read.bound >= read.minimum
ServedReadCoversAcknowledgements == read.phase = "served" => read.served >= read.minimum
NeverServesOnNewLoglet ==
    ~(phase = "new" /\ read.phase = "served" /\ read.route = "new" /\ read.minimum > 0)
NeverSamplesPastPublished ==
    ~(read.phase = "confirmed" /\ read.bound > applied /\ read.bound > acknowledged)

===================================================================================
