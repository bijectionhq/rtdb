--------------------------- MODULE SystemClock ----------------------------
EXTENDS Integers, Sequences

CONSTANTS
    \* @type: Int;
    MaxTime,
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Seq(Int);
    resolved,
    \* @type: Int;
    durable,
    \* @type: Bool;
    online,
    \* @type: Bool;
    restarting,
    \* @type: Bool;
    resumedCatalog

Vars == <<resolved, durable, online, restarting, resumedCatalog>>
Kinds == {"automatic", "explicit", "catalog"}
Times == 1..MaxTime

Init ==
    /\ resolved = <<>>
    /\ durable = 0
    /\ online = TRUE
    /\ restarting = FALSE
    /\ resumedCatalog = FALSE

At(position) == IF position = 0 THEN 0 ELSE resolved[position]

Floor ==
    CASE Mutation = "durable_head" -> At(durable)
      [] Mutation = "empty_restart" /\ restarting -> 0
      [] OTHER -> At(Len(resolved))

Automatic(clock, floor) == IF clock > floor THEN clock ELSE floor + 1

Assigned(kind, clock, requested) ==
    CASE kind = "catalog" /\ Mutation = "raw_catalog" -> clock
      [] kind = "explicit" /\ requested > Floor -> requested
      [] OTHER -> Automatic(clock, Floor)

Stage ==
    /\ online
    /\ Len(resolved) - durable < 2
    /\ \E kind \in Kinds, clock \in Times, requested \in Times:
        LET time == Assigned(kind, clock, requested) IN
        /\ time \in Times
        /\ resolved' = Append(resolved, time)
        /\ resumedCatalog' = (resumedCatalog \/
            (restarting /\ durable > 0 /\ kind = "catalog" /\ clock <= At(durable)))
    /\ restarting' = FALSE
    /\ UNCHANGED <<durable, online>>

Publish ==
    /\ online
    /\ durable < Len(resolved)
    /\ durable' = durable + 1
    /\ UNCHANGED <<resolved, online, restarting, resumedCatalog>>

Crash ==
    /\ online
    /\ online' = FALSE
    /\ resolved' = SubSeq(resolved, 1, durable)
    /\ restarting' = FALSE
    /\ UNCHANGED <<durable, resumedCatalog>>

Restart ==
    /\ ~online
    /\ online' = TRUE
    /\ restarting' = TRUE
    /\ UNCHANGED <<resolved, durable, resumedCatalog>>

Next == Stage \/ Publish \/ Crash \/ Restart
Spec == Init /\ [][Next]_Vars

StrictSystemTime ==
    \A earlier, later \in 1..Len(resolved):
        earlier < later => resolved[earlier] < resolved[later]

NoCatalogAfterRestart == ~resumedCatalog

=============================================================================
