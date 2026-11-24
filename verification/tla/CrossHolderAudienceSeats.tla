---- MODULE CrossHolderAudienceSeats ----
EXTENDS Naturals, FiniteSets

\* The installed drain-before-reuse protocol. AudienceSeatResourceFence
\* models the alternative with a durable check at the accepting resource.
CONSTANTS Ceiling, Sweep, Moves, Mutation
VARIABLES epoch, cells, current, retired, pending, phase, violation

vars == <<epoch, cells, current, retired, pending, phase, violation>>
Seats == {1, 2}
Holders == {1, 2}
Positions == Holders \X Seats
Holder(e) == IF e % 2 = 0 THEN 1 ELSE 2
Empty == [occupied |-> FALSE, open |-> FALSE, io |-> FALSE,
          reported |-> FALSE, placement |-> 0, age |-> 0, originalAge |-> 0]
Idle == [seat |-> 0, route |-> 0, placement |-> 0, minted |-> FALSE]

Init ==
    /\ epoch = 0
    /\ cells = [p \in Positions |-> Empty]
    /\ current = 0
    /\ retired = 0
    /\ pending = Idle
    /\ phase = 0
    /\ violation = "none"

Move ==
    /\ epoch < Moves
    /\ epoch' = epoch + 1
    /\ UNCHANGED <<cells, current, retired, pending, phase, violation>>

Route ==
    /\ pending = Idle
    /\ (retired = 0 \/ Mutation = "before_local_drain")
    /\ LET seat == IF current = 1 THEN 2 ELSE 1
           route == IF Mutation = "cached_assignment" THEN 1 ELSE Holder(epoch)
       IN /\ pending' = [Idle EXCEPT !.seat = seat, !.route = route, !.placement = epoch]
          /\ violation' = CASE route # Holder(epoch) -> "route"
                              [] retired # 0 -> "local"
                              [] OTHER -> violation
    /\ UNCHANGED <<epoch, cells, current, retired, phase>>

Handle ==
    /\ pending # Idle
    /\ ~pending.minted
    /\ LET p == <<pending.route, pending.seat>>
           s == cells[p]
           currentPlacement == pending.placement = epoch /\ pending.route = Holder(epoch)
           retry == s.open /\ s.age < Ceiling /\ ~s.reported /\ s.placement = epoch
           free == \A h \in Holders : ~cells[<<h, pending.seat>>].occupied
           mint == ~s.occupied /\ (free \/ Mutation = "before_remote_drain")
       IN /\ currentPlacement \/ Mutation = "unfenced_mint"
          /\ retry \/ mint
          /\ cells' = IF retry THEN cells ELSE
                         [q \in Positions |->
                             CASE q = p -> [Empty EXCEPT !.occupied = TRUE,
                                             !.open = TRUE, !.placement = epoch]
                               [] Mutation = "reset_ceiling" /\ cells[q].placement < epoch
                                    -> [cells[q] EXCEPT !.age = 0]
                               [] OTHER -> cells[q]]
          /\ pending' = [pending EXCEPT !.minted = TRUE]
    /\ violation' = IF pending.placement # epoch \/ pending.route # Holder(epoch)
                    THEN "mint" ELSE violation
    /\ UNCHANGED <<epoch, current, retired, phase>>

LoseReply ==
    /\ pending # Idle
    /\ pending' = Idle
    /\ UNCHANGED <<epoch, cells, current, retired, phase, violation>>

ReceiveReply ==
    /\ pending.minted
    /\ LET p == <<pending.route, pending.seat>>
       IN /\ cells[p].open
          /\ cells[p].placement = pending.placement
          /\ cells' = [cells EXCEPT ![p].reported = TRUE]
    /\ retired' = current
    /\ current' = pending.seat
    /\ pending' = Idle
    /\ UNCHANGED <<epoch, phase, violation>>

LocalDrain ==
    /\ retired # 0
    /\ retired' = 0
    /\ UNCHANGED <<epoch, cells, current, pending, phase, violation>>

Close(p) ==
    /\ cells[p].open
    /\ cells[p].reported
    /\ p[2] # current
    /\ p[2] # retired
    /\ cells' = [cells EXCEPT ![p].open = FALSE]
    /\ UNCHANGED <<epoch, current, retired, pending, phase, violation>>

StartData(p) ==
    /\ cells[p].open /\ cells[p].reported /\ ~cells[p].io
    /\ cells[p].age < Ceiling
    /\ cells[p].placement = epoch \/ Mutation = "displaced_data"
    /\ cells' = [cells EXCEPT ![p].io = TRUE]
    /\ violation' = IF cells[p].placement # epoch THEN "data" ELSE violation
    /\ UNCHANGED <<epoch, current, retired, pending, phase>>

EndData(p) ==
    /\ cells[p].io
    /\ cells' = [cells EXCEPT ![p].io = FALSE]
    /\ UNCHANGED <<epoch, current, retired, pending, phase, violation>>

Release(p) ==
    /\ cells[p].occupied /\ ~cells[p].open
    /\ ~cells[p].io \/ Mutation = "release_while_io"
    /\ cells' = [cells EXCEPT ![p] = [Empty EXCEPT !.io = cells[p].io]]
    /\ UNCHANGED <<epoch, current, retired, pending, phase, violation>>

MaxAge == Ceiling + Sweep
Inc(a) == IF a < MaxAge THEN a + 1 ELSE a

Tick ==
    /\ phase' = (phase + 1) % Sweep
    /\ cells' = [p \in Positions |->
          LET s == cells[p]
          IN IF ~s.open THEN s ELSE [s EXCEPT
              !.age = Inc(s.age), !.originalAge = Inc(s.originalAge),
              !.open = ~(phase' = 0 /\ Inc(s.age) >= Ceiling /\ Mutation # "drop_sweep")]]
    /\ UNCHANGED <<epoch, current, retired, pending, violation>>

Next ==
    \/ Move \/ Route \/ Handle \/ LoseReply \/ ReceiveReply \/ LocalDrain \/ Tick
    \/ \E p \in Positions : Close(p) \/ StartData(p) \/ EndData(p) \/ Release(p)

Spec == Init /\ [][Next]_vars /\ WF_vars(Tick)

Capacity == Cardinality({p \in Positions : cells[p].occupied}) <= Cardinality(Seats)
CurrentRoute == violation # "route"
CurrentMint == violation # "mint"
LocalGenerationDrained == violation # "local"
RetainedIoRemainsCharged == \A p \in Positions : cells[p].io => cells[p].occupied
CleanupOnly == violation # "data"
CloseByCeilingPlusSweep ==
    \A p \in Positions : cells[p].open /\ cells[p].placement < epoch
         => cells[p].originalAge < MaxAge
OpenAt(p, e) == cells[p].open /\ cells[p].placement = e
DisplacedCloses ==
    \A p \in Positions, e \in 0..Moves :
        (OpenAt(p, e) /\ e < epoch) ~> ~OpenAt(p, e)
NeverConcurrentReplacement ==
    ~\E p, q \in Positions : cells[p].io /\ cells[p].placement < epoch
        /\ cells[q].io /\ cells[q].placement = epoch

====
