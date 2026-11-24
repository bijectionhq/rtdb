------------------------ MODULE AudienceWriterHandoff ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS SwitchAt, Limit, Turns, Mutation

VARIABLES now, phase, epoch, position, preparation, fence, opening, settlement, turns
vars == <<now, phase, epoch, position, preparation, fence, opening, settlement, turns>>
StartEpoch == 1
NextEpoch == 2
LagAndSkew == 1
Original == "incumbent certificate"
Prepared == "prepared certificate"
Handoff == <<"incumbent generation", "prepared generation", "source UUID", "tablet",
             StartEpoch, NextEpoch, SwitchAt>>

Init ==
    /\ now = 0 /\ phase = "incumbent" /\ epoch = StartEpoch /\ position = 0
    /\ preparation = 0 /\ fence = 0 /\ opening = 0 /\ settlement = <<>>
    /\ turns = [i \in 1..Turns |-> [state |-> "unused", certificate |-> "",
           cutoff |-> 0, writerCutoff |-> 0, pinned |-> 0, dispatched |-> 0, released |-> 0]]

Prepare ==
    /\ phase = "incumbent" /\ now < SwitchAt
    /\ phase' = IF Mutation = "park" THEN "parked" ELSE "prepared"
    /\ preparation' = position + 1 /\ position' = preparation'
    /\ UNCHANGED <<now, epoch, fence, opening, settlement, turns>>

Fence ==
    /\ phase = "prepared"
    /\ (now = SwitchAt \/ Mutation = "early-fence")
    /\ phase' = "fenced" /\ fence' = position + 1 /\ position' = fence'
    /\ epoch' = IF Mutation = "reuse-epoch" THEN epoch
                 ELSE IF Mutation = "skip-epoch" THEN epoch + 2 ELSE epoch + 1
    /\ UNCHANGED <<now, preparation, opening, settlement, turns>>

Open ==
    /\ phase = "fenced" /\ Mutation # "stalled-opening"
    /\ (now = SwitchAt \/ Mutation = "early-fence")
    /\ phase' = "opened" /\ position' = position + 1
    /\ opening' = IF Mutation = "equal-coordinates" THEN fence ELSE position'
    /\ settlement' = IF Mutation = "foreign-settlement" THEN <<Handoff, 0>>
                     ELSE <<Handoff, preparation, fence, opening'>>
    /\ UNCHANGED <<now, epoch, preparation, fence, turns>>

Tick ==
    /\ now < Limit
    /\ (now + 1 < SwitchAt \/ preparation > 0)
    /\ (now # SwitchAt \/ phase = "opened" \/ Mutation = "tick-fenced")
    /\ now' = now + 1
    /\ UNCHANGED <<phase, epoch, position, preparation, fence, opening, settlement, turns>>

Pin(i, cutoff, writerCutoff) ==
    /\ turns[i].state = "unused" /\ now < cutoff
    /\ (now < SwitchAt \/ phase = "opened")
    /\ turns' = [turns EXCEPT ![i] = [state |-> "pinned",
           certificate |-> IF phase = "opened" THEN Prepared ELSE Original,
           cutoff |-> cutoff, writerCutoff |-> writerCutoff, pinned |-> now, dispatched |-> 0, released |-> 0]]
    /\ UNCHANGED <<now, phase, epoch, position, preparation, fence, opening, settlement>>

Dispatch(i) ==
    /\ turns[i].state = "pinned"
    /\ LET t == turns[i]
           upper == now + LagAndSkew
           allowed == upper < t.writerCutoff /\ upper < t.cutoff
               /\ (IF t.certificate = Original
                   THEN ((phase = "incumbent" /\ upper < SwitchAt)
                         \/ (phase = "prepared" /\ t.writerCutoff <= SwitchAt))
                        \/ Mutation = "late-dispatch"
                   ELSE phase = "opened")
       IN turns' = [turns EXCEPT ![i].state = IF allowed THEN "accepted" ELSE "refused",
                         ![i].dispatched = now]
    /\ UNCHANGED <<now, phase, epoch, position, preparation, fence, opening, settlement>>

Release(i) ==
    /\ turns[i].state = "accepted"
    /\ (now < turns[i].cutoff \/ (Mutation = "inclusive-release" /\ now = turns[i].cutoff))
    /\ turns' = [turns EXCEPT ![i].state = "released", ![i].released = now,
           ![i].certificate = IF Mutation = "replace-certificate" /\ phase = "opened" THEN Prepared ELSE @,
           ![i].cutoff = IF Mutation = "inclusive-release" /\ now = @ THEN now + 1 ELSE @]
    /\ UNCHANGED <<now, phase, epoch, position, preparation, fence, opening, settlement>>

Next == Prepare \/ Fence \/ Open \/ Tick
    \/ (\E i \in 1..Turns : Dispatch(i) \/ Release(i) \/ (\E cutoff \in 1..(Limit + 1): \E guard \in 1..cutoff : Pin(i, cutoff, guard)))

Spec == Init /\ [][Next]_vars /\ WF_vars(Prepare) /\ WF_vars(Fence) /\ WF_vars(Open) /\ WF_vars(Tick)

Continuity == IF now < SwitchAt THEN phase \in {"incumbent", "prepared"} /\ epoch = StartEpoch
              ELSE IF now > SwitchAt THEN phase = "opened" ELSE phase # "parked"
EpochOrder == IF fence = 0 THEN epoch = StartEpoch ELSE epoch = StartEpoch + 1
SettlementOrder == settlement # <<>> => 0 < preparation /\ preparation < fence /\ fence < opening
ExactSettlement == settlement # <<>> => settlement = <<Handoff, preparation, fence, opening>>
OriginalDispatch == \A i \in 1..Turns : turns[i].state \in {"accepted", "released"} =>
    /\ turns[i].dispatched + LagAndSkew < turns[i].writerCutoff
    /\ (turns[i].pinned < SwitchAt => turns[i].dispatched + LagAndSkew < SwitchAt)
OriginalRelease == \A i \in 1..Turns : turns[i].state = "released" =>
    /\ turns[i].certificate = (IF turns[i].pinned < SwitchAt THEN Original ELSE Prepared)
    /\ turns[i].released < turns[i].cutoff
StableCutoff == \A i \in 1..Turns :
    turns[i].state # "unused" =>
        turns'[i].cutoff = turns[i].cutoff /\ turns'[i].writerCutoff = turns[i].writerCutoff
CutoffsUnchanged == [][StableCutoff]_vars
EventuallyOpens == <> (phase = "opened")
NeverStraddles == ~(phase = "opened" /\ (\E i \in 1..Turns :
    turns[i].state = "released" /\ turns[i].pinned < SwitchAt /\ turns[i].released >= SwitchAt))
NeverRefuses == ~(phase = "opened" /\ (\E i \in 1..Turns :
    turns[i].state = "refused" /\ turns[i].pinned < SwitchAt /\ turns[i].dispatched >= SwitchAt))
=============================================================================
