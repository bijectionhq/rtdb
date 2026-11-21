----------------------- MODULE ReplicaHoldLifecycle -----------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES readers, authorized, physical

Readers == {1, 2}
Offsets == 0..2
NoPin == -1
Vars == <<readers, authorized, physical>>

Init ==
    /\ readers = [r \in Readers |-> [phase |-> "idle", request |-> "none",
                                   first |-> NoPin, pin |-> NoPin,
                                   stopped |-> FALSE, timedOut |-> FALSE]]
    /\ authorized = 0
    /\ physical = 0

Put(r, value) == /\ readers' = [readers EXCEPT ![r] = value]
                 /\ UNCHANGED <<authorized, physical>>

Request(r, first) ==
    /\ readers[r].phase = "idle"
    /\ Put(r, [readers[r] EXCEPT !.phase = "opening", !.request = "pending", !.first = first])

Apply(r) ==
    LET current == readers[r] IN
    /\ current.request = "pending"
    /\ current.first >= authorized
    /\ Put(r, [current EXCEPT !.request = "applied", !.pin = current.first])

Receive(r) ==
    /\ readers[r].phase = "opening"
    /\ readers[r].request = "applied"
    /\ Put(r, [readers[r] EXCEPT !.phase = "open"])

BeginRead(r) ==
    /\ readers[r].phase = "open"
    /\ Put(r, [readers[r] EXCEPT !.phase = "reading"])

Cancel(r) ==
    LET phase == readers[r].phase IN
    /\ phase \in {"opening", "open", "reading"}
    /\ Put(r, [readers[r] EXCEPT !.phase = IF phase = "reading" THEN "draining" ELSE "drained"])

CompleteRead(r) ==
    LET phase == readers[r].phase IN
    /\ phase \in {"reading", "draining"}
    /\ Put(r, [readers[r] EXCEPT !.phase = IF phase = "draining" THEN "drained" ELSE "open"])

ProcessDies(r) ==
    /\ readers[r].phase \in {"opening", "open", "reading", "draining", "drained"}
    /\ Put(r, [readers[r] EXCEPT !.phase = "gone"])

ObserveStop(r) ==
    /\ readers[r].phase = "gone"
    /\ Put(r, [readers[r] EXCEPT !.stopped = TRUE])

ObserveTimeout(r) ==
    /\ readers[r].phase \in {"opening", "open", "reading", "draining", "drained", "gone"}
    /\ Put(r, [readers[r] EXCEPT !.timedOut = TRUE])

Close(r) ==
    LET current == readers[r] IN
    /\ current.phase = "drained"
       \/ (current.phase = "gone" /\ current.stopped)
       \/ (Mutation = "close_on_cancel" /\ current.phase = "draining")
       \/ (Mutation = "use_timeout_as_stop" /\ current.timedOut /\ current.phase # "released")
    /\ Put(r, [current EXCEPT !.phase = "released", !.pin = NoPin,
                 !.request = IF Mutation = "skip_empty_close_cas" /\ current.pin = NoPin
                             THEN current.request ELSE "fenced"])

Authorize(first) ==
    /\ authorized < first
    /\ \A r \in Readers : readers[r].pin = NoPin \/ first <= readers[r].pin
    /\ authorized' = first
    /\ UNCHANGED <<readers, physical>>

ApplyTrim(first) ==
    /\ physical < first
    /\ first <= authorized
    /\ physical' = first
    /\ UNCHANGED <<readers, authorized>>

Next ==
    \/ \E r \in Readers :
         (\E first \in Offsets : Request(r, first))
         \/ Apply(r) \/ Receive(r) \/ BeginRead(r) \/ Cancel(r) \/ CompleteRead(r)
         \/ ProcessDies(r) \/ ObserveStop(r) \/ ObserveTimeout(r) \/ Close(r)
    \/ \E first \in Offsets : Authorize(first) \/ ApplyTrim(first)

Spec == Init /\ [][Next]_Vars

ActiveHoldCovered ==
    \A r \in Readers : readers[r].phase \in {"open", "reading", "draining"}
         => readers[r].pin = readers[r].first /\ physical <= readers[r].first

ReleaseAfterDrainOrStop ==
    [][\A r \in Readers : readers'[r].phase = "released" /\ readers[r].phase # "released"
         => readers[r].phase = "drained" \/ (readers[r].phase = "gone" /\ readers[r].stopped)]_Vars

ClosedNeverRegainsHold ==
    \A r \in Readers : readers[r].phase = "released" => readers[r].pin = NoPin

NeverTrimsAfterConfirmedDeath ==
    ~(physical = 2 /\ \E r \in Readers :
        readers[r].phase = "released" /\ readers[r].first = 0 /\ readers[r].stopped)

=============================================================================
