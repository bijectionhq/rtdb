----------------------------- MODULE Checkpoint -----------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Workers, MaxEvents, CountOnly, PublishEarly

VARIABLES log, prepared, objects, manifests, sweep, reader

vars == <<log, prepared, objects, manifests, sweep, reader>>
Symmetry == Permutations(Workers)
Absent == 0
Pending == 3
Idle == [frontier |-> 0, prefix |-> <<>>]
IdleRead == [frontier |-> 0, basis |-> 0, valid |-> 0, id |-> 1,
             selected |-> Idle, result |-> Pending, active |-> FALSE]
Slots == {"directory", "posting"}
Times == 0..2
Ids == {1}
Keys == {1, 2}
Ops == {"put", "delete", "erase"}
Events == {e \in [id : Ids, lo : Times, hi : Times, op : Ops, key : Keys] :
  e.lo < e.hi /\ (e.op # "erase" \/ (e.lo = 0 /\ e.hi = 2))}

Greatest(set) == CHOOSE n \in set : \A m \in set : m <= n
Prefix(events, n) == SubSeq(events, 1, n)

\* The table definition picks the last eligible operation after the latest erase.
Table(events, basis, valid, iid) ==
  LET erased == Greatest({0} \cup {i \in 1..Len(events) :
                            events[i].op = "erase" /\ events[i].id = iid})
      winner == Greatest({0} \cup {i \in 1..basis :
                            i > erased /\ events[i].id = iid
                            /\ events[i].lo <= valid /\ valid < events[i].hi})
  IN IF winner = 0 THEN Absent
     ELSE IF events[winner].op = "put" THEN events[winner].key ELSE Absent

\* The index processes events forwards, retaining the current point projection.
RECURSIVE Replay(_, _, _, _, _, _)
Replay(events, first, basis, valid, iid, initial) ==
  IF first > Len(events) THEN initial
  ELSE
    LET e == events[first]
        next == IF e.id # iid THEN initial
                ELSE IF e.op = "erase" THEN Absent
                ELSE IF first <= basis /\ e.lo <= valid /\ valid < e.hi
                     THEN IF e.op = "put" THEN e.key ELSE Absent
                     ELSE initial
    IN Replay(events, first + 1, basis, valid, iid, next)

Page(p, slot) == [frontier |-> p.frontier, prefix |-> p.prefix, slot |-> slot]
References(p) == {Page(p, slot) : slot \in Slots}

Init ==
  /\ log = <<>>
  /\ prepared = [w \in Workers |-> Idle]
  /\ objects = {}
  /\ manifests = {}
  /\ sweep = [w \in Workers |-> {}]
  /\ reader = IdleRead

Commit ==
  /\ Len(log) < MaxEvents
  /\ \E ev \in Events : log' = Append(log, ev)
  /\ UNCHANGED <<prepared, objects, manifests, sweep, reader>>

Prepare(w) ==
  /\ prepared[w] = Idle
  /\ Len(log) > 0
  /\ \E n \in 1..Len(log) :
       LET exact == Prefix(log, n)
           copied == IF CountOnly /\ n > 1
                     THEN Append(Prefix(log, n - 1), log[n - 1])
                     ELSE exact
       IN prepared' = [prepared EXCEPT ![w] = [frontier |-> n, prefix |-> copied]]
  /\ UNCHANGED <<log, objects, manifests, sweep, reader>>

Upload(w) ==
  /\ prepared[w] # Idle
  /\ \E page \in References(prepared[w]) \ objects :
       objects' = objects \cup {page}
  /\ UNCHANGED <<log, prepared, manifests, sweep, reader>>

Publish(w) ==
  /\ prepared[w] # Idle
  /\ \A p \in manifests : p.frontier # prepared[w].frontier
  /\ IF PublishEarly THEN TRUE ELSE References(prepared[w]) \subseteq objects
  /\ IF CountOnly THEN Len(prepared[w].prefix) = prepared[w].frontier
     ELSE prepared[w].prefix = Prefix(log, prepared[w].frontier)
  /\ manifests' = manifests \cup {prepared[w]}
  /\ prepared' = [prepared EXCEPT ![w] = Idle]
  /\ UNCHANGED <<log, objects, sweep, reader>>

Capture ==
  /\ reader = IdleRead
  /\ \E basis \in 0..Len(log), valid \in Times, iid \in Ids :
       reader' = [frontier |-> Len(log), basis |-> basis, valid |-> valid, id |-> iid,
                  selected |-> Idle, result |-> Pending, active |-> TRUE]
  /\ UNCHANGED <<log, prepared, objects, manifests, sweep>>

Select ==
  /\ reader # IdleRead
  /\ reader.selected = Idle
  /\ reader.result = Pending
  /\ \E p \in manifests :
       /\ p.frontier <= reader.frontier
       /\ reader' = [reader EXCEPT !.selected = p]
  /\ UNCHANGED <<log, prepared, objects, manifests, sweep>>

Answer ==
  /\ reader # IdleRead
  /\ reader.result = Pending
  /\ LET p == reader.selected
         captured == Prefix(log, reader.frontier)
         initial == IF p = Idle THEN Absent
                    ELSE Replay(p.prefix, 1, reader.basis, reader.valid, reader.id, Absent)
         result == IF p = Idle THEN Table(captured, reader.basis, reader.valid, reader.id)
                   ELSE IF References(p) \subseteq objects
                        THEN Replay(captured, p.frontier + 1, reader.basis, reader.valid, reader.id, initial)
                        ELSE Table(captured, reader.basis, reader.valid, reader.id)
     IN reader' = [reader EXCEPT !.result = result]
  /\ UNCHANGED <<log, prepared, objects, manifests, sweep>>

Release ==
  /\ reader # IdleRead
  /\ reader.result # Pending
  /\ reader' = IdleRead
  /\ UNCHANGED <<log, prepared, objects, manifests, sweep>>

Mark(w) ==
  /\ sweep[w] = {}
  /\ \E p \in manifests :
       /\ \A other \in manifests : other.frontier <= p.frontier
       /\ LET garbage == {page \in objects : page.frontier <= p.frontier} \ References(p)
          IN /\ garbage # {}
             /\ sweep' = [sweep EXCEPT ![w] = garbage]
  /\ UNCHANGED <<log, prepared, objects, manifests, reader>>

Collect(w) ==
  /\ \E page \in sweep[w] :
       /\ objects' = objects \ {page}
       /\ sweep' = [sweep EXCEPT ![w] = @ \ {page}]
  /\ UNCHANGED <<log, prepared, manifests, reader>>

Retire ==
  /\ \E p, newer \in manifests :
       /\ p.frontier < newer.frontier
       /\ manifests' = manifests \ {p}
  /\ UNCHANGED <<log, prepared, objects, sweep, reader>>

Crash(w) ==
  /\ prepared[w] # Idle \/ sweep[w] # {}
  /\ prepared' = [prepared EXCEPT ![w] = Idle]
  /\ sweep' = [sweep EXCEPT ![w] = {}]
  /\ UNCHANGED <<log, objects, manifests, reader>>

Next ==
  \/ Commit \/ Capture \/ Select \/ Answer \/ Release \/ Retire
  \/ \E w \in Workers : Prepare(w) \/ Upload(w) \/ Publish(w)
                         \/ Mark(w) \/ Collect(w) \/ Crash(w)

Spec == Init /\ [][Next]_vars

AnswersExact ==
  IF reader = IdleRead THEN TRUE
  ELSE IF reader.result = Pending THEN TRUE
  ELSE reader.result = Table(Prefix(log, reader.frontier), reader.basis, reader.valid, reader.id)

ManifestPrefix ==
  \A p \in manifests : p.prefix = Prefix(log, p.frontier)

NewestReachable ==
  \A p \in manifests :
    (\A other \in manifests : other.frontier <= p.frontier) =>
      References(p) \subseteq objects

=============================================================================
