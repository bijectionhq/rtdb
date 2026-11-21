---------------------- MODULE ResidenceReaderRetention ----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Policy, Mutation
Readers == 1..2
Catalogs == 0..2
Idle == 3
Objects == {"old", "overlap", "later", "live"}
Dependencies == [g \in Catalogs |->
    CASE g = 0 -> {"old", "overlap"}
      [] g = 1 -> {"overlap", "later", "live"}
      [] OTHER -> {"live"}]
RetiresAt == [object \in Objects |->
    CASE object = "old" -> 1
      [] object \in {"overlap", "later"} -> 2
      [] OTHER -> Idle]

(* A captured hold persists while the handle is open or its provider read is active. *)
VARIABLES current, readers, released, present, pending, sweep
vars == <<current, readers, released, present, pending, sweep>>
NoReader == [catalog |-> Idle, open |-> FALSE, reading |-> FALSE]
NoSweep == [head |-> Idle, floor |-> Idle, protected |-> {}]

Init ==
    /\ current = 0
    /\ readers = [r \in Readers |-> NoReader]
    /\ released = {}
    /\ present = Dependencies[0]
    /\ pending = {}
    /\ sweep = NoSweep

StepCatalog ==
    /\ current < 2
    /\ current' = current + 1
    /\ present' = present \cup (Dependencies[current'] \ Dependencies[current])
    /\ UNCHANGED <<readers, released, pending, sweep>>

Admit(reader, g) ==
    /\ readers[reader] = NoReader
    /\ g <= current
    /\ g = current \/ Mutation = "admit_stale"
    /\ readers' = [readers EXCEPT ![reader] = [catalog |-> g, open |-> TRUE, reading |-> FALSE]]
    /\ UNCHANGED <<current, released, present, pending, sweep>>

BeginRead(reader) ==
    /\ readers[reader].open
    /\ ~readers[reader].reading
    /\ readers' = [readers EXCEPT ![reader].reading = TRUE]
    /\ UNCHANGED <<current, released, present, pending, sweep>>

Close(reader) ==
    /\ readers[reader].open
    /\ readers' = [readers EXCEPT ![reader] =
          IF @.reading THEN [@ EXCEPT !.open = FALSE] ELSE NoReader]
    /\ UNCHANGED <<current, released, present, pending, sweep>>

Drain(reader) ==
    /\ readers[reader].reading
    /\ readers' = [readers EXCEPT ![reader] =
          IF @.open THEN [@ EXCEPT !.reading = FALSE] ELSE NoReader]
    /\ UNCHANGED <<current, released, present, pending, sweep>>

PublishRelease(object) ==
    /\ RetiresAt[object] <= current
    /\ object \notin released
    /\ released' = released \cup {object}
    /\ UNCHANGED <<current, readers, present, pending, sweep>>

CaptureSweep ==
    /\ sweep = NoSweep
    /\ LET active == IF Mutation = "ignore_holds" THEN {}
                     ELSE {readers[r].catalog : r \in
                          {id \in Readers : readers[id].open \/ Mutation # "ignore_drain"}} \ {Idle}
           reachable == active \cup {current}
       IN sweep' = [head |-> current,
                    floor |-> CHOOSE m \in reachable : \A n \in reachable : m <= n,
                    protected |-> UNION {Dependencies[g] : g \in reachable}]
    /\ UNCHANGED <<current, readers, released, present, pending>>

Authorize(object) ==
    /\ sweep # NoSweep
    /\ object \in (released \cap present) \ pending
    /\ (IF Policy = "exact" THEN /\ RetiresAt[object] <= sweep.head \/ Mutation = "trust_late_release"
             /\ object \notin sweep.protected
        ELSE RetiresAt[object] <= sweep.floor \/ Mutation = "trust_late_release")
    /\ pending' = pending \cup {object}
    /\ UNCHANGED <<current, readers, released, present, sweep>>

EndSweep ==
    /\ sweep # NoSweep
    /\ sweep' = NoSweep
    /\ UNCHANGED <<current, readers, released, present, pending>>

CompleteDelete(object) ==
    /\ object \in pending
    /\ pending' = pending \ {object}
    /\ present' = present \ {object}
    /\ UNCHANGED <<current, readers, released, sweep>>

Next ==
    \/ StepCatalog
    \/ \E r \in Readers :
         \/ \E g \in Catalogs : Admit(r, g)
         \/ BeginRead(r)
         \/ Close(r)
         \/ Drain(r)
    \/ \E object \in Objects :
         \/ PublishRelease(object)
         \/ Authorize(object)
         \/ CompleteDelete(object)
    \/ CaptureSweep
    \/ EndSweep
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ current \in Catalogs
    /\ readers \in [Readers -> [catalog : Catalogs \cup {Idle}, open : BOOLEAN, reading : BOOLEAN]]
    /\ released \subseteq Objects
    /\ present \subseteq Objects
    /\ pending \subseteq (present \cap released)
    /\ sweep \in [head : Catalogs \cup {Idle}, floor : Catalogs \cup {Idle}, protected : SUBSET Objects]

ReaderReadable ==
    \A r \in Readers : readers[r].catalog # Idle => Dependencies[readers[r].catalog] \subseteq present

ServingReadable == Dependencies[current] \subseteq present

ReleasedWhileLatestReaderNeverObserved ==
    ~("old" \notin present /\ "overlap" \notin present
      /\ \E r \in Readers : readers[r].open /\ readers[r].catalog = 2)

LaterObjectRetainedWithEarlierReader ==
    (current = 2 /\ \E r \in Readers : readers[r].catalog = 0) => "later" \in present
=============================================================================
