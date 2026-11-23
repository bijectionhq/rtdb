---------------------- MODULE RTDBCellTransferAttempts ----------------------
(* The source selects one destination copy attempt at a time. A staging     *)
(* attempt can close, drain and give way to a fresh generation; a prepared  *)
(* attempt and its receipt are final. Mutation names one removed guard.     *)
EXTENDS Naturals

CONSTANTS Attempts, Mutation

VARIABLES sourceOpen, sourceMedia, selected, installed, closed, stage,
          objects, inflight, receipts, route, active, destinationOpen

vars == <<sourceOpen, sourceMedia, selected, installed, closed, stage,
          objects, inflight, receipts, route, active, destinationOpen>>

Max == Attempts
Ordinals == 1..Max

Init ==
    /\ sourceOpen = TRUE
    /\ sourceMedia = TRUE
    /\ selected = 1
    /\ installed = 1
    /\ closed = {}
    /\ stage = [a \in Ordinals |-> "staging"]
    /\ objects = [a \in Ordinals |-> "empty"]
    /\ inflight = [a \in Ordinals |-> FALSE]
    /\ receipts = {}
    /\ route = "source"
    /\ active = 0
    /\ destinationOpen = FALSE

Fence ==
    /\ sourceOpen
    /\ sourceOpen' = FALSE
    /\ UNCHANGED <<sourceMedia, selected, installed, closed, stage, objects,
                   inflight, receipts, route, active, destinationOpen>>

\* Bytes left by an earlier copier or another writer: only attempt one meets them.
Conflict ==
    /\ objects[1] \in {"empty", "partial"}
    /\ stage[1] = "staging"
    /\ objects' = [objects EXCEPT ![1] = "conflict"]
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   inflight, receipts, route, active, destinationOpen>>

Admit(a) ==
    /\ ~sourceOpen
    /\ a = installed /\ a = selected /\ a \notin closed
    /\ stage[a] = "staging"
    /\ objects[a] \in {"empty", "partial"}
    /\ ~inflight[a]
    /\ inflight' = [inflight EXCEPT ![a] = TRUE]
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   objects, receipts, route, active, destinationOpen>>

\* An admitted copier writes only its own generation, even once it is stale.
Write(a) ==
    /\ inflight[a]
    /\ objects[a] \in {"empty", "partial", "erased"}
    /\ objects' = [objects EXCEPT ![a] = IF objects[a] = "erased" THEN "late" ELSE "partial"]
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   inflight, receipts, route, active, destinationOpen>>

Finish(a) ==
    /\ inflight[a]
    /\ objects[a] # "empty"
    /\ inflight' = [inflight EXCEPT ![a] = FALSE]
    /\ objects' = [objects EXCEPT ![a] = IF objects[a] = "partial" THEN "good" ELSE objects[a]]
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   receipts, route, active, destinationOpen>>

Prepare(a) ==
    /\ stage[a] = "staging"
    /\ objects[a] = "good"
    /\ ~inflight[a]
    /\ (Mutation = "stale_attempt" \/ (a = selected /\ a = installed /\ a \notin closed))
    /\ stage' = [stage EXCEPT ![a] = "prepared"]
    /\ receipts' = receipts \cup {a}
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, objects,
                   inflight, route, active, destinationOpen>>

Close(a) ==
    /\ a = installed /\ a = selected /\ a \notin closed
    /\ (stage[a] = "staging" \/ Mutation = "close_prepared")
    /\ (objects[a] = "conflict" \/ a < Max)
    /\ closed' = closed \cup {a}
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, stage, objects,
                   inflight, receipts, route, active, destinationOpen>>

SelectNext ==
    /\ selected = installed
    /\ selected \in closed
    /\ selected < Max
    /\ (~inflight[selected] \/ Mutation = "skip_drain")
    /\ selected' = selected + 1
    /\ objects' = IF Mutation = "reuse_generation"
                  THEN [objects EXCEPT ![selected + 1] = objects[selected]]
                  ELSE objects
    /\ UNCHANGED <<sourceOpen, sourceMedia, installed, closed, stage, inflight,
                   receipts, route, active, destinationOpen>>

Install ==
    /\ installed < selected
    /\ installed \in closed
    /\ installed' = selected
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, closed, stage, objects,
                   inflight, receipts, route, active, destinationOpen>>

Route(r) ==
    /\ route = "source"
    /\ r \in receipts
    /\ (Mutation = "stale_attempt" \/ (r = selected /\ r = installed /\ r \notin closed))
    /\ route' = "destination"
    /\ active' = r
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   objects, inflight, receipts, destinationOpen>>

Activate ==
    /\ route = "destination"
    /\ ~destinationOpen
    /\ destinationOpen' = TRUE
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   objects, inflight, receipts, route, active>>

Dispose(a) ==
    /\ a \in closed
    /\ (a # selected \/ Mutation = "dispose_selected")
    /\ (a # installed \/ Mutation = "dispose_selected")
    /\ (~inflight[a] \/ Mutation = "skip_drain")
    /\ objects[a] # "erased"
    /\ objects' = [objects EXCEPT ![a] = "erased"]
    /\ UNCHANGED <<sourceOpen, sourceMedia, selected, installed, closed, stage,
                   inflight, receipts, route, active, destinationOpen>>

DisposeSource ==
    /\ sourceMedia
    /\ ~sourceOpen
    /\ (route = "destination" \/ Mutation = "early_source_disposal")
    /\ sourceMedia' = FALSE
    /\ UNCHANGED <<sourceOpen, selected, installed, closed, stage, objects,
                   inflight, receipts, route, active, destinationOpen>>

Next ==
    \/ Fence \/ Conflict \/ SelectNext \/ Install \/ Activate \/ DisposeSource
    \/ \E a \in Ordinals : Admit(a) \/ Write(a) \/ Finish(a) \/ Prepare(a) \/ Close(a) \/ Dispose(a)
    \/ \E r \in Ordinals : Route(r)

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(Fence)
    /\ WF_vars(SelectNext)
    /\ WF_vars(Install)
    /\ WF_vars(Activate)
    /\ \A a \in Ordinals :
        /\ WF_vars(Admit(a)) /\ WF_vars(Write(a)) /\ WF_vars(Finish(a))
        /\ WF_vars(Prepare(a)) /\ WF_vars(Close(a))
    /\ \A r \in Ordinals : WF_vars(Route(r))

TypeOK ==
    /\ selected \in Ordinals /\ installed \in Ordinals /\ closed \subseteq Ordinals
    /\ receipts \subseteq Ordinals /\ active \in 0..Max

SingleWriter == ~(sourceOpen /\ destinationOpen)
\* The routed destination is the exact selected, prepared and intact copy.
RoutedIsSelected ==
    route = "destination" =>
        /\ active = selected /\ active = installed
        /\ stage[active] = "prepared" /\ objects[active] = "good"
\* A receipt handed to a placement catalog stays the one the source selects.
ReceiptsStaySelected == \A r \in receipts : r = selected /\ r \notin closed
SelectedNeverErased == objects[selected] # "erased" /\ objects[installed] # "erased"
\* Disposal follows the drain, so no admitted write lands in an emptied generation.
ErasedStaysEmpty == \A a \in Ordinals : objects[a] # "late"
SourceRetainedThroughSelection == ~sourceMedia => route = "destination"
EventuallyActive == <>destinationOpen
=============================================================================
