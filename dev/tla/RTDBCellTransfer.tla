-------------------------- MODULE RTDBCellTransfer --------------------------
CONSTANT Mode

VARIABLES sourceOpen, destinationOpen, sourceMedia, destinationObjects,
          destinationJournal, ready, route

vars == <<sourceOpen, destinationOpen, sourceMedia, destinationObjects,
          destinationJournal, ready, route>>

Init ==
    /\ sourceOpen = TRUE
    /\ destinationOpen = FALSE
    /\ sourceMedia = TRUE
    /\ destinationObjects = FALSE
    /\ destinationJournal = FALSE
    /\ ready = FALSE
    /\ route = "source"

Fence ==
    /\ sourceOpen
    /\ sourceOpen' = FALSE
    /\ UNCHANGED <<destinationOpen, sourceMedia, destinationObjects,
                   destinationJournal, ready, route>>

StageObjects ==
    /\ (~sourceOpen \/ Mode = "allow_two_writers")
    /\ sourceMedia
    /\ ~destinationObjects
    /\ destinationObjects' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationJournal, ready, route>>

StageJournal ==
    /\ (~sourceOpen \/ Mode = "allow_two_writers")
    /\ sourceMedia
    /\ ~destinationJournal
    /\ destinationJournal' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, ready, route>>

RecordReady ==
    /\ ~ready
    /\ destinationObjects
    /\ (destinationJournal \/ Mode = "ignore_journal")
    /\ ready' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, destinationJournal, route>>

SelectDestination ==
    /\ route = "source"
    /\ (ready \/ Mode = "early_route")
    /\ route' = "destination"
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, destinationJournal, ready>>

Activate ==
    /\ route = "destination"
    /\ ready
    /\ ~destinationOpen
    /\ destinationOpen' = TRUE
    /\ UNCHANGED <<sourceOpen, sourceMedia, destinationObjects, destinationJournal, ready, route>>

DisposeSource ==
    /\ sourceMedia
    /\ ~sourceOpen
    /\ (route = "destination" \/ Mode = "early_disposal")
    /\ sourceMedia' = FALSE
    /\ destinationObjects' = IF Mode = "reuse_identity" THEN FALSE ELSE destinationObjects
    /\ UNCHANGED <<sourceOpen, destinationOpen, destinationJournal, ready, route>>

Next == Fence \/ StageObjects \/ StageJournal \/ RecordReady \/ SelectDestination \/ Activate \/ DisposeSource
Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(Fence)
    /\ WF_vars(StageObjects)
    /\ WF_vars(StageJournal)
    /\ WF_vars(RecordReady)
    /\ WF_vars(SelectDestination)
    /\ WF_vars(Activate)

SingleWriter == ~(sourceOpen /\ destinationOpen)
RoutedCopyDurable == route = "destination" => destinationObjects /\ destinationJournal
SourceRetainedThroughSelection == route = "source" => sourceMedia
EventuallyActive == <>destinationOpen
=============================================================================
