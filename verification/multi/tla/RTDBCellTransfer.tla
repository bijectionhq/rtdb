-------------------------- MODULE RTDBCellTransfer --------------------------
CONSTANT Mode

VARIABLES sourceOpen, destinationOpen, sourceMedia, destinationObjects,
          destinationJournal, framedCheckpoint, ready, route

vars == <<sourceOpen, destinationOpen, sourceMedia, destinationObjects,
          destinationJournal, framedCheckpoint, ready, route>>

Init ==
    /\ sourceOpen = TRUE
    /\ destinationOpen = FALSE
    /\ sourceMedia = TRUE
    /\ destinationObjects = FALSE
    /\ destinationJournal = FALSE
    /\ framedCheckpoint = FALSE
    /\ ready = FALSE
    /\ route = "source"

Fence ==
    /\ sourceOpen
    /\ sourceOpen' = FALSE
    /\ UNCHANGED <<destinationOpen, sourceMedia, destinationObjects,
                   destinationJournal, framedCheckpoint, ready, route>>

StageObjects ==
    /\ (~sourceOpen \/ Mode = "allow_two_writers")
    /\ sourceMedia
    /\ ~destinationObjects
    /\ destinationObjects' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationJournal, framedCheckpoint, ready, route>>

StageJournal ==
    /\ (~sourceOpen \/ Mode = "allow_two_writers")
    /\ sourceMedia
    /\ ~destinationJournal
    /\ destinationJournal' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, framedCheckpoint, ready, route>>

QualifyCheckpoint ==
    /\ destinationObjects
    /\ ~framedCheckpoint
    /\ framedCheckpoint' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, destinationJournal, ready, route>>

RecordReady ==
    /\ ~ready
    /\ destinationObjects
    /\ (destinationJournal \/ Mode = "ignore_journal")
    /\ (framedCheckpoint \/ Mode = "skip_frame_validation")
    /\ ready' = TRUE
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, destinationJournal, framedCheckpoint, route>>

SelectDestination ==
    /\ route = "source"
    /\ (ready \/ Mode = "early_route")
    /\ route' = "destination"
    /\ UNCHANGED <<sourceOpen, destinationOpen, sourceMedia, destinationObjects, destinationJournal, framedCheckpoint, ready>>

Activate ==
    /\ route = "destination"
    /\ ready
    /\ ~destinationOpen
    /\ destinationOpen' = TRUE
    /\ UNCHANGED <<sourceOpen, sourceMedia, destinationObjects, destinationJournal, framedCheckpoint, ready, route>>

DisposeSource ==
    /\ sourceMedia
    /\ ~sourceOpen
    /\ (route = "destination" \/ Mode = "early_disposal")
    /\ sourceMedia' = FALSE
    /\ destinationObjects' = IF Mode = "reuse_identity" THEN FALSE ELSE destinationObjects
    /\ UNCHANGED <<sourceOpen, destinationOpen, destinationJournal, framedCheckpoint, ready, route>>

Next == Fence \/ StageObjects \/ StageJournal \/ QualifyCheckpoint \/ RecordReady \/ SelectDestination \/ Activate \/ DisposeSource
Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(Fence)
    /\ WF_vars(StageObjects)
    /\ WF_vars(StageJournal)
    /\ WF_vars(QualifyCheckpoint)
    /\ WF_vars(RecordReady)
    /\ WF_vars(SelectDestination)
    /\ WF_vars(Activate)

SingleWriter == ~(sourceOpen /\ destinationOpen)
RoutedCopyDurable == route = "destination" => destinationObjects /\ destinationJournal
RoutedCheckpointFramed == route = "destination" => framedCheckpoint
SourceRetainedThroughSelection == route = "source" => sourceMedia
EventuallyActive == <>destinationOpen
=============================================================================
