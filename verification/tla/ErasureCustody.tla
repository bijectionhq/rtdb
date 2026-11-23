------------------------------ MODULE ErasureCustody ------------------------------
EXTENDS Integers, FiniteSets

CONSTANT Mutation

SourceKeys == {"source", "source_copy"}
DestinationKeys == {"destination"}
Keys == SourceKeys \cup DestinationKeys
Deadline == 2

VARIABLES state, time, reachable, copy, fork, export, oldView, successor

Vars == <<state, time, reachable, copy, fork, export, oldView, successor>>

Init ==
    /\ state = "open" /\ time = 0 /\ reachable = {"source"}
    /\ copy = "none" /\ fork = FALSE /\ export = "none"
    /\ oldView = FALSE /\ successor = FALSE

StartCopy ==
    /\ state = "open" /\ copy = "none"
    /\ copy' = "unresolved"
    /\ UNCHANGED <<state, time, reachable, fork, export, oldView, successor>>

ResolveCopy ==
    /\ copy = "unresolved"
    /\ \E installed \in BOOLEAN :
        /\ copy' = IF installed THEN "installed" ELSE "absent"
        /\ reachable' = IF installed THEN reachable \cup {"source_copy"} ELSE reachable
    /\ UNCHANGED <<state, time, fork, export, oldView, successor>>

PublishFork ==
    /\ state = "open" /\ ~fork /\ fork' = TRUE
    /\ UNCHANGED <<state, time, reachable, copy, export, oldView, successor>>

PublishExport ==
    /\ state = "open" /\ export = "none"
    /\ export' = IF Mutation = "share_export_key" THEN "source" ELSE "destination"
    /\ reachable' = reachable \cup {export'}
    /\ UNCHANGED <<state, time, copy, fork, oldView, successor>>

HoldOldView ==
    /\ state = "open" /\ ~oldView /\ oldView' = TRUE
    /\ UNCHANGED <<state, time, reachable, copy, fork, export, successor>>

ConfirmSuccessor ==
    /\ state = "open" /\ ~successor /\ successor' = TRUE
    /\ UNCHANGED <<state, time, reachable, copy, fork, export, oldView>>

CommitErase ==
    /\ state = "open"
    /\ successor \/ Mutation = "skip_successor"
    /\ copy # "unresolved" \/ Mutation = "accept_unknown_copy"
    /\ state' = "committed"
    /\ UNCHANGED <<time, reachable, copy, fork, export, oldView, successor>>

Advance ==
    /\ state = "committed" /\ time < Deadline /\ Mutation # "no_expiry"
    /\ time' = time + 1
    /\ reachable' = IF time' = Deadline
                    THEN IF Mutation = "keep_copied_key" THEN reachable \ {"source"}
                         ELSE reachable \ SourceKeys
                    ELSE reachable
    /\ UNCHANGED <<state, copy, fork, export, oldView, successor>>

Next == StartCopy \/ ResolveCopy \/ PublishFork \/ PublishExport \/ HoldOldView
        \/ ConfirmSuccessor \/ CommitErase \/ Advance

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Advance)

FreshRootKeepsSurvivors == state = "committed" => successor
NoLateSourceAccess == time = Deadline => reachable \cap SourceKeys = {}
IndependentExportReadable == export # "none" => export \in reachable
ExpiryProgress == (state = "committed") ~> (time = Deadline)
NoUnresolvedKeyInstall == state = "committed" => copy # "unresolved"
HeldViewReadableUntilDeadline ==
    oldView /\ time < Deadline => "source" \in reachable
NeverCompletesWithBothKindsOfFork ==
    ~(time = Deadline /\ fork /\ export = "destination" /\ oldView /\ copy = "installed")

===============================================================================
