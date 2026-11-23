-------------------------- MODULE RTDBServingFailure --------------------------
EXTENDS Integers

CONSTANTS FailureLimit, ProbePgwire, FenceEndpoints, RetryDial
VARIABLES endpoints, pgwireUp, sample, evidence, streak, confirmed, holder, client

Vars == <<endpoints, pgwireUp, sample, evidence, streak, confirmed, holder, client>>

Init ==
    /\ endpoints = <<0, 0>>
    /\ pgwireUp = FALSE
    /\ sample = [endpoints |-> <<0, 0>>, failed |-> FALSE, ready |-> FALSE]
    /\ evidence = endpoints
    /\ streak = 0
    /\ confirmed = FALSE
    /\ holder = "original"
    /\ client = "waiting"

StartProbe ==
    /\ ~confirmed /\ ~sample.ready
    /\ sample' = [endpoints |-> endpoints, failed |-> (ProbePgwire /\ ~pgwireUp), ready |-> TRUE]
    /\ UNCHANGED <<endpoints, pgwireUp, evidence, streak, confirmed, holder, client>>

FinishProbe ==
    /\ ~confirmed /\ sample.ready
    /\ sample' = [sample EXCEPT !.ready = FALSE]
    /\ evidence' = sample.endpoints
    /\ streak' = IF ~sample.failed THEN 0
                  ELSE IF sample.endpoints # evidence THEN 1
                  ELSE IF streak < FailureLimit THEN streak + 1 ELSE streak
    /\ UNCHANGED <<endpoints, pgwireUp, confirmed, holder, client>>

ReplaceEndpoint ==
    /\ ~confirmed /\ endpoints = <<0, 0>>
    /\ endpoints' \in {<<0, 1>>, <<1, 0>>}
    /\ pgwireUp' = (endpoints'[2] # endpoints[2])
    /\ UNCHANGED <<sample, evidence, streak, confirmed, holder, client>>

Confirm ==
    /\ ~confirmed /\ streak >= FailureLimit
    /\ (~FenceEndpoints \/ evidence = endpoints)
    /\ confirmed' = TRUE
    /\ UNCHANGED <<endpoints, pgwireUp, sample, evidence, streak, holder, client>>

Claim ==
    /\ confirmed /\ holder = "original"
    /\ holder' = "replacement"
    /\ UNCHANGED <<endpoints, pgwireUp, sample, evidence, streak, confirmed, client>>

Dial ==
    /\ client = "waiting"
    /\ client' = IF holder = "replacement" \/ (pgwireUp /\ ~confirmed) THEN "connected"
                  ELSE IF RetryDial THEN "waiting" ELSE "gave-up"
    /\ UNCHANGED <<endpoints, pgwireUp, sample, evidence, streak, confirmed, holder>>

Next == StartProbe \/ FinishProbe \/ ReplaceEndpoint \/ Confirm \/ Claim \/ Dial
Spec == Init /\ [][Next]_Vars
        /\ WF_Vars(StartProbe) /\ WF_Vars(FinishProbe) /\ WF_Vars(Confirm)
        /\ WF_Vars(Claim) /\ WF_Vars(Dial)

EvidenceMatchesCurrentEndpoints == confirmed => evidence = endpoints
StartupEventuallyConnects == <>(client = "connected")

=============================================================================
