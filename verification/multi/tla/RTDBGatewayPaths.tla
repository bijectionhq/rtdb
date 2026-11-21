---------------------------- MODULE RTDBGatewayPaths ----------------------------
EXTENDS Naturals

CONSTANTS CheckWireReply, AllowRelay, RequireEquivalentAuth, IgnoreCrowdedSample
VARIABLES scenario, signed, addressRules, failures, holder, connected

Vars == <<scenario, signed, addressRules, failures, holder, connected>>
Scenarios == {"gateway-isolated", "writer-stalled", "startup-overflow"}
LocalPgwireReady == scenario = "gateway-isolated"
ControllerPgwireReady == LocalPgwireReady \/ ~CheckWireReply
                         \/ (scenario = "startup-overflow" /\ IgnoreCrowdedSample)

Init ==
    /\ scenario \in Scenarios
    /\ signed \in BOOLEAN
    /\ addressRules \in BOOLEAN
    /\ failures = 0
    /\ holder = "original"
    /\ connected = "pending"

Probe ==
    /\ holder = "original"
    /\ failures < 2
    /\ failures' = IF ControllerPgwireReady THEN 0 ELSE failures + 1
    /\ UNCHANGED <<scenario, signed, addressRules, holder, connected>>

Promote ==
    /\ holder = "original"
    /\ failures = 2
    /\ holder' = "successor"
    /\ UNCHANGED <<scenario, signed, addressRules, failures, connected>>

Connect ==
    /\ connected = "pending"
    /\ IF holder = "successor" \/ scenario = "startup-overflow"
       THEN connected' = "direct"
       ELSE /\ AllowRelay /\ LocalPgwireReady /\ signed
            /\ (~RequireEquivalentAuth \/ ~addressRules)
            /\ connected' = "relayed"
    /\ UNCHANGED <<scenario, signed, addressRules, failures, holder>>

Spec == Init /\ [][Probe \/ Promote \/ Connect]_Vars
             /\ WF_Vars(Probe) /\ WF_Vars(Promote) /\ WF_Vars(Connect)

IsolatedGatewayKeepsWriter == scenario = "gateway-isolated" => holder = "original"
CrowdedProbeKeepsWriter == scenario = "startup-overflow" => holder = "original"
RelayPreservesAuthentication == connected = "relayed" => signed /\ ~addressRules
EligibleStartupCompletes ==
    (scenario = "writer-stalled" \/ (signed /\ ~addressRules)) ~> (connected # "pending")

=======================================================================================
