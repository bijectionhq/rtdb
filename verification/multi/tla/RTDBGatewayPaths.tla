---------------------------- MODULE RTDBGatewayPaths ----------------------------
EXTENDS Naturals

CONSTANTS CheckWireReply, AllowRelay, RequireEquivalentAuth, IgnoreCrowdedSample, PreferRelay
VARIABLES scenario, signed, addressRules, failures, holder, connected

Vars == <<scenario, signed, addressRules, failures, holder, connected>>
Scenarios == {"gateway-isolated", "writer-stalled", "startup-overflow", "isolated-and-crowded"}
Isolated == scenario \in {"gateway-isolated", "isolated-and-crowded"}
Overflow == scenario \in {"startup-overflow", "isolated-and-crowded"}
ControllerPgwireReady == scenario = "gateway-isolated" \/ ~CheckWireReply
                         \/ (Overflow /\ IgnoreCrowdedSample)

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
       ELSE /\ AllowRelay /\ Isolated /\ signed
            /\ (~RequireEquivalentAuth \/ ~addressRules)
            /\ (~Overflow \/ PreferRelay)
            /\ connected' = "relayed"
    /\ UNCHANGED <<scenario, signed, addressRules, failures, holder>>

Spec == Init /\ [][Probe \/ Promote \/ Connect]_Vars
             /\ WF_Vars(Probe) /\ WF_Vars(Promote) /\ WF_Vars(Connect)

IsolatedGatewayKeepsWriter == Isolated /\ ~Overflow => holder = "original"
CrowdedProbeKeepsWriter == Overflow => holder = "original"
RelayPreservesAuthentication == connected = "relayed" => signed /\ ~addressRules
EligibleStartupCompletes ==
    (scenario = "writer-stalled" \/ (signed /\ ~addressRules)) ~> (connected # "pending")

=======================================================================================
