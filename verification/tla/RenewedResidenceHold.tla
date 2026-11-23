----------------------- MODULE RenewedResidenceHold -----------------------
EXTENDS Integers

CONSTANT Mutation
VARIABLES now, ticket, deadline, renewal, publishedRenewal, seenTicket, seenAge, requestUntil, stopped, present

Vars == <<now, ticket, deadline, renewal, publishedRenewal, seenTicket, seenAge, requestUntil, stopped, present>>
None == -1
Timeout == 3
Margin == 1
IoBound == IF Mutation = "ignore_io_bound" THEN 4 ELSE 1
LastRenewal == 4
ClockLimit == LastRenewal + 2 * Timeout

Init ==
    /\ now = 0 /\ ticket = 0 /\ deadline = Timeout - Margin /\ renewal = None
    /\ publishedRenewal = None
    /\ seenTicket = None /\ seenAge = None /\ requestUntil = None
    /\ stopped = FALSE /\ present = TRUE

Tick ==
    /\ now < ClockLimit \/ seenAge \in 0..(Timeout - 1)
    /\ now' = IF now < ClockLimit THEN now + 1 ELSE now
    /\ seenAge' = IF seenAge \in 0..(Timeout - 1) THEN seenAge + 1 ELSE seenAge
    /\ UNCHANGED <<ticket, deadline, renewal, publishedRenewal, seenTicket, requestUntil, stopped, present>>

BeginRenewal ==
    /\ ~stopped /\ renewal = None /\ publishedRenewal = None /\ now < deadline /\ now > ticket /\ now <= LastRenewal
    /\ renewal' = now
    /\ UNCHANGED <<now, ticket, deadline, publishedRenewal, seenTicket, seenAge, requestUntil, stopped, present>>

PublishRenewal ==
    /\ renewal # None /\ ticket # None /\ renewal > ticket
    /\ ticket' = renewal
    /\ publishedRenewal' = renewal /\ renewal' = None
    /\ UNCHANGED <<now, deadline, seenTicket, seenAge, requestUntil, stopped, present>>

AcknowledgeRenewal ==
    /\ publishedRenewal # None
    /\ deadline' = (IF Mutation = "deadline_at_reply" THEN now ELSE publishedRenewal) + Timeout - Margin
    /\ publishedRenewal' = None
    /\ UNCHANGED <<now, ticket, renewal, seenTicket, seenAge, requestUntil, stopped, present>>

Abandon ==
    /\ ~stopped /\ stopped' = TRUE /\ renewal' = None /\ publishedRenewal' = None
    /\ UNCHANGED <<now, ticket, deadline, seenTicket, seenAge, requestUntil, present>>

Observe ==
    /\ ticket # None /\ ticket # seenTicket
    /\ seenTicket' = ticket /\ seenAge' = 0
    /\ UNCHANGED <<now, ticket, deadline, renewal, publishedRenewal, requestUntil, stopped, present>>

Expire ==
    /\ ticket # None /\ seenTicket # None
    /\ ticket = seenTicket \/ Mutation = "reuse_observation"
    /\ seenAge = Timeout \/ Mutation = "expire_on_first_observation"
    /\ ticket' = None
    /\ UNCHANGED <<now, deadline, renewal, publishedRenewal, seenTicket, seenAge, requestUntil, stopped, present>>

StartIO ==
    /\ ~stopped /\ requestUntil = None
    /\ now < deadline \/ Mutation = "omit_io_fence"
    /\ requestUntil' = now + IoBound
    /\ UNCHANGED <<now, ticket, deadline, renewal, publishedRenewal, seenTicket, seenAge, stopped, present>>

EndIO ==
    /\ requestUntil # None /\ now >= requestUntil /\ requestUntil' = None
    /\ UNCHANGED <<now, ticket, deadline, renewal, publishedRenewal, seenTicket, seenAge, stopped, present>>

Collect ==
    /\ ticket = None /\ present /\ present' = FALSE
    /\ UNCHANGED <<now, ticket, deadline, renewal, publishedRenewal, seenTicket, seenAge, requestUntil, stopped>>

Next == Tick \/ BeginRenewal \/ PublishRenewal \/ AcknowledgeRenewal \/ Abandon \/ Observe \/ Expire \/ StartIO \/ EndIO \/ Collect

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Tick) /\ WF_Vars(Observe) /\ (IF Mutation = "unfair_expiry" THEN TRUE ELSE WF_Vars(Expire))
        /\ WF_Vars(Collect)
Entitled == (~stopped /\ now < deadline) \/ (requestUntil # None /\ now < requestUntil)
EntitlementPreservesBytes == Entitled => present
AbandonEventuallyCollects == stopped ~> ~present
NeverCollectsAfterRenewal == ~(ticket = None /\ ~present /\ deadline > Timeout - Margin)
=============================================================================
