-------------------------- MODULE CaptureScheduledExpiry --------------------------
EXTENDS Naturals, TLC

CONSTANT Mutation

VARIABLES now, due, fenced, stored, acknowledged, restarted, working, returned, lost

Vars == <<now, due, fenced, stored, acknowledged, restarted, working, returned, lost>>

Init ==
    /\ now = 0 /\ due = 2
    /\ fenced = FALSE /\ stored = FALSE /\ acknowledged = FALSE /\ restarted = FALSE
    /\ working = FALSE /\ returned = FALSE /\ lost = FALSE

Prepare ==
    /\ ~fenced /\ fenced' = TRUE
    /\ UNCHANGED <<now, due, stored, acknowledged, restarted, working, returned, lost>>

Arm ==
    /\ fenced /\ ~stored /\ stored' = TRUE /\ returned' = FALSE
    /\ UNCHANGED <<now, due, fenced, acknowledged, restarted, working, lost>>

LoseReply ==
    /\ stored /\ ~acknowledged /\ ~lost /\ lost' = TRUE
    /\ UNCHANGED <<now, due, fenced, stored, acknowledged, restarted, working, returned>>

Acknowledge ==
    /\ ~acknowledged /\ (stored \/ (fenced /\ Mutation = "ack_before_arm"))
    /\ acknowledged' = TRUE
    /\ UNCHANGED <<now, due, fenced, stored, restarted, working, returned, lost>>

Restart ==
    /\ ~restarted /\ restarted' = TRUE /\ working' = FALSE
    /\ stored' = IF Mutation = "forget_schedule" THEN FALSE ELSE stored
    /\ UNCHANGED <<now, due, fenced, acknowledged, returned, lost>>

RetryWithAnotherDue ==
    /\ Mutation = "change_due" /\ stored /\ due = 2 /\ due' = 3
    /\ UNCHANGED <<now, fenced, stored, acknowledged, restarted, working, returned, lost>>

Tick ==
    /\ now < 3 /\ now' = now + 1 /\ returned' = FALSE
    /\ UNCHANGED <<due, fenced, stored, acknowledged, restarted, working, lost>>

Invoke ==
    /\ ~working /\ (~stored \/ now < due) /\ working' = TRUE
    /\ UNCHANGED <<now, due, fenced, stored, acknowledged, restarted, returned, lost>>

Finish ==
    /\ working /\ working' = FALSE
    /\ returned' = (~stored \/ now < due \/ Mutation = "skip_completion_check")
    /\ UNCHANGED <<now, due, fenced, stored, acknowledged, restarted, lost>>

Next == Prepare \/ Arm \/ LoseReply \/ Acknowledge \/ Restart
     \/ RetryWithAnotherDue \/ Tick \/ Invoke \/ Finish

Spec == Init /\ [][Next]_Vars

AcknowledgedIsDurable == acknowledged => stored
DueIsImmutable == stored => due = 2
NoCryptoResultAtOrAfterExpiry == stored /\ now >= due => ~returned
NeverRecoversArmingAfterLostReply ==
    ~(lost /\ acknowledged /\ restarted /\ now > due /\ ~returned)

===============================================================================
