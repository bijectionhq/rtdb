------------------------ MODULE CatalogCutPublication ------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    Mutation

Idle == 2

VARIABLES
    \* @type: Int;
    received,
    \* @type: Int;
    installed,
    \* @type: Int;
    transactionHead,
    \* @type: Int;
    floor,
    \* @type: Int;
    capture,
    \* @type: Int;
    stamp,
    \* @type: Str;
    readState,
    \* @type: Bool;
    plan,
    \* @type: Bool;
    present
vars == <<received, installed, transactionHead, floor, capture, stamp, readState, plan, present>>

Init ==
    /\ received = 0
    /\ installed = 0
    /\ transactionHead = 0
    /\ floor = 0
    /\ capture = Idle
    /\ stamp = Idle
    /\ readState = "idle"
    /\ plan = FALSE
    /\ present = TRUE

Receive ==
    /\ received = 0
    /\ received' = 1
    /\ UNCHANGED <<installed, transactionHead, floor, capture, stamp, readState, plan, present>>

Install ==
    /\ received = 1
    /\ installed = 0
    /\ installed' = 1
    /\ UNCHANGED <<received, transactionHead, floor, capture, stamp, readState, plan, present>>

CompleteTransaction ==
    /\ transactionHead = 0
    /\ transactionHead' = 1
    /\ UNCHANGED <<received, installed, floor, capture, stamp, readState, plan, present>>

PublishFloor ==
    /\ floor = 0
    /\ floor' = 1
    /\ UNCHANGED <<received, installed, transactionHead, capture, stamp, readState, plan, present>>

Capture ==
    /\ readState = "idle"
    /\ capture' = installed
    /\ stamp' = CASE Mutation = "remote_status" -> received
                   [] Mutation = "transaction_basis" -> transactionHead
                   [] OTHER -> installed
    /\ readState' = "captured"
    /\ UNCHANGED <<received, installed, transactionHead, floor, plan, present>>

CandidateStamp == IF Mutation = "sample_late" THEN installed ELSE stamp

Admit ==
    /\ readState = "captured"
    /\ CandidateStamp >= floor
    /\ stamp' = CandidateStamp
    /\ readState' = "admitted"
    /\ UNCHANGED <<received, installed, transactionHead, floor, capture, plan, present>>

Refuse ==
    /\ readState = "captured"
    /\ CandidateStamp < floor
    /\ readState' = "refused"
    /\ UNCHANGED <<received, installed, transactionHead, floor, capture, stamp, plan, present>>

Finish ==
    /\ readState = "admitted"
    /\ readState' = "finished"
    /\ UNCHANGED <<received, installed, transactionHead, floor, capture, stamp, plan, present>>

Authorize ==
    /\ floor = 1
    /\ present
    /\ ~plan
    /\ readState # "admitted" \/ stamp = 1
    /\ plan' = TRUE
    /\ UNCHANGED <<received, installed, transactionHead, floor, capture, stamp, readState, present>>

Delete ==
    /\ plan
    /\ present
    /\ present' = FALSE
    /\ UNCHANGED <<received, installed, transactionHead, floor, capture, stamp, readState, plan>>

Next == Receive \/ Install \/ CompleteTransaction \/ PublishFloor \/ Capture
        \/ Admit \/ Refuse \/ Finish \/ Authorize \/ Delete
Spec == Init /\ [][Next]_vars

Readable == (readState = "admitted" /\ capture = 0) => present
NoRefusedStale == readState # "refused"
NoNewReaderAfterDeletion == ~(readState = "admitted" /\ capture = 1 /\ ~present)
=============================================================================
