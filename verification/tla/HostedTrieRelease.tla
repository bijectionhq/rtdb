----------------------------- MODULE HostedTrieRelease -----------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Bool;
    pending,
    \* @type: Bool;
    installed,
    \* @type: Bool;
    receipt,
    \* @type: Bool;
    floor,
    \* @type: Bool;
    reader,
    \* @type: Bool;
    failed,
    \* @type: Bool;
    recovered,
    \* @type: Set(Str);
    released

State == <<pending, installed, receipt, floor, reader, failed, recovered, released>>
Files == {"data", "metadata"}

Init ==
    /\ pending = FALSE /\ installed = FALSE /\ receipt = FALSE
    /\ floor = FALSE /\ reader = FALSE /\ failed = FALSE /\ recovered = FALSE
    /\ released = {}

Stage ==
    /\ ~pending /\ ~installed /\ pending' = TRUE
    /\ UNCHANGED <<installed, receipt, floor, reader, failed, recovered, released>>

Install ==
    /\ pending /\ ~installed /\ installed' = TRUE
    /\ UNCHANGED <<pending, receipt, floor, reader, failed, recovered, released>>

RecordReceipt ==
    /\ pending /\ installed /\ ~receipt /\ receipt' = TRUE
    /\ UNCHANGED <<pending, installed, floor, reader, failed, recovered, released>>

AcquireReader ==
    /\ ~reader /\ (~floor \/ Mutation = "admit_below_floor") /\ reader' = TRUE
    /\ UNCHANGED <<pending, installed, receipt, floor, failed, recovered, released>>

CloseReader ==
    /\ reader /\ reader' = FALSE
    /\ UNCHANGED <<pending, installed, receipt, floor, failed, recovered, released>>

AdvanceFloor ==
    /\ pending /\ receipt /\ ~floor /\ ~reader /\ floor' = TRUE
    /\ UNCHANGED <<pending, installed, receipt, reader, failed, recovered, released>>

Release(file) ==
    /\ pending /\ file \notin released /\ (~failed \/ recovered)
    /\ receipt \/ Mutation = "release_before_receipt"
    /\ floor \/ Mutation \in {"release_before_receipt", "release_before_floor"}
    /\ released' = released \cup {file}
    /\ UNCHANGED <<pending, installed, receipt, floor, reader, failed, recovered>>

InterruptRelease ==
    /\ pending /\ released \in {{"data"}, {"metadata"}} /\ ~failed /\ failed' = TRUE
    /\ UNCHANGED <<pending, installed, receipt, floor, reader, recovered, released>>

Recover ==
    /\ failed /\ ~recovered /\ recovered' = TRUE
    /\ UNCHANGED <<pending, installed, receipt, floor, reader, failed, released>>

ForgetPlan ==
    /\ pending /\ receipt /\ floor
    /\ released = Files \/ (Mutation = "forget_after_one" /\ released # {})
    /\ pending' = FALSE
    /\ UNCHANGED <<installed, receipt, floor, reader, failed, recovered, released>>

Next == Stage \/ Install \/ RecordReceipt \/ AcquireReader \/ CloseReader \/ AdvanceFloor
        \/ (\E file \in Files: Release(file)) \/ InterruptRelease \/ Recover \/ ForgetPlan

Spec == Init /\ [][Next]_State
ReleaseHasReceipt == released = {} \/ receipt
ReleaseHasFloor == released = {} \/ floor
ReleaseExcludesOldReader == released = {} \/ ~reader
PlanCoversUnreleased == ~receipt \/ pending \/ released = Files
NeverRecoversPartialRelease == ~(recovered /\ released = Files /\ ~pending)

Safety == ReleaseHasReceipt /\ ReleaseHasFloor /\ ReleaseExcludesOldReader /\ PlanCoversUnreleased
====================================================================================
