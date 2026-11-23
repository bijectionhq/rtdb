------------------------ MODULE ReferenceDisposalPublication ------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    generation,
    \* @type: Bool;
    started,
    \* @type: Bool;
    closed,
    \* @type: Bool;
    active,
    \* @type: Bool;
    present,
    \* @type: Bool;
    receipt,
    \* @type: Str;
    inventory,
    \* @type: Bool;
    childCopy,
    \* @type: Bool;
    grandchildCopy,
    \* @type: Str;
    fork,
    \* @type: Int;
    preparedAt,
    \* @type: Bool;
    refersToSource

\* @type: <<Int, Bool, Bool, Bool, Bool, Bool, Str, Bool, Bool, Str, Int, Bool>>;
vars == <<generation, started, closed, active, present, receipt, inventory,
          childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Init ==
    /\ generation = 0 /\ started = FALSE /\ closed = FALSE /\ active = FALSE /\ present = TRUE /\ receipt = FALSE
    /\ inventory = "unscanned" /\ childCopy = FALSE /\ grandchildCopy = FALSE
    /\ fork = "waiting" /\ preparedAt = 0 /\ refersToSource = FALSE

Prepare ==
    /\ ~closed /\ fork = "waiting"
    /\ fork' = "prepared" /\ preparedAt' = generation /\ refersToSource' = ~childCopy
    /\ UNCHANGED <<generation, started, closed, active, present, receipt, inventory, childCopy, grandchildCopy>>

Publish ==
    /\ ~closed /\ fork = "prepared"
    /\ (preparedAt = generation \/ Mutation = "publish_stale")
    /\ fork' = "published"
    /\ UNCHANGED <<generation, started, closed, active, present, receipt, inventory, childCopy, grandchildCopy, preparedAt, refersToSource>>

Reprepare ==
    /\ ~closed /\ fork = "prepared" /\ preparedAt # generation
    /\ fork' = "waiting"
    /\ UNCHANGED <<generation, started, closed, active, present, receipt, inventory, childCopy, grandchildCopy, preparedAt, refersToSource>>

Close ==
    /\ ~started
    /\ started' = TRUE /\ closed' = TRUE /\ active' = TRUE /\ generation' = generation + 1
    /\ UNCHANGED <<present, receipt, inventory, childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Restart ==
    /\ closed /\ active
    /\ active' = FALSE /\ closed' = (Mutation # "forget_gate_on_restart")
    /\ inventory' = "unscanned"
    /\ UNCHANGED <<generation, started, present, receipt, childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Resume ==
    /\ closed /\ ~active
    /\ active' = TRUE
    /\ UNCHANGED <<generation, started, closed, present, receipt, inventory, childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Scan ==
    /\ ((closed /\ active) \/ (Mutation = "scan_before_close" /\ ~started))
    /\ present /\ inventory = "unscanned"
    /\ inventory' = IF fork = "published" /\ refersToSource THEN "both" ELSE "child"
    /\ UNCHANGED <<generation, started, closed, active, present, receipt, childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Transfer ==
    /\ closed /\ active /\ present /\ inventory # "unscanned"
    /\ childCopy' = TRUE /\ grandchildCopy' = (inventory = "both")
    /\ UNCHANGED <<generation, started, closed, active, present, receipt, inventory, fork, preparedAt, refersToSource>>

Erase ==
    /\ closed /\ active /\ present /\ inventory # "unscanned"
    /\ ((childCopy /\ (inventory # "both" \/ grandchildCopy)) \/ Mutation = "skip_copy")
    /\ present' = FALSE
    /\ UNCHANGED <<generation, started, closed, active, receipt, inventory, childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Settle ==
    /\ closed /\ active /\ (~present \/ Mutation = "early_reopen")
    /\ closed' = FALSE /\ receipt' = TRUE /\ active' = FALSE
    /\ UNCHANGED <<generation, started, present, inventory, childCopy, grandchildCopy, fork, preparedAt, refersToSource>>

Next == Prepare \/ Publish \/ Reprepare \/ Close \/ Restart \/ Resume \/ Scan \/ Transfer \/ Erase \/ Settle
Spec == Init /\ [][Next]_vars

ForksReadable == (present \/ childCopy) /\ (fork # "published" \/ ~refersToSource \/ present \/ grandchildCopy)
ReceiptFollowsErasure == receipt => ~present
DurableGate == started /\ ~receipt => closed
Safe == ForksReadable /\ ReceiptFollowsErasure /\ DurableGate
NeverPublishesAfterSettlement == ~(receipt /\ fork = "published" /\ ~refersToSource)
=============================================================================
