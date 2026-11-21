--------------------------- MODULE ResidenceRetirement ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Int;
    head,
    \* @type: Int;
    intent,
    \* @type: Int;
    accepted,
    \* @type: Int;
    receipt,
    \* @type: Int;
    floor,
    \* @type: Int;
    checkpoint,
    \* @type: Int;
    reader,
    \* @type: Int;
    io,
    \* @type: Int;
    recovery,
    \* @type: Int;
    fork,
    \* @type: Set(Str);
    physical,
    \* @type: Set(Str);
    grants

Vars == <<head, intent, accepted, receipt, floor, checkpoint, reader, io, recovery, fork, physical, grants>>
None == -1
Objects == {"old", "shared", "new", "replacement"}
Requires(generation) ==
    CASE generation = 0 -> {"old", "shared"}
      [] generation = 1 -> {"shared", "new"}
      [] generation = 2 -> {"new", "replacement"}
      [] OTHER -> {}
RetiredAt(object) ==
    CASE object = "old" -> 1
      [] object = "shared" -> IF Mutation = "retire_shared_early" THEN 1 ELSE 2
      [] OTHER -> 3
Key(object) == IF object = "replacement" THEN "old" ELSE object
ReaderCuts == {reader, io, recovery, checkpoint, fork} \ {None}
ProtectedCuts ==
    ({reader}
      \cup (IF Mutation = "forget_provider_io" THEN {} ELSE {io})
      \cup (IF Mutation = "forget_recovery" THEN {} ELSE {recovery})
      \cup (IF Mutation = "forget_checkpoint" THEN {} ELSE {checkpoint})
      \cup (IF Mutation = "forget_fork" THEN {} ELSE {fork})) \ {None}

Init ==
    /\ head = 0 /\ intent = None /\ accepted = 0 /\ receipt = 0 /\ floor = 0
    /\ checkpoint = 0 /\ reader = None /\ io = None /\ recovery = None /\ fork = None
    /\ physical = Objects /\ grants = {}

Stage ==
    /\ head < 2 /\ intent = None /\ accepted = head
    /\ intent' = head + 1 /\ accepted' = intent'
    /\ UNCHANGED <<head, receipt, floor, checkpoint, reader, io, recovery, fork, physical, grants>>

Install ==
    /\ intent = head + 1 /\ Requires(intent) \subseteq physical
    /\ head' = intent
    /\ UNCHANGED <<intent, accepted, receipt, floor, checkpoint, reader, io, recovery, fork, physical, grants>>

Acknowledge ==
    /\ intent # None /\ receipt < intent
    /\ head = intent \/ Mutation = "reply_before_install"
    /\ receipt' = intent
    /\ UNCHANGED <<head, intent, accepted, floor, checkpoint, reader, io, recovery, fork, physical, grants>>

Certify ==
    /\ intent = receipt /\ receipt > floor
    /\ floor' = receipt /\ intent' = None
    /\ UNCHANGED <<head, accepted, receipt, checkpoint, reader, io, recovery, fork, physical, grants>>

AdvanceCheckpoint ==
    /\ checkpoint < head
    /\ checkpoint' = head
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, reader, io, recovery, fork, physical, grants>>

OpenReader ==
    /\ reader = None
    /\ \E cut \in 0..head :
        /\ cut >= floor \/ Mutation = "ignore_floor"
        /\ reader' = cut
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, io, recovery, fork, physical, grants>>

CloseReader ==
    /\ reader # None /\ reader' = None
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, io, recovery, fork, physical, grants>>

StartIO ==
    /\ reader # None /\ io = None /\ io' = reader
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, recovery, fork, physical, grants>>

FinishIO ==
    /\ io # None /\ io' = None
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, recovery, fork, physical, grants>>

OpenRecovery ==
    /\ recovery = None /\ recovery' = checkpoint
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, io, fork, physical, grants>>

CloseRecovery ==
    /\ recovery # None /\ recovery' = None
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, io, fork, physical, grants>>

PublishFork ==
    /\ fork = None /\ reader # None /\ fork' = reader
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, io, recovery, physical, grants>>

DropFork ==
    /\ fork # None /\ fork' = None
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, io, recovery, physical, grants>>

Crash ==
    /\ reader' = None /\ io' = None /\ recovery' = None
    /\ intent' = IF Mutation = "forget_intent" \/ (Mutation = "forget_installed_intent" /\ head = intent)
                 THEN None ELSE intent
    /\ UNCHANGED <<head, accepted, receipt, floor, checkpoint, fork, physical, grants>>

Authorize(object) ==
    /\ object \in physical \ grants /\ RetiredAt(object) <= floor
    /\ \A cut \in ProtectedCuts : cut >= RetiredAt(object)
    /\ grants' = grants \cup {object}
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, io, recovery, fork, physical>>

Erase(object) ==
    /\ object \in grants \cap physical
    /\ physical' = IF Mutation = "erase_by_key"
                   THEN {other \in physical : Key(other) # Key(object)}
                   ELSE physical \ {object}
    /\ UNCHANGED <<head, intent, accepted, receipt, floor, checkpoint, reader, io, recovery, fork, grants>>

Next == Stage \/ Install \/ Acknowledge \/ Certify \/ AdvanceCheckpoint \/ OpenReader \/ CloseReader
        \/ StartIO \/ FinishIO \/ OpenRecovery \/ CloseRecovery \/ PublishFork \/ DropFork \/ Crash
        \/ \E object \in Objects : Authorize(object) \/ Erase(object)

Spec == Init /\ [][Next]_Vars
CoveredIntent == accepted <= floor \/ intent = accepted
ReceiptCoversInstalled == receipt <= head
HeadReadable == Requires(head) \subseteq physical
RetainedRootsReadable == \A cut \in ReaderCuts : Requires(cut) \subseteq physical
Safety == CoveredIntent /\ ReceiptCoversInstalled /\ HeadReadable /\ RetainedRootsReadable
RetirementWitness == ~(head = 2 /\ physical = {"new", "replacement"})

=============================================================================
