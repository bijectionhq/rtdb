-------------------------- MODULE CatalogAdmissionFloor --------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation,
    \* @type: Int;
    MaxRevision
VARIABLES
    \* @type: Int;
    head,
    \* @type: Int;
    receipt,
    \* @type: Int;
    floor,
    \* @type: Int;
    held,
    \* @type: Int;
    revision,
    \* @type: Int;
    writer,
    \* @type: <<Int, Int>>;
    reading,
    \* @type: <<Int, Int, Int>>;
    advancing

Vars == <<head, receipt, floor, held, revision, writer, reading, advancing>>
Absent == -2
Opaque == -1
Cuts == 0..2
Allows(cut, proposed) == proposed = 0 \/ (cut \in Cuts /\ cut >= proposed)
ReaderClears(proposed) == held = Absent \/ Allows(held, proposed)

Init ==
    /\ head = 0 /\ receipt = 0 /\ floor = 0 /\ held = Absent
    /\ revision = 0 /\ writer = 0 /\ reading = <<Absent, 0>>
    /\ advancing = <<Absent, 0, 0>>

Install ==
    /\ head < 2 /\ head' = head + 1
    /\ UNCHANGED <<receipt, floor, held, revision, writer, reading, advancing>>

RecordReceipt ==
    /\ receipt < head
    /\ receipt' \in Cuts
    /\ receipt < receipt' /\ receipt' <= head
    /\ UNCHANGED <<head, floor, held, revision, writer, reading, advancing>>

ReadAdmission ==
    /\ reading[1] = Absent /\ held = Absent
    /\ \E captured \in {Opaque} \cup Cuts :
        /\ captured <= head
        /\ Allows(captured, floor) \/ Mutation = "ignore_floor"
        /\ reading' = <<captured, revision>>
    /\ UNCHANGED <<head, receipt, floor, held, revision, writer, advancing>>

CommitRead ==
    /\ reading[1] # Absent /\ held = Absent /\ revision < MaxRevision
    /\ reading[2] = revision \/ Mutation = "read_without_cas"
    /\ held' = reading[1] /\ revision' = revision + 1
    /\ reading' = <<Absent, 0>>
    /\ UNCHANGED <<head, receipt, floor, writer, advancing>>

ReleaseRead ==
    /\ held # Absent /\ revision < MaxRevision
    /\ held' = Absent /\ revision' = revision + 1
    /\ UNCHANGED <<head, receipt, floor, writer, reading, advancing>>

ProposeFloor ==
    /\ advancing[1] = Absent
    /\ \E cut \in Cuts :
        /\ floor < cut /\ cut <= receipt
        /\ ReaderClears(cut) \/ (Mutation = "ignore_opaque" /\ held = Opaque)
        /\ advancing' = <<cut, revision, writer>>
    /\ UNCHANGED <<head, receipt, floor, held, revision, writer, reading>>

CommitFloor ==
    /\ advancing[1] # Absent /\ revision < MaxRevision
    /\ advancing[2] = revision \/ Mutation = "floor_without_cas"
    /\ advancing[3] = writer
    /\ floor' = advancing[1] /\ revision' = revision + 1
    /\ advancing' = <<Absent, 0, 0>>
    /\ UNCHANGED <<head, receipt, held, writer, reading>>

ReplaceWriter ==
    /\ writer = 0 /\ revision < MaxRevision
    /\ writer' = 1 /\ revision' = revision + 1
    /\ UNCHANGED <<head, receipt, floor, held, reading, advancing>>

Retry ==
    /\ reading' = <<Absent, 0>> /\ advancing' = <<Absent, 0, 0>>
    /\ UNCHANGED <<head, receipt, floor, held, revision, writer>>

Next == Install \/ RecordReceipt \/ ReadAdmission \/ CommitRead \/ ReleaseRead
        \/ ProposeFloor \/ CommitFloor \/ ReplaceWriter \/ Retry
Spec == Init /\ [][Next]_Vars

CoveredByReceipt == floor <= receipt /\ receipt <= head
HeldViewAdmissible == ReaderClears(floor)
Safety == CoveredByReceipt /\ HeldViewAdmissible
FreshReadAfterFailoverWitness == ~(writer = 1 /\ floor = 2 /\ held = 2)

=============================================================================
