--------------------------- MODULE PackPublicationObservation ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    current,
    \* @type: Int;
    cached,
    \* @type: Int;
    head,
    \* @type: Int;
    trailer,
    \* @type: Int;
    index,
    \* @type: Int;
    body

\* @type: <<Int, Int, Int, Int, Int, Int>>;
Vars == <<current, cached, head, trailer, index, body>>

\* 1 is the original pack, 2 a different version at its path, and 3 a fork's copy.
Init ==
    /\ current = 1 /\ cached \in {0, 1}
    /\ head = 0 /\ trailer = 0 /\ index = 0 /\ body = 0

ChangePublication ==
    /\ current = 1 /\ current' \in {0, 2, 3}
    /\ UNCHANGED <<cached, head, trailer, index, body>>

ObserveHead ==
    /\ head = 0 /\ current # 0
    /\ cached = 0 \/ cached = current \/ current = 3 \/ Mutation = "rebind_same_path"
    /\ head' = current
    /\ trailer' = IF cached = current THEN current ELSE 0
    /\ index' = IF cached = current \/ (cached = 1 /\ current = 3 /\ Mutation = "reuse_copied_index") THEN cached ELSE 0
    /\ UNCHANGED <<current, cached, body>>

ReadTrailer ==
    /\ head # 0 /\ trailer = 0 /\ index = 0 /\ current # 0
    /\ current = head \/ Mutation = "ignore_trailer_version"
    /\ trailer' = current
    /\ UNCHANGED <<current, cached, head, index, body>>

ReadIndex ==
    /\ trailer # 0 /\ index = 0 /\ current # 0
    /\ current = head \/ Mutation = "ignore_index_version"
    /\ index' = current
    /\ UNCHANGED <<current, cached, head, trailer, body>>

ReadMember ==
    /\ index # 0 /\ body = 0 /\ current # 0
    /\ current = index \/ Mutation = "ignore_member_version"
    /\ body' = current
    /\ UNCHANGED <<current, cached, head, trailer, index>>

Next == ChangePublication \/ ObserveHead \/ ReadTrailer \/ ReadIndex \/ ReadMember
Spec == Init /\ [][Next]_Vars

TrailerCoherent == trailer # 0 => trailer = head
IndexCoherent == index # 0 => index = trailer /\ index = head
MemberCoherent == body # 0 => body = index
CachedPathStable == cached = 1 => head # 2
Safe == TrailerCoherent /\ IndexCoherent /\ MemberCoherent /\ CachedPathStable
NeverReadsCopy == ~(cached = 1 /\ body = 3)
NeverReadsOriginal == ~(cached = 0 /\ body = 1)
=========================================================================================
