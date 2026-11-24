---------------------------- MODULE OrderedReaderRelease ----------------------------
EXTENDS Integers

CONSTANT Mutation

Original == [history |-> "root", position |-> 1, frontier |-> 7]
Substitution == [history |-> "other", position |-> 1, frontier |-> 7]
Checkpoint == 1

VARIABLE s

Init ==
    s = [origin |-> TRUE, witness |-> {}, authorized |-> FALSE, hold |-> {},
         lease |-> FALSE, applied |-> 0, accepted |-> FALSE, released |-> FALSE,
         now |-> 0, evidence |-> {}]

Seal ==
    /\ s.origin /\ s.witness = {}
    /\ s' = [s EXCEPT !.witness = {Original}]

CloseOrigin ==
    /\ s.origin
    /\ s' = [s EXCEPT !.origin = FALSE]

Protect(enrolled) ==
    /\ s.hold = {}
    /\ (enrolled \/ Mutation = "route_only")
    /\ s' = [s EXCEPT !.authorized = enrolled, !.hold = {Checkpoint}]

Lease ==
    /\ s.hold = {Checkpoint} /\ s.now < 2 /\ ~s.lease
    /\ s' = [s EXCEPT !.lease = TRUE]

Apply ==
    /\ s.lease /\ s.now < 2 /\ s.applied = 0
    /\ s' = [s EXCEPT !.applied = Checkpoint]

Validate(evidence) ==
    /\ ~s.accepted
    /\ (evidence \in s.witness \/ Mutation = "same_offset")
    /\ s.applied = Checkpoint
    /\ s' = [s EXCEPT !.accepted = TRUE, !.evidence = {evidence}]

DropHold ==
    /\ s.hold # {} /\ ~s.released /\ (s.now = 2 \/ Mutation = "drop_delivery_hold")
    /\ s' = [s EXCEPT !.hold = {}, !.lease = FALSE]

Release ==
    /\ s.accepted /\ ~s.released
    /\ (s.now < 2 /\ s.lease /\ s.hold = {Checkpoint})
        \/ Mutation \in {"ignore_delivery_cutoff", "drop_delivery_hold"}
    /\ s' = [s EXCEPT !.released = TRUE]

Tick ==
    /\ s.now < 2 /\ ~s.released
    /\ s' = [s EXCEPT !.now = @ + 1]

Next ==
    \/ Seal \/ CloseOrigin \/ Lease \/ Apply \/ DropHold \/ Release \/ Tick
    \/ \E enrolled \in {FALSE, TRUE} : Protect(enrolled)
    \/ \E e \in {Original, Substitution} : Validate(e)

Spec == Init /\ [][Next]_s

EnrollmentOwnsLease == s.lease => s.authorized
OriginalEvidence == s.accepted => s.evidence = {Original} /\ Original \in s.witness
ProtectedDelivery == s.released => s.hold = {Checkpoint} /\ s.lease
TimelyDelivery == s.released => s.now < 2

VacantReleaseUnsupported == ~(~s.origin /\ s.released)

=============================================================================
