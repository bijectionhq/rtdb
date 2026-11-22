----------------------- MODULE RecoveryCustodyHandoff -----------------------
EXTENDS Naturals

CONSTANT Mutation
VARIABLES cut, media, keys, independent, confirmed, clock, deadline

Vars == <<cut, media, keys, independent, confirmed, clock, deadline>>
Old == {"old-root", "old-suffix", "receipt-body"}
Clean == {"clean-root", "clean-suffix"}
IndependentCopy == "independent-copy"
Units == Old \cup Clean \cup {IndependentCopy}
Unarmed == 4

Root(at) == IF at = 0 THEN {"old-root", "receipt-body"} ELSE {"clean-root"}
Suffix(at) ==
    IF at = 0 THEN {"old-suffix"}
    ELSE IF Mutation = "reuse_erased_suffix" THEN {"old-suffix", "clean-suffix"}
    ELSE {"clean-suffix"}
Needed(at) == Root(at) \cup Suffix(at)

Init ==
    /\ cut = 0 /\ media = Old \cup {IndependentCopy} /\ keys = media
    /\ independent = {} /\ confirmed = 0 /\ clock = 0 /\ deadline = Unarmed

Stage ==
    /\ deadline = Unarmed
    /\ \E unit \in Clean \ media :
        /\ media' = media \cup {unit} /\ keys' = keys \cup {unit}
    /\ UNCHANGED <<cut, independent, confirmed, clock, deadline>>

KeepIndependent ==
    /\ \E unit \in (media \cap keys) \ independent :
        /\ (deadline = Unarmed \/ unit \notin Old)
        /\ independent' = independent \cup {unit}
    /\ UNCHANGED <<cut, media, keys, confirmed, clock, deadline>>

Confirm ==
    /\ confirmed < 2
    /\ \E position \in (confirmed + 1)..2 :
        /\ (position <= cut + 1 \/ Mutation = "confirm_without_coverage")
        /\ confirmed' = position
    /\ UNCHANGED <<cut, media, keys, independent, clock, deadline>>

Arm ==
    /\ deadline = Unarmed /\ clock < 3
    /\ (Needed(1) \subseteq (media \cap keys) \/ Mutation = "incomplete_replacement")
    /\ (Needed(1) \cap Old = {} \/ Mutation = "reuse_erased_suffix")
    /\ (independent \cap Old = {} \/ Mutation = "ignore_independent")
    /\ cut' = 1 /\ deadline' = clock + 1
    /\ UNCHANGED <<media, keys, independent, confirmed, clock>>

Tick ==
    /\ clock < 3 /\ clock' = clock + 1
    /\ keys' = IF clock' >= deadline /\ Mutation # "miss_deadline"
                THEN keys \ Old ELSE keys
    /\ UNCHANGED <<cut, media, independent, confirmed, deadline>>

Collect ==
    /\ \E unit \in media \ keys : media' = media \ {unit}
    /\ UNCHANGED <<cut, keys, independent, confirmed, clock, deadline>>

Next == Stage \/ KeepIndependent \/ Confirm \/ Arm \/ Tick \/ Collect

TypeOK ==
    /\ cut \in 0..1 /\ media \subseteq Units /\ keys \subseteq media
    /\ independent \subseteq Units /\ confirmed \in 0..2
    /\ clock \in 0..3 /\ deadline \in 1..Unarmed

RecoveryReadable == (Needed(cut) \cup independent) \subseteq (media \cap keys)
AcknowledgmentCovered == confirmed <= cut + 1
RevocationScoped == deadline = Unarmed \/ ((Needed(cut) \cup independent) \cap Old = {})
DeadlineMet == clock < deadline \/ (keys \cap Old = {})
Safe == TypeOK /\ RecoveryReadable /\ AcknowledgmentCovered /\ RevocationScoped /\ DeadlineMet
NeverFinishesWithIndependentCopy ==
    ~(cut = 1 /\ clock >= deadline /\ media \cap Old = {} /\ confirmed = 2
      /\ IndependentCopy \in independent /\ RecoveryReadable)

Spec == Init /\ [][Next]_Vars

=============================================================================
