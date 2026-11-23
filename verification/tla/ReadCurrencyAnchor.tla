------------------------- MODULE ReadCurrencyAnchor -------------------------
EXTENDS Integers

CONSTANTS Readers, Bound, Mutation
VARIABLES now, published, installed, observations, pending, anchor

vars == <<now, published, installed, observations, pending, anchor>>

Empty == [at |-> -1, needed |-> -1, tip |-> -1]
Init ==
    /\ now = 0 /\ published = 0 /\ installed = 0
    /\ observations = [reader \in Readers |-> Empty]
    /\ pending = Empty /\ anchor = Empty

Tick ==
    /\ now < Bound /\ now' = now + 1
    /\ UNCHANGED <<published, installed, observations, pending, anchor>>

Publish ==
    /\ published < Bound /\ published' = published + 1
    /\ UNCHANGED <<now, installed, observations, pending, anchor>>

Install ==
    /\ installed < published /\ installed' = installed + 1
    /\ UNCHANGED <<now, published, observations, pending, anchor>>

ReadClock(reader) ==
    /\ observations[reader] = Empty
    /\ observations' = [observations EXCEPT ![reader] = [at |-> now, needed |-> published, tip |-> -1]]
    /\ UNCHANGED <<now, published, installed, pending, anchor>>

ReadTip(reader) ==
    /\ observations[reader].at # -1 /\ observations[reader].tip = -1
    /\ observations' = [observations EXCEPT ![reader].tip = published]
    /\ UNCHANGED <<now, published, installed, pending, anchor>>

Record(reader) ==
    /\ observations[reader].tip # -1
    /\ LET sampled == observations[reader]
           candidate == IF Mutation = "clock_after_tip"
                        THEN [sampled EXCEPT !.at = now, !.needed = published]
                        ELSE sampled
       IN pending' = IF candidate.tip > pending.tip
                         \/ (Mutation = "replace_equal_tip" /\ candidate.tip = pending.tip)
                     THEN candidate
                     ELSE pending
    /\ observations' = [observations EXCEPT ![reader] = Empty]
    /\ UNCHANGED <<now, published, installed, anchor>>

Confirm ==
    /\ pending # Empty /\ (installed >= pending.tip \/ Mutation = "confirm_early")
    /\ anchor' = IF pending.at >= anchor.at \/ Mutation = "rewind_anchor" THEN pending ELSE anchor
    /\ pending' = Empty
    /\ UNCHANGED <<now, published, installed, observations>>

Next == Tick \/ Publish \/ Install \/ Confirm
        \/ \E reader \in Readers : ReadClock(reader) \/ ReadTip(reader) \/ Record(reader)

Spec == Init /\ [][Next]_vars

AnchorCoversPrefix == anchor.at = -1 \/ (anchor.needed <= anchor.tip /\ anchor.tip <= installed)
AnchorAdvances == anchor.at <= anchor'.at
AnchorNeverRewinds == [][AnchorAdvances]_vars
KeepsNewerPending ==
    (pending # Empty /\ pending' # Empty /\ pending' # pending) => pending.tip < pending'.tip
PendingDoesNotRegress == [][KeepsNewerPending]_vars
NeverConfirmsWhileAnotherObservationIsPending ==
    ~(anchor.at = Bound /\ anchor.tip = Bound /\ \E reader \in Readers : observations[reader] # Empty)
=============================================================================
