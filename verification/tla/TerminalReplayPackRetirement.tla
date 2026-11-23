---------------------- MODULE TerminalReplayPackRetirement ----------------------
EXTENDS Naturals

CONSTANT Mutation

VARIABLE s

Init == s = [
    object |-> "original", proof |-> "none", phase |-> "open",
    capture |-> "first", firstRetained |-> TRUE, pin |-> FALSE,
    journal |-> "required", reader |-> FALSE, copying |-> FALSE,
    upload |-> 0, writer |-> FALSE, witnessed |-> FALSE
]

AdmitReader ==
    /\ ~s.reader /\ s.object \in {"original", "replacement"}
    /\ s.phase = "open" \/ Mutation = "ignore_read_fence"
    /\ s' = [s EXCEPT !.reader = TRUE]

DrainReader == s.reader /\ s' = [s EXCEPT !.reader = FALSE]

PrepareCopy ==
    /\ s.phase = "open" /\ ~s.copying /\ s.object = "original"
    /\ s' = [s EXCEPT !.copying = TRUE]

FinishCopy == s.copying /\ s' = [s EXCEPT !.copying = FALSE]

CertifyPrefix ==
    /\ s.journal = "required" /\ s' = [s EXCEPT !.journal = "covered"]

Verify ==
    /\ s.phase = "open" /\ s.proof = "none"
    /\ s.capture = "first" /\ s.firstRetained /\ s.object = "original"
    /\ \E proof \in {"exact", "mismatched-member", "missing-member"}:
        s' = [s EXCEPT !.proof = proof]

Authorize ==
    /\ s.phase = "open" /\ s.capture = "first" /\ s.firstRetained
    /\ ~s.reader /\ ~s.copying
    /\ s.journal = "covered" \/ Mutation = "ignore_prefix"
    /\ s.proof = "exact"
        \/ (Mutation = "unconditioned_member" /\ s.proof = "mismatched-member")
        \/ (Mutation = "ignore_member" /\ s.proof = "missing-member")
    /\ s' = [s EXCEPT !.phase = "authorized", !.pin = TRUE]

SelectNewCapture ==
    /\ s.capture = "first"
    /\ s' = [s EXCEPT !.capture = "second"]

DisposeFirst ==
    /\ s.capture = "second" /\ s.firstRetained
    /\ ~s.pin \/ Mutation = "release_archive"
    /\ s' = [s EXCEPT !.firstRetained = FALSE]

PublishReplacement ==
    /\ s.object = "original"
    /\ s' = [s EXCEPT !.object = "replacement"]

AdmitWriter ==
    /\ s.phase = "open" /\ ~s.writer
    /\ s' = [s EXCEPT !.writer = TRUE]

ApplyWriter ==
    /\ s.writer
    /\ s' = [s EXCEPT !.writer = FALSE,
              !.object = IF s.object = "gone" \/ (s.object = "tombstone" /\ Mutation = "overwrite_tombstone")
                            THEN "original" ELSE s.object]

Submit ==
    /\ s.phase = "authorized" /\ s.upload < 2
    /\ s' = [s EXCEPT !.upload = @ + 1]

ApplyTombstone ==
    /\ s.upload > 0
    /\ s' = [s EXCEPT !.upload = @ - 1,
              !.object = IF s.object = "original" \/ Mutation = "unconditional_tombstone"
                            THEN "tombstone" ELSE s.object]

LosePublication ==
    /\ s.object = "original"
    /\ s' = [s EXCEPT !.object = "gone"]

ObserveSettlement ==
    /\ s.phase = "authorized" /\ ~s.witnessed
    /\ s.object = "tombstone" \/ (Mutation = "absence_is_receipt" /\ s.object = "gone")
    /\ s' = [s EXCEPT !.witnessed = TRUE]

Settle ==
    /\ s.phase = "authorized" /\ s.witnessed
    /\ s.upload = 0 \/ Mutation = "forget_upload"
    /\ s' = [s EXCEPT !.phase = "settled", !.pin = FALSE]

Next == AdmitReader \/ DrainReader \/ PrepareCopy \/ FinishCopy \/ CertifyPrefix
    \/ Verify \/ Authorize \/ SelectNewCapture \/ DisposeFirst \/ PublishReplacement
    \/ AdmitWriter \/ ApplyWriter \/ Submit \/ ApplyTombstone \/ LosePublication
    \/ ObserveSettlement \/ Settle

Spec == Init /\ [][Next]_s

CoveredBeforeEffect == s.object = "tombstone" =>
    s.proof = "exact" /\ s.journal = "covered" /\ ~s.reader /\ ~s.copying
ArchivePinned == s.pin => s.firstRetained
SettlementKeepsTombstone == s.phase = "settled" => s.object = "tombstone" /\ s.upload = 0
ReplacementPreserved == [][s.object = "replacement" => s'.object = "replacement"]_s
NeverSettles == s.phase # "settled"
=================================================================================
