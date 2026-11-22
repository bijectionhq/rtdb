----------------------- MODULE RetainedReleaseReceipt -----------------------
CONSTANT Mutation

VARIABLES phase, receipt, marker, reader, authorized, versions, resumed

Vars == <<phase, receipt, marker, reader, authorized, versions, resumed>>

Init ==
    /\ phase = "queued"
    /\ receipt \in {"matching", "other_cut", "unsigned"}
    /\ marker = TRUE /\ reader = FALSE /\ authorized = "none"
    /\ versions = {"retired", "replacement"} /\ resumed = FALSE

AdmitRead ==
    /\ marker /\ ~reader /\ phase = "queued"
    /\ reader' = TRUE
    /\ UNCHANGED <<phase, receipt, marker, authorized, versions, resumed>>

DrainRead ==
    /\ reader /\ reader' = FALSE
    /\ UNCHANGED <<phase, receipt, marker, authorized, versions, resumed>>

ForgetVisiblePath ==
    /\ Mutation = "discard_hidden_marker" /\ phase = "queued" /\ marker
    /\ marker' = FALSE
    /\ UNCHANGED <<phase, receipt, reader, authorized, versions, resumed>>

Authorize ==
    /\ phase = "queued" /\ marker
    /\ (~reader \/ Mutation = "ignore_marker_reader")
    /\ (receipt = "matching" \/ (receipt = "other_cut" /\ Mutation = "ignore_cut")
                           \/ (receipt = "unsigned" /\ Mutation = "ignore_authentication"))
    /\ authorized' = IF receipt = "matching" THEN "retired" ELSE "replacement"
    /\ phase' = "authorized"
    /\ UNCHANGED <<receipt, marker, reader, versions, resumed>>

LoseReceipt ==
    /\ receipt # "missing" /\ receipt' = "missing"
    /\ UNCHANGED <<phase, marker, reader, authorized, versions, resumed>>

DeleteExact ==
    /\ phase = "authorized" /\ authorized \in versions
    /\ versions' = versions \ {authorized}
    /\ resumed' = (resumed \/ receipt = "missing")
    /\ UNCHANGED <<phase, receipt, marker, reader, authorized>>

Settle ==
    /\ phase = "authorized" /\ authorized \notin versions
    /\ phase' = "settled" /\ marker' = FALSE
    /\ UNCHANGED <<receipt, reader, authorized, versions, resumed>>

Next == AdmitRead \/ DrainRead \/ ForgetVisiblePath \/ Authorize \/ LoseReceipt \/ DeleteExact \/ Settle
Spec == Init /\ [][Next]_Vars

KeepsEvidence == phase = "queued" => marker
KeepsReaders == reader => marker
KeepsReplacement == "replacement" \in versions
Safe == KeepsEvidence /\ KeepsReaders /\ KeepsReplacement
NeverResumesWithoutReceipt == ~(resumed /\ phase = "settled")
=============================================================================
