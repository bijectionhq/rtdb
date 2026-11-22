----------------------------- MODULE RepairArchiveCustody ----------------------------
CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Str;
    phase,
    \* @type: Set(Str);
    held,
    \* @type: Set(Str);
    retired,
    \* @type: Set(Str);
    erased,
    \* @type: Bool;
    reading,
    \* @type: Bool;
    uploadPending,
    \* @type: Bool;
    admittedClosed

\* @type: <<Str, Set(Str), Set(Str), Set(Str), Bool, Bool, Bool>>;
Vars == <<phase, held, retired, erased, reading, uploadPending, admittedClosed>>

Inputs == {"repair", "destination"}
Archives == Inputs \cup {"unrelated"}

Init ==
    /\ phase = "idle"
    /\ held = {} /\ retired = {} /\ erased = {}
    /\ reading = FALSE /\ uploadPending = FALSE /\ admittedClosed = FALSE

Admit ==
    /\ phase = "idle"
    /\ (Inputs \cap retired = {}) \/ Mutation = "admit_after_retirement"
    /\ phase' = "open"
    /\ held' = IF Mutation = "drop_repair_hold" THEN {"destination"} ELSE Inputs
    /\ admittedClosed' = (Inputs \cap retired # {})
    /\ UNCHANGED <<retired, erased, reading, uploadPending>>

Read ==
    /\ phase = "open" \/ (phase = "closed" /\ Mutation = "read_when_closed")
    /\ ~reading /\ "repair" \in held
    /\ reading' = TRUE
    /\ UNCHANGED <<phase, held, retired, erased, uploadPending, admittedClosed>>

BeginUpload ==
    /\ phase = "open" /\ ~uploadPending /\ "destination" \in held
    /\ uploadPending' = TRUE
    /\ UNCHANGED <<phase, held, retired, erased, reading, admittedClosed>>

Retire ==
    /\ \E archive \in Archives \ retired : retired' = retired \cup {archive}
    /\ UNCHANGED <<phase, held, erased, reading, uploadPending, admittedClosed>>

Erase ==
    /\ \E archive \in retired \ erased :
        /\ archive \notin held \/ (archive = "repair" /\ Mutation = "ignore_repair_when_erasing")
        /\ erased' = erased \cup {archive}
    /\ UNCHANGED <<phase, held, retired, reading, uploadPending, admittedClosed>>

Cancel ==
    /\ phase = "open" /\ phase' = "draining"
    /\ UNCHANGED <<held, retired, erased, reading, uploadPending, admittedClosed>>

Drain ==
    /\ reading /\ reading' = FALSE
    /\ UNCHANGED <<phase, held, retired, erased, uploadPending, admittedClosed>>

Close ==
    /\ phase = "draining" /\ (~reading \/ Mutation = "release_before_drain")
    /\ phase' = IF uploadPending THEN "closed" ELSE "done"
    /\ held' = IF uploadPending THEN held ELSE {}
    /\ UNCHANGED <<retired, erased, reading, uploadPending, admittedClosed>>

Settle ==
    /\ phase = "closed" /\ uploadPending /\ uploadPending' = FALSE
    /\ held' = {} /\ phase' = "done"
    /\ UNCHANGED <<retired, erased, reading, admittedClosed>>

Next == Admit \/ Read \/ BeginUpload \/ Retire \/ Erase \/ Cancel \/ Drain \/ Close \/ Settle
Spec == Init /\ [][Next]_Vars

AdmissionCoversBoth == phase \in {"open", "draining", "closed"} => held = Inputs
RetainedArchivesSurvive == held \cap erased = {}
ReadsStayAdmitted == reading => phase \in {"open", "draining"} /\ "repair" \in held
PendingPublicationKeepsCustody == uploadPending => "destination" \in held
AdmissionUsesOpenArchives == ~admittedClosed
Safe == AdmissionCoversBoth /\ RetainedArchivesSurvive /\ ReadsStayAdmitted
        /\ PendingPublicationKeepsCustody /\ AdmissionUsesOpenArchives

NeverReadsRepair == ~reading
NeverClosesWithProviderDebt == phase # "closed"
NeverErasesUnrelatedDuringRead == ~(reading /\ "unrelated" \in erased)
NeverErasesBothAfterRelease == ~(phase = "done" /\ Inputs \subseteq erased)
======================================================================================
