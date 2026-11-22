------------------------ MODULE UnversionedRetirement ------------------------
EXTENDS Naturals

CONSTANT Mutation

VARIABLES object, expected, held, fenced, phase, versioned, hidden,
          requests, pending, lateCreate, witnessed

Vars == <<object, expected, held, fenced, phase, versioned, hidden,
          requests, pending, lateCreate, witnessed>>

Init ==
    /\ object \in {"original", "missing"}
    /\ expected = "none" /\ held = FALSE /\ fenced = FALSE /\ phase = "open"
    /\ versioned = FALSE /\ hidden = FALSE /\ witnessed = FALSE
    /\ requests = 0 /\ pending = 0 /\ lateCreate = FALSE

Read ==
    /\ ~held /\ held' = TRUE
    /\ (~fenced \/ Mutation = "skip_read_fence")
    /\ object = "original"
    /\ UNCHANGED <<object, expected, fenced, phase, versioned, hidden,
                   requests, pending, lateCreate, witnessed>>

ReleaseRead ==
    /\ held /\ held' = FALSE
    /\ UNCHANGED <<object, expected, fenced, phase, versioned, hidden,
                   requests, pending, lateCreate, witnessed>>

AdmitCreate ==
    /\ phase = "open" /\ ~lateCreate /\ lateCreate' = TRUE
    /\ UNCHANGED <<object, expected, held, fenced, phase, versioned, hidden,
                   requests, pending, witnessed>>

ApplyCreate ==
    /\ lateCreate /\ lateCreate' = FALSE
    /\ object' = IF object = "missing" THEN "replacement" ELSE object
    /\ UNCHANGED <<expected, held, fenced, phase, versioned, hidden,
                   requests, pending, witnessed>>

Authorize ==
    /\ phase = "open" /\ ~held /\ ~versioned
    /\ object \in {"original", "missing"}
    /\ expected' = object /\ phase' = "authorized" /\ fenced' = TRUE
    /\ UNCHANGED <<object, held, versioned, hidden, requests, pending, lateCreate, witnessed>>

SendReplacement ==
    /\ phase = "authorized" /\ requests < 2 /\ ~versioned
    /\ requests' = requests + 1 /\ pending' = pending + 1
    /\ UNCHANGED <<object, expected, held, fenced, phase, versioned, hidden,
                   lateCreate, witnessed>>

ApplyReplacement ==
    /\ pending > 0 /\ pending' = pending - 1
    /\ LET matched == object = expected \/ Mutation = "unconditional_replace" IN
           /\ object' = IF matched THEN "tombstone" ELSE object
           /\ hidden' = (hidden \/ (matched /\ versioned /\ object \in {"original", "replacement"}))
    /\ UNCHANGED <<expected, held, fenced, phase, versioned, requests, lateCreate, witnessed>>

ChangeBucket ==
    /\ Mutation = "allow_versioning" /\ ~versioned /\ versioned' = TRUE
    /\ UNCHANGED <<object, expected, held, fenced, phase, hidden,
                   requests, pending, lateCreate, witnessed>>

Witness ==
    /\ phase = "authorized" /\ ~witnessed /\ witnessed' = TRUE
    /\ ~versioned
    /\ (object = "tombstone" \/ (Mutation = "settle_on_missing" /\ object = "missing"))
    /\ UNCHANGED <<object, expected, held, fenced, phase, versioned, hidden,
                   requests, pending, lateCreate>>

Settle ==
    /\ phase = "authorized" /\ witnessed
    /\ (~versioned \/ Mutation = "allow_versioning")
    /\ phase' = "settled" /\ fenced' = (Mutation = "leave_owner_closed")
    /\ UNCHANGED <<object, expected, held, versioned, hidden, requests, pending, lateCreate, witnessed>>

PurgeTombstone ==
    /\ Mutation = "purge_before_disposal" /\ phase = "settled" /\ object = "tombstone"
    /\ object' = "missing"
    /\ UNCHANGED <<expected, held, fenced, phase, versioned, hidden,
                   requests, pending, lateCreate, witnessed>>

Next == Read \/ ReleaseRead \/ AdmitCreate \/ ApplyCreate \/ Authorize \/ SendReplacement
        \/ ApplyReplacement \/ ChangeBucket \/ Witness \/ Settle \/ PurgeTombstone
Spec == Init /\ [][Next]_Vars

DrainBeforeEffect == fenced => ~held
OneWayKey == phase = "settled" => object = "tombstone"
RetainsReplacement == object = "replacement" => object' = "replacement"
KeepsReplacement == [][RetainsReplacement]_Vars
NoHiddenPayload == ~hidden
AtomicReopen == fenced = (phase = "authorized")
Safe == DrainBeforeEffect /\ OneWayKey /\ NoHiddenPayload /\ AtomicReopen
NeverSettlesWithLateCreate == ~(phase = "settled" /\ lateCreate)
NeverSettlesWithLateRetirement == ~(phase = "settled" /\ pending > 0)
=============================================================================
