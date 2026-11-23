----------------------------- MODULE PublicationVersion -----------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Int;
    latest,
    \* @type: Bool;
    acknowledged,
    \* @type: Int;
    checkedBody,
    \* @type: Bool;
    matchedAttempt,
    \* @type: Int;
    catalog,
    \* @type: Int;
    retirement

Vars == <<latest, acknowledged, checkedBody, matchedAttempt, catalog, retirement>>
None == 0
ExpectedBytes == {1, 3}
CheckEvidence == {checkedBody} \cup (IF acknowledged \/ matchedAttempt THEN {1} ELSE {})

Init ==
    /\ latest = None /\ acknowledged = FALSE /\ checkedBody = None
    /\ matchedAttempt = FALSE /\ catalog = None /\ retirement = None

CompleteWrite ==
    /\ latest = None /\ latest' = 1
    /\ acknowledged' \in BOOLEAN
    /\ UNCHANGED <<checkedBody, matchedAttempt, catalog, retirement>>

Replace ==
    /\ latest = 1 /\ latest' \in {2, 3}
    /\ UNCHANGED <<acknowledged, checkedBody, matchedAttempt, catalog, retirement>>

VerifyExistingBody ==
    /\ catalog = None /\ checkedBody = None /\ latest \in ExpectedBytes
    /\ checkedBody' = latest
    /\ UNCHANGED <<latest, acknowledged, matchedAttempt, catalog, retirement>>

BindAcknowledgement ==
    /\ catalog = None /\ acknowledged
    /\ catalog' = IF Mutation = "head_after_write" THEN latest ELSE 1
    /\ UNCHANGED <<latest, acknowledged, checkedBody, matchedAttempt, retirement>>

BindVerifiedBody ==
    /\ catalog = None /\ checkedBody # None
    /\ catalog' = IF Mutation = "head_after_read" THEN latest ELSE checkedBody
    /\ UNCHANGED <<latest, acknowledged, checkedBody, matchedAttempt, retirement>>

RecoverOwnAttempt ==
    /\ catalog = None /\ ~acknowledged /\ latest # None
    /\ latest = 1 \/ Mutation = "ignore_attempt"
    /\ matchedAttempt' = (latest = 1)
    /\ catalog' = latest
    /\ UNCHANGED <<latest, acknowledged, checkedBody, retirement>>

ObserveRetirement ==
    /\ catalog # None /\ retirement = None
    /\ latest = catalog \/ Mutation = "ignore_publication"
    /\ retirement' = latest
    /\ UNCHANGED <<latest, acknowledged, checkedBody, matchedAttempt, catalog>>

Next == CompleteWrite \/ Replace \/ VerifyExistingBody \/ BindAcknowledgement
        \/ BindVerifiedBody \/ RecoverOwnAttempt \/ ObserveRetirement
Spec == Init /\ [][Next]_Vars

BindsCheckedVersion == catalog = None \/ catalog \in CheckEvidence
RetirementMatchesBinding == retirement = None \/ retirement = catalog
Safety == BindsCheckedVersion /\ RetirementMatchesBinding
LostReplyWitness == ~(matchedAttempt /\ retirement = 1)
VerifiedRetryWitness == ~(checkedBody = 3 /\ retirement = 3)

=============================================================================
