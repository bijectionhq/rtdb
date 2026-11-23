--------------------------- MODULE CheckpointPackEvidence ---------------------------
CONSTANT
    \* @type: Str;
    Mode

VARIABLES
    \* @type: Bool;
    staged,
    \* @type: Bool;
    published,
    \* @type: Bool;
    present,
    \* @type: Bool;
    cached,
    \* @type: Bool;
    checked,
    \* @type: Bool;
    presentAtCheck,
    \* @type: Bool;
    submitted,
    \* @type: Bool;
    presentAtSubmit

\* @type: <<Bool, Bool, Bool, Bool, Bool, Bool, Bool, Bool>>;
Vars == <<staged, published, present, cached, checked, presentAtCheck, submitted, presentAtSubmit>>

Init ==
    /\ staged = FALSE /\ published = FALSE /\ present = FALSE /\ cached = FALSE
    /\ checked = FALSE /\ presentAtCheck = FALSE
    /\ submitted = FALSE /\ presentAtSubmit = FALSE

Stage ==
    /\ ~staged /\ ~published /\ staged' = TRUE
    /\ UNCHANGED <<published, present, cached, checked, presentAtCheck, submitted, presentAtSubmit>>

Publish ==
    /\ staged /\ ~published
    /\ staged' = FALSE /\ published' = TRUE /\ present' = TRUE
    /\ UNCHANGED <<cached, checked, presentAtCheck, submitted, presentAtSubmit>>

CacheIndex ==
    /\ present /\ ~cached /\ cached' = TRUE
    /\ UNCHANGED <<staged, published, present, checked, presentAtCheck, submitted, presentAtSubmit>>

Collect ==
    /\ present
    /\ (Mode # "hold" \/ ~checked \/ submitted)
    /\ present' = FALSE
    /\ UNCHANGED <<staged, published, cached, checked, presentAtCheck, submitted, presentAtSubmit>>

Observe ==
    /\ ~checked
    /\ (present \/ (Mode = "trust_staged" /\ staged) \/ (Mode = "trust_cached" /\ cached))
    /\ checked' = TRUE /\ presentAtCheck' = present
    /\ UNCHANGED <<staged, published, present, cached, submitted, presentAtSubmit>>

Submit ==
    /\ checked /\ ~submitted
    /\ submitted' = TRUE /\ presentAtSubmit' = present
    /\ UNCHANGED <<staged, published, present, cached, checked, presentAtCheck>>

Next == Stage \/ Publish \/ CacheIndex \/ Collect \/ Observe \/ Submit
Spec == Init /\ [][Next]_Vars

CheckRequiresStoredPack == checked => presentAtCheck
SubmitRequiresStoredPack == submitted => presentAtSubmit
NeverSubmitsStoredPack == ~(submitted /\ presentAtSubmit)
=============================================================================
