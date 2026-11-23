-------------------------- MODULE CheckpointPhysicalOwners ---------------------------
CONSTANTS
    \* @type: Str;
    Design,
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Set(Str);
    held,
    \* @type: Set(Str);
    available,
    \* @type: Set(Str);
    authorized,
    \* @type: Str;
    phase,
    \* @type: Bool;
    reading,
    \* @type: Bool;
    accepted,
    \* @type: Bool;
    partialRefusal

\* @type: <<Set(Str), Set(Str), Set(Str), Str, Bool, Bool, Bool>>;
Vars == <<held, available, authorized, phase, reading, accepted, partialRefusal>>

\* Each owner authorizes removal of a separate, exact physical identity.
\* Their publications are both required by this checkpoint source plan.
Required == {"serving", "archive"}

Init ==
    /\ available = Required /\ authorized = {} /\ held = {}
    /\ phase = "idle" /\ reading = FALSE /\ accepted = FALSE /\ partialRefusal = FALSE

Start ==
    /\ phase = "idle" /\ phase' = "acquiring"
    /\ UNCHANGED <<held, available, authorized, reading, accepted, partialRefusal>>

Pin ==
    /\ phase = "acquiring"
    /\ IF Design = "atomic"
       THEN /\ Required \subseteq available \ authorized
            /\ held = {} /\ held' = Required
       ELSE /\ \E owner \in (IF held = {} THEN {"serving"} ELSE Required \ held):
                   /\ owner \in available
                   /\ owner \notin authorized \/ Mutation = "ignore_prior_authorization"
                   /\ held' = held \cup {owner}
    /\ UNCHANGED <<available, authorized, phase, reading, accepted, partialRefusal>>

Authorize ==
    /\ \E owner \in available \ (held \cup authorized):
        authorized' = authorized \cup {owner}
    /\ UNCHANGED <<held, available, phase, reading, accepted, partialRefusal>>

Erase ==
    /\ \E owner \in available \cap authorized:
        available' = available \ {owner}
    /\ UNCHANGED <<held, authorized, phase, reading, accepted, partialRefusal>>

BeginRead ==
    /\ phase = "acquiring"
    /\ held = Required \/ (held # {} /\ Mutation = "accept_one_owner")
    /\ phase' = "reading" /\ reading' = TRUE
    /\ UNCHANGED <<held, available, authorized, accepted, partialRefusal>>

AcceptDestination ==
    /\ phase = "reading" /\ reading
    /\ Required \subseteq available
    /\ phase' = "durable" /\ reading' = FALSE /\ accepted' = TRUE
    /\ UNCHANGED <<held, available, authorized, partialRefusal>>

Publish ==
    /\ phase = "durable" /\ accepted
    /\ phase' = "published" /\ held' = {}
    /\ UNCHANGED <<available, authorized, reading, accepted, partialRefusal>>

Cancel ==
    /\ phase \in {"acquiring", "reading", "durable"} /\ phase' = "closing"
    /\ partialRefusal' = (phase = "acquiring" /\ held = {"serving"} /\ "archive" \in authorized)
    /\ UNCHANGED <<held, available, authorized, reading, accepted>>

DrainProvider ==
    /\ phase = "closing" /\ reading /\ reading' = FALSE
    /\ UNCHANGED <<held, available, authorized, phase, accepted, partialRefusal>>

Release ==
    /\ phase = "closing"
    /\ ~reading \/ Mutation = "release_before_drain"
    /\ \E owner \in held : held' = held \ {owner}
    /\ UNCHANGED <<available, authorized, phase, reading, accepted, partialRefusal>>

Closed ==
    /\ phase = "closing" /\ held = {} /\ ~reading /\ phase' = "cancelled"
    /\ UNCHANGED <<held, available, authorized, reading, accepted, partialRefusal>>

Next == Start \/ Pin \/ Authorize \/ Erase \/ BeginRead \/ AcceptDestination \/ Publish
        \/ Cancel \/ DrainProvider \/ Release \/ Closed
Spec == Init /\ [][Next]_Vars

ReadsRequireEveryOwner == reading => held = Required
HeldVersionsAreRetained == held \subseteq available \ authorized
PublicationRequiresDurability == phase = "published" => accepted /\ ~reading
Safe == ReadsRequireEveryOwner /\ HeldVersionsAreRetained /\ PublicationRequiresDurability

NeverHasPartialAcquisition == phase = "acquiring" => held = {} \/ held = Required
NeverCancelsAfterRefusal == ~(phase = "cancelled" /\ partialRefusal /\ "serving" \in authorized)
NeverPublishes == phase # "published"
======================================================================================
