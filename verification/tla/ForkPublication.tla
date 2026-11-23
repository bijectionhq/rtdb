-------------------------- MODULE ForkPublication --------------------------
CONSTANTS
    \* @type: Str;
    Policy,
    \* @type: Str;
    Mutation
Objects == {"root", "payload"}

VARIABLES
    \* @type: Bool;
    source,
    \* @type: Str;
    forkState,
    \* @type: Set(Str);
    own,
    \* @type: Str;
    sweep,
    \* @type: Str;
    plan
vars == <<source, forkState, own, sweep, plan>>

Visible == forkState = "published"
ChildDependsOnSource == Policy = "reference" /\ Visible /\ own # Objects

Init ==
    /\ source = TRUE
    /\ forkState = "idle"
    /\ own = {}
    /\ sweep = "idle"
    /\ plan = "idle"

Prepare ==
    /\ forkState = "idle"
    /\ source
    /\ forkState' = "prepared"
    /\ UNCHANGED <<source, own, sweep, plan>>

Publish ==
    /\ forkState = "prepared"
    /\ (IF Policy = "copy"
        THEN own = Objects \/ (Mutation = "publish_partial" /\ "root" \in own)
        ELSE source /\ (plan = "idle" \/ Mutation = "admit_during_delete"))
    /\ forkState' = "published"
    /\ UNCHANGED <<source, own, sweep, plan>>

Abandon ==
    /\ forkState = "prepared"
    /\ ~source
    /\ (Policy = "reference" \/ own # Objects)
    /\ forkState' = "refused"
    /\ own' = {}
    /\ UNCHANGED <<source, sweep, plan>>

Capture ==
    /\ sweep = "idle"
    /\ source
    /\ sweep' = IF Policy = "reference" /\ Visible THEN "transfer" ELSE "empty"
    /\ UNCHANGED <<source, forkState, own, plan>>

Authorize ==
    /\ sweep # "idle"
    /\ plan = "idle"
    /\ source
    /\ (Policy = "copy" \/ (sweep = "transfer") = Visible \/ Mutation = "ignore_new_fork")
    /\ plan' = sweep
    /\ sweep' = "idle"
    /\ UNCHANGED <<source, forkState, own>>

Refresh ==
    /\ sweep # "idle"
    /\ sweep' = "idle"
    /\ UNCHANGED <<source, forkState, own, plan>>

Transfer(object) ==
    /\ source
    /\ object \notin own
    /\ (IF Policy = "copy" THEN forkState = "prepared" ELSE Visible /\ plan = "transfer")
    /\ own' = own \cup {object}
    /\ UNCHANGED <<source, forkState, sweep, plan>>

Collect ==
    /\ plan # "idle"
    /\ source
    /\ (plan = "empty" \/ own = Objects \/ Mutation = "delete_before_copy")
    /\ source' = FALSE
    /\ plan' = "idle"
    /\ UNCHANGED <<forkState, own, sweep>>

Next == Prepare \/ Publish \/ Abandon \/ Capture \/ Authorize \/ Refresh \/ Collect
        \/ \E object \in Objects : Transfer(object)
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ source \in BOOLEAN
    /\ forkState \in {"idle", "prepared", "published", "refused"}
    /\ own \subseteq Objects
    /\ sweep \in {"idle", "empty", "transfer"}
    /\ plan \in {"idle", "empty", "transfer"}

PublishedChildReadable ==
    Visible => (own = Objects \/ (ChildDependsOnSource /\ source))

NeverServesAfterSourceCollection ==
    ~(Visible /\ ~source /\ own = Objects)

NeverRefusesAfterSourceCollection ==
    ~(forkState = "refused" /\ ~source)
=============================================================================
