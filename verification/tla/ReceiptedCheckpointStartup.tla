--------------------------- MODULE ReceiptedCheckpointStartup ---------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    published,
    \* @type: Int;
    version,
    \* @type: Int;
    selected,
    \* @type: Str;
    phase,
    \* @type: Bool;
    oldRoot,
    \* @type: Bool;
    oldSuffix,
    \* @type: Bool;
    differentRoot,
    \* @type: Int;
    verified,
    \* @type: Int;
    plan,
    \* @type: Bool;
    authorized

\* @type: <<Int, Int, Int, Str, Bool, Bool, Bool, Int, Int, Bool>>;
Vars == <<published, version, selected, phase, oldRoot, oldSuffix, differentRoot, verified, plan, authorized>>

Init ==
    /\ published = 0 /\ version = 0 /\ selected = -1 /\ phase = "idle"
    /\ oldRoot = TRUE /\ oldSuffix = TRUE /\ differentRoot = FALSE
    /\ verified = -1 /\ plan = -1 /\ authorized = FALSE

Publish ==
    /\ published = 0 /\ published' = 1 /\ version' = version + 1
    /\ UNCHANGED <<selected, phase, oldRoot, oldSuffix, differentRoot, verified, plan, authorized>>

Admit ==
    /\ phase = "idle"
    /\ selected' = published /\ phase' = "admitted" /\ version' = version + 1
    /\ UNCHANGED <<published, oldRoot, oldSuffix, differentRoot, verified, plan, authorized>>

HeldRoot == selected = 0 /\ phase \in {"admitted", "opened"}
HeldSuffix == selected = 0 /\ (phase = "cursor" \/ (HeldRoot /\ Mutation # "omit_suffix_hold"))

ReadRoot ==
    /\ phase = "admitted"
    /\ selected # 0 \/ oldRoot
    /\ ~differentRoot \/ selected = 1 \/ Mutation = "ignore_digest"
    /\ verified' = IF differentRoot /\ selected = 0 THEN 2 ELSE selected
    /\ phase' = "opened"
    /\ UNCHANGED <<published, version, selected, oldRoot, oldSuffix, differentRoot, plan, authorized>>

StartCursor ==
    /\ phase \in {"opened", "gap"} /\ (selected # 0 \/ oldSuffix)
    /\ phase' = "cursor"
    /\ UNCHANGED <<published, version, selected, oldRoot, oldSuffix, differentRoot, verified, plan, authorized>>

DropBeforeCursor ==
    /\ Mutation = "release_before_cursor" /\ phase = "opened"
    /\ phase' = "gap" /\ version' = version + 1
    /\ UNCHANGED <<published, selected, oldRoot, oldSuffix, differentRoot, verified, plan, authorized>>

Finish ==
    /\ phase = "cursor" /\ phase' = "done" /\ version' = version + 1
    /\ UNCHANGED <<published, selected, oldRoot, oldSuffix, differentRoot, verified, plan, authorized>>

ObserveTrim ==
    /\ plan = -1 /\ ~HeldSuffix
    /\ plan' = version
    /\ UNCHANGED <<published, version, selected, phase, oldRoot, oldSuffix, differentRoot, verified, authorized>>

AuthorizeTrim ==
    /\ published = 1 /\ plan # -1 /\ ~authorized
    /\ plan = version \/ Mutation = "trust_stale_trim"
    /\ authorized' = TRUE
    /\ UNCHANGED <<published, version, selected, phase, oldRoot, oldSuffix, differentRoot, verified, plan>>

Trim ==
    /\ authorized /\ oldSuffix /\ oldSuffix' = FALSE
    /\ UNCHANGED <<published, version, selected, phase, oldRoot, differentRoot, verified, plan, authorized>>

Collect ==
    /\ published = 1 /\ oldRoot /\ (~HeldRoot \/ Mutation = "ignore_root_hold")
    /\ oldRoot' = FALSE
    /\ UNCHANGED <<published, version, selected, phase, oldSuffix, differentRoot, verified, plan, authorized>>

ChangeRootBytes ==
    /\ oldRoot /\ ~differentRoot /\ differentRoot' = TRUE
    /\ UNCHANGED <<published, version, selected, phase, oldRoot, oldSuffix, verified, plan, authorized>>

Next == Publish \/ Admit \/ ReadRoot \/ StartCursor \/ DropBeforeCursor \/ Finish \/ ObserveTrim \/ AuthorizeTrim \/ Trim \/ Collect \/ ChangeRootBytes
Spec == Init /\ [][Next]_Vars

ReceiptMatches == verified # -1 => verified = selected
RootProtected == HeldRoot => oldRoot
SuffixProtected == selected = 0 /\ phase \in {"admitted", "opened", "gap", "cursor"} => oldSuffix
Safe == ReceiptMatches /\ RootProtected /\ SuffixProtected
NeverRetiresAfterRecovery == ~(selected = 0 /\ phase = "done" /\ ~oldRoot /\ ~oldSuffix)
=========================================================================================
