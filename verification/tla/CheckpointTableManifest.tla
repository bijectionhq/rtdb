------------------------ MODULE CheckpointTableManifest ------------------------
EXTENDS Integers

CONSTANT
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int -> Int;
    stored,
    \* @type: Set(Int);
    confirmed,
    \* @type: Bool;
    published,
    \* @type: Bool;
    completeAtPublish,
    \* @type: Bool;
    opened,
    \* @type: Int -> Int;
    captured

Vars == <<stored, confirmed, published, completeAtPublish, opened, captured>>
Tables == {0, 1}
Empty == [t \in Tables |-> 0]
Expected == [t \in Tables |-> t + 1]

Init ==
    /\ stored = Empty /\ confirmed = {}
    /\ published = FALSE /\ completeAtPublish = TRUE
    /\ opened = FALSE /\ captured = Empty

Write(t) ==
    /\ ~published /\ stored[t] = 0
    /\ stored' = [stored EXCEPT ![t] = Expected[t]]
    /\ UNCHANGED <<confirmed, published, completeAtPublish, opened, captured>>

Confirm(t) ==
    /\ ~published /\ t \notin confirmed
    /\ stored[t] = Expected[t]
    /\ confirmed' = confirmed \cup {t}
    /\ UNCHANGED <<stored, published, completeAtPublish, opened, captured>>

Publish ==
    /\ ~published
    /\ IF Mutation = "partial_manifest" THEN confirmed # {} ELSE confirmed = Tables
    /\ published' = TRUE /\ completeAtPublish' = (stored = Expected)
    /\ UNCHANGED <<stored, confirmed, opened, captured>>

(* Represents a valid table block presented under the wrong registered path. *)
Substitute(t) ==
    /\ published /\ stored[t] = Expected[t]
    /\ stored' = [stored EXCEPT ![t] = Expected[1 - t]]
    /\ UNCHANGED <<confirmed, published, completeAtPublish, opened, captured>>

Open ==
    /\ published /\ ~opened
    /\ IF Mutation = "trust_path"
        THEN \A t \in Tables : stored[t] # 0
        ELSE stored = Expected
    /\ opened' = TRUE /\ captured' = stored
    /\ UNCHANGED <<stored, confirmed, published, completeAtPublish>>

Next == Publish \/ Open \/ (\E t \in Tables : Write(t) \/ Confirm(t) \/ Substitute(t))
Spec == Init /\ [][Next]_Vars

CompletePublication == published => completeAtPublish
AcceptedMetadataMatches == opened => captured = Expected
Safe == CompletePublication /\ AcceptedMetadataMatches
NeverOpensPublishedRoot == ~opened
=============================================================================
