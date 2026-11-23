-------------------------- MODULE CaptureKeyRevocation --------------------------
EXTENDS TLC

CONSTANT Mutation

VARIABLES phase, closed, held, handle, using, exposed, exportKey, empty, lost

Vars == <<phase, closed, held, handle, using, exposed, exportKey, empty, lost>>

Init ==
    /\ phase = "open" /\ closed = FALSE /\ held = FALSE /\ handle = FALSE /\ using = FALSE
    /\ exposed = FALSE /\ exportKey = "none" /\ empty = FALSE /\ lost = FALSE

Hold ==
    /\ phase = "open" /\ ~held
    /\ held' = TRUE /\ handle' = TRUE
    /\ UNCHANGED <<phase, closed, using, exposed, exportKey, empty, lost>>

Release ==
    /\ held /\ held' = FALSE
    /\ UNCHANGED <<phase, closed, handle, using, exposed, exportKey, empty, lost>>

Invoke ==
    /\ handle /\ ~using /\ (~closed \/ Mutation = "bypass_tombstone")
    /\ using' = TRUE
    /\ exposed' = (Mutation = "export_material")
    /\ UNCHANGED <<phase, closed, held, handle, exportKey, empty, lost>>

Complete ==
    /\ using /\ using' = FALSE
    /\ UNCHANGED <<phase, closed, held, handle, exposed, exportKey, empty, lost>>

IndependentCopy ==
    /\ phase = "open" /\ held /\ exportKey = "none"
    /\ exportKey' = IF Mutation = "alias_destination" THEN "archive" ELSE "destination"
    /\ UNCHANGED <<phase, closed, held, handle, using, exposed, empty, lost>>

Prepare ==
    /\ phase = "open" /\ phase' = "prepared"
    /\ UNCHANGED <<closed, held, handle, using, exposed, exportKey, empty, lost>>

Close ==
    /\ phase = "prepared" /\ ~closed /\ ~held
    /\ (~using \/ Mutation = "ignore_active_provider")
    /\ closed' = TRUE
    /\ UNCHANGED <<phase, held, handle, using, exposed, exportKey, empty, lost>>

LoseReply ==
    /\ phase = "prepared" /\ closed /\ ~lost /\ lost' = TRUE
    /\ UNCHANGED <<phase, closed, held, handle, using, exposed, exportKey, empty>>

Publish ==
    /\ phase = "prepared" /\ (closed \/ Mutation = "publish_before_withdrawal")
    /\ phase' = "revoked"
    /\ UNCHANGED <<closed, held, handle, using, exposed, exportKey, empty, lost>>

ObserveEmpty ==
    /\ phase = "revoked" /\ ~empty /\ empty' = TRUE
    /\ UNCHANGED <<phase, closed, held, handle, using, exposed, exportKey, lost>>

Settle ==
    /\ phase = "revoked" /\ (empty \/ Mutation = "assume_inventory_empty")
    /\ phase' = "settled"
    /\ UNCHANGED <<closed, held, handle, using, exposed, exportKey, empty, lost>>

Next == Hold \/ Release \/ Invoke \/ Complete \/ IndependentCopy \/ Prepare \/ Close
     \/ LoseReply \/ Publish \/ ObserveEmpty \/ Settle

Spec == Init /\ [][Next]_Vars

NoPostClosureUse == closed => ~using
CommitHasClosedKey == phase \in {"revoked", "settled"} => closed /\ ~exposed
SettlementHasEmptyInventory == phase = "settled" => empty
IndependentCopyStaysReadable == exportKey # "none" => exportKey = "destination" \/ ~closed

NeverRecoverAfterLostReply ==
    ~(phase = "settled" /\ lost /\ handle /\ exportKey = "destination")

===============================================================================
