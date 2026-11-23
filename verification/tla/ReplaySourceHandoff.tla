-------------------------- MODULE ReplaySourceHandoff --------------------------
EXTENDS TLC

CONSTANT Mutation

VARIABLES l0, journal, copy, staged, synced, published, pendingUpload, reader

Vars == <<l0, journal, copy, staged, synced, published, pendingUpload, reader>>
Required == {"primary", "exact-events"}
Members == Required \cup {"foreign-events"}

Init ==
    /\ l0 = TRUE /\ journal = TRUE /\ pendingUpload = TRUE
    /\ copy = "idle" /\ reader = "closed"
    /\ staged = {} /\ synced = {} /\ published = FALSE

BeginCopy ==
    /\ copy = "idle" /\ journal
    /\ copy' = "reading"
    /\ UNCHANGED <<l0, journal, staged, synced, published, pendingUpload, reader>>

ReadMember ==
    /\ copy = "reading" /\ journal
    /\ \E member \in Members \ staged :
        staged' = staged \cup {member}
    /\ UNCHANGED <<l0, journal, copy, synced, published, pendingUpload, reader>>

CloseCopy ==
    /\ copy = "reading"
    /\ copy' = "closing"
    /\ UNCHANGED <<l0, journal, staged, synced, published, pendingUpload, reader>>

DrainCopy ==
    /\ copy = "closing"
    /\ copy' = "idle"
    /\ UNCHANGED <<l0, journal, staged, synced, published, pendingUpload, reader>>

SyncMember ==
    /\ \E member \in staged \ synced :
        synced' = synced \cup {member}
    /\ UNCHANGED <<l0, journal, copy, staged, published, pendingUpload, reader>>

CrashBeforeSync ==
    /\ staged # synced /\ copy = "idle"
    /\ staged' = synced
    /\ UNCHANGED <<l0, journal, copy, synced, published, pendingUpload, reader>>

Publish ==
    /\ ~published
    /\ CASE Mutation = "publish_unsynced" -> Required \subseteq staged
         [] Mutation = "accept_foreign_cut" -> {"primary", "foreign-events"} \subseteq synced
         [] OTHER -> Required \subseteq synced
    /\ published' = TRUE
    /\ UNCHANGED <<l0, journal, copy, staged, synced, pendingUpload, reader>>

ConfirmUpload ==
    /\ pendingUpload /\ l0
    /\ pendingUpload' = FALSE
    /\ UNCHANGED <<l0, journal, copy, staged, synced, published, reader>>

OpenReader ==
    /\ reader = "closed"
    /\ reader' = IF published THEN "archive" ELSE "original"
    /\ UNCHANGED <<l0, journal, copy, staged, synced, published, pendingUpload>>

CloseReader ==
    /\ reader # "closed"
    /\ reader' = "closed"
    /\ UNCHANGED <<l0, journal, copy, staged, synced, published, pendingUpload>>

RetireL0 ==
    /\ l0 /\ published
    /\ reader # "original" \/ Mutation = "ignore_original_reader"
    /\ ~pendingUpload \/ Mutation = "ignore_confirmation"
    /\ l0' = FALSE
    /\ UNCHANGED <<journal, copy, staged, synced, published, pendingUpload, reader>>

TrimJournal ==
    /\ journal /\ published
    /\ copy = "idle" \/ Mutation = "trim_before_drain"
    /\ journal' = FALSE
    /\ UNCHANGED <<l0, copy, staged, synced, published, pendingUpload, reader>>

Next ==
    \/ BeginCopy \/ ReadMember \/ CloseCopy \/ DrainCopy
    \/ SyncMember \/ CrashBeforeSync \/ Publish \/ ConfirmUpload
    \/ OpenReader \/ CloseReader \/ RetireL0 \/ TrimJournal

Spec == Init /\ [][Next]_Vars
ArchiveComplete == published => Required \subseteq synced
OriginalReadable == (~published \/ reader = "original" \/ pendingUpload) => l0
CopyReadable == copy # "idle" => journal
NeverReadsArchiveAfterOriginalRetirement == l0 \/ journal \/ reader # "archive"
===============================================================================
