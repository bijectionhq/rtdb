------------------------ MODULE ArchiveCustodyTransitions ------------------------
CONSTANT Mutation
VARIABLES source, readStarted, readActive, cancelled, resident, retiredWhileReading,
          archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline

Vars == <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
          archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

Required == {"root-pack", "trie"}
Publications == Required \cup {"unrelated"}

Init ==
    /\ source = "discovery" /\ resident = Publications
    /\ readStarted = FALSE /\ readActive = FALSE /\ cancelled = FALSE /\ retiredWhileReading = FALSE
    /\ archive = "open" /\ archiveRead = FALSE /\ keyPresent = TRUE /\ contentsPresent = TRUE
    /\ independentKey = TRUE /\ acceptedDeadline = FALSE

Bind ==
    /\ source = "discovery" /\ Required \subseteq resident
    /\ source' = IF Mutation = "skip_exact_binding" THEN "unprotected" ELSE "exact"
    /\ UNCHANGED <<readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

StartRead ==
    /\ source \in {"exact", "unprotected"} /\ ~readStarted /\ ~cancelled
    /\ readStarted' = TRUE /\ readActive' = TRUE
    /\ UNCHANGED <<source, cancelled, resident, retiredWhileReading,
                   archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

Cancel ==
    /\ readActive /\ ~cancelled
    /\ cancelled' = TRUE
    /\ source' = IF Mutation = "release_before_drain" THEN "released" ELSE source
    /\ UNCHANGED <<readStarted, readActive, resident, retiredWhileReading,
                   archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

Drain ==
    /\ readActive
    /\ readActive' = FALSE /\ source' = "released"
    /\ UNCHANGED <<readStarted, cancelled, resident, retiredWhileReading,
                   archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

Retire ==
    /\ \E publication \in resident :
          /\ source # "discovery"
          /\ source # "exact" \/ publication \notin Required
          /\ resident' = resident \ {publication}
          /\ retiredWhileReading' = (retiredWhileReading \/ (publication = "unrelated" /\ readActive))
    /\ UNCHANGED <<source, readStarted, readActive, cancelled,
                   archive, archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

StartArchiveRead ==
    /\ archive = "open" /\ ~archiveRead
    /\ archiveRead' = TRUE
    /\ UNCHANGED <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archive, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

DrainArchiveRead ==
    /\ archiveRead /\ archiveRead' = FALSE
    /\ UNCHANGED <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archive, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

Prepare ==
    /\ archive = "open" /\ archive' = "prepared"
    /\ UNCHANGED <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archiveRead, keyPresent, contentsPresent, independentKey, acceptedDeadline>>

PromiseDeadline ==
    /\ Mutation = "unqualified_deadline" /\ ~acceptedDeadline
    /\ acceptedDeadline' = TRUE
    /\ UNCHANGED <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archive, archiveRead, keyPresent, contentsPresent, independentKey>>

Withdraw ==
    /\ archive = "prepared"
    /\ ~archiveRead \/ Mutation = "withdraw_before_drain"
    /\ archive' = "withdrawn" /\ keyPresent' = FALSE
    /\ independentKey' = (independentKey /\ Mutation # "withdraw_independent_key")
    /\ UNCHANGED <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archiveRead, contentsPresent, acceptedDeadline>>

Settle ==
    /\ archive = "withdrawn" \/ (archive = "prepared" /\ Mutation = "settle_before_withdrawal")
    /\ archive' = "settled" /\ contentsPresent' = FALSE
    /\ UNCHANGED <<source, readStarted, readActive, cancelled, resident, retiredWhileReading,
                   archiveRead, keyPresent, independentKey, acceptedDeadline>>

Next == Bind \/ StartRead \/ Cancel \/ Drain \/ Retire \/ StartArchiveRead \/ DrainArchiveRead
        \/ Prepare \/ PromiseDeadline \/ Withdraw \/ Settle
Spec == Init /\ [][Next]_Vars

SourceRetained == source \in {"discovery", "exact", "unprotected"} \/ readActive => Required \subseteq resident
ArchiveReadRetained == archiveRead => keyPresent /\ contentsPresent
SettlementHasWithdrawal == archive = "settled" => ~keyPresent /\ ~contentsPresent
IndependentCustody == independentKey
QualifiedDeadlinesOnly == ~acceptedDeadline
Safe == SourceRetained /\ ArchiveReadRetained /\ SettlementHasWithdrawal /\ IndependentCustody /\ QualifiedDeadlinesOnly

NeverRetiresUnrelated == ~retiredWhileReading
NeverSettles == archive # "settled"
===================================================================================
