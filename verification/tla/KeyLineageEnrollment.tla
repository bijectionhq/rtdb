-------------------------- MODULE KeyLineageEnrollment --------------------------
CONSTANT Mutation

Attempts == {"first", "second"}
None == "none"

VARIABLES anchor, key, ciphertext, directoryHistory, retired, opened
Vars == <<anchor, key, ciphertext, directoryHistory, retired, opened>>

Init ==
    /\ anchor = <<"unclaimed", None>> /\ key = None /\ ciphertext = None
    /\ directoryHistory = FALSE /\ retired = FALSE /\ opened = None

Reserve(attempt) ==
    /\ anchor[1] = "unclaimed"
       \/ (Mutation = "directory_only" /\ ~directoryHistory /\ key = None /\ anchor[1] = "active")
       \/ (Mutation = "forget_tombstone" /\ anchor[1] = "withdrawn")
    /\ anchor' = <<"pending", attempt>>
    /\ UNCHANGED <<key, ciphertext, directoryHistory, retired, opened>>

Install(attempt) ==
    /\ anchor[1] = "pending"
    /\ attempt = anchor[2] \/ Mutation = "unbound_install"
    /\ anchor' = <<"active", anchor[2]>> /\ key' = attempt
    /\ UNCHANGED <<ciphertext, directoryHistory, retired, opened>>

SealCiphertext ==
    /\ anchor[1] = "active" /\ key # None /\ ciphertext = None
    /\ ciphertext' = key /\ directoryHistory' = TRUE
    /\ UNCHANGED <<anchor, key, retired, opened>>

LoseDirectoryHistory ==
    /\ directoryHistory /\ directoryHistory' = FALSE /\ opened' = None
    /\ UNCHANGED <<anchor, key, ciphertext, retired>>

LoseKey ==
    /\ key # None /\ key' = None
    /\ UNCHANGED <<anchor, ciphertext, directoryHistory, retired, opened>>

Open ==
    /\ anchor[1] = "active" /\ key = anchor[2] /\ key # None /\ opened = None
    /\ opened' = key
    /\ UNCHANGED <<anchor, key, ciphertext, directoryHistory, retired>>

Withdraw ==
    /\ anchor[1] = "active"
    /\ anchor' = <<"withdrawn", anchor[2]>> /\ key' = None /\ retired' = TRUE
    /\ UNCHANGED <<ciphertext, directoryHistory, opened>>

Next ==
    \/ \E attempt \in Attempts : Reserve(attempt) \/ Install(attempt)
    \/ SealCiphertext \/ LoseDirectoryHistory \/ LoseKey \/ Open \/ Withdraw
Spec == Init /\ [][Next]_Vars

InstalledKeyMatchesAnchor == key # None => anchor = <<"active", key>>
AnchorBindsCiphertext == ciphertext # None => anchor[2] = ciphertext
RetirementIsTerminal == retired => anchor[1] = "withdrawn" /\ key = None
NeverReopensWithoutDirectoryHistory ==
    ~(ciphertext # None /\ ~directoryHistory /\ opened = ciphertext)

================================================================================
