----------------------- MODULE HostedReplayPackRetirement -----------------------
EXTENDS Integers

CONSTANT Mutation

VARIABLES proof, archive, retained, journal, reader, preparation, descendant,
          phase, pinned, pending, original, successor, replacement, reopened

vars == <<proof, archive, retained, journal, reader, preparation, descendant,
          phase, pinned, pending, original, successor, replacement, reopened>>

Init ==
    /\ proof = "none" /\ archive = "none" /\ retained = {}
    /\ journal = "required" /\ reader = "closed" /\ preparation = FALSE /\ descendant = FALSE
    /\ phase = "open" /\ pinned = "none" /\ pending = 0
    /\ original = TRUE /\ successor = TRUE /\ replacement = FALSE /\ reopened = FALSE

InstallArchive ==
    /\ archive = "none" /\ archive' = "a" /\ retained' = {"a"}
    /\ UNCHANGED <<proof, journal, reader, preparation, descendant, phase, pinned,
                    pending, original, successor, replacement, reopened>>

AdvanceArchive ==
    /\ archive = "a" /\ archive' = "b" /\ retained' = retained \cup {"b"}
    /\ UNCHANGED <<proof, journal, reader, preparation, descendant, phase, pinned,
                    pending, original, successor, replacement, reopened>>

DisposeArchive ==
    \E a \in retained:
        /\ a # archive
        /\ a # pinned \/ Mutation = "release_pinned_archive"
        /\ a # reader
        /\ retained' = retained \ {a}
        /\ UNCHANGED <<proof, archive, journal, reader, preparation, descendant,
                        phase, pinned, pending, original, successor, replacement, reopened>>

AdmitReader ==
    /\ reader = "closed"
    /\ reader' = IF archive = "none" THEN "original" ELSE archive
    /\ reopened' = (reopened \/ ~original)
    /\ UNCHANGED <<proof, archive, retained, journal, preparation, descendant,
                    phase, pinned, pending, original, successor, replacement>>

AdmitRawRead ==
    /\ reader = "closed" /\ original
    /\ phase \in {"open", "reserved"} \/ Mutation = "ignore_read_fence"
    /\ reader' = "original"
    /\ UNCHANGED <<proof, archive, retained, journal, preparation, descendant,
                    phase, pinned, pending, original, successor, replacement, reopened>>

DrainReader ==
    /\ reader # "closed" /\ reader' = "closed"
    /\ UNCHANGED <<proof, archive, retained, journal, preparation, descendant,
                    phase, pinned, pending, original, successor, replacement, reopened>>

PrepareFork ==
    /\ phase = "open" /\ original /\ ~preparation
    /\ preparation' = TRUE
    /\ UNCHANGED <<proof, archive, retained, journal, reader, descendant, phase,
                    pinned, pending, original, successor, replacement, reopened>>

PublishFork ==
    /\ phase = "open" /\ preparation
    /\ preparation' = FALSE /\ descendant' = TRUE
    /\ UNCHANGED <<proof, archive, retained, journal, reader, phase, pinned,
                    pending, original, successor, replacement, reopened>>

WithdrawFork ==
    /\ phase = "open" /\ descendant /\ descendant' = FALSE
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, phase,
                    pinned, pending, original, successor, replacement, reopened>>

CertifyPrefix ==
    /\ journal = "required" /\ journal' = "covered"
    /\ UNCHANGED <<proof, archive, retained, reader, preparation, descendant,
                    phase, pinned, pending, original, successor, replacement, reopened>>

Reserve ==
    /\ phase = "open" /\ original /\ ~preparation
    /\ phase' = "reserved"
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    pinned, pending, original, successor, replacement, reopened>>

ObservePack ==
    /\ phase = "reserved" /\ archive \in retained /\ proof = "none"
    /\ proof' \in {"exact", "foreign-version", "missing-member"}
    /\ pinned' = archive
    /\ UNCHANGED <<archive, retained, journal, reader, preparation, descendant,
                    phase, pending, original, successor, replacement, reopened>>

FenceOwner ==
    /\ phase = "reserved" /\ archive = pinned /\ reader = "closed" /\ ~descendant
    /\ proof = "exact"
       \/ (proof = "foreign-version" /\ Mutation = "skip_version_match")
       \/ (proof = "missing-member" /\ Mutation = "skip_member")
    /\ journal = "covered" \/ Mutation = "skip_prefix"
    /\ phase' = "fenced"
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    pinned, pending, original, successor, replacement, reopened>>

Authorize ==
    /\ phase = "fenced" /\ phase' = "authorized"
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    pinned, pending, original, successor, replacement, reopened>>

Abandon ==
    /\ phase \in {"reserved", "fenced"}
       \/ (phase = "authorized" /\ Mutation = "abandon_authorized")
    /\ phase' = "open" /\ proof' = "none" /\ pinned' = "none"
    /\ UNCHANGED <<archive, retained, journal, reader, preparation, descendant,
                    pending, original, successor, replacement, reopened>>

Submit ==
    /\ phase = "authorized" /\ pending < 2
    /\ pending' = pending + 1
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    phase, pinned, original, successor, replacement, reopened>>

PublishReplacement ==
    /\ ~replacement /\ replacement' = TRUE
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    phase, pinned, pending, original, successor, reopened>>

ProviderEffect ==
    /\ pending > 0 /\ pending' = pending - 1
    /\ original' = FALSE
    /\ successor' = (successor /\ Mutation # "erase_successor")
    /\ replacement' = (replacement /\ Mutation # "erase_current")
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    phase, pinned, reopened>>

Settle ==
    /\ phase = "authorized"
    /\ ~original \/ Mutation = "settle_before_absence"
    /\ phase' = "settled" /\ pinned' = "none"
    /\ UNCHANGED <<proof, archive, retained, journal, reader, preparation, descendant,
                    pending, original, successor, replacement, reopened>>

Next == InstallArchive \/ AdvanceArchive \/ DisposeArchive \/ AdmitReader \/ DrainReader
    \/ AdmitRawRead \/ PrepareFork \/ PublishFork \/ WithdrawFork \/ CertifyPrefix \/ Reserve
    \/ ObservePack \/ FenceOwner \/ Authorize \/ Abandon \/ Submit \/ PublishReplacement
    \/ ProviderEffect \/ Settle

Spec == Init /\ [][Next]_vars

CoveredPack == ~original => proof = "exact" /\ archive \in retained /\ journal = "covered"
DependenciesIntact == ~original => ~descendant /\ ~preparation /\ reader # "original"
CustodyRetained == pinned # "none" => pinned \in retained
AuthorizedUntilAbsent == pending > 0 /\ original => phase = "authorized"
SuccessorSurvives == successor
ReplacementSurvives == [][replacement => replacement']_vars
NeverReopens == ~reopened
==================================================================================
