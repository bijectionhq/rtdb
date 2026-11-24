---------------------------- MODULE ActiveLogletReadIndex ----------------------------
EXTENDS Integers, FiniteSets

CONSTANT Mutation

Old == {"a", "b", "c"}
New == {"d", "e", "f"}
Journals == Old \cup New
Digests == {"x", "y"}
Genesis == [position |-> 0, digest |-> "genesis"]
Cut(digest) == [position |-> 1, digest |-> digest]
EmptyReply == [nonce |-> 0, route |-> "none", cut |-> Genesis]
Quorum(voters, roster) == voters \subseteq roster /\ Cardinality(voters) >= 2
Handoff(cut, destination) == [cut |-> cut, source |-> Old, destination |-> destination,
                                checkpoint |-> cut, epoch |-> 2]

VARIABLE s

ReceiptCut == IF s.acknowledged THEN Cut(s.digest) ELSE Genesis
Active == CASE s.phase = "old" -> Old \ s.sealed
            [] s.phase = "new" -> New
            [] OTHER -> {}
Roster == IF s.phase = "new" THEN New ELSE Old
DurableOn(cut, roster) == Quorum({j \in roster : s.installed[j] = cut}, roster)
MaxCut(cuts) == CHOOSE c \in cuts : \A d \in cuts : d.position <= c.position
Settled == s.admission # "guarded" \/ s.outcome # "unknown"
Higher(a, b) == IF a.position > b.position THEN a ELSE b

Initial ==
    [phase |-> "old", admission |-> "none", digest |-> "x",
         staged |-> {}, copied |-> {}, timely |-> {}, observed |-> {},
         sealed |-> {}, online |-> Journals, expired |-> FALSE,
         writer |-> TRUE, certified |-> FALSE, acknowledged |-> FALSE,
         readerHelp |-> FALSE,
         outcome |-> "unknown", prepared |-> Genesis,
         handoffVotes |-> [j \in Old |-> {}], handoff |-> Handoff(Genesis, New),
         installed |-> [j \in Journals |-> Genesis], stable |-> Genesis,
         applied |-> {Genesis}, holds |-> {}, output |-> Genesis,
         read |-> [step |-> "idle", nonce |-> 0, route |-> "none",
                   phaseAtSample |-> "none", minimum |-> Genesis, cut |-> Genesis,
                   probes |-> [j \in Journals |-> EmptyReply], captured |-> Genesis]]

Init == s = Initial

Offer(admission, digest) ==
    /\ s.phase = "old" /\ s.writer /\ s.admission = "none" /\ s.sealed = {}
    /\ s' = [s EXCEPT !.admission = admission, !.digest = digest]

Stage(journal, eligible, retain) ==
    /\ s.writer /\ s.phase = "old" /\ s.admission # "none"
    /\ journal \in (Active \cap s.online) \ s.staged
    /\ ~eligible \/ s.admission = "guarded"
    /\ ~retain \/ s.admission = "guarded"
    /\ s' = [s EXCEPT !.staged = @ \cup {journal},
                     !.timely = IF eligible /\ ~s.expired THEN @ \cup {journal} ELSE @,
                     !.observed = IF retain THEN @ \cup {journal} ELSE @]

Copy(journal) ==
    /\ s.phase = "old" /\ s.admission = "guarded" /\ s.staged # {}
    /\ journal \in (Active \cap s.online) \ (s.copied \cup s.staged)
    /\ s' = [s EXCEPT !.copied = @ \cup {journal}]

RecoverOrdinary ==
    /\ s.phase = "sealed" /\ s.admission = "ordinary"
    /\ s.staged \cap s.sealed # {} /\ ~s.certified
    /\ Quorum(s.sealed \cap s.online, Old)
    /\ s' = [s EXCEPT !.staged = @ \cup (s.sealed \cap s.online)]

Evidence ==
    IF s.admission = "guarded"
    THEN (s.timely \cap s.observed \cap s.online)
         \cup (IF Mutation = "copy_votes" THEN s.copied ELSE {})
         \cup (IF Mutation = "equal_cutoff_votes" /\ s.expired
                THEN (s.observed \cap s.staged) ELSE {})
    ELSE s.staged \cap s.online

Certify ==
    /\ s.phase \in {"old", "sealed"} /\ ~s.certified /\ s.admission # "none"
    /\ s.writer \/ s.phase = "sealed"
    /\ s.outcome # "excluded"
    /\ Quorum(Evidence, Old)
    /\ s' = [s EXCEPT !.certified = TRUE, !.outcome = "accept"]

Install(journal) ==
    /\ s.phase \in {"old", "sealed"} /\ journal \in Old \cap s.online
    /\ s.certified
    /\ (s.phase = "sealed" /\ journal \in s.sealed)
        \/ (journal \in Active /\ (s.writer \/ s.read.cut = Cut(s.digest)))
    /\ s.installed[journal] = Genesis
    /\ LET installed == [s.installed EXCEPT ![journal] = Cut(s.digest)]
           stable == Quorum({j \in Old : installed[j] = Cut(s.digest)}, Old)
       IN s' = [s EXCEPT !.installed = installed,
                         !.stable = IF stable THEN Cut(s.digest) ELSE @,
                         !.readerHelp = @ \/ (stable /\ ~s.writer /\ s.phase = "old")]

Acknowledge ==
    /\ ~s.acknowledged /\ s.writer
    /\ s.stable = Cut(s.digest)
       \/ (Mutation = "ack_payload" /\ Quorum(s.staged, Old))
    /\ s' = [s EXCEPT !.acknowledged = TRUE]

Seal(journal) ==
    /\ s.phase = "old" /\ journal \in (Old \cap s.online) \ s.sealed
    /\ LET sealed == s.sealed \cup {journal}
       IN s' = [s EXCEPT !.sealed = sealed,
                         !.phase = IF Quorum(sealed, Old) THEN "sealed" ELSE @]

Exclude ==
    /\ s.phase = "sealed" /\ s.admission = "guarded" /\ ~Settled /\ ~s.certified
    /\ LET negative == {j \in (s.sealed \cap s.online) :
                           j \notin s.staged \/ (j \in s.observed /\ j \notin s.timely)}
       IN ~Quorum(Old \ negative, Old) \/ (Mutation = "silence_excludes" /\ s.expired)
    /\ s' = [s EXCEPT !.outcome = "excluded"]

HandoffReady ==
    /\ s.phase = "sealed"
    /\ (Settled \/ Mutation = "activate_unknown")
    /\ (~s.certified \/ DurableOn(Cut(s.digest), Old))
    /\ (s.admission # "ordinary" \/ s.staged \cap s.sealed = {} \/ s.certified)

SignHandoff(journal) ==
    /\ HandoffReady
    /\ journal \in s.sealed \cap s.online /\ s.handoffVotes[journal] = {}
    /\ s' = [s EXCEPT !.handoffVotes[journal] = {Handoff(s.stable, New)}]

SignedFor(handoff) == {j \in Old : handoff \in s.handoffVotes[j]}

Prepare ==
    /\ HandoffReady
    /\ LET proposal == Handoff(s.stable, New)
       IN /\ \/ Quorum(SignedFor(proposal), Old)
             \/ Mutation = "one_handoff_vote" /\ SignedFor(proposal) # {}
          /\ s' = [s EXCEPT !.phase = "prepared", !.prepared = s.stable,
                         !.handoff = IF Mutation = "swap_activation_roster" THEN Handoff(s.stable, Old)
                                      ELSE IF Mutation = "swap_handoff_checkpoint"
                                           THEN [proposal EXCEPT !.checkpoint = Cut("y")]
                                           ELSE proposal]

Activate ==
    /\ s.phase = "prepared" /\ Quorum(s.online \cap New, New)
    /\ (Quorum(SignedFor(s.handoff), Old) /\ s.handoff = Handoff(s.prepared, New))
        \/ Mutation \in {"swap_activation_roster", "swap_handoff_checkpoint", "one_handoff_vote"}
    /\ s' = [s EXCEPT !.phase = "new",
               !.installed = [j \in Journals |-> IF j \in New \cap s.online
                             THEN (IF Mutation = "lose_floor" THEN Genesis ELSE s.prepared)
                             ELSE s.installed[j]]]

Begin(route) ==
    /\ s.read.step \in {"idle", "served"} /\ s.read.nonce < 2
    /\ s' = [s EXCEPT !.read = [step |-> "probing", nonce |-> s.read.nonce + 1,
              route |-> route, phaseAtSample |-> "none", captured |-> Genesis,
              minimum |-> Higher(Higher(s.stable, s.output), ReceiptCut), cut |-> Genesis,
              probes |-> [j \in Journals |-> EmptyReply]]]

Probe(journal) ==
    /\ s.read.step = "probing" /\ s.read.probes[journal] = EmptyReply
    /\ journal \in s.online
    /\ (s.read.route = s.phase /\ journal \in Active)
       \/ (Mutation = "prepared_vote" /\ s.phase = "prepared" /\ s.read.route = "new" /\ journal \in New)
       \/ (Mutation = "replay_probe" /\ s.phase = "new" /\ s.read.route = "new" /\ journal \in New)
    /\ s' = [s EXCEPT !.read.probes[journal] =
              [nonce |-> s.read.nonce, route |-> s.read.route,
               cut |-> IF Mutation = "replay_probe" /\ s.phase = "new"
                       THEN Genesis ELSE s.installed[journal]]]

Select ==
    /\ s.read.step = "probing"
    /\ LET roster == IF s.read.route = "old" THEN Old ELSE New
           replies == {j \in roster : s.read.probes[j].nonce = s.read.nonce
                                      /\ s.read.probes[j].route = s.read.route}
       IN /\ Quorum(replies, roster)
          /\ LET cut == MaxCut({s.read.probes[j].cut : j \in replies})
              IN s' = [s EXCEPT !.read.step = "selected", !.read.phaseAtSample = s.phase,
                                 !.read.cut = IF Mutation = "wrong_digest" /\ cut.position = 1
                                              THEN Cut(IF cut.digest = "x" THEN "y" ELSE "x") ELSE cut]

Protect ==
    /\ s.read.step = "selected"
    /\ (s.read.cut.position <= s.stable.position \/ Mutation = "single_install")
    /\ s' = [s EXCEPT !.read.step = "held", !.holds = @ \cup {s.read.cut}]

Apply ==
    /\ s.stable.position > 0 /\ s.stable \notin s.applied
    /\ s' = [s EXCEPT !.applied = @ \cup {s.stable}]

Serve ==
    /\ s.read.step = "held"
    /\ (s.read.cut \in s.holds \/ Mutation = "skip_hold")
    /\ (s.read.cut \in s.applied \/ Mutation \in {"skip_apply", "single_install", "wrong_digest"})
    /\ s' = [s EXCEPT !.read.step = "served",
              !.read.captured = s.read.cut, !.output = s.read.cut, !.holds = {}]

Trim(journal) ==
    /\ s.installed[journal].position > 0
    /\ IF Mutation = "forget_witness" THEN s' = [s EXCEPT !.installed[journal] = Genesis]
       ELSE UNCHANGED s

LoseHold ==
    /\ Mutation = "skip_hold" /\ s.read.step = "held" /\ s.holds # {}
    /\ s' = [s EXCEPT !.holds = {}]

Next ==
    \/ \E admission \in {"ordinary", "guarded"}, d \in Digests : Offer(admission, d)
    \/ \E j \in Journals, eligible \in BOOLEAN, retain \in BOOLEAN : Stage(j, eligible, retain)
    \/ \E j \in Journals : Copy(j) \/ Install(j) \/ Seal(j) \/ SignHandoff(j) \/ Probe(j) \/ Trim(j)
    \/ \E j \in Journals : (j \in s.online /\ s' = [s EXCEPT !.online = @ \ {j}])
    \/ \E j \in Journals : (j \notin s.online /\ s' = [s EXCEPT !.online = @ \cup {j}])
    \/ (s.writer /\ s' = [s EXCEPT !.writer = FALSE])
    \/ (~s.expired /\ s' = [s EXCEPT !.expired = TRUE])
    \/ \E route \in {"old", "new"} : Begin(route)
    \/ RecoverOrdinary \/ Certify \/ Acknowledge \/ Exclude \/ Prepare \/ Activate \/ Select \/ Protect \/ Apply \/ Serve \/ LoseHold

Spec == Init /\ [][Next]_s

StableCutCovered ==
    s.read.step \in {"selected", "held", "served"} => s.read.cut.position >= s.read.minimum.position
ReadReadMonotone ==
    s.read.step = "served" => s.output.position >= s.read.minimum.position
ExactDecision ==
    s.read.step \in {"selected", "held", "served"} /\ s.read.cut.position = 1 => s.read.cut = Cut(s.digest)
ReadStabilized == s.read.step = "served" => s.output.position <= s.stable.position
ProtectedAppliedCapture ==
    s.read.step = "served" => s.read.captured \in s.applied /\ s.read.captured = s.read.cut
HoldAtExposure ==
    s.read.step = "held" => s.read.cut \in s.holds
ActiveAuthority ==
    s.read.step \in {"selected", "held", "served"} /\ s.read.route = "new" =>
    s.read.phaseAtSample = "new"
ActivationContinuity ==
    s.phase = "new" => Quorum({j \in New : s.installed[j] = s.prepared}, New)
PortableActivation ==
    s.phase = "new" =>
        /\ s.handoff = Handoff(s.prepared, New)
        /\ Quorum(SignedFor(s.handoff), Old)
StableWitnessRetained ==
    s.phase = "old" /\ s.stable.position = 1 => DurableOn(s.stable, Old)
StrictOriginalAcceptance ==
    s.admission = "guarded" /\ s.certified => Quorum(s.timely, Old)
PermanentExclusion == s.outcome = "excluded" => ~Quorum(s.timely, Old)
ClosedWhileUnknown == s.phase \in {"prepared", "new"} => Settled

NoStalledTailRead ==
    ~(s.phase = "old" /\ s.read.step = "served" /\ ~s.writer /\ s.staged = {"a"} /\ "c" \notin s.online)
NoReaderStabilization ==
    ~(s.phase = "old" /\ s.read.step = "served" /\ s.readerHelp /\ s.output.position = 1)
NoExpiredUnknown ==
    ~(s.phase = "sealed" /\ s.admission = "guarded" /\ s.expired /\ ~Settled /\ Quorum(s.timely, Old))

ProgressInit ==
    s = [Initial EXCEPT !.admission = "ordinary", !.writer = FALSE,
                        !.staged = {"a"}, !.online = {"a", "b"}]
ProgressNext ==
    Begin("old") \/ Probe("a") \/ Probe("b") \/ Select \/ Protect \/ Serve
ProgressSpec == ProgressInit /\ [][ProgressNext]_s /\ WF_s(ProgressNext)
SurvivingQuorumProgress == <>(s.read.step = "served")

=====================================================================================
