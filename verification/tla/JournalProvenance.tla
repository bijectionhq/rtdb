---------------------------- MODULE JournalProvenance ----------------------------
EXTENDS Naturals, Sequences

CONSTANTS MaxEntries, MaxEpoch, Mutation
VARIABLE state

ClockSamples == 1..2
NoSample == 0
NoReport == [lsn |-> 0, original |-> FALSE, sample |-> NoSample]
Entry(epoch, original) == [epoch |-> epoch, original |-> original, sample |-> NoSample]
Positions(entries) == 1..Len(entries)

Init ==
    state = [online |-> TRUE, held |-> <<>>, frames |-> <<>>,
             writer |-> 1, commit |-> 0, pending |-> {}, report |-> NoReport,
             segment |-> FALSE, baseThrough |-> 0, replayedBase |-> FALSE,
             copied |-> {}, moved |-> {}, interrupted |-> {}, replacedSampled |-> {}]

WriteEntries(held, frames) ==
    [state EXCEPT !.held = held, !.frames = frames, !.report = NoReport,
                  !.replayedBase = FALSE]

AppendOriginal ==
    /\ state.online
    /\ Len(state.held) < MaxEntries
    /\ LET lsn == Len(state.held) + 1
           entry == Entry(state.writer, TRUE)
       IN state' = [WriteEntries(Append(state.held, entry), Append(state.frames, entry)) EXCEPT
                    !.commit = lsn - 1, !.pending = @ \cup {lsn}]

AppendCopy(epoch, sourceSample) ==
    /\ state.online
    /\ Len(state.held) < MaxEntries
    /\ epoch \in 1..state.writer
    /\ LET lsn == Len(state.held) + 1
           copy == [Entry(epoch, FALSE) EXCEPT
                     !.sample = IF Mutation = "import_move_sample" THEN sourceSample ELSE NoSample]
       IN state' = [WriteEntries(Append(state.held, copy), Append(state.frames, copy)) EXCEPT
                    !.copied = @ \cup {lsn},
                    !.moved = IF sourceSample = NoSample THEN @ ELSE @ \cup {lsn}]

MoveIn == \E epoch \in 1..MaxEpoch, sourceSample \in ClockSamples : AppendCopy(epoch, sourceSample)

Overwrite(from, epochs) ==
    /\ state.online
    /\ state.writer < MaxEpoch
    /\ from \in (state.commit + 1)..Len(state.held)
    /\ LET old == state.held
           prefix == 1..(from - 1)
           replaced == {i \in from..Len(old) : i - from + 1 \in Positions(epochs)}
           copies == [j \in Positions(epochs) |-> Entry(epochs[j], FALSE)]
           held == SubSeq(old, 1, from - 1) \o copies
           frames == SubSeq(state.frames, 1, from - 1) \o copies
       IN /\ (Mutation = "overwrite_observed" \/
              \A i \in replaced : ~old[i].original \/ old[i].epoch # epochs[i - from + 1])
          /\ state' = [WriteEntries(held, frames) EXCEPT !.writer = @ + 1,
                       !.pending = @ \cap prefix, !.copied = (@ \cap prefix) \cup (from..Len(held)),
                       !.moved = @ \cap prefix, !.interrupted = @ \cap prefix,
                       !.baseThrough = IF @ < from THEN @ ELSE from - 1,
                       !.replacedSampled = (@ \cap prefix) \cup {i \in replaced : old[i].sample # NoSample}]

Observe(lsn, sample) ==
    /\ state.online
    /\ lsn \in Positions(state.held)
    /\ state.held[lsn].sample = NoSample
    /\ ((lsn \in state.pending /\ state.held[lsn].original) \/
        (Mutation = "sample_copy" /\ ~state.held[lsn].original) \/
        (Mutation = "resample_replayed_original" /\ state.replayedBase /\ state.held[lsn].original))
    /\ LET held == [state.held EXCEPT ![lsn].sample = sample]
           frames == [state.frames EXCEPT ![lsn].sample = sample]
       IN state' = [WriteEntries(held, frames) EXCEPT !.pending = @ \ {lsn}]

SkipObservation(lsn) ==
    /\ state.online
    /\ lsn \in state.pending
    /\ state' = [state EXCEPT !.pending = @ \ {lsn}]

Compact ==
    /\ state.online
    /\ state.pending = {}
    /\ Len(state.held) > 0
    /\ LET frames == [i \in Positions(state.held) |->
              CASE Mutation = "compact_drops_provenance" -> [state.held[i] EXCEPT !.original = FALSE]
                [] Mutation = "compact_drops_sample" -> [state.held[i] EXCEPT !.sample = NoSample]
                [] OTHER -> state.held[i]]
       IN state' = [state EXCEPT !.frames = frames, !.segment = ~@, !.baseThrough = Len(frames),
                    !.replayedBase = FALSE, !.report = NoReport]

Crash ==
    /\ state.online
    /\ state' = [state EXCEPT !.online = FALSE, !.held = <<>>, !.pending = {},
                 !.interrupted = @ \cup state.pending, !.report = NoReport]

Replay ==
    /\ ~state.online
    /\ LET held == [i \in Positions(state.frames) |->
              CASE Mutation = "replay_drops_provenance" -> [state.frames[i] EXCEPT !.original = FALSE]
                [] Mutation = "replay_drops_sample" -> [state.frames[i] EXCEPT !.sample = NoSample]
                [] OTHER -> state.frames[i]]
       IN state' = [state EXCEPT !.online = TRUE, !.held = held, !.replayedBase = state.baseThrough > 0]

Report(lsn) ==
    /\ state.online
    /\ lsn \in Positions(state.held)
    /\ lsn \notin state.pending
    /\ LET entry == state.held[lsn]
       IN state' = [state EXCEPT !.report = [lsn |-> lsn, sample |-> entry.sample,
                    original |-> entry.original \/ Mutation = "report_copy_as_original"]]

Next ==
    \/ AppendOriginal \/ MoveIn \/ Compact \/ Crash \/ Replay
    \/ \E epoch \in 1..MaxEpoch : AppendCopy(epoch, NoSample)
    \/ \E i \in 1..MaxEntries :
           \/ Report(i) \/ SkipObservation(i)
           \/ \E sample \in ClockSamples : Observe(i, sample)
           \/ \E n \in 0..(MaxEntries - i + 1) :
                  \E epochs \in [1..n -> 1..(state.writer + 1)] : Overwrite(i, epochs)

Spec == Init /\ [][Next]_state

NewSamplesHaveOriginalSync ==
    [][\A i \in Positions(state.frames') :
          LET frame == state.frames'[i]
          IN (frame.sample # NoSample /\
              (i > Len(state.frames) \/ state.frames[i].sample # frame.sample \/ state.frames[i].epoch # frame.epoch)) =>
                 i \in state.pending /\ frame.original /\ i \notin state.copied' /\
                 state.frames[i].epoch = frame.epoch]_state

OverwriteProtectsObservedOriginal ==
    [][state.writer' > state.writer =>
          \A i \in Positions(state.held) \cap Positions(state.held') :
              (state.held[i].original /\ state.held[i].epoch = state.held'[i].epoch) =>
                  state.held'[i] = state.held[i]]_state

CompactionPreservesEvidence ==
    [][state.segment' # state.segment => state.frames' = state.frames /\ state.held' = state.held]_state

ReplayRestoresEvidence ==
    [][(~state.online /\ state.online') => state.held' = state.frames]_state

ReportsMatchLocalEvidence ==
    LET i == state.report.lsn
    IN i # 0 =>
         /\ state.report.original = (i \notin state.copied)
         /\ state.report.sample = state.frames[i].sample
         /\ i \in state.copied => state.report.sample = NoSample

ReportedFromReplayedBase == state.replayedBase /\ state.report.lsn \in 1..state.baseThrough

NeverReportsSampleAfterCompactedReplay ==
    ~(ReportedFromReplayedBase /\ state.report.original /\ state.report.sample # NoSample)

NeverReportsInterruptedOriginalAfterCompactedReplay ==
    ~(ReportedFromReplayedBase /\ state.report.lsn \in state.interrupted /\
      state.report.original /\ state.report.sample = NoSample)

NeverReportsMovedCopyAfterCompactedReplay ==
    ~(ReportedFromReplayedBase /\ state.report.lsn \in state.moved /\
      ~state.report.original /\ state.report.sample = NoSample)

NeverReportsReplacementOfSampledOriginal ==
    ~(state.report.lsn \in state.replacedSampled /\ ~state.report.original /\ state.report.sample = NoSample)

==================================================================================
