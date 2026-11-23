--------------------------- MODULE ScopeRetirement --------------------------
EXTENDS Naturals

CONSTANTS Writers, Readers, SettleUploads, DrainReaders, ScrubReplay
VARIABLES generation, uploads, readers, copies, objects, dirty, published, acknowledged

Vars == <<generation, uploads, readers, copies, objects, dirty, published, acknowledged>>
Generations == {0, 1}
Idle == 2

Init ==
  /\ generation = 0
  /\ uploads = [writer \in Writers |-> Idle]
  /\ readers = [reader \in Readers |-> Idle]
  /\ copies = {}
  /\ objects = {0}
  /\ dirty = {0}
  /\ published = {0}
  /\ acknowledged = FALSE

TypeOK ==
  /\ generation \in Generations
  /\ uploads \in [Writers -> Generations \cup {Idle}]
  /\ readers \in [Readers -> Generations \cup {Idle}]
  /\ copies \subseteq {reader \in Readers : readers[reader] # Idle}
  /\ objects \subseteq Generations
  /\ dirty \subseteq objects
  /\ published \subseteq objects
  /\ acknowledged \in BOOLEAN

\* Admission and retirement use the same generation authority.
AdmitUpload(writer, target) ==
  /\ target >= generation
  /\ uploads[writer] = Idle
  /\ uploads' = [uploads EXCEPT ![writer] = target]
  /\ UNCHANGED <<generation, readers, copies, objects, dirty, published, acknowledged>>

\* An operation already admitted can complete after its generation closes.
SettleUpload(writer, committed) ==
  LET target == uploads[writer] IN
  /\ target # Idle
  /\ uploads' = [uploads EXCEPT ![writer] = Idle]
  /\ objects' = IF committed THEN objects \cup {target} ELSE objects
  /\ dirty' = IF committed /\ (target = 0 \/ ~ScrubReplay) THEN dirty \cup {target} ELSE dirty
  /\ UNCHANGED <<generation, readers, copies, published, acknowledged>>

Publish ==
  /\ 1 \in objects \ published
  /\ published' = published \cup {1}
  /\ UNCHANGED <<generation, uploads, readers, copies, objects, dirty, acknowledged>>

AdmitRead(reader) ==
  /\ readers[reader] = Idle
  /\ generation \in published
  /\ readers' = [readers EXCEPT ![reader] = generation]
  /\ UNCHANGED <<generation, uploads, copies, objects, dirty, published, acknowledged>>

ReadCopy(reader) ==
  /\ readers[reader] \in objects
  /\ reader \notin copies
  /\ copies' = copies \cup {reader}
  /\ UNCHANGED <<generation, uploads, readers, objects, dirty, published, acknowledged>>

\* The reader drops its decoded pages before releasing its admission.
ReleaseRead(reader) ==
  /\ readers[reader] # Idle
  /\ readers' = [readers EXCEPT ![reader] = Idle]
  /\ copies' = copies \ {reader}
  /\ UNCHANGED <<generation, uploads, objects, dirty, published, acknowledged>>

Retire ==
  /\ generation = 0
  /\ 1 \in published
  /\ generation' = 1
  /\ UNCHANGED <<uploads, readers, copies, objects, dirty, published, acknowledged>>

EraseObjects ==
  /\ generation = 1
  /\ 0 \in objects
  /\ ~SettleUploads \/ (\A writer \in Writers : uploads[writer] # 0)
  /\ ~DrainReaders \/ (\A reader \in Readers : readers[reader] # 0)
  /\ objects' = objects \ {0}
  /\ dirty' = dirty \ {0}
  /\ published' = published \ {0}
  /\ UNCHANGED <<generation, uploads, readers, copies, acknowledged>>

Acknowledge ==
  /\ generation = 1
  /\ ~acknowledged
  /\ 0 \notin objects
  /\ ~SettleUploads \/ (\A writer \in Writers : uploads[writer] # 0)
  /\ ~DrainReaders \/ (\A reader \in Readers : readers[reader] # 0)
  /\ acknowledged' = TRUE
  /\ UNCHANGED <<generation, uploads, readers, copies, objects, dirty, published>>

Next ==
  \/ \E writer \in Writers, target \in Generations : AdmitUpload(writer, target)
  \/ \E writer \in Writers, committed \in BOOLEAN : SettleUpload(writer, committed)
  \/ \E reader \in Readers : AdmitRead(reader) \/ ReadCopy(reader) \/ ReleaseRead(reader)
  \/ Publish \/ Retire \/ EraseObjects \/ Acknowledge
  \/ acknowledged /\ UNCHANGED Vars

Spec == Init /\ [][Next]_Vars

NoRetiredStorage == acknowledged => 0 \notin objects
NoRetiredReaderCopy == acknowledged => \A reader \in copies : readers[reader] # 0
NoRestoredErasedPayload == 1 \notin dirty

ReleaseSpec ==
  /\ Spec
  /\ WF_Vars(Retire)
  /\ \A writer \in Writers : WF_Vars(\E committed \in BOOLEAN : SettleUpload(writer, committed))
  /\ \A reader \in Readers : WF_Vars(ReleaseRead(reader))
  /\ WF_Vars(EraseObjects)
  /\ WF_Vars(Acknowledge)

ReplacementPublished == <>(1 \in published)
CompletesRetirement == ReplacementPublished => <>acknowledged

=============================================================================
