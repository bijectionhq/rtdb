------------------------- MODULE InstalledCheckpoint -------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    Mutation
VARIABLES
    \* @type: Int;
    current,
    \* @type: Int;
    installed,
    \* @type: Bool;
    published,
    \* @type: Set(Str);
    durable,
    \* @type: Int;
    reader,
    \* @type: Set(Str);
    deleting

Vars == <<current, installed, published, durable, reader, deleting>>
Cuts == {0, 1}
NoReader == 2
Live(cut) == IF cut = 0 THEN {"old-live"} ELSE {"replacement"}
Pending(cut) == IF cut = 0 THEN {"old-nascent"} ELSE {}
Required(cut) == Live(cut) \cup Pending(cut)
Objects == Required(0) \cup Required(1)

Init ==
    /\ current = 0
    /\ installed = 0
    /\ published = FALSE
    /\ durable = Required(0)
    /\ reader = NoReader
    /\ deleting = {}

TypeOK ==
    /\ current \in Cuts
    /\ installed \in Cuts
    /\ published \in BOOLEAN
    /\ durable \subseteq Objects
    /\ reader \in Cuts \cup {NoReader}
    /\ deleting \subseteq durable

WriteReplacement ==
    /\ "replacement" \notin durable
    /\ durable' = durable \cup {"replacement"}
    /\ UNCHANGED <<current, installed, published, reader, deleting>>

Supersede ==
    /\ current = 0
    /\ Required(1) \subseteq durable
    /\ current' = 1
    /\ UNCHANGED <<installed, published, durable, reader, deleting>>

PublishCheckpoint ==
    /\ current = 1
    /\ ~published
    /\ published' = TRUE
    /\ UNCHANGED <<current, installed, durable, reader, deleting>>

AdoptCheckpoint ==
    /\ published
    /\ installed = 0
    /\ installed' = 1
    /\ UNCHANGED <<current, published, durable, reader, deleting>>

OpenReader ==
    /\ reader = NoReader
    /\ reader' = current
    /\ UNCHANGED <<current, installed, published, durable, deleting>>

CloseReader ==
    /\ reader # NoReader
    /\ reader' = NoReader
    /\ UNCHANGED <<current, installed, published, durable, deleting>>

ReaderFiles == IF reader = NoReader THEN {} ELSE Live(reader)

CheckpointFiles ==
    CASE Mutation = "follow_current" -> Required(current)
      [] Mutation = "ignore_nascent" -> Live(installed)
      [] OTHER -> Required(installed)

Authorize(file) ==
    /\ file \in Required(0) \ Required(current)
    /\ file \in durable \ deleting
    /\ file \notin (CheckpointFiles \cup ReaderFiles)
    /\ deleting' = deleting \cup {file}
    /\ UNCHANGED <<current, installed, published, durable, reader>>

Delete(file) ==
    /\ file \in deleting
    /\ durable' = durable \ {file}
    /\ deleting' = deleting \ {file}
    /\ UNCHANGED <<current, installed, published, reader>>

Next ==
    \/ WriteReplacement \/ Supersede \/ PublishCheckpoint \/ AdoptCheckpoint
    \/ OpenReader \/ CloseReader
    \/ \E file \in Objects : Authorize(file) \/ Delete(file)

Spec == Init /\ [][Next]_Vars
InstalledReadable == Required(installed) \subseteq durable
ReaderReadable == ReaderFiles \subseteq durable
RetentionSafe == InstalledReadable /\ ReaderReadable
OldFilesNeverCollected == Required(0) \cap durable # {}

=============================================================================
