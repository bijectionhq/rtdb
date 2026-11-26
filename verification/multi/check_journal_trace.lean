import ReplayClosure

set_option warningAsError true

/-!
Checks a Multi journal trace recorded by the deferred-checkpoint tests in
`rust/crates/db/src/multi/tests.rs` (`RTDB_JOURNAL_TRACE_DIR`) against the
`ReplayClosure.DeferredBase` model. Each line is one event:

- `commit LSN` and `control LSN`: a transaction or control record at the next LSN.
- `capture CUT`: the writer captured a checkpoint of the head.
- `announce LSN CUT`: the checkpoint record of a captured cut.
- `trim THROUGH`: the writer trimmed through an announced cut.
- `replay FLOOR BASE HEAD`: a fresh process replayed the retained suffix and
  installed the checkpoint announced at BASE, or none when BASE is 0.

The model folds the number of committed transactions, so a checkpoint's image
is the number of commits through its cut.
-/

open RTDB.Recovery.ReplayClosure.DeferredBase

structure Journal where
  records : List (Entry Nat Unit) := []
  trimmed : Nat := 0
  captured : List Nat := []
  replays : Nat := 0
  lastBase : Nat := 0

def Journal.head (journal : Journal) : Nat := journal.records.length

def count : Nat → Unit → Nat := fun state _ => state + 1

private def readNat (value : String) : Except String Nat :=
  match value.toNat? with
  | some number => .ok number
  | none => .error s!"expected natural number, got {value}"

/-- The LSN of the first retained checkpoint whose cut precedes the floor. -/
def baseLsn (journal : Journal) : Nat :=
  if journal.trimmed = 0 then 0 else
    let retained := (journal.records.drop journal.trimmed).zipIdx (journal.trimmed + 1)
    match retained.find? (fun (entry, _) => match entry with
        | .checkpoint through _ => through == journal.trimmed
        | _ => false) with
    | some (_, lsn) => lsn
    | none => 0

def stepEvent (journal : Journal) (fields : List String) : Except String Journal := do
  match fields with
  | ["commit", lsn] =>
    if (← readNat lsn) != journal.head + 1 then throw "journal_order"
    return { journal with records := journal.records ++ [.tx ()] }
  | ["control", lsn] =>
    if (← readNat lsn) != journal.head + 1 then throw "journal_order"
    return { journal with records := journal.records ++ [.control] }
  | ["capture", cut] =>
    let cut ← readNat cut
    if cut != journal.head then throw "capture_at_head"
    return { journal with captured := journal.captured ++ [cut] }
  | ["announce", lsn, cut] =>
    let lsn ← readNat lsn
    let cut ← readNat cut
    if lsn != journal.head + 1 then throw "journal_order"
    if !journal.captured.contains cut || cut < journal.trimmed then throw "announce_captured_cut"
    let image := replay count 0 (journal.records.take cut)
    return { journal with records := journal.records ++ [.checkpoint cut image] }
  | ["trim", through] =>
    let through ← readNat through
    let announced := (journal.records.drop journal.trimmed).any fun entry => match entry with
      | .checkpoint cut _ => cut == through
      | _ => false
    if through < journal.trimmed || !announced then throw "trim_through_announced_cut"
    return { journal with trimmed := through }
  | ["replay", floor, base, head] =>
    let floor ← readNat floor
    let base ← readNat base
    let head ← readNat head
    if floor != journal.trimmed + 1 || head != journal.head then throw "replay_floor"
    if base != baseLsn journal then throw "replay_base"
    let retained := journal.records.drop journal.trimmed
    if replaySuffix count 0 journal.trimmed retained != some (replay count 0 journal.records) then
      throw "replay_from_lsn_one"
    return { journal with replays := journal.replays + 1, lastBase := base }
  | _ => throw "unknown journal event"

def check (lines : List String) : Except String (Nat × Journal) := do
  let mut journal : Journal := {}
  let mut line := 0
  for input in lines do
    line := line + 1
    journal ← (stepEvent journal (input.splitOn " ")).mapError (fun msg => s!"line {line}: {msg}")
  return (line, journal)

def main (args : List String) : IO UInt32 := do
  let [trace] := args | IO.eprintln "usage: check_journal_trace.lean TRACE"; return 2
  let content ← IO.FS.readFile trace
  let lines := content.splitOn "\n"
  let lines := if lines.getLast? = some "" then lines.dropLast else lines
  if lines.isEmpty then
    IO.eprintln "empty trace"
    return 2
  match check lines with
  | .error msg => IO.eprintln msg; return 1
  | .ok (events, journal) =>
    let commits := replay count 0 journal.records
    IO.println s!"{events} events, floor {journal.trimmed + 1}, base {journal.lastBase}, head {journal.head}, commits {commits}, replays {journal.replays}"
    return 0
