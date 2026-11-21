import History

open RTDB

def natural (text : String) : Except String Nat :=
  match text.toNat? with
  | some value => pure value
  | none => throw s!"expected a natural number, got {text}"

def integer (text : String) : Except String Int :=
  match text.toInt? with
  | some value => pure value
  | none => throw s!"expected an integer, got {text}"

def addFact (history : History) (commit : Nat) (fact : Fact) : Except String History :=
  if commit = history.length + 1 then
    pure ([fact] :: history)
  else if commit = history.length then
    match history with
    | batch :: older => pure ((batch ++ [fact]) :: older)
    | [] => throw "commit numbering starts at one"
  else
    throw "commits must be contiguous and oldest first"

def replay (lines : List String) : Except String Nat := do
  let mut history : History := []
  let mut name := ""
  let mut observations := 0
  for line in lines do
    match line.splitOn "\t" with
    | [""] => pure ()
    | ["case", label] =>
      history := []
      name := label
    | ["put", commit, key, lower, upper, value] =>
      history ← addFact history (← natural commit)
        (.put (← natural key) (← natural value) ⟨← integer lower, ← integer upper⟩)
    | ["delete", commit, key, lower, upper] =>
      history ← addFact history (← natural commit)
        (.delete (← natural key) ⟨← integer lower, ← integer upper⟩)
    | ["erase", commit, key] =>
      history ← addFact history (← natural commit) (.erase (← natural key))
    | ["expect", horizon, system, key, valid, value] =>
      let actual := point (admit history (← natural horizon)) (← natural system)
        (← natural key) (← integer valid)
      let expected ← if value = "none" then pure none else some <$> natural value
      unless actual = expected do
        throw s!"{name}: {line}: expected {expected}, got {actual}"
      observations := observations + 1
    | _ => throw s!"{name}: malformed trace row {line}"
  pure observations

def main (args : List String) : IO UInt32 := do
  match args with
  | [source] =>
    let content ← IO.FS.readFile source
    match replay (content.splitOn "\n") with
    | .ok count =>
      IO.println s!"{count} trace observations match"
      pure (if count > 0 then 0 else 1)
    | .error message =>
      IO.eprintln message
      pure 1
  | _ =>
    IO.eprintln "usage: Trace.lean <temporal.tsv>"
    pure 1
