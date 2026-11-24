import ReplicationDriver

set_option warningAsError true

open RTDB.Recovery RTDB.Recovery.ReplicationDriver

private def readNat (value : String) : Except String Nat :=
  match value.toNat? with
  | some number => .ok number
  | none => .error s!"expected natural number, got {value}"

private def readInt (value : String) : Except String Int :=
  match value.toInt? with
  | some number => .ok number
  | none => .error s!"expected integer, got {value}"

private def nibble (digit : Char) : Except String Nat :=
  if '0' ≤ digit && digit ≤ '9' then .ok (digit.toNat - '0'.toNat)
  else if 'a' ≤ digit && digit ≤ 'f' then .ok (10 + digit.toNat - 'a'.toNat)
  else .error s!"invalid hexadecimal digit {digit}"

private def bytes : List Char → Except String (List UInt8)
  | [] => .ok []
  | hi :: lo :: rest => do
    let high ← nibble hi
    let low ← nibble lo
    return UInt8.ofNat (16 * high + low) :: (← bytes rest)
  | [_] => .error "incomplete hexadecimal byte"

private def fingerprint (value : String) : Except String (List UInt8) :=
  if value.length = 64 then bytes value.toList else .error "expected 32-byte fingerprint"

private def readIdentity (term sequence digest : String) : Except String NativeRecordIdentity := do
  return ⟨← readInt term, some (← readInt sequence), some (← fingerprint digest)⟩

private def parse (state : State) (fields : List String) : Except String Action := do
  match fields with
  | ["empty"] => return .worker .emptyRead
  | ["grant", grant] => return .worker (.observeGrant (← readNat grant))
  | ["promote", term] => return .worker (.promote (← readInt term))
  | ["enqueue", id, tx] => return .worker (.enqueue (← readNat id) (← readNat tx))
  | ["dispatch", id, term, seq, digest] =>
    return .dispatch (← readNat id) (← readInt term) (← readInt seq) (← fingerprint digest) false
  | ["choose", id, rawOffset] =>
    let nonce ← readNat id
    let offset ← readNat rawOffset
    if offset != state.journal.head.next then throw "chosen data offset"
    return .choose nonce offset ⟨1, 1⟩ nonce nonce (offset + 1) (offset + 10) 0 0 .ordinary
  | ["replace", nonce] => return .replace (← readNat nonce)
  | ["providerFault", id] => return .worker (.providerFault (← readNat id))
  | ["read", term, seq, digest, resolved] =>
    if resolved != "0" && resolved != "1" then throw "expected resolved flag"
    return .worker (.readBack (← readIdentity term seq digest) (resolved == "1"))
  | ["replay", term, seq, digest, "1"] => return .worker (.replay (← readIdentity term seq digest))
  | ["applyBegin"] => return .worker .applyBegin
  | ["applyFinished", tx] => return .worker (.applyFinished (← readNat tx))
  | ["applyFault"] => return .worker .applyFault
  | ["ack", id, tx] => return .worker (.acknowledge (← readNat id) (← readNat tx))
  | ["advance"] => return .worker .advance
  | ["termLoss"] => return .worker .termLoss
  | ["crash"] => return .worker .crash
  | _ => throw "unknown driver event"

private def rejectName : Reject → String
  | .acknowledgeAfterApply => "acknowledge_after_apply"
  | .staleWriter => "current_writer_at_choice"
  | .routeOnce => "route_once"
  | reason => reprStr reason

private def check (lines : List String) : Except String (Nat × Nat × State) := do
  let mut state : State := {}
  let mut line := 0
  let mut acknowledgements := 0
  for input in lines do
    line := line + 1
    let action ← (parse state (input.splitOn " ")).mapError (fun msg => s!"line {line}: {msg}")
    match step state action with
    | .error reason => throw s!"line {line}: {rejectName reason}"
    | .ok result =>
      state := result.state
      if result.acknowledgement.isSome then acknowledgements := acknowledgements + 1
  return (line, acknowledgements, state)

def main (args : List String) : IO UInt32 := do
  let [trace] := args | IO.eprintln "usage: check_driver_trace.lean TRACE"; return 2
  let content ← IO.FS.readFile trace
  let lines := content.splitOn "\n"
  let lines := if lines.getLast? = some "" then lines.dropLast else lines
  if lines.isEmpty then
    IO.eprintln "empty trace"
    return 2
  match check lines with
  | .error msg => IO.eprintln msg; return 1
  | .ok (events, routes, state) =>
    IO.println s!"{events} actions, {routes} acknowledgements, head {state.journal.head.next}, cursor {state.worker.cursor}, halted {state.worker.halted}"
    return 0
