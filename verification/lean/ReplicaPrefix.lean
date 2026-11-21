import Std

namespace RTDB.ReplicaPrefix

structure Key where
  id : Int
  time : Int
  deriving DecidableEq

structure Body where
  key : Key
  source : Int
  tableEffects : List (Nat × List Nat)
  outcome : Option Nat

structure Staged where
  predecessor : Option Key
  block : Nat
  body : Body

structure State where
  head : Option Key
  block : Nat
  source : Int
  history : List Body

def advances (after : Key) : Option Key → Prop
  | none => True
  | some before => before.id < after.id ∧ before.time ≤ after.time

instance (after : Key) (before : Option Key) : Decidable (advances after before) :=
  match before with
  | none => isTrue trivial
  | some prior => inferInstanceAs (Decidable (prior.id < after.id ∧ prior.time ≤ after.time))

def applyLocal (state : State) (staged : Staged) : Option State :=
  if staged.predecessor = state.head ∧ staged.block = state.block ∧
      advances staged.body.key state.head ∧ state.source ≤ staged.body.source then
    some { state with
      head := some staged.body.key
      source := staged.body.source
      history := state.history ++ [staged.body] }
  else none

def decodeFor (state : State) (body : Body) : Staged :=
  ⟨state.head, state.block, body⟩

def applyReplica (state : State) (body : Body) : Option State :=
  applyLocal state (decodeFor state body)

def localPrefix (state : State) : List Staged → Option State
  | [] => some state
  | staged :: rest => (applyLocal state staged).bind (fun next => localPrefix next rest)

def replicaPrefix (decode : Wire → Option Body) (state : State) : List Wire → Option State
  | [] => some state
  | frame :: rest => (decode frame).bind fun body =>
      (applyReplica state body).bind (fun next => replicaPrefix decode next rest)

theorem successful_local_apply_appends_one_result (before after : State) (staged : Staged)
    (success : applyLocal before staged = some after) :
    after.history = before.history ++ [staged.body] ∧ after.head = some staged.body.key ∧
    after.source = staged.body.source := by
  simp only [applyLocal] at success
  split at success
  next => cases success; exact ⟨rfl, rfl, rfl⟩
  next => cases success

theorem a_successful_stage_uses_the_decoded_predecessor (before after : State) (staged : Staged)
    (success : applyLocal before staged = some after) : decodeFor before staged.body = staged := by
  simp only [applyLocal] at success
  split at success
  next valid =>
    cases staged with
    | mk predecessor block body =>
        have headMatches : predecessor = before.head := by grind
        have blockMatches : block = before.block := by grind
        simp [decodeFor, headMatches, blockMatches]
  next => cases success

theorem a_successful_stage_advances_the_transaction_and_source (before after : State) (staged : Staged)
    (success : applyLocal before staged = some after) :
    advances staged.body.key before.head ∧ before.source ≤ staged.body.source := by
  simp only [applyLocal] at success
  split at success
  next valid => grind
  next => cases success

theorem a_linked_prefix_replays_to_the_same_published_state (encode : Body → Wire)
    (decode : Wire → Option Body) (stages : List Staged) (before after : State)
    (roundTrip : ∀ staged ∈ stages, decode (encode staged.body) = some staged.body)
    (success : localPrefix before stages = some after) :
    replicaPrefix decode before (stages.map (encode ∘ Staged.body)) = some after := by
  induction stages generalizing before after with
  | nil => exact success
  | cons staged rest ih =>
      cases applied : applyLocal before staged with
      | none => simp [localPrefix, applied] at success
      | some next =>
          have decoded := a_successful_stage_uses_the_decoded_predecessor before next staged applied
          have decodedBody := roundTrip staged (List.mem_cons_self ..)
          have remaining : ∀ entry ∈ rest, decode (encode entry.body) = some entry.body :=
            fun entry present => roundTrip entry (List.mem_cons_of_mem staged present)
          simp [localPrefix, applied] at success
          simpa [replicaPrefix, applyReplica, decodedBody, decoded, applied] using ih next after remaining success

#print axioms successful_local_apply_appends_one_result
#print axioms a_successful_stage_uses_the_decoded_predecessor
#print axioms a_successful_stage_advances_the_transaction_and_source
#print axioms a_linked_prefix_replays_to_the_same_published_state

end RTDB.ReplicaPrefix
