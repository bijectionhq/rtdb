import Std

namespace RTDB.CheckpointMetadataRetention

structure State (Reader : Type) where
  published : Nat
  held : Reader → Option Nat
  planned : Nat → Prop
  deleted : Nat → Prop

variable {Reader : Type} [DecidableEq Reader]

def initial : State Reader := ⟨0, fun _ => none, fun _ => False, fun _ => False⟩

def setHold (state : State Reader) (reader : Reader) (floor : Option Nat) : State Reader :=
  { state with held := fun current => if current = reader then floor else state.held current }

def MayAdmit (state : State Reader) (floor : Nat) : Prop :=
  state.published ≤ floor ∨ ∃ reader first, state.held reader = some first ∧ first ≤ floor

def MayAuthorize (state : State Reader) (block : Nat) : Prop :=
  block < state.published ∧
  ∀ reader first, state.held reader = some first → block < first

def Safe (state : State Reader) : Prop :=
  (∀ block, state.planned block → MayAuthorize state block) ∧
  (∀ block, state.deleted block → state.planned block)

inductive Step : State Reader → State Reader → Prop where
  | publish (state : State Reader) (head : Nat) :
      state.published ≤ head → Step state { state with published := head }
  | admit (state : State Reader) (reader : Reader) (floor : Nat) :
      state.held reader = none → MayAdmit state floor → Step state (setHold state reader (some floor))
  | release (state : State Reader) (reader : Reader) : Step state (setHold state reader none)
  | authorize (state : State Reader) (block : Nat) : MayAuthorize state block →
      Step state { state with planned := fun key => key = block ∨ state.planned key }
  | delete (state : State Reader) (block : Nat) : state.planned block →
      Step state { state with deleted := fun key => key = block ∨ state.deleted key }

inductive Reachable : State Reader → Prop where
  | start : Reachable initial
  | next {state updated : State Reader} : Reachable state → Step state updated → Reachable updated

variable {state updated : State Reader}

theorem plans_survive_interleavings (transition : Step state updated) (safe : Safe state) : Safe updated := by
  rcases safe with ⟨plans, deletions⟩
  cases transition with
  | publish head forward =>
    refine ⟨?_, deletions⟩
    intro block planned
    obtain ⟨before, bounds⟩ := plans block planned
    exact ⟨Nat.lt_of_lt_of_le before forward, bounds⟩
  | admit reader floor _ admission =>
    refine ⟨?_, deletions⟩
    intro block planned
    obtain ⟨before, bounds⟩ := plans block planned
    refine ⟨before, ?_⟩
    intro current first held
    by_cases same : current = reader
    · subst current
      have sameFloor : floor = first := by simpa [setHold] using held
      subst first
      rcases admission with currentHead | ⟨predecessor, boundary, held, covered⟩
      · exact Nat.lt_of_lt_of_le before currentHead
      · exact Nat.lt_of_lt_of_le (bounds predecessor boundary held) covered
    · exact bounds current first (by simpa [setHold, same] using held)
  | release reader =>
    refine ⟨?_, deletions⟩
    intro block planned
    obtain ⟨before, bounds⟩ := plans block planned
    refine ⟨before, ?_⟩
    intro current first held
    by_cases same : current = reader
    · simp [setHold, same] at held
    · exact bounds current first (by simpa [setHold, same] using held)
  | authorize block permitted =>
    constructor
    · intro key time
      rcases time with same | earlier
      · exact same ▸ permitted
      · exact plans key earlier
    · intro key erased
      exact Or.inr (deletions key erased)
  | delete block planned =>
    refine ⟨plans, ?_⟩
    intro key erased
    rcases erased with same | earlier
    · exact same ▸ planned
    · exact deletions key earlier

theorem reachable_plans_are_safe (reach : Reachable state) : Safe state := by
  induction reach with
  | start => simp [Safe, initial]
  | next _ transition safe => exact plans_survive_interleavings transition safe

theorem admitted_metadata_cannot_be_deleted (reach : Reachable state) (reader : Reader) (first block : Nat)
    (held : state.held reader = some first) (inScope : first ≤ block) : ¬ state.deleted block := by
  intro erased
  let safe := reachable_plans_are_safe reach
  exact (Nat.not_lt_of_ge inScope) ((safe.1 block (safe.2 block erased)).2 reader first held)

omit [DecidableEq Reader] in
theorem another_owner_can_inherit_the_same_floor (reader : Reader) (floor : Nat)
    (held : state.held reader = some floor) : MayAdmit state floor :=
  Or.inr ⟨reader, floor, held, Nat.le_refl floor⟩

def MayCollectPack (state : State Reader) (pack youngest : Nat) : Prop :=
  MayAuthorize state pack ∧ MayAuthorize state youngest

theorem pack_member_is_below_every_hold (pack member youngest : Nat)
    (covered : member ≤ youngest) (allowed : MayCollectPack state pack youngest) :
    MayAuthorize state member := by
  have bound : MayAuthorize state youngest := allowed.2
  obtain ⟨before, holds⟩ := bound
  exact ⟨Nat.lt_of_le_of_lt covered before,
    fun reader first held => Nat.lt_of_le_of_lt covered (holds reader first held)⟩

#print axioms pack_member_is_below_every_hold
#print axioms another_owner_can_inherit_the_same_floor
#print axioms plans_survive_interleavings
#print axioms reachable_plans_are_safe
#print axioms admitted_metadata_cannot_be_deleted

end RTDB.CheckpointMetadataRetention
