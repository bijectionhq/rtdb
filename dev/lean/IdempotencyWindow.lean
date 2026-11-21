import Std

namespace RTDB.Recovery.IdempotencyWindow

structure CommandState (Outcome : Type) where
  committed : Option Outcome
  mutations : Nat
  reply : Option Outcome

def empty : CommandState Outcome :=
  ⟨none, 0, none⟩

inductive Step : CommandState Outcome → CommandState Outcome → Prop where
  | commit (s : CommandState Outcome) (outcome : Outcome) (unused : s.committed = none) :
      Step s ⟨some outcome, s.mutations + 1, none⟩
  | reply (s : CommandState Outcome) (outcome : Outcome) (known : s.committed = some outcome) :
      Step s {s with reply := some outcome}
  | disconnect (s : CommandState Outcome) :
      Step s {s with reply := none}

inductive Reachable : CommandState Outcome → Prop where
  | init : Reachable empty
  | next : Reachable s → Step s t → Reachable t

def Valid (s : CommandState Outcome) : Prop :=
  (s.committed = none → s.mutations = 0) ∧
  (∀ outcome, s.committed = some outcome → s.mutations = 1) ∧
  (∀ outcome, s.reply = some outcome → s.committed = some outcome)

theorem valid_step (before : Valid s) (step : Step s t) : Valid t := by
  obtain ⟨unused, used, _⟩ := before
  cases step with
  | commit outcome absent =>
      simp [Valid, unused absent]
  | reply outcome known =>
      refine ⟨unused, used, ?_⟩
      intro candidate sent
      simp at sent
      subst candidate
      exact known
  | disconnect =>
      exact ⟨unused, used, by simp⟩

theorem reachable_valid (trace : Reachable s) : Valid s := by
  induction trace with
  | init => simp [Valid, empty]
  | next _ step ih => exact valid_step ih step

theorem reply_identifies_one_committed_mutation
    (trace : Reachable s) (seen : s.reply = some outcome) :
    s.committed = some outcome ∧ s.mutations = 1 :=
  let valid := reachable_valid trace
  let recorded := valid.2.2 outcome seen
  ⟨recorded, valid.2.1 outcome recorded⟩

theorem retry_preserves_the_original_commit
    (step : Step s t) (recorded : s.committed = some outcome) :
    t.committed = some outcome ∧ t.mutations = s.mutations := by
  cases step with
  | commit _ absent => rw [absent] at recorded; contradiction
  | reply => exact ⟨recorded, rfl⟩
  | disconnect => exact ⟨recorded, rfl⟩

#print axioms reply_identifies_one_committed_mutation
#print axioms retry_preserves_the_original_commit

end RTDB.Recovery.IdempotencyWindow
