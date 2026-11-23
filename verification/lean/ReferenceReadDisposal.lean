import Std

namespace RTDB.ReferenceReadDisposal

structure State (Read : Type) where
  closed : Bool
  erased : Bool
  copy : Bool
  held : Read → Prop
  pending : Read → Prop

variable {Read : Type}

def initial : State Read :=
  ⟨false, false, false, fun _ => False, fun _ => False⟩

def Covered (s : State Read) : Prop :=
  ∀ r, s.pending r → s.held r

def Safe (s : State Read) : Prop :=
  Covered s ∧ (s.erased = true → s.copy = true ∧ ∀ r, ¬ s.pending r)

inductive Step : State Read → State Read → Prop where
  | admit (s) (r : Read) :
      s.closed = false → s.erased = false →
      Step s { s with held := fun other => other = r ∨ s.held other,
                      pending := fun other => other = r ∨ s.pending other }
  | cancel (s) :
      Step s s
  | drain (s) (r : Read) :
      Step s { s with pending := fun other => other ≠ r ∧ s.pending other }
  | release (s) (r : Read) :
      ¬ s.pending r →
      Step s { s with held := fun other => other ≠ r ∧ s.held other }
  | fence (s) :
      Step s { s with closed := true }
  | transfer (s) :
      Step s { s with copy := true }
  | erase (s) :
      s.closed = true → s.copy = true → (∀ r, ¬ s.held r) →
      Step s { s with erased := true }

inductive Reachable : State Read → Prop where
  | start : Reachable initial
  | step {s next} : Reachable s → Step s next → Reachable next

theorem every_step_keeps_reads_and_the_fork {s next : State Read}
    (transition : Step s next) (safe : Safe s) : Safe next := by
  obtain ⟨covered, afterErase⟩ := safe
  cases transition with
  | admit r _ available =>
    refine ⟨?_, ?_⟩
    · intro other active
      exact active.elim Or.inl (fun pending => Or.inr (covered other pending))
    · intro erased
      simp [available] at erased
  | cancel => exact ⟨covered, afterErase⟩
  | drain r =>
    exact ⟨fun other active => covered other active.2,
      fun erased => ⟨(afterErase erased).1, fun other active => (afterErase erased).2 other active.2⟩⟩
  | release r drained =>
    refine ⟨?_, afterErase⟩
    intro other active
    exact ⟨fun same => drained (same ▸ active), covered other active⟩
  | fence => exact ⟨covered, afterErase⟩
  | transfer => exact ⟨covered, fun erased => ⟨rfl, (afterErase erased).2⟩⟩
  | erase _ copied released =>
    exact ⟨covered, fun _ => ⟨copied, fun r active => released r (covered r active)⟩⟩

theorem all_reachable_states_preserve_the_fork_and_its_reads {s : State Read}
    (reachable : Reachable s) : Safe s := by
  induction reachable with
  | start => exact ⟨fun _ h => h.elim, by simp [initial]⟩
  | step _ transition inductiveProof => exact every_step_keeps_reads_and_the_fork transition inductiveProof

theorem erasure_excludes_every_pending_reader {s : State Read} (reachable : Reachable s)
    (erased : s.erased = true) (r : Read) : ¬ s.pending r :=
  ((all_reachable_states_preserve_the_fork_and_its_reads reachable).2 erased).2 r

#print axioms every_step_keeps_reads_and_the_fork
#print axioms all_reachable_states_preserve_the_fork_and_its_reads
#print axioms erasure_excludes_every_pending_reader

end RTDB.ReferenceReadDisposal
