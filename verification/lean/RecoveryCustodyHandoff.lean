import Std

namespace RTDB.RecoveryCustodyHandoff

structure Cut (Unit : Type) where
  through : Nat
  root : Unit → Prop
  suffix : Unit → Prop

def Cut.needs {Unit : Type} (cut : Cut Unit) (unit : Unit) : Prop :=
  cut.root unit ∨ cut.suffix unit

structure Erasure (Unit : Type) where
  units : Unit → Prop
  deadline : Nat

structure State (Unit : Type) where
  cut : Cut Unit
  media : Unit → Prop
  keys : Unit → Prop
  independent : Unit → Prop
  confirmed : Nat
  clock : Nat
  erasure : Option (Erasure Unit)

def State.needs {Unit : Type} (s : State Unit) (unit : Unit) : Prop :=
  s.cut.needs unit ∨ s.independent unit

def Readable {Unit : Type} (s : State Unit) (unit : Unit) : Prop :=
  s.media unit ∧ s.keys unit

def Disjoint {Unit : Type} (needed removed : Unit → Prop) : Prop :=
  ∀ unit, needed unit → ¬ removed unit

def Safe {Unit : Type} (s : State Unit) : Prop :=
  (∀ unit, s.needs unit → Readable s unit) ∧
  s.confirmed ≤ s.cut.through ∧
  ∀ erasure, s.erasure = some erasure →
    Disjoint s.needs erasure.units ∧
      (erasure.deadline ≤ s.clock → ∀ unit, erasure.units unit → ¬ s.keys unit)

inductive Step {Unit : Type} : State Unit → State Unit → Prop
  | confirm (s : State Unit) (along : Nat) (progress : s.confirmed ≤ along) (within : along ≤ s.cut.through) :
      Step s { s with confirmed := along }
  | keepIndependent (s : State Unit) (unit : Unit) (ready : Readable s unit)
      (outside : ∀ plan, s.erasure = some plan → ¬ plan.units unit) :
      Step s { s with independent := fun existing => s.independent existing ∨ existing = unit }
  | arm (s : State Unit) (replacement : Cut Unit) (plan : Erasure Unit)
      (first : s.erasure = none) (ahead : s.cut.through ≤ replacement.through)
      (complete : ∀ unit, replacement.needs unit → Readable s unit)
      (clean : Disjoint replacement.needs plan.units)
      (forksRetained : Disjoint s.independent plan.units)
      (future : s.clock < plan.deadline) :
      Step s { s with cut := replacement, erasure := some plan }
  | tickBeforeDeadline (s : State Unit) (now : Nat) (elapsed : s.clock ≤ now)
      (before : ∀ plan, s.erasure = some plan → now < plan.deadline) :
      Step s { s with clock := now }
  | revoke (s : State Unit) (plan : Erasure Unit) (now : Nat)
      (installed : s.erasure = some plan) (elapsed : s.clock ≤ now) (due : plan.deadline ≤ now) :
      Step s { s with clock := now, keys := fun unit => s.keys unit ∧ ¬ plan.units unit }
  | remove (s : State Unit) (unit : Unit) (noGrant : ¬ s.keys unit) :
      Step s { s with media := fun existing => s.media existing ∧ existing ≠ unit }

theorem step_preserves_joint_safety {Unit : Type} {before after : State Unit}
    (safe : Safe before) (step : Step before after) : Safe after := by
  obtain ⟨readable, acknowledged, erase⟩ := safe
  cases step with
  | confirm along progress within =>
    exact ⟨readable, within, erase⟩
  | keepIndependent unit ready outside =>
    refine ⟨?_, acknowledged, ?_⟩
    · intro existing needed
      rcases needed with based | fork | equal
      · exact readable existing (Or.inl based)
      · exact readable existing (Or.inr fork)
      · subst existing
        exact ready
    · intro plan installed
      obtain ⟨disjoint, deadline⟩ := erase plan installed
      refine ⟨?_, deadline⟩
      intro existing needed
      rcases needed with based | fork | equal
      · exact disjoint existing (Or.inl based)
      · exact disjoint existing (Or.inr fork)
      · subst existing
        exact outside plan installed
  | arm replacement plan first ahead complete clean forksRetained future =>
    refine ⟨?_, Nat.le_trans acknowledged ahead, ?_⟩
    · intro unit needed
      exact needed.elim (complete unit) (fun fork => readable unit (Or.inr fork))
    · intro installed same
      have equals := Option.some.inj same
      subst installed
      exact ⟨fun unit needed => needed.elim (clean unit) (forksRetained unit),
        fun due => False.elim (Nat.not_le.mpr future due)⟩
  | tickBeforeDeadline now elapsed beforeDeadline =>
    exact ⟨readable, acknowledged, fun plan installed =>
      ⟨(erase plan installed).1, fun due => False.elim (Nat.not_le.mpr (beforeDeadline plan installed) due)⟩⟩
  | revoke plan now installed elapsed due =>
    obtain ⟨disjoint, _⟩ := erase plan installed
    refine ⟨?_, acknowledged, ?_⟩
    · intro unit needed
      obtain ⟨present, key⟩ := readable unit needed
      exact ⟨present, key, disjoint unit needed⟩
    · intro selected selectedIsInstalled
      have same := Option.some.inj (installed.symm.trans selectedIsInstalled)
      subst selected
      exact ⟨disjoint, fun _ _ erased retained => retained.2 erased⟩
  | remove unit noGrant =>
    refine ⟨?_, acknowledged, erase⟩
    intro existing needed
    obtain ⟨present, key⟩ := readable existing needed
    refine ⟨⟨present, ?_⟩, key⟩
    intro same
    subst existing
    exact noGrant key

inductive Reached {Unit : Type} (initial : State Unit) : State Unit → Prop
  | start : Reached initial initial
  | step {before after : State Unit} (prior : Reached initial before) (transition : Step before after) :
      Reached initial after

theorem all_reachable_states_preserve_recovery_and_erasure {Unit : Type}
    {initial current : State Unit} (safe : Safe initial) (run : Reached initial current) :
    Safe current := by
  induction run with
  | start => exact safe
  | step _ transition invariant => exact step_preserves_joint_safety invariant transition

theorem recovery_and_deadline_exclude_the_same_suffix_unit {Unit : Type}
    {s : State Unit} {plan : Erasure Unit} {unit : Unit}
    (safe : Safe s) (installed : s.erasure = some plan)
    (due : plan.deadline ≤ s.clock) (erased : plan.units unit) :
    ¬ s.cut.suffix unit := by
  intro inSuffix
  have available := safe.1 unit (Or.inl (Or.inr inSuffix))
  exact (safe.2.2 plan installed).2 due unit erased available.2

theorem commit_can_arm_erasure_with_a_clean_replacement {Unit : Type}
    {s : State Unit} {next : Cut Unit} {plan : Erasure Unit}
    (none : s.erasure = none) (covered : s.cut.through ≤ next.through)
    (ready : ∀ unit, next.needs unit → Readable s unit)
    (clean : Disjoint next.needs plan.units) (independent : Disjoint s.independent plan.units)
    (future : s.clock < plan.deadline) :
    Step s { s with cut := next, erasure := some plan } :=
  Step.arm s next plan none covered ready clean independent future

#print axioms step_preserves_joint_safety
#print axioms all_reachable_states_preserve_recovery_and_erasure
#print axioms recovery_and_deadline_exclude_the_same_suffix_unit
#print axioms commit_can_arm_erasure_with_a_clean_replacement

end RTDB.RecoveryCustodyHandoff
