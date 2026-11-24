import Std

namespace RTDB.ReadIndexFrontiers

structure Cut where
  history : Nat
  entries : List Nat
  deriving DecidableEq, Repr

def Extends (later earlier : Cut) : Prop :=
  later.history = earlier.history ∧ ∃ suffix, later.entries = earlier.entries ++ suffix

theorem extension_transitive {a b c : Cut} (ab : Extends a b) (bc : Extends b c) :
    Extends a c := by
  obtain ⟨abHistory, abSuffix, abEntries⟩ := ab
  obtain ⟨bcHistory, bcSuffix, bcEntries⟩ := bc
  exact ⟨abHistory.trans bcHistory, bcSuffix ++ abSuffix, by
    rw [abEntries, bcEntries, List.append_assoc]⟩

variable {Journal : Type}

def Intersects (install probe : Journal → Prop) : Prop :=
  ∃ journal, install journal ∧ probe journal

def Installed (voters : Journal → Prop) (cut : Cut) (durable : Journal → Cut) : Prop :=
  ∀ journal, voters journal → Extends (durable journal) cut

/-- Probe samples may occur at different times. Each accepted reply extends
that journal's installed cut at invocation. -/
theorem selected_covers_prior_installation
    {install probe : Journal → Prop} {prior selected : Cut}
    {durable sampled : Journal → Cut}
    (intersection : Intersects install probe)
    (installed : Installed install prior durable)
    (monotone : ∀ j, probe j → Extends (sampled j) (durable j))
    (maximum : ∀ j, probe j → Extends selected (sampled j)) :
    Extends selected prior := by
  obtain ⟨journal, wrote, answered⟩ := intersection
  exact extension_transitive (maximum journal answered)
    (extension_transitive (monotone journal answered) (installed journal wrote))

/-- Every admission to exposure establishes the same premise a writer has
to establish before replying. -/
theorem successive_reads_are_monotone
    {firstInstall secondProbe : Journal → Prop} {firstRead secondRead : Cut}
    {atInvocation sampled : Journal → Cut}
    (intersection : Intersects firstInstall secondProbe)
    (beforeExposure : Installed firstInstall firstRead atInvocation)
    (monotone : ∀ j, secondProbe j → Extends (sampled j) (atInvocation j))
    (maximum : ∀ j, secondProbe j → Extends secondRead (sampled j)) :
    Extends secondRead firstRead :=
  selected_covers_prior_installation intersection beforeExposure monotone maximum

inductive Handoff : Cut → Cut → Prop
  | same (cut : Cut) : Handoff cut cut
  | activate {old floor newer : Cut} (sealed : Handoff old floor) (installed : Extends newer floor) :
      Handoff old newer

theorem handoff_preserves_installed_prefix {old new : Cut} (handoff : Handoff old new) :
    Extends new old := by
  induction handoff with
  | same cut => exact ⟨rfl, [], by simp⟩
  | activate _ installed ancestor => exact extension_transitive installed ancestor

def readFrom (replicas : List Nat) : Nat := replicas.foldl max 0

theorem single_installation_permits_read_inversion :
    readFrom [1, 0] = 1 ∧ readFrom [0, 0] = 0 := by decide

theorem matching_offsets_do_not_bind_history :
    let original := ({ history := 1, entries := [7] } : Cut)
    let foreign := ({ history := 2, entries := [7] } : Cut)
    original.entries.length = foreign.entries.length ∧ ¬ Extends foreign original := by
  simp [Extends]

#print axioms extension_transitive
#print axioms selected_covers_prior_installation
#print axioms successive_reads_are_monotone
#print axioms handoff_preserves_installed_prefix
#print axioms single_installation_permits_read_inversion
#print axioms matching_offsets_do_not_bind_history

end RTDB.ReadIndexFrontiers
