import Std

namespace RTDB.RepairArchiveCustody

structure Archives (Archive : Type) where
  destination : Option Archive
  repair : Option Archive

def Covers {Archive : Type} (slots : Archives Archive) (archive : Archive) : Prop :=
  slots.destination = some archive ∨ slots.repair = some archive

structure State (Archive Region : Type) where
  regions : Region → Option (Archives Archive)
  reading : Region → Prop
  closed : Archive → Prop
  erased : Archive → Prop

variable {Archive Region : Type} [DecidableEq Region]

def setRegion (state : State Archive Region) (region : Region) (slots : Option (Archives Archive)) :
    State Archive Region :=
  { state with regions := fun other => if other = region then slots else state.regions other }

def Safe (state : State Archive Region) : Prop :=
  (∀ archive, state.erased archive → state.closed archive) ∧
  (∀ region slots archive, state.regions region = some slots → Covers slots archive → ¬ state.erased archive) ∧
  ∀ region, state.reading region → ∃ slots, state.regions region = some slots

inductive Step : State Archive Region → State Archive Region → Prop where
  | admit (state) (region : Region) (slots : Archives Archive) :
      state.regions region = none →
      (∀ archive, Covers slots archive → ¬ state.closed archive) →
      Step state (setRegion state region (some slots))
  | startRead (state) (region : Region) (slots : Archives Archive) :
      state.regions region = some slots →
      Step state { state with reading := fun other => other = region ∨ state.reading other }
  | drain (state) (region : Region) :
      Step state { state with reading := fun other => other ≠ region ∧ state.reading other }
  | release (state) (region : Region) :
      ¬ state.reading region → Step state (setRegion state region none)
  | fence (state) (archive : Archive) :
      Step state { state with closed := fun other => other = archive ∨ state.closed other }
  | erase (state) (archive : Archive) :
      state.closed archive →
      (∀ region slots, state.regions region = some slots → ¬ Covers slots archive) →
      Step state { state with erased := fun other => other = archive ∨ state.erased other }

variable {state updated : State Archive Region}

theorem transitions_preserve_archive_custody (step : Step state updated) (safe : Safe state) : Safe updated := by
  obtain ⟨erasures, retained, reading⟩ := safe
  cases step with
  | admit region slots _ openArchives =>
    refine ⟨erasures, ?_, ?_⟩
    · intro other manifest archive held covered erased
      by_cases same : other = region
      · have manifest_eq : slots = manifest := by simpa [setRegion, same] using held
        exact openArchives archive (manifest_eq.symm ▸ covered) (erasures archive erased)
      · exact retained other manifest archive (by simpa [setRegion, same] using held) covered erased
    · intro other active
      by_cases same : other = region
      · exact ⟨slots, by simp [setRegion, same]⟩
      · obtain ⟨manifest, held⟩ := reading other active
        exact ⟨manifest, by simpa [setRegion, same] using held⟩
  | startRead region slots held =>
    refine ⟨erasures, retained, ?_⟩
    intro other active
    rcases active with same | already
    · exact ⟨slots, same ▸ held⟩
    · exact reading other already
  | drain _ =>
    exact ⟨erasures, retained, fun region active => reading region active.2⟩
  | release region drained =>
    refine ⟨erasures, ?_, ?_⟩
    · intro other slots archive held covered
      by_cases same : other = region
      · simp [setRegion, same] at held
      · exact retained other slots archive (by simpa [setRegion, same] using held) covered
    · intro other active
      have different : other ≠ region := by
        intro same
        exact drained (same ▸ active)
      obtain ⟨slots, held⟩ := reading other active
      exact ⟨slots, by simpa [setRegion, different] using held⟩
  | fence _ =>
    exact ⟨fun archive erased => Or.inr (erasures archive erased), retained, reading⟩
  | erase archive closed outsideRegions =>
    refine ⟨?_, ?_, reading⟩
    · intro other erased
      rcases erased with same | prior
      · exact same ▸ closed
      · exact erasures other prior
    · intro region slots other held covered erased
      rcases erased with same | prior
      · exact outsideRegions region slots held (same ▸ covered)
      · exact retained region slots other held covered prior

inductive Schedule : State Archive Region → State Archive Region → Prop where
  | start (initial : State Archive Region) : Schedule initial initial
  | next {first middle final : State Archive Region} : Schedule first middle → Step middle final → Schedule first final

theorem finite_schedules_preserve_archive_custody (history : Schedule state updated)
    (safe : Safe state) : Safe updated := by
  induction history with
  | start => exact safe
  | next _ step ih => exact transitions_preserve_archive_custody step ih

omit [DecidableEq Region] in
theorem a_region_retains_its_repair {slots : Archives Archive} {archive : Archive}
    (repair : slots.repair = some archive) : Covers slots archive :=
  Or.inr repair

theorem an_admitted_repair_survives_collection (history : Schedule state updated) (safe : Safe state)
    (region : Region) (slots : Archives Archive) (archive : Archive)
    (held : updated.regions region = some slots) (repair : slots.repair = some archive) :
    ¬ updated.erased archive :=
  (finite_schedules_preserve_archive_custody history safe).2.1 region slots archive held (a_region_retains_its_repair repair)

#print axioms transitions_preserve_archive_custody
#print axioms finite_schedules_preserve_archive_custody
#print axioms a_region_retains_its_repair
#print axioms an_admitted_repair_survives_collection

end RTDB.RepairArchiveCustody
