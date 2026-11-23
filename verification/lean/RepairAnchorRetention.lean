import Std

namespace RTDB.RepairAnchorRetention

structure State (File : Type) where
  head : Nat
  anchor : Option Nat
  durableAnchor : Option Nat
  published : File → Prop
  present : File → Prop
  selected : Option File

def initial {File : Type} (files : File → Prop) : State File :=
  ⟨0, none, none, files, files, none⟩

def protects (anchor : Option Nat) (block : Nat) : Prop :=
  ∃ first, anchor = some first ∧ first ≤ block

def covered {File : Type} (block : File → Nat) (s : State File) : Prop :=
  ∀ file, s.published file →
    (s.head ≤ block file ∨ protects s.anchor (block file) ∨ protects s.durableAnchor (block file)) →
    s.present file

inductive Step {File : Type} (block : File → Nat) : State File → State File → Prop
  | publish (s : State File) (file : File) :
      Step block s { s with
        published := fun old => s.published old ∨ old = file
        present := fun old => s.present old ∨ old = file }
  | advance (s : State File) (head : Nat) (ordered : s.head ≤ head) :
      Step block s { s with head }
  | pin (s : State File) (first : Nat) (atHead : first = s.head) :
      Step block s { s with anchor := some first }
  | synchronize (s : State File) :
      Step block s { s with durableAnchor := s.anchor }
  | select (s : State File) (file : File) (superseded : block file < s.head) :
      Step block s { s with selected := some file }
  | remove (s : State File) (file : File) (selected : s.selected = some file)
      (superseded : block file < s.head) (unheld : ¬ protects s.anchor (block file)) :
      Step block s { s with
        durableAnchor := s.anchor
        present := fun old => s.present old ∧ old ≠ file
        selected := none }
  | restart (s : State File) :
      Step block s { s with anchor := s.durableAnchor, selected := none }

theorem a_fresh_anchor_is_covered {File : Type} {block : File → Nat} {s : State File}
    (safe : covered block s) :
    covered block { s with anchor := some s.head } := by
  intro file pub needed
  apply safe file pub
  rcases needed with current | ⟨first, identity, firstBefore⟩ | durable
  · exact Or.inl current
  · exact Or.inl ((Option.some.inj identity) ▸ firstBefore)
  · exact Or.inr (Or.inr durable)

theorem every_transition_preserves_coverage {File : Type} {block : File → Nat}
    {before after : State File} (safe : covered block before) (step : Step block before after) :
    covered block after := by
  cases step with
  | publish file =>
    intro old pub needed
    exact pub.elim (fun wasPublished => Or.inl (safe old wasPublished needed)) Or.inr
  | advance head ordered =>
    intro file pub needed
    exact safe file pub (needed.imp (Nat.le_trans ordered) id)
  | pin first atHead =>
    subst first
    exact a_fresh_anchor_is_covered safe
  | synchronize =>
    intro file pub needed
    exact safe file pub (needed.imp id (fun reads => Or.inl (reads.elim id id)))
  | select => exact safe
  | remove file selected superseded unheld =>
    intro old pub needed
    have coveredByVisible : before.head ≤ block old ∨ protects before.anchor (block old) :=
      needed.imp id (fun reads => reads.elim id id)
    refine ⟨safe old pub (coveredByVisible.imp id Or.inl), ?_⟩
    intro same
    subst old
    exact coveredByVisible.elim (Nat.not_le.mpr superseded) unheld
  | restart =>
    intro file pub needed
    exact safe file pub (needed.imp id (fun reads => Or.inr (reads.elim id id)))

inductive Reachable {File : Type} (block : File → Nat) (files : File → Prop) : State File → Prop
  | start : Reachable block files (initial files)
  | next {before after : State File} (prior : Reachable block files before)
      (step : Step block before after) : Reachable block files after

theorem all_reachable_anchors_preserve_their_suffix {File : Type} {block : File → Nat}
    {files : File → Prop} {s : State File} (history : Reachable block files s) :
    covered block s := by
  induction history with
  | start => exact fun _ pub _ => pub
  | next _ step invariant => exact every_transition_preserves_coverage invariant step

theorem advancing_the_anchor_releases_an_older_file {File : Type} {block : File → Nat}
    {s : State File} {file : File} (old : block file < s.head) :
    Step block { s with anchor := some s.head, selected := some file }
      { s with
        anchor := some s.head
        durableAnchor := some s.head
        present := fun other => s.present other ∧ other ≠ file
        selected := none } := by
  apply Step.remove { s with anchor := some s.head, selected := some file } file rfl old
  rintro ⟨first, equal, earlier⟩
  exact Nat.not_le.mpr old ((Option.some.inj equal) ▸ earlier)

#print axioms a_fresh_anchor_is_covered
#print axioms every_transition_preserves_coverage
#print axioms all_reachable_anchors_preserve_their_suffix
#print axioms advancing_the_anchor_releases_an_older_file

end RTDB.RepairAnchorRetention
