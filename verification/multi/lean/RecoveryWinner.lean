import Std

namespace RTDB.Recovery

structure Rank where
  synced : Nat
  last : Int

def Dominates (better other : Rank) : Prop :=
  other.synced < better.synced ∨
    (other.synced = better.synced ∧ other.last ≤ better.last)

instance (better other : Rank) : Decidable (Dominates better other) :=
  inferInstanceAs (Decidable (_ ∨ (_ ∧ _)))

def choose (current candidate : Rank) : Rank :=
  if Dominates candidate current then candidate else current

private theorem dominance_trans {first second third : Rank}
    (top : Dominates first second) (mid : Dominates second third) : Dominates first third := by
  unfold Dominates at *
  rcases top with higher | ⟨sameTop, longerTop⟩ <;>
    rcases mid with lower | ⟨sameMid, longerMid⟩ <;>
    first | left; omega | right; constructor <;> omega

theorem choose_dominates_both (current candidate : Rank) :
    Dominates (choose current candidate) current ∧ Dominates (choose current candidate) candidate := by
  by_cases prefer : Dominates candidate current
  · rw [choose, ite_eq_left prefer]
    exact ⟨prefer, by simp [Dominates]⟩
  · rw [choose, ite_eq_right prefer]
    refine ⟨by simp [Dominates], ?_⟩
    unfold Dominates at prefer ⊢
    rcases Nat.lt_trichotomy candidate.synced current.synced with lower | same | higher
    · exact Or.inl lower
    · right
      exact ⟨same, by omega⟩
    · exact (prefer (Or.inl higher)).elim

theorem selected_history_dominates_every_reply (first : Rank) (rest : List Rank) :
    ∀ reply ∈ first :: rest, Dominates (rest.foldl choose first) reply := by
  induction rest generalizing first with
  | nil =>
    intro reply member
    obtain rfl := List.mem_singleton.mp member
    simp [Dominates]
  | cons candidate rest ih =>
    intro reply member
    rw [List.foldl_cons]
    have aboveChosen := ih (choose first candidate) (choose first candidate) List.mem_cons_self
    obtain ⟨aboveFirst, aboveCandidate⟩ := choose_dominates_both first candidate
    rcases List.mem_cons.mp member with rfl | tail
    · exact dominance_trans aboveChosen aboveFirst
    · rcases List.mem_cons.mp tail with rfl | later
      · exact dominance_trans aboveChosen aboveCandidate
      · exact ih _ reply (List.mem_cons_of_mem _ later)

theorem the_winner_matches_a_retained_commit_witness_or_has_a_later_sync
    (first : Rank) (rest : List Rank) (commitEpoch : Nat) (commitLsn : Int) :
    (∃ witness ∈ first :: rest, witness.synced = commitEpoch ∧ commitLsn ≤ witness.last) →
    let selected := rest.foldl choose first
    commitEpoch < selected.synced ∨
      (commitEpoch = selected.synced ∧ commitLsn ≤ selected.last) := by
  rintro ⟨witness, member, synced, covers⟩
  have ranked := selected_history_dominates_every_reply first rest witness member
  unfold Dominates at ranked
  rw [synced] at ranked
  exact ranked.imp id (fun ⟨same, lasts⟩ => ⟨same, Int.le_trans covers lasts⟩)

#print axioms choose_dominates_both
#print axioms selected_history_dominates_every_reply
#print axioms the_winner_matches_a_retained_commit_witness_or_has_a_later_sync

end RTDB.Recovery
