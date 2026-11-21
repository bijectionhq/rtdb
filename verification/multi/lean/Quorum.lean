import Std

namespace RTDB.Recovery

def HasMajority (replicas : List Replica) (votes : Replica → Bool) : Prop :=
  replicas.length < 2 * replicas.countP votes

theorem disjoint_votes_fit_inventory (replicas : List Replica) (left right : Replica → Bool)
    (separate : ∀ replica ∈ replicas, left replica = true → right replica = false) :
    replicas.countP left + replicas.countP right ≤ replicas.length := by
  induction replicas with
  | nil => simp
  | cons replica rest ih =>
      have tailBound := ih (fun item present => separate item (List.mem_cons_of_mem _ present))
      have headSeparate := separate replica List.mem_cons_self
      simp only [List.countP_cons, List.length_cons]
      cases leftVote : left replica <;> cases rightVote : right replica <;>
        simp [leftVote, rightVote] at headSeparate ⊢ <;> omega

theorem majorities_intersect (replicas : List Replica) (left right : Replica → Bool)
    (leftMajority : HasMajority replicas left) (rightMajority : HasMajority replicas right) :
    ∃ replica ∈ replicas, left replica = true ∧ right replica = true := by
  apply Classical.byContradiction
  intro disjoint
  have separate : ∀ replica ∈ replicas, left replica = true → right replica = false := by
    intro replica present leftVote
    cases rightVote : right replica with
    | false => rfl
    | true => exact (disjoint ⟨replica, present, leftVote, rightVote⟩).elim
  have fit := disjoint_votes_fit_inventory replicas left right separate
  unfold HasMajority at leftMajority rightMajority
  omega

theorem a_seal_quorum_excludes_a_later_stale_append_quorum
    (replicas : List Replica) (sealed accepted : Replica → Bool)
    (sealMajority : HasMajority replicas sealed)
    (refuses : ∀ replica ∈ replicas, sealed replica = true → accepted replica = false) :
    ¬HasMajority replicas accepted := by
  intro appendMajority
  obtain ⟨replica, present, sealVote, appendVote⟩ :=
    majorities_intersect replicas sealed accepted sealMajority appendMajority
  rw [refuses replica present sealVote] at appendVote
  contradiction

#print axioms majorities_intersect
#print axioms a_seal_quorum_excludes_a_later_stale_append_quorum

end RTDB.Recovery
