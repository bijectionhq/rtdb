import Std

namespace RTDB.Recovery

def chain (link : Digest → Entry → Digest) (seed : Digest) (entries : List Entry) : Digest :=
  entries.foldl link seed

def onlineChain (link : Digest → Entry → Digest) (seed : Digest) (pages : List (List Entry)) : Digest :=
  pages.foldl (chain link) seed

theorem online_pages_preserve_the_flat_committed_chain (link : Digest → Entry → Digest)
    (seed : Digest) (pages : List (List Entry)) :
    onlineChain link seed pages = chain link seed pages.flatten := by
  induction pages generalizing seed with
  | nil => rfl
  | cons first rest ih =>
    simp only [onlineChain, List.foldl_cons, List.flatten_cons]
    rw [show List.foldl (chain link) (chain link seed first) rest = _ from ih (chain link seed first)]
    simp [chain, List.foldl_append]

theorem an_adjacent_saved_prefix_preserves_the_complete_chain (link : Digest → Entry → Digest)
    (seed : Digest) (before after : List Entry) :
    chain link (chain link seed before) after = chain link seed (before ++ after) := by
  simp [chain, List.foldl_append]

inductive Verdict (Digest : Type) where
  | agreed (digest : Digest)
  | insufficient
  | divergent

def strictAgreement [DecidableEq Digest] (proofs : List (Option Digest)) (quorum : Nat) : Verdict Digest :=
  let covered := proofs.filterMap id
  match covered with
  | [] => .insufficient
  | first :: rest =>
    if rest.any (· != first) then .divergent
    else if quorum ≤ covered.length then .agreed first else .insufficient

theorem strict_agreement_requires_quorum_coverage [DecidableEq Digest]
    (proofs : List (Option Digest)) (quorum : Nat) (digest : Digest)
    (accepted : strictAgreement proofs quorum = .agreed digest) :
    quorum ≤ (proofs.filterMap id).length := by
  cases coverage : proofs.filterMap id with
  | nil => simp [strictAgreement, coverage] at accepted
  | cons first rest =>
    by_cases enough : quorum ≤ rest.length + 1
    · exact enough
    · by_cases differs : (rest.any (· != first)) = true <;>
        simp [strictAgreement, coverage, enough, differs] at accepted

theorem strict_agreement_requires_equal_returned_digests [DecidableEq Digest]
    (proofs : List (Option Digest)) (quorum : Nat) (digest : Digest)
    (accepted : strictAgreement proofs quorum = .agreed digest) :
    ∀ returned ∈ proofs.filterMap id, returned = digest := by
  cases coverage : proofs.filterMap id with
  | nil => simp
  | cons first rest =>
    by_cases differs : rest.any (· != first) = true
    · simp [strictAgreement, coverage, differs] at accepted
    · by_cases enough : quorum ≤ rest.length + 1
      · have sameDigest : first = digest := Verdict.agreed.inj
          (by simpa [strictAgreement, coverage, enough, differs] using accepted)
        subst digest
        intro returned member
        rcases List.mem_cons.mp member with rfl | present
        · rfl
        · by_cases correct : returned = first
          · exact correct
          · exact (differs (List.any_eq_true.mpr ⟨returned, present, by simp [correct]⟩)).elim
      · simp [strictAgreement, coverage, enough, differs] at accepted

theorem strict_agreement_reports_insufficient_when_agreeing_proofs_fall_short [DecidableEq Digest]
    (proofs : List (Option Digest)) (quorum : Nat)
    (short : (proofs.filterMap id).length < quorum)
    (agree : ∀ first ∈ proofs.filterMap id, ∀ later ∈ proofs.filterMap id, later = first) :
    strictAgreement proofs quorum = .insufficient := by
  cases coverage : proofs.filterMap id with
  | nil => simp [strictAgreement, coverage]
  | cons first rest =>
    rw [coverage] at agree short
    have identical : ¬rest.any (· != first) = true := by
      intro differs
      obtain ⟨later, present, mismatch⟩ := List.any_eq_true.mp differs
      rw [agree first List.mem_cons_self later (List.mem_cons_of_mem _ present)] at mismatch
      simp at mismatch
    have notEnough : ¬quorum ≤ rest.length + 1 := Nat.not_le_of_gt short
    simp [strictAgreement, coverage, identical, notEnough]

#print axioms online_pages_preserve_the_flat_committed_chain
#print axioms an_adjacent_saved_prefix_preserves_the_complete_chain
#print axioms strict_agreement_requires_quorum_coverage
#print axioms strict_agreement_requires_equal_returned_digests
#print axioms strict_agreement_reports_insufficient_when_agreeing_proofs_fall_short

end RTDB.Recovery
