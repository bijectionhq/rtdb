import Std

namespace RTDB.SearchMemberBounds

def startsFrom (start : Nat) : List Nat → List Nat
  | [] => []
  | count :: rest => start :: startsFrom (start + count) rest

def prefixCount (doc : Nat) (starts : List Nat) : Nat :=
  (starts.takeWhile (· ≤ doc)).length

def locate (doc : Nat) : List Nat → Option (Nat × Nat)
  | [] => none
  | count :: rest =>
    if doc < count then some (0, doc)
    else (locate (doc - count) rest).map (fun (member, index) => (member + 1, index))

private theorem shifted_starts (counts : List Nat) (start delta : Nat) :
    startsFrom (delta + start) counts = (startsFrom start counts).map (delta + ·) := by
  induction counts generalizing start with
  | nil => rfl
  | cons count rest ih =>
    simp only [startsFrom, List.map_cons]
    rw [Nat.add_assoc, ih]

private theorem offset_count (counts : List Nat) (base doc : Nat) (covered : base ≤ doc) :
    prefixCount doc (startsFrom base counts) = prefixCount (doc - base) (startsFrom 0 counts) := by
  have shifted := shifted_starts counts 0 base
  simp only [Nat.add_zero] at shifted
  have inclusive (start : Nat) : decide (base + start ≤ doc) = decide (start ≤ doc - base) := by
    by_cases bound : start ≤ doc - base
    · have left : base + start ≤ doc := by omega
      simp [left, bound]
    · have left : ¬ base + start ≤ doc := by omega
      simp [left, bound]
  unfold prefixCount
  rw [shifted, List.takeWhile_map, List.length_map]
  have samePredicate : ((fun x => decide (x ≤ doc)) ∘ fun x => base + x) =
      (fun x => decide (x ≤ doc - base)) := funext inclusive
  rw [samePredicate]

theorem cumulative_starts_give_the_local_member (counts : List Nat) (doc member index : Nat)
    (located : locate doc counts = some (member, index)) :
    prefixCount doc (startsFrom 0 counts) = member + 1 := by
  induction counts generalizing doc member index with
  | nil => simp [locate] at located
  | cons count rest ih =>
    by_cases inFirst : doc < count
    · simp [locate, inFirst] at located
      obtain ⟨rfl, _⟩ := located
      cases rest <;> simp [prefixCount, startsFrom, show ¬ count ≤ doc by omega]
    · simp [locate, inFirst] at located
      obtain ⟨tailMember, tailLocated, rfl⟩ := located
      simp only [prefixCount, startsFrom, Nat.zero_le, decide_true, List.takeWhile_cons, ite_true,
        List.length_cons, Nat.zero_add]
      change prefixCount doc (startsFrom count rest) + 1 = tailMember + 1 + 1
      rw [offset_count rest count doc (by omega), ih _ _ index tailLocated]

theorem every_document_in_the_file_has_a_member (counts : List Nat) (doc : Nat)
    (inFile : doc < counts.sum) : ∃ member index, locate doc counts = some (member, index) := by
  induction counts generalizing doc with
  | nil => simp at inFile
  | cons count rest ih =>
    by_cases inFirst : doc < count
    · exact ⟨0, doc, by simp [locate, inFirst]⟩
    · obtain ⟨member, index, tail⟩ := ih (doc - count) (by simp at inFile; omega)
      exact ⟨member + 1, index, by simp [locate, inFirst, tail]⟩

def byDirectory (counts : List Nat) (doc : Nat) : Nat :=
  prefixCount doc (startsFrom 0 counts) - 1

theorem directory_owner_is_the_reference_owner (counts : List Nat) (doc member index : Nat)
    (located : locate doc counts = some (member, index)) :
    byDirectory counts doc = member := by
  simp [byDirectory, cumulative_starts_give_the_local_member counts doc member index located]

theorem empty_members_on_either_side_do_not_own_the_document :
    byDirectory [0, 1, 0] 0 = 1 := by decide

#print axioms cumulative_starts_give_the_local_member
#print axioms every_document_in_the_file_has_a_member
#print axioms directory_owner_is_the_reference_owner
#print axioms empty_members_on_either_side_do_not_own_the_document

end RTDB.SearchMemberBounds
