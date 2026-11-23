import Std

namespace RTDB.Recovery.OrderedQuorum

inductive Voter where
  | a | b | c
  deriving DecidableEq

def Majority (votes : Voter → Prop) : Prop :=
  (votes .a ∧ votes .b) ∨ (votes .a ∧ votes .c) ∨ (votes .b ∧ votes .c)

theorem replacement_keeps_majority_after_one_zone_loss (lost : Voter) :
    Majority (· ≠ lost) := by
  cases lost <;> simp [Majority]

theorem intersects (leftVotes : Majority left) (rightVotes : Majority right) :
    ∃ voter, left voter ∧ right voter := by
  rcases leftVotes with ⟨la, lb⟩ | ⟨la, lc⟩ | ⟨lb, lc⟩ <;>
    rcases rightVotes with ⟨ra, rb⟩ | ⟨ra, rc⟩ | ⟨rb, rc⟩ <;>
    first
    | exact ⟨.a, by assumption, by assumption⟩
    | exact ⟨.b, by assumption, by assumption⟩
    | exact ⟨.c, by assumption, by assumption⟩

structure Value where
  ballot : Nat
  entries : List Nat

def Extends (past next : List Nat) : Prop := ∃ suffix, next = past ++ suffix

theorem extends_trans (first : Extends a b) (second : Extends b c) : Extends a c := by
  rcases first with ⟨early, rfl⟩
  rcases second with ⟨later, rfl⟩
  exact ⟨early ++ later, List.append_assoc _ _ _⟩

/-- A previously chosen prefix survives recovery even if its application reply
was lost. `remembers` is the durable-prepare contract. `validHigher` is the
ballot induction hypothesis and `highest` is the actual accepted-value
selection obligation. Prepared payloads without an accept vote supply no
input to any of these premises. -/
theorem recovering_a_chosen_prefix (chosen next : Value)
    (accept prepared : Voter → Prop) (reported : Voter → Option Value) (picked : Value)
    (accepted : Majority accept) (quorum : Majority prepared)
    (remembers : ∀ voter, accept voter → prepared voter →
      ∃ value, reported voter = some value ∧ chosen.ballot ≤ value.ballot)
    (highest : ∀ voter value, prepared voter → reported voter = some value → value.ballot ≤ picked.ballot)
    (validHigher : chosen.ballot ≤ picked.ballot → Extends chosen.entries picked.entries)
    (extendsPicked : Extends picked.entries next.entries) : Extends chosen.entries next.entries := by
  obtain ⟨voter, acceptedHere, preparedHere⟩ := intersects accepted quorum
  obtain ⟨value, report, notOlder⟩ := remembers voter acceptedHere preparedHere
  have latest := Nat.le_trans notOlder (highest voter value preparedHere report)
  exact extends_trans (validHigher latest) extendsPicked

structure Seal where
  predecessor : Nat
  successor : Nat
  entries : List Nat

/-- Durable votes name one successor and one sealed prefix. A different
successor or a different prefix would need an intersecting voter to attest
both. The persisted vote map is the assumption supplied by the journal. -/
theorem sealed_successor_and_prefix_are_unique (durable : Voter → Option Seal)
    (a b : Seal) (first : Majority fun voter => durable voter = some a)
    (second : Majority fun voter => durable voter = some b) : a = b := by
  obtain ⟨voter, firstVote, secondVote⟩ := intersects first second
  exact Option.some.inj (firstVote.symm.trans secondVote)

theorem sealed_voters_cannot_form_a_late_append_majority (sealed append : Voter → Prop)
    (chosen : Majority sealed) (wouldRefuse : ∀ voter, sealed voter → ¬append voter) :
    ¬Majority append := by
  intro accepts
  obtain ⟨voter, didSeal, didAppend⟩ := intersects chosen accepts
  exact wouldRefuse voter didSeal didAppend

#print axioms replacement_keeps_majority_after_one_zone_loss
#print axioms recovering_a_chosen_prefix
#print axioms sealed_successor_and_prefix_are_unique
#print axioms sealed_voters_cannot_form_a_late_append_majority

end RTDB.Recovery.OrderedQuorum
