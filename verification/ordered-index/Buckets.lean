import Init

set_option autoImplicit false

namespace RTDB.OrderedIndex

structure Bucket where
  base : Nat
  level : Nat
  deriving DecidableEq

def Bucket.size (bucket : Bucket) : Nat := 2 ^ bucket.level

def Bucket.has (bucket : Bucket) (point : Nat) : Prop :=
  bucket.base ≤ point ∧ point < bucket.base + bucket.size

instance (bucket : Bucket) (point : Nat) : Decidable (bucket.has point) :=
  inferInstanceAs (Decidable (bucket.base ≤ point ∧ point < bucket.base + bucket.size))

-- This decomposition traverses aligned descendants of one complete binary domain.
def cover (lower upper base : Nat) : Nat → List Bucket
  | 0 => if lower ≤ base ∧ base < upper then [⟨base, 0⟩] else []
  | level + 1 =>
    if lower ≤ base ∧ base + 2 ^ (level + 1) ≤ upper then [⟨base, level + 1⟩]
    else if upper ≤ base ∨ base + 2 ^ (level + 1) ≤ lower then []
    else cover lower upper base level ++ cover lower upper (base + 2 ^ level) level

def hits (point : Nat) (buckets : List Bucket) : Nat :=
  (buckets.filter (fun bucket => decide (bucket.has point))).length

theorem hits_append (point : Nat) (left right : List Bucket) :
    hits point (left ++ right) = hits point left + hits point right := by
  simp [hits, List.filter_append]

/-- Every requested point contributes once within the domain, including at a split boundary. -/
theorem cover_hits (lower upper base level point : Nat) :
    hits point (cover lower upper base level) =
      if (⟨base, level⟩ : Bucket).has point ∧ lower ≤ point ∧ point < upper then 1 else 0 := by
  induction level generalizing base with
  | zero =>
    simp only [cover, Bucket.has, Bucket.size, Nat.pow_zero]
    by_cases h₁ : lower ≤ base ∧ base < upper
    · rw [if_pos h₁]
      by_cases h₂ : base ≤ point ∧ point < base + 1
      · have he : point = base := by omega
        subst point
        simp [hits, Bucket.has, Bucket.size, h₁]
      · simp [hits, Bucket.has, Bucket.size, h₂]
    · rw [if_neg h₁]
      apply Eq.symm
      apply if_neg
      omega
  | succ level ih =>
    have hpow : 2 ^ (level + 1) = 2 ^ level + 2 ^ level := by
      simp [Nat.pow_succ, Nat.mul_two]
    simp only [cover, Bucket.has, Bucket.size, hpow]
    by_cases full : lower ≤ base ∧ base + (2 ^ level + 2 ^ level) ≤ upper
    · rw [if_pos full]
      by_cases h : base ≤ point ∧ point < base + (2 ^ level + 2 ^ level)
      · have hq : lower ≤ point ∧ point < upper := by omega
        simp [hits, Bucket.has, Bucket.size, hpow, h, hq]
      · simp [hits, Bucket.has, Bucket.size, hpow, h]
    · rw [if_neg full]
      by_cases outside : upper ≤ base ∨ base + (2 ^ level + 2 ^ level) ≤ lower
      · rw [if_pos outside]
        apply Eq.symm
        apply if_neg
        omega
      · rw [if_neg outside, hits_append, ih, ih]
        simp only [Bucket.has, Bucket.size]
        split <;> split <;> split <;> omega

end RTDB.OrderedIndex
