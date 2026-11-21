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

private theorem countChildren (left right parent : Prop)
    [Decidable left] [Decidable right] [Decidable parent]
    (covers : (left ∨ right) ↔ parent) (disjoint : ¬(left ∧ right)) :
    (if left then 1 else 0) + (if right then 1 else 0) =
      (if parent then 1 else 0) := by
  by_cases hl : left <;> by_cases hr : right <;> by_cases hp : parent <;> simp_all

/-- Every requested point contributes once within the domain, including at a split boundary. -/
theorem cover_hits (lower upper base level point : Nat) :
    hits point (cover lower upper base level) =
      if (⟨base, level⟩ : Bucket).has point ∧ lower ≤ point ∧ point < upper then 1 else 0 := by
  induction level generalizing base with
  | zero =>
    simp only [cover, Bucket.has, Bucket.size, Nat.pow_zero]
    by_cases h₁ : lower ≤ base ∧ base < upper
    · rw [ite_eq_left h₁]
      by_cases h₂ : base ≤ point ∧ point < base + 1
      · have he : point = base := by omega
        subst point
        simp [hits, Bucket.has, Bucket.size, h₁]
      · simp [hits, Bucket.has, Bucket.size, h₂]
    · rw [ite_eq_right h₁]
      apply Eq.symm
      apply ite_eq_right
      omega
  | succ level ih =>
    have hpow : 2 ^ (level + 1) = 2 ^ level + 2 ^ level := by
      simp [Nat.pow_succ, Nat.mul_two]
    simp only [cover, Bucket.has, Bucket.size, hpow]
    by_cases full : lower ≤ base ∧ base + (2 ^ level + 2 ^ level) ≤ upper
    · rw [ite_eq_left full]
      by_cases h : base ≤ point ∧ point < base + (2 ^ level + 2 ^ level)
      · have hq : lower ≤ point ∧ point < upper := by omega
        simp [hits, Bucket.has, Bucket.size, hpow, h, hq]
      · simp [hits, Bucket.has, Bucket.size, hpow, h]
    · rw [ite_eq_right full]
      by_cases outside : upper ≤ base ∨ base + (2 ^ level + 2 ^ level) ≤ lower
      · rw [ite_eq_left outside]
        apply Eq.symm
        apply ite_eq_right
        omega
      · rw [ite_eq_right outside, hits_append, ih, ih]
        simp only [Bucket.has, Bucket.size]
        apply countChildren <;> omega

/-- A cover built left to right must place each next bucket at the current
frontier, and stop exactly at the upper endpoint. -/
inductive Tiling : Nat → Nat → List Bucket → Prop
  | done (upper : Nat) : Tiling upper upper []
  | step (bucket : Bucket) {upper : Nat} {rest : List Bucket} :
      Tiling (bucket.base + bucket.size) upper rest →
      Tiling bucket.base upper (bucket :: rest)

theorem Tiling.ordered {lower upper : Nat} {buckets : List Bucket}
    (tiling : Tiling lower upper buckets) : lower ≤ upper := by
  induction tiling with
  | done => exact Nat.le_refl _
  | step _ _ ih => omega

/-- Membership is independent of which aligned width the builder chooses.
The greedy iterator only needs to make a positive, non-overshooting step. -/
theorem tiling_hits {lower upper : Nat} {buckets : List Bucket}
    (tiling : Tiling lower upper buckets) (point : Nat) :
    hits point buckets = if lower ≤ point ∧ point < upper then 1 else 0 := by
  induction tiling with
  | done =>
    simp only [hits, List.filter_nil, List.length_nil]
    apply Eq.symm
    apply ite_eq_right
    omega
  | step bucket restTiling ih =>
    change hits point ([bucket] ++ _) = _
    rw [hits_append, ih]
    have singleton : hits point [bucket] = if bucket.has point then 1 else 0 := by
      by_cases inside : bucket.has point <;> simp [hits, inside]
    rw [singleton]
    have ordered := restTiling.ordered
    apply countChildren <;> simp only [Bucket.has] <;> omega

end RTDB.OrderedIndex
