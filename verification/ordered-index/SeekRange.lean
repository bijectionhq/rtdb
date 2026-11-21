import Init

set_option autoImplicit false

namespace RTDB.OrderedIndex

/-- A key's rank within the ordered inputs to a seek. -/
def Below (predicate : Nat → Bool) : Prop :=
  ∀ a b, a ≤ b → predicate b = true → predicate a = true

def Above (predicate : Nat → Bool) : Prop :=
  ∀ a b, a ≤ b → predicate a = true → predicate b = true

def zone (lower upper : Nat → Bool) (key : Nat) : Nat :=
  if lower key then 0 else if upper key then 2 else 1

theorem zone_inside (lower upper : Nat → Bool) (key : Nat) :
    zone lower upper key = 1 ↔ lower key = false ∧ upper key = false := by
  cases h₁ : lower key <;> cases h₂ : upper key <;> simp [zone, h₁, h₂]

/-- Multiple contradictory bounds can make the window empty while preserving
the binary search partition. The lower tests take precedence. -/
theorem zone_monotone (lower upper : Nat → Bool) (hl : Below lower) (hu : Above upper)
    (a b : Nat) (ordered : a ≤ b) : zone lower upper a ≤ zone lower upper b := by
  cases ha : lower a with
  | true => simp [zone, ha]
  | false =>
    have hb : lower b = false := by
      cases h : lower b with
      | false => rfl
      | true =>
        have contradiction := hl a b ordered h
        simp [ha] at contradiction
    cases ha' : upper a with
    | true => simp [zone, ha, hb, ha', hu a b ordered ha']
    | false => cases hb' : upper b <;> simp [zone, ha, hb, ha', hb']

/-- A page outside the interval can be skipped without examining its entries. -/
theorem page_before_range (lower upper : Nat → Bool) (hl : Below lower) (hu : Above upper)
    (key last : Nat) (inPage : key ≤ last) (fence : zone lower upper last = 0) :
    zone lower upper key = 0 := by
  have h := zone_monotone lower upper hl hu key last inPage
  omega

theorem page_after_range (lower upper : Nat → Bool) (hl : Below lower) (hu : Above upper)
    (first key : Nat) (inPage : first ≤ key) (fence : zone lower upper first = 2) :
    zone lower upper key = 2 := by
  have h := zone_monotone lower upper hl hu first key inPage
  have bound : zone lower upper key ≤ 2 := by
    cases h₁ : lower key <;> cases h₂ : upper key <;> simp [zone, h₁, h₂]
  omega

def prefixWidth (predicate : Nat → Bool) : Nat → Nat
  | 0 => 0
  | length + 1 => if predicate length then length + 1 else prefixWidth predicate length

theorem prefixWidth_bound (predicate : Nat → Bool) (length : Nat) :
    prefixWidth predicate length ≤ length := by
  induction length with
  | zero => simp [prefixWidth]
  | succ length ih =>
    cases h : predicate length <;> simp [prefixWidth, h, Nat.le_succ_of_le ih]

/-- Every downward-closed predicate over a finite page has the boundary
required by the production binary search. -/
theorem prefixWidth_exact (predicate : Nat → Bool) (h : Below predicate)
    (length index : Nat) (inPage : index < length) :
    predicate index = true ↔ index < prefixWidth predicate length := by
  induction length with
  | zero => omega
  | succ length ih =>
    cases last : predicate length with
    | true =>
      have value := h index length (by omega) last
      simp [prefixWidth, last, value, inPage]
    | false =>
      by_cases previous : index < length
      · simpa [prefixWidth, last] using ih previous
      · have equal : index = length := by omega
        subst index
        have bound := prefixWidth_bound predicate length
        have beyond : ¬ length < prefixWidth predicate length := Nat.not_lt.mpr bound
        simp [prefixWidth, last, beyond]


/-- The ascending cut is also the count of entries on its left. Subtracting
it from the page length gives the number consumed before a reverse seek. -/
theorem reverse_prefix_width (predicate : Nat → Bool) (h : Below predicate)
    (length offset : Nat) (inPage : offset < length) :
    predicate (length - 1 - offset) = false ↔ offset < length - prefixWidth predicate length := by
  have hindex : length - 1 - offset < length := by omega
  have exactCut := prefixWidth_exact predicate h length (length - 1 - offset) hindex
  have hcut := prefixWidth_bound predicate length
  cases found : predicate (length - 1 - offset) <;> simp [found] at exactCut ⊢ <;> omega

end RTDB.OrderedIndex
