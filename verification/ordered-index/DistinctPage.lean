import Init

set_option autoImplicit false

namespace RTDB.OrderedIndex

def orderedAfter (previous : Option Nat) (key : Nat) : Prop :=
  match previous with
  | none => True
  | some prior => prior ≤ key

def distinctRuns (previous : Option Nat) : List Nat → List Nat
  | [] => []
  | key :: rest =>
    if previous = some key then distinctRuns previous rest
    else key :: distinctRuns (some key) rest

theorem distinctRuns_mem (previous : Option Nat) (keys : List Nat) (key : Nat)
    (sorted : keys.Pairwise (· ≤ ·))
    (after : ∀ x ∈ keys, orderedAfter previous x) :
    key ∈ distinctRuns previous keys ↔ key ∈ keys ∧ previous ≠ some key := by
  induction keys generalizing previous with
  | nil => simp [distinctRuns]
  | cons head tail ih =>
    have headAfter := after head (by simp)
    obtain ⟨headBefore, tailOrder⟩ := List.pairwise_cons.mp sorted
    by_cases same : previous = some head
    · have tailAfter : ∀ x ∈ tail, orderedAfter previous x :=
        fun x hx => after x (by simp [hx])
      rw [distinctRuns, ite_eq_left same, ih previous tailOrder tailAfter]
      by_cases headKey : key = head
      · subst key
        simp [same]
      · simp [headKey]
    · rw [distinctRuns, ite_eq_right same]
      simp only [List.mem_cons]
      rw [ih (some head) tailOrder (fun x hx => headBefore x hx)]
      by_cases headKey : key = head
      · subst key
        simp [same]
      · have dropPrior : key ∈ tail → previous ≠ some key := by
          intro inTail eq
          rw [eq] at headAfter
          exact headKey (Nat.le_antisymm headAfter (headBefore key inTail))
        have dropHead : some head ≠ some key := fun eq => headKey (Option.some.inj eq).symm
        simpa [headKey, dropHead] using
          (show key ∈ tail ↔ key ∈ tail ∧ previous ≠ some key from
            ⟨fun h => ⟨h, dropPrior h⟩, And.left⟩)

theorem distinctRuns_strict (previous : Option Nat) (keys : List Nat)
    (sorted : keys.Pairwise (· ≤ ·))
    (after : ∀ x ∈ keys, orderedAfter previous x) :
    (distinctRuns previous keys).Pairwise (· < ·) := by
  induction keys generalizing previous with
  | nil => exact .nil
  | cons head tail ih =>
    obtain ⟨headBefore, tailOrder⟩ := List.pairwise_cons.mp sorted
    by_cases same : previous = some head
    · rw [distinctRuns, ite_eq_left same]
      exact ih previous tailOrder (fun x hx => after x (by simp [hx]))
    · have afterHead : ∀ x ∈ tail, orderedAfter (some head) x := fun x hx => headBefore x hx
      rw [distinctRuns, ite_eq_right same, List.pairwise_cons]
      refine ⟨?_, ih (some head) tailOrder afterHead⟩
      intro key hkey
      obtain ⟨inTail, different⟩ := (distinctRuns_mem (some head) tail key tailOrder afterHead).mp hkey
      have order := headBefore key inTail
      have unequal : head ≠ key := fun equal => different (congrArg some equal)
      omega

/-- A distinct key in the global prefix survives each per-stream bound
in any stream that contains it. This is the bound used before overlay. -/
theorem global_prefix_survives_local_limit (stream global : List Nat)
    (sub : stream.Sublist global) (unique : global.Nodup)
    (key count : Nat) (member : key ∈ stream) (winner : key ∈ global.take count) :
    key ∈ stream.take count := by
  induction sub generalizing count with
  | slnil => simp at member
  | @cons localKeys _ dropped sub ih =>
    obtain ⟨different, uniqueTail⟩ := List.nodup_cons.mp unique
    have notKey : key ≠ dropped := by
      intro eq
      subst dropped
      exact different (sub.subset member)
    cases count with
    | zero => simp at winner
    | succ count =>
      simp [List.take_succ_cons, notKey] at winner
      have found := ih uniqueTail count member winner
      have smaller := List.take_sublist count (localKeys.take (count + 1))
      rw [List.take_take, Nat.min_eq_left (Nat.le_succ count)] at smaller
      exact smaller.subset found
  | cons_cons both sub ih =>
    obtain ⟨_, uniqueTail⟩ := List.nodup_cons.mp unique
    cases count with
    | zero => simp at winner
    | succ count =>
      by_cases head : key = both
      · simp [head]
      · simp [head] at member
        simp [List.take_succ_cons, head] at winner
        simp [head, ih uniqueTail count member winner]

private theorem ordered_keys_eq (left right : List Nat)
    (leftStrict : left.Pairwise (· < ·)) (rightStrict : right.Pairwise (· < ·))
    (same : ∀ key, key ∈ left ↔ key ∈ right) : left = right := by
  induction left generalizing right with
  | nil =>
    cases right with
    | nil => rfl
    | cons head tail =>
      have impossible := (same head).mpr (by simp)
      simp at impossible
  | cons head tail ih =>
    cases right with
    | nil =>
      have impossible := (same head).mp (by simp)
      simp at impossible
    | cons other rest =>
      obtain ⟨beforeTail, strictTail⟩ := List.pairwise_cons.mp leftStrict
      obtain ⟨beforeRest, strictRest⟩ := List.pairwise_cons.mp rightStrict
      have inOthers := (same head).mp (by simp)
      have inLeft := (same other).mpr (by simp)
      have lower : other ≤ head := by
        rcases List.mem_cons.mp inOthers with equal | later
        · exact Nat.le_of_eq equal.symm
        · exact Nat.le_of_lt (beforeRest head later)
      have upper : head ≤ other := by
        rcases List.mem_cons.mp inLeft with equal | later
        · exact Nat.le_of_eq equal.symm
        · exact Nat.le_of_lt (beforeTail other later)
      have equal := Nat.le_antisymm upper lower
      subst other
      apply congrArg (List.cons head)
      apply ih rest strictTail strictRest
      intro key
      by_cases first : key = head
      · subst key
        exact ⟨fun h => (Nat.lt_irrefl head (beforeTail head h)).elim,
               fun h => (Nat.lt_irrefl head (beforeRest head h)).elim⟩
      · simpa [first] using same key

/-- Once clearance is applied, ordered postings with the same candidate
keys give the same stream after eliminating duplicate runs. -/
theorem distinct_runs_refine_membership (left right : List Nat)
    (leftSorted : left.Pairwise (· ≤ ·)) (rightSorted : right.Pairwise (· ≤ ·))
    (same : ∀ key, key ∈ left ↔ key ∈ right) :
    distinctRuns none left = distinctRuns none right := by
  have leftStrict := distinctRuns_strict none left leftSorted (by simp [orderedAfter])
  have rightStrict := distinctRuns_strict none right rightSorted (by simp [orderedAfter])
  apply ordered_keys_eq _ _ leftStrict rightStrict
  intro key
  rw [distinctRuns_mem none left key leftSorted (by simp [orderedAfter]),
      distinctRuns_mem none right key rightSorted (by simp [orderedAfter])]
  simpa using same key

structure Posting where
  key : Nat
  label : Nat

def admittedKeys (clear : Nat → Bool) (postings : List Posting) : List Nat :=
  (postings.filter fun posting => clear posting.label).map Posting.key

def distinctPage (clear : Nat → Bool) (postings : List Posting) (limit : Nat) : List Nat :=
  (distinctRuns none (admittedKeys clear postings)).take limit

theorem hidden_postings_preserve_distinct_page
    (clear : Nat → Bool) (front hidden tail : List Posting) (limit : Nat)
    (denied : ∀ posting ∈ hidden, clear posting.label = false) :
    distinctPage clear (front ++ hidden ++ tail) limit =
      distinctPage clear (front ++ tail) limit := by
  have empty : (hidden.filter fun posting => clear posting.label) = [] := by
    induction hidden with
    | nil => rfl
    | cons posting rest ih =>
      simp [denied posting (by simp), ih (fun p hp => denied p (by simp [hp]))]
  simp [distinctPage, admittedKeys, List.filter_append, empty]

end RTDB.OrderedIndex
