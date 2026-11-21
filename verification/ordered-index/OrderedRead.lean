import Init

set_option autoImplicit false

namespace RTDB.OrderedIndex

variable {A K V : Type}

structure Order (A : Type) where
  le : A → A → Bool
  trans : ∀ a b c, le a b → le b c → le a c
  total : ∀ a b, le a b || le b a
  antisymm : ∀ a b, le a b → le b a → a = b

def mergeStreams (order : Order A) : List (List A) → List A
  | [] => []
  | stream :: rest => List.merge stream (mergeStreams order rest) order.le

theorem mergeStreams_perm (order : Order A) (streams : List (List A)) :
    (mergeStreams order streams).Perm streams.flatten := by
  induction streams with
  | nil => exact .nil
  | cons stream rest ih =>
    exact (List.merge_perm_append order.le).trans (List.Perm.append (.refl stream) ih)

theorem mergeStreams_sorted (order : Order A) (streams : List (List A))
    (sorted : ∀ stream ∈ streams, stream.Pairwise (fun a b => order.le a b = true)) :
    (mergeStreams order streams).Pairwise (fun a b => order.le a b = true) := by
  induction streams with
  | nil => exact .nil
  | cons stream rest ih =>
    refine List.pairwise_merge order.trans order.total _ _ (sorted stream (by simp)) ?_
    exact ih (fun xs hx => sorted xs (by simp [hx]))

/-- A complete tie-break makes the list with these contents and this order unique. -/
theorem mergeStreams_refines_sort (order : Order A) (streams : List (List A))
    (sorted : ∀ stream ∈ streams, stream.Pairwise (fun a b => order.le a b = true)) :
    mergeStreams order streams = streams.flatten.mergeSort order.le := by
  apply List.Perm.eq_of_pairwise (le := fun a b => order.le a b = true)
  · exact fun a b _ _ => order.antisymm a b
  · exact mergeStreams_sorted order streams sorted
  · exact List.pairwise_mergeSort order.trans order.total streams.flatten
  · exact (mergeStreams_perm order streams).trans (List.mergeSort_perm _ _).symm

def firstMatches (keep : A → Bool) (limit : Nat) (rows : List A) : List A :=
  match rows with
  | [] => []
  | row :: rest =>
    match limit with
    | 0 => []
    | limit + 1 =>
      if keep row then row :: firstMatches keep limit rest
      else firstMatches keep (limit + 1) rest

theorem firstMatches_eq_take (keep : A → Bool) (limit : Nat) (rows : List A) :
    firstMatches keep limit rows = (rows.filter keep).take limit := by
  induction rows generalizing limit with
  | nil => cases limit <;> rfl
  | cons row rows ih =>
    cases limit with
    | zero => simp [firstMatches]
    | succ n => cases h : keep row <;> simp [firstMatches, h, ih]

theorem ordered_page (order : Order A) (streams : List (List A))
    (sorted : ∀ stream ∈ streams, stream.Pairwise (fun a b => order.le a b = true))
    (keep : A → Bool) (limit : Nat) :
    firstMatches keep limit (mergeStreams order streams) =
      ((streams.flatten.mergeSort order.le).filter keep).take limit := by
  rw [firstMatches_eq_take, mergeStreams_refines_sort order streams sorted]

/-- A keyset continuation compares the full order key at the same basis. -/
theorem continuation (order : Order A) (front tail : List A) (last : A)
    (before : ∀ row ∈ front, order.le row last = true)
    (after : ∀ row ∈ tail, order.le row last = false) :
    (front ++ tail).filter (fun row => !order.le row last) = tail := by
  have hp : front.filter (fun row => !order.le row last) = [] := by
    induction front with
    | nil => rfl
    | cons row rows ih =>
      simp [before row (by simp), ih (fun row hr => before row (by simp [hr]))]
  have ht : tail.filter (fun row => !order.le row last) = tail := by
    induction tail with
    | nil => rfl
    | cons row rows ih =>
      simp [after row (by simp), ih (fun row hr => after row (by simp [hr]))]
  simp [List.filter_append, hp, ht]

/-- The finite ordered key domain includes gaps, whose observed value is none. -/
def inspected (read : K → Option V) (limit : Nat) : List K → List K
  | [] => []
  | key :: rest =>
    match limit with
    | 0 => []
    | limit + 1 =>
      key :: inspected read (if (read key).isSome then limit else limit + 1) rest

theorem witnessed_prefix (before after : K → Option V) (keys : List K) (limit : Nat)
    (agreement : ∀ key ∈ inspected before limit keys, before key = after key) :
    (keys.filterMap before).take limit = (keys.filterMap after).take limit := by
  induction keys generalizing limit with
  | nil => cases limit <;> rfl
  | cons key keys ih =>
    cases limit with
    | zero => simp
    | succ n =>
      have same : before key = after key := agreement key (by simp [inspected])
      cases hb : before key with
      | none =>
        have ha : after key = none := same.symm.trans hb
        simp only [List.filterMap_cons, hb, ha]
        exact ih (n + 1) (fun x hx => agreement x (by simp [inspected, hb, hx]))
      | some value =>
        have ha : after key = some value := same.symm.trans hb
        simp only [List.filterMap_cons, hb, ha, List.take_succ_cons]
        exact congrArg (List.cons value)
          (ih n (fun x hx => agreement x (by simp [inspected, hb, hx])))

def observation (rows : List V) (limit : Nat) : List V × Bool :=
  (rows.take limit, decide (limit < rows.length))

/-- The witness through the first lookahead covers page rows, exhaustion, and hasMore. -/
theorem page_and_more_refine (before after : K → Option V) (keys : List K) (limit : Nat)
    (agreement : ∀ key ∈ inspected before (limit + 1) keys, before key = after key) :
    observation (keys.filterMap before) limit = observation (keys.filterMap after) limit := by
  have h := witnessed_prefix before after keys (limit + 1) agreement
  have hp := congrArg (List.take limit) h
  have hl := congrArg List.length h
  simp only [List.take_take, Nat.min_eq_left (Nat.le_succ limit)] at hp
  simp only [List.length_take] at hl
  have hm : (limit < (keys.filterMap before).length) = (limit < (keys.filterMap after).length) := by
    apply propext
    omega
  simp [observation, hp, hm]

end RTDB.OrderedIndex
