import Std

namespace RTDB.MemoryTrie

inductive Bucket where
  | zero | one | two | three
  deriving DecidableEq

inductive Trie (Row : Type) where
  | leaf (rows : List Row)
  | branch (zero one two three : Trie Row)

def contents : Trie Row → List Row
  | .leaf rows => rows
  | .branch zero one two three =>
      contents zero ++ (contents one ++ (contents two ++ contents three))

variable {Row Iid : Type}

def inBucket (bucketOf : Row → Nat → Bucket) (depth : Nat) (bucket : Bucket)
    (rows : List Row) : List Row :=
  rows.filter fun row => decide (bucketOf row depth = bucket)

theorem buckets_preserve_rows (bucketOf : Row → Nat → Bucket) (depth : Nat) (rows : List Row) :
    (inBucket bucketOf depth .zero rows ++ (inBucket bucketOf depth .one rows ++
      (inBucket bucketOf depth .two rows ++ inBucket bucketOf depth .three rows))).Perm rows := by
  induction rows with
  | nil => simp [inBucket]
  | cons row rest ih =>
      cases atBucket : bucketOf row depth with
      | zero => simpa [inBucket, atBucket] using ih.cons row
      | one =>
          simp only [inBucket, List.filter_cons, atBucket, reduceCtorEq, decide_false,
            decide_true, Bool.false_eq_true, ↓reduceIte]
          exact List.perm_middle.trans (ih.cons row)
      | two =>
          have move := List.perm_middle (a := row)
            (l₁ := inBucket bucketOf depth .zero rest ++ inBucket bucketOf depth .one rest)
            (l₂ := inBucket bucketOf depth .two rest ++ inBucket bucketOf depth .three rest)
          have cons := move.trans (by simpa only [List.append_assoc] using ih.cons row)
          simpa [inBucket, atBucket, List.append_assoc] using cons
      | three =>
          have move := List.perm_middle (a := row)
            (l₁ := inBucket bucketOf depth .zero rest ++ (inBucket bucketOf depth .one rest ++
              inBucket bucketOf depth .two rest))
            (l₂ := inBucket bucketOf depth .three rest)
          have cons := move.trans (by simpa only [List.append_assoc] using ih.cons row)
          simpa [inBucket, atBucket, List.append_assoc] using cons

def rebuild (bucketOf : Row → Nat → Bucket)
    (split : Nat → List Row → Bool) (depth : Nat) (remaining : Nat) (rows : List Row) : Trie Row :=
  match remaining with
  | 0 => .leaf rows
  | n + 1 =>
      if split depth rows then
        .branch (rebuild bucketOf split (depth + 1) n (inBucket bucketOf depth .zero rows))
          (rebuild bucketOf split (depth + 1) n (inBucket bucketOf depth .one rows))
          (rebuild bucketOf split (depth + 1) n (inBucket bucketOf depth .two rows))
          (rebuild bucketOf split (depth + 1) n (inBucket bucketOf depth .three rows))
      else .leaf rows

theorem rebuild_preserves_rows (bucketOf : Row → Nat → Bucket)
    (split : Nat → List Row → Bool) (depth remaining : Nat) (rows : List Row) :
    (contents (rebuild bucketOf split depth remaining rows)).Perm rows := by
  induction remaining generalizing depth rows with
  | zero => simp [rebuild, contents]
  | succ remaining ih =>
      by_cases h : split depth rows
      · simp only [rebuild, h, ↓reduceIte, contents]
        exact ((ih ..).append ((ih ..).append ((ih ..).append (ih ..)))).trans
          (buckets_preserve_rows bucketOf depth rows)
      · simp [rebuild, h, contents]

def addAll (bucketOf : Row → Nat → Bucket) (split : Nat → List Row → Bool) (maxDepth : Nat)
    (depth : Nat) (trie : Trie Row) (rows : List Row) : Trie Row :=
  match trie with
  | .leaf old => rebuild bucketOf split depth (maxDepth - depth) (old ++ rows)
  | .branch zero one two three =>
      .branch (addAll bucketOf split maxDepth (depth + 1) zero (inBucket bucketOf depth .zero rows))
        (addAll bucketOf split maxDepth (depth + 1) one (inBucket bucketOf depth .one rows))
        (addAll bucketOf split maxDepth (depth + 1) two (inBucket bucketOf depth .two rows))
        (addAll bucketOf split maxDepth (depth + 1) three (inBucket bucketOf depth .three rows))

private theorem collect_appended {left leftBefore leftAfter right rightBefore rightAfter : List Row}
    (l : left.Perm (leftBefore ++ leftAfter)) (r : right.Perm (rightBefore ++ rightAfter)) :
    (left ++ right).Perm ((leftBefore ++ rightBefore) ++ (leftAfter ++ rightAfter)) := by
  apply (l.append r).trans
  simpa only [List.append_assoc] using
    (List.perm_append_comm_assoc leftAfter rightBefore rightAfter).append_left leftBefore

theorem addAll_preserves_rows (bucketOf : Row → Nat → Bucket) (split : Nat → List Row → Bool)
    (maxDepth depth : Nat) (trie : Trie Row) (rows : List Row) :
    (contents (addAll bucketOf split maxDepth depth trie rows)).Perm (contents trie ++ rows) := by
  induction trie generalizing depth rows with
  | leaf =>
      exact rebuild_preserves_rows bucketOf split _ _ _
  | branch zero one two three iz io it ih =>
      simp only [addAll, contents]
      refine (collect_appended (iz ..) (collect_appended (io ..)
        (collect_appended (it ..) (ih ..)))).trans ?_
      exact (buckets_preserve_rows bucketOf depth rows).append_left _

variable [DecidableEq Iid]

def matching (iidOf : Row → Iid) (target : Iid) (rows : List Row) : List Row :=
  rows.filter fun row => decide (iidOf row = target)

def lookup (iidOf : Row → Iid) (route : Iid → Nat → Bucket) (target : Iid)
    (depth : Nat) : Trie Row → List Row
  | .leaf rows => matching iidOf target rows
  | .branch zero one two three =>
      match route target depth with
      | .zero => lookup iidOf route target (depth + 1) zero
      | .one => lookup iidOf route target (depth + 1) one
      | .two => lookup iidOf route target (depth + 1) two
      | .three => lookup iidOf route target (depth + 1) three

private theorem matching_routed (iidOf : Row → Iid) (route : Iid → Nat → Bucket)
    (target : Iid) (depth : Nat) (rows : List Row) :
    matching iidOf target (inBucket (fun row => route (iidOf row)) depth (route target depth) rows) =
      matching iidOf target rows := by
  simp only [matching, inBucket, List.filter_filter]
  apply List.filter_congr
  intro row _
  by_cases same : iidOf row = target <;> simp [same]

theorem lookup_rebuild (iidOf : Row → Iid) (route : Iid → Nat → Bucket)
    (split : Nat → List Row → Bool) (target : Iid) (depth remaining : Nat) (rows : List Row) :
    lookup iidOf route target depth (rebuild (fun row => route (iidOf row)) split depth remaining rows) =
      matching iidOf target rows := by
  induction remaining generalizing depth rows with
  | zero => simp [rebuild, lookup]
  | succ remaining ih =>
      by_cases h : split depth rows
      · cases bucket : route target depth <;>
          simpa [rebuild, h, lookup, bucket, ih] using
            matching_routed iidOf route target depth rows
      · simp [rebuild, h, lookup]

theorem lookup_addAll (iidOf : Row → Iid) (route : Iid → Nat → Bucket)
    (split : Nat → List Row → Bool) (target : Iid) (maxDepth depth : Nat)
    (trie : Trie Row) (rows : List Row) :
    lookup iidOf route target depth
        (addAll (fun row => route (iidOf row)) split maxDepth depth trie rows) =
      lookup iidOf route target depth trie ++ matching iidOf target rows := by
  induction trie generalizing depth rows with
  | leaf =>
      simp [addAll, lookup_rebuild, lookup, matching]
  | branch zero one two three iz io it ih =>
      cases bucket : route target depth <;>
        simp only [addAll, lookup, bucket, iz, io, it, ih] <;>
        rw [← bucket, matching_routed]

def insertBatches (bucketOf : Row → Nat → Bucket) (split : Nat → List Row → Bool)
    (maxDepth depth : Nat) (trie : Trie Row) (batches : List (List Row)) : Trie Row :=
  batches.foldl (addAll bucketOf split maxDepth depth) trie

theorem batched_lookup_matches_history (iidOf : Row → Iid) (route : Iid → Nat → Bucket)
    (split : Nat → List Row → Bool) (target : Iid) (maxDepth depth : Nat)
    (batches : List (List Row)) (trie : Trie Row) :
    lookup iidOf route target depth
        (insertBatches (fun row => route (iidOf row)) split maxDepth depth trie batches) =
      lookup iidOf route target depth trie ++ matching iidOf target batches.flatten := by
  induction batches generalizing trie with
  | nil => simp [insertBatches, matching]
  | cons rows rest ih =>
      simp [insertBatches, List.foldl_cons] at ih ⊢
      rw [ih, lookup_addAll, List.append_assoc]
      simp [matching]

theorem batched_rows_match_history (bucketOf : Row → Nat → Bucket)
    (split : Nat → List Row → Bool) (maxDepth depth : Nat) (batches : List (List Row)) (trie : Trie Row) :
    (contents (insertBatches bucketOf split maxDepth depth trie batches)).Perm
      (contents trie ++ batches.flatten) := by
  induction batches generalizing trie with
  | nil => simp [insertBatches]
  | cons rows rest ih =>
      simp only [insertBatches, List.foldl_cons, List.flatten_cons, ← List.append_assoc] at *
      exact (ih _).trans ((addAll_preserves_rows ..).append_right rest.flatten)

theorem regrouping_preserves_observations (iidOf : Row → Iid) (route : Iid → Nat → Bucket)
    (leftSplit rightSplit : Nat → List Row → Bool) (maxDepth depth : Nat)
    (left right : List (List Row)) (sameHistory : left.flatten = right.flatten) :
    let l := insertBatches (fun row => route (iidOf row)) leftSplit maxDepth depth (.leaf []) left
    let r := insertBatches (fun row => route (iidOf row)) rightSplit maxDepth depth (.leaf []) right
    (contents l).Perm (contents r) ∧
      ∀ target, lookup iidOf route target depth l = lookup iidOf route target depth r := by
  dsimp only
  constructor
  · apply (batched_rows_match_history ..).trans
    rw [sameHistory]
    exact (batched_rows_match_history ..).symm
  · intro target
    rw [batched_lookup_matches_history, batched_lookup_matches_history, sameHistory]

#print axioms rebuild_preserves_rows
#print axioms lookup_rebuild
#print axioms batched_lookup_matches_history
#print axioms batched_rows_match_history
#print axioms regrouping_preserves_observations

end RTDB.MemoryTrie
