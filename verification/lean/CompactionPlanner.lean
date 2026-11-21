import Std

namespace RTDB.CompactionPlanner

abbrev Bucket := Fin 4
abbrev Iid := Nat → Bucket
abbrev Path := List Bucket

inductive PageTree (Row : Type) where
  | empty
  | leaf (rows : List Row)
  | recency (older newer : PageTree Row)
  | iid (zero one two three : PageTree Row)

def PageTree.pick (zero one two three : PageTree Row) (bucket : Bucket) : PageTree Row :=
  if bucket.val = 0 then zero
  else if bucket.val = 1 then one
  else if bucket.val = 2 then two
  else three

def PageTree.expose (digit : Bucket) : PageTree Row → PageTree Row
  | .empty => .empty
  | .leaf rows => .leaf rows
  | .recency older newer => .recency (older.expose digit) (newer.expose digit)
  | .iid zero one two three => pick zero one two three digit

def PageTree.terminal : PageTree Row → Bool
  | .empty | .leaf _ => true
  | .recency older newer => older.terminal && newer.terminal
  | .iid _ _ _ _ => false

def PageTree.rows : PageTree Row → List Row
  | .empty => []
  | .leaf rows => rows
  | .recency older newer => older.rows ++ newer.rows
  | .iid zero one two three => zero.rows ++ one.rows ++ two.rows ++ three.rows

def PageTree.route (iid : Iid) (depth : Nat) : PageTree Row → List Row
  | .empty => []
  | .leaf rows => rows
  | .recency older newer => older.route iid depth ++ newer.route iid depth
  | .iid zero one two three =>
    if (iid depth).val = 0 then zero.route iid (depth + 1)
    else if (iid depth).val = 1 then one.route iid (depth + 1)
    else if (iid depth).val = 2 then two.route iid (depth + 1)
    else three.route iid (depth + 1)

theorem expose_preserves_route (tree : PageTree Row) (iid : Iid) (depth : Nat) :
    (tree.expose (iid depth)).route iid (depth + 1) = tree.route iid depth := by
  induction tree with
  | empty => rfl
  | leaf _ => rfl
  | recency _ _ older newer => simp [PageTree.expose, PageTree.route, older, newer]
  | iid z o t h _ _ _ _ =>
    simp only [PageTree.expose, PageTree.pick, PageTree.route]
    split <;> try rfl
    split <;> try rfl
    split <;> rfl

theorem terminal_route_is_flattened (tree : PageTree Row) (iid : Iid) (depth : Nat)
    (terminal : tree.terminal) : tree.route iid depth = tree.rows := by
  induction tree with
  | empty => rfl
  | leaf _ => rfl
  | recency _ _ older newer =>
    obtain ⟨left, right⟩ := Bool.and_eq_true_iff.mp terminal
    simp [PageTree.route, PageTree.rows, older left, newer right]
  | iid _ _ _ _ _ _ _ _ => simp [PageTree.terminal] at terminal

/-- The outline records where the simultaneous overlay decides to split. -/
inductive Outline where
  | task
  | split (zero one two three : Outline)

def exposeForest (digit : Bucket) (forest : List (PageTree Row)) : List (PageTree Row) :=
  forest.map (·.expose digit)

inductive Admitted {Row : Type} : Outline → List (PageTree Row) → Prop where
  | task : (∀ tree ∈ forest, tree.terminal) → Admitted .task forest
  | split :
    Admitted zero (exposeForest 0 forest) →
    Admitted one (exposeForest 1 forest) →
    Admitted two (exposeForest 2 forest) →
    Admitted three (exposeForest 3 forest) →
    Admitted (.split zero one two three) forest

def readTask (iid : Iid) (depth : Nat) (forest : List (PageTree Row)) : Outline → List Row
  | .task => forest.flatMap PageTree.rows
  | .split zero one two three =>
    let digit := iid depth
    let remaining := exposeForest digit forest
    if digit.val = 0 then readTask iid (depth + 1) remaining zero
    else if digit.val = 1 then readTask iid (depth + 1) remaining one
    else if digit.val = 2 then readTask iid (depth + 1) remaining two
    else readTask iid (depth + 1) remaining three

theorem routed_forest_exposure (iid : Iid) (depth : Nat) (forest : List (PageTree Row)) :
    (exposeForest (iid depth) forest).flatMap (PageTree.route iid (depth + 1)) =
      forest.flatMap (PageTree.route iid depth) := by
  induction forest with
  | nil => rfl
  | cons tree forest later =>
    simp only [exposeForest, List.map_cons, List.flatMap_cons] at later ⊢
    rw [expose_preserves_route, later]

theorem read_task_agrees_with_per_segment_route (iid : Iid) (forest : List (PageTree Row))
    (admitted : Admitted outline forest) (depth : Nat) :
    readTask iid depth forest outline = forest.flatMap (PageTree.route iid depth) := by
  induction admitted generalizing depth with
  | @task pending all =>
    simp only [readTask]
    induction pending with
    | nil => rfl
    | cons tree forest rest =>
      rw [List.flatMap_cons, List.flatMap_cons,
        terminal_route_is_flattened tree iid depth (all tree (by simp)),
        rest (fun later member => all later (List.mem_cons_of_mem _ member))]
  | @split zero forest one two three z o t h hz ho ht hh =>
    simp only [readTask]
    split
    · rename_i digit
      have equal : iid depth = 0 := Fin.ext digit
      rw [equal, hz, ← equal, routed_forest_exposure]
    · rename_i nz
      split
      · rename_i digit
        have equal : iid depth = 1 := Fin.ext digit
        rw [equal, ho, ← equal, routed_forest_exposure]
      · rename_i no
        split
        · rename_i digit
          have equal : iid depth = 2 := Fin.ext digit
          rw [equal, ht, ← equal, routed_forest_exposure]
        · rename_i nt
          have equal : iid depth = 3 := Fin.ext (by have bound := (iid depth).isLt; omega)
          rw [equal, hh, ← equal, routed_forest_exposure]

#print axioms read_task_agrees_with_per_segment_route

def PageTree.holdsBucket (key : Row → Iid) (depth : Nat) (bucket : Bucket)
    (tree : PageTree Row) : Prop := ∀ row ∈ tree.rows, key row depth = bucket

def PageTree.aligned (key : Row → Iid) (depth : Nat) : PageTree Row → Prop
  | .empty | .leaf _ => True
  | .recency older newer => older.aligned key depth ∧ newer.aligned key depth
  | .iid zero one two three =>
    (zero.holdsBucket key depth 0 ∧ zero.aligned key (depth + 1)) ∧
    (one.holdsBucket key depth 1 ∧ one.aligned key (depth + 1)) ∧
    (two.holdsBucket key depth 2 ∧ two.aligned key (depth + 1)) ∧
    (three.holdsBucket key depth 3 ∧ three.aligned key (depth + 1))

theorem wrong_bucket_has_no_match (key : Row → Iid) (iid : Iid)
    (keeps : Row → Bool) (sound : ∀ row, keeps row → key row = iid)
    (depth : Nat) (tree : PageTree Row) (bucket : Bucket)
    (aligned : tree.holdsBucket key depth bucket) (different : iid depth ≠ bucket) :
    tree.rows.filter keeps = [] := by
  apply List.filter_eq_nil_iff.mpr
  intro row member kept
  have digit := aligned row member
  rw [sound row kept] at digit
  exact different digit

theorem routed_rows_match_flat_rows (key : Row → Iid) (iid : Iid)
    (keeps : Row → Bool) (sound : ∀ row, keeps row → key row = iid)
    (tree : PageTree Row) (depth : Nat) (aligned : tree.aligned key depth) :
    (tree.route iid depth).filter keeps = tree.rows.filter keeps := by
  induction tree generalizing depth with
  | empty => rfl
  | leaf _ => rfl
  | recency _ _ older newer =>
    obtain ⟨left, right⟩ := aligned
    simp [PageTree.route, PageTree.rows, List.filter_append, older _ left, newer _ right]
  | iid zero one two three base next third last =>
    obtain ⟨⟨zp, za⟩, ⟨op, oa⟩, ⟨tp, ta⟩, ⟨hp, ha⟩⟩ := aligned
    have diff (b : Bucket) (no : (iid depth).val ≠ b.val) : iid depth ≠ b :=
      fun equal => no (congrArg Fin.val equal)
    have omitOther (tree : PageTree Row) (b : Bucket)
        (holds : tree.holdsBucket key depth b) (away : (iid depth).val ≠ b.val) :
        tree.rows.filter keeps = [] :=
      wrong_bucket_has_no_match key iid keeps sound depth tree b holds (diff b away)
    simp only [PageTree.route, PageTree.rows, List.filter_append]
    split
    · rename_i z
      have o : (iid depth).val ≠ (1 : Bucket).val := by simp [z]
      have t : (iid depth).val ≠ (2 : Bucket).val := by simp [z]
      have h : (iid depth).val ≠ (3 : Bucket).val := by change (iid depth).val ≠ 3; omega
      simp [base _ za, omitOther one 1 op o, omitOther two 2 tp t, omitOther three 3 hp h]
    · rename_i z
      split
      · rename_i o
        have t : (iid depth).val ≠ (2 : Bucket).val := by simp [o]
        have h : (iid depth).val ≠ (3 : Bucket).val := by change (iid depth).val ≠ 3; omega
        simp [next _ oa, omitOther zero 0 zp z, omitOther two 2 tp t, omitOther three 3 hp h]
      · rename_i o
        split
        · rename_i t
          have h : (iid depth).val ≠ (3 : Bucket).val := by change (iid depth).val ≠ 3; omega
          simp [third _ ta, omitOther zero 0 zp z, omitOther one 1 op o, omitOther three 3 hp h]
        · rename_i t
          simp [last _ ha, omitOther zero 0 zp z, omitOther one 1 op o, omitOther two 2 tp t]

theorem read_task_preserves_matching_occurrences (iid : Iid) (key : Row → Iid)
    (keeps : Row → Bool) (sound : ∀ row, keeps row → key row = iid)
    (forest : List (PageTree Row)) (admitted : Admitted outline forest)
    (depth : Nat) (aligned : ∀ tree ∈ forest, tree.aligned key depth) :
    (readTask iid depth forest outline).filter keeps =
      (forest.flatMap PageTree.rows).filter keeps := by
  rw [read_task_agrees_with_per_segment_route iid forest admitted depth]
  clear admitted
  induction forest with
  | nil => rfl
  | cons tree forest tail =>
    simp only [List.flatMap_cons, List.filter_append]
    rw [routed_rows_match_flat_rows key iid keeps sound tree depth (aligned tree (by simp))]
    rw [tail (fun rest member => aligned rest (List.mem_cons_of_mem _ member))]

#print axioms read_task_preserves_matching_occurrences

def admitsPrefix (iid : Iid) (depth : Nat) : Path → Bool
  | [] => true
  | bucket :: path => decide (iid depth = bucket) && admitsPrefix iid (depth + 1) path

def readPart (iid : Iid) (part : Path) (depth : Nat) (keeps : Row → Bool)
    (forest : List (PageTree Row)) (outline : Outline) : List Row :=
  if admitsPrefix iid 0 part then (readTask iid depth forest outline).filter keeps else []

theorem read_part_preserves_matching_occurrences (iid : Iid) (key : Row → Iid) (part : Path)
    (keeps : Row → Bool) (sound : ∀ row, keeps row → key row = iid)
    (forest : List (PageTree Row)) (admitted : Admitted outline forest)
    (depth : Nat) (aligned : ∀ tree ∈ forest, tree.aligned key depth) :
    readPart iid part depth keeps forest outline =
      (forest.flatMap PageTree.rows).filter (fun row => keeps row && admitsPrefix (key row) 0 part) := by
  have clip : (fun row => keeps row && admitsPrefix (key row) 0 part) =
      (fun row => keeps row && admitsPrefix iid 0 part) := by
    funext row
    cases kept : keeps row
    · simp
    · rw [sound row kept]
  rw [clip]
  by_cases within : admitsPrefix iid 0 part
  · simpa [readPart, within] using
      read_task_preserves_matching_occurrences iid key keeps sound forest admitted depth aligned
  · simp [readPart, within]

#print axioms read_part_preserves_matching_occurrences

end RTDB.CompactionPlanner
