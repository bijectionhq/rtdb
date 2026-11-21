import Std

namespace RTDB.OuterJoin

inductive Truth where
  | yes
  | no
  | unknown

def accepts : Truth → Bool
  | .yes => true
  | _ => false

def conjunction : Truth → Truth → Truth
  | .no, _ | _, .no => .no
  | .yes, truth | truth, .yes => truth
  | .unknown, .unknown => .unknown

theorem accepts_conjunction (left right : Truth) :
    accepts (conjunction left right) = (accepts left && accepts right) := by
  cases left <;> cases right <;> rfl

theorem an_unknown_predicate_does_not_accept : accepts .unknown = false := by
  rfl

def matchFor (on : Left → Right → Truth) (left : Left) (right : List Right) : List (Option Right) :=
  match right.filter (fun row => accepts (on left row)) with
  | [] => [none]
  | head :: rest => (head :: rest).map some

def leftJoin (on : Left → Right → Truth) (left : List Left) (right : List Right) : List (Left × Option Right) :=
  left.flatMap fun row => (matchFor on row right).map (fun matched => (row, matched))

theorem preserved_side_group_filter (predicate : Left → Truth) (left : Left) (rows : List (Option Right)) :
    ((rows.map fun matched => (left, matched)).filter (fun pair => accepts (predicate pair.1))) =
      if accepts (predicate left) then rows.map (fun matched => (left, matched)) else [] := by
  induction rows with
  | nil => simp
  | cons matched rest ih =>
    cases accepted : accepts (predicate left) <;> simp [accepted] at ih ⊢ <;> simp [ih]

theorem preserved_side_where_filters_before_left_join (on : Left → Right → Truth) (predicate : Left → Truth)
    (left : List Left) (right : List Right) :
    (leftJoin on left right).filter (fun pair => accepts (predicate pair.1)) =
      leftJoin on (left.filter fun row => accepts (predicate row)) right := by
  induction left with
  | nil => rfl
  | cons first rest ih =>
    simp only [leftJoin, List.flatMap_cons, List.filter_append, preserved_side_group_filter]
    change ((if accepts (predicate first) then (matchFor on first right).map (fun matched => (first, matched)) else []) ++
      (leftJoin on rest right).filter (fun pair => accepts (predicate pair.1))) =
        leftJoin on ((first :: rest).filter fun row => accepts (predicate row)) right
    rw [ih]
    cases accepted : accepts (predicate first) <;> simp [accepted, leftJoin]

theorem unpreserved_side_on_filters_before_left_join (on : Left → Right → Truth) (predicate : Right → Truth)
    (left : List Left) (right : List Right) :
    leftJoin (fun l r => conjunction (on l r) (predicate r)) left right =
      leftJoin on left (right.filter fun row => accepts (predicate row)) := by
  have sameMatches (row : Left) : matchFor (fun l r => conjunction (on l r) (predicate r)) row right =
      matchFor on row (right.filter fun item => accepts (predicate item)) := by
    simp [matchFor, accepts_conjunction, List.filter_filter]
  simp [leftJoin, sameMatches]

def isNull : Option Unit → Truth
  | none => .yes
  | some _ => .no

theorem moving_nullable_side_where_can_create_a_row :
    let on : Unit → Unit → Truth := fun _ _ => .yes
    let left : List Unit := [()]
    let right : List Unit := [()]
    (leftJoin on left right).filter (fun pair => accepts (isNull pair.2)) ≠
      (leftJoin on left (right.filter fun item => accepts (isNull (some item)))).filter
        (fun pair => accepts (isNull pair.2)) := by
  decide

#print axioms accepts_conjunction
#print axioms an_unknown_predicate_does_not_accept
#print axioms preserved_side_where_filters_before_left_join
#print axioms unpreserved_side_on_filters_before_left_join
#print axioms moving_nullable_side_where_can_create_a_row

end RTDB.OuterJoin
