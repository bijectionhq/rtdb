import Temporal

set_option autoImplicit false

namespace RTDB.OrderedIndex

variable {K : Type}

/-- Meaning of a decoded, complete checkpoint. The source is the exact ordered
prefix. Physical pages and their references are modeled separately. -/
structure Checkpoint (K : Type) where
  source : List (Event K)
  root : Nat → List (Span K)
  agrees : ∀ systemAt validAt, seekSpan validAt (root systemAt) = tableAt source systemAt validAt

def extend (checkpoint : Checkpoint K) (tail : List (Event K)) : Checkpoint K where
  source := checkpoint.source ++ tail
  root := fun systemAt => tail.foldl (edits systemAt) (checkpoint.root systemAt)
  agrees := by
    intro systemAt validAt
    rw [replay_refines_table_from, checkpoint.agrees]
    simp [tableAt, List.foldl_append]

def build (events : List (Event K)) : Checkpoint K where
  source := events
  root := fun systemAt => events.foldl (edits systemAt) []
  agrees := replay_refines_table events

theorem extend_preserves_basis (checkpoint : Checkpoint K) (tail : List (Event K))
    (systemAt : Nat) (validAt : Int) :
    seekSpan validAt ((extend checkpoint tail).root systemAt) =
      seekSpan validAt ((build (checkpoint.source ++ tail)).root systemAt) := by
  rw [(extend checkpoint tail).agrees, (build (checkpoint.source ++ tail)).agrees]
  rfl

/-- A storage reader offers a checkpoint only after decoding a complete
authoritative prefix, and retains its continuation from the same capture. -/
inductive Coverage (capture : List (Event K)) where
  | unavailable
  | usable (before : Checkpoint K) (tail : List (Event K))
      (exact : before.source ++ tail = capture)

def serve (capture : List (Event K)) (coverage : Coverage capture)
    (systemAt : Nat) (validAt : Int) : Option K :=
  match coverage with
  | .unavailable => tableAt capture systemAt validAt
  | .usable before tail _ => seekSpan validAt ((extend before tail).root systemAt)

theorem serve_exact (capture : List (Event K)) (coverage : Coverage capture)
    (systemAt : Nat) (validAt : Int) :
    serve capture coverage systemAt validAt = tableAt capture systemAt validAt := by
  cases coverage with
  | unavailable => rfl
  | usable before tail exactPrefix =>
    rw [serve, (extend before tail).agrees]
    exact congrArg (fun events => tableAt events systemAt validAt) exactPrefix

/-- The result is independent of physical compaction, abandoned uploads,
and loss of a checkpoint if coverage uses the captured log. -/
theorem serving_independent (capture : List (Event K)) (left right : Coverage capture)
    (systemAt : Nat) (validAt : Int) :
    serve capture left systemAt validAt = serve capture right systemAt validAt := by
  rw [serve_exact, serve_exact]


def weight {A : Type} (rows : A → Nat) : List A → Nat
  | [] => 0
  | block :: blocks => rows block + weight rows blocks

theorem sublist_weight_le {A : Type} (rows : A → Nat) {actual expected : List A}
    (identity : actual.Sublist expected) :
    weight rows actual ≤ weight rows expected := by
  induction identity with
  | slnil => simp [weight]
  | cons block _ ih => simp only [weight]; omega
  | cons_cons block _ ih => simp only [weight]; omega

/-- A count closes coverage after the input loader establishes ordered inclusion
and every omitted block would account for a positive number of events. -/
theorem count_completes_ordered_inclusion {A : Type} (rows : A → Nat)
    {actual expected : List A} (identity : actual.Sublist expected)
    (nonempty : ∀ block ∈ expected, 0 < rows block)
    (count : weight rows actual = weight rows expected) :
    actual = expected := by
  induction identity with
  | slnil => rfl
  | cons block sub _ =>
    have hl := sublist_weight_le rows sub
    have ht := nonempty block (by simp)
    simp only [weight] at count
    omega
  | cons_cons block _ ih =>
    apply congrArg (List.cons block)
    apply ih
    · exact fun tailBlock h => nonempty tailBlock (by simp [h])
    · simp only [weight] at count
      omega

theorem count_alone_loses_a_retraction :
    let put : Event Nat := ⟨1, ⟨0, 1⟩, .put 1⟩
    let del : Event Nat := ⟨2, ⟨0, 1⟩, .delete⟩
    [put, put].length = [put, del].length ∧
      tableAt [put, put] 2 0 ≠ tableAt [put, del] 2 0 := by
  decide

end RTDB.OrderedIndex
