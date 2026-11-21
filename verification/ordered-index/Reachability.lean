import Init

set_option autoImplicit false

namespace RTDB.OrderedIndex

variable {Page Block : Type}

structure Layout (Page : Type) where
  frontier : Nat
  reachable : Page → Prop

/-- The persisted graph of a block shares old reachable pages and writes its own pages. -/
structure Advances (generation : Page → Nat) (previous next : Layout Page) : Prop where
  ordered : previous.frontier < next.frontier
  source : ∀ page, next.reachable page →
    previous.reachable page ∨ generation page = next.frontier

inductive AdvancesFrom (generation : Page → Nat) (old : Layout Page) : Layout Page → Prop
  | same : AdvancesFrom generation old old
  | step {previous next : Layout Page} :
      AdvancesFrom generation old previous →
      Advances generation previous next → AdvancesFrom generation old next

theorem frontier_monotone (generation : Page → Nat) (old current : Layout Page)
    (history : AdvancesFrom generation old current) : old.frontier ≤ current.frontier := by
  induction history with
  | same => exact Nat.le_refl _
  | step _ change ih => exact Nat.le_trans ih (Nat.le_of_lt change.ordered)

/-- A newly reachable page at or below an older frontier must be reachable at that
frontier. A mark taken there remains safe across arbitrarily many later blocks. -/
theorem older_page_is_shared (generation : Page → Nat) (old current : Layout Page)
    (history : AdvancesFrom generation old current) (page : Page)
    (cutoff : generation page ≤ old.frontier) (reach : current.reachable page) :
    old.reachable page := by
  induction history with
  | same => exact reach
  | step earlier change ih =>
    rcases change.source page reach with inherited | created
    · exact ih inherited
    · have h₁ := frontier_monotone generation old _ earlier
      have h₂ := change.ordered
      omega

def collectible (generation : Page → Nat) (marked : Layout Page) (page : Page) : Prop :=
  generation page ≤ marked.frontier ∧ ¬marked.reachable page

theorem stale_mark_preserves_current (generation : Page → Nat) (marked current : Layout Page)
    (history : AdvancesFrom generation marked current) (page : Page)
    (remove : collectible generation marked page) :
    ¬current.reachable page := by
  intro reach
  exact remove.2 (older_page_is_shared generation marked current history page remove.1 reach)

theorem retained_references_survive_sweep (generation : Page → Nat)
    (marked current : Layout Page) (history : AdvancesFrom generation marked current)
    (stored : Page → Prop) (complete : ∀ page, current.reachable page → stored page) :
    ∀ page, current.reachable page → stored page ∧ ¬collectible generation marked page := by
  intro page reach
  exact ⟨complete page reach, fun remove =>
    stale_mark_preserves_current generation marked current history page remove reach⟩

/-- A replay transition derives its page generation from the source block.
Resuming from an exact prefix and rebuilding from empty use identical
transitions, regardless of the requested final frontier. -/
theorem rebuilt_layout_matches (writeBlock : Layout Page → Block → Layout Page)
    (empty : Layout Page) (older tail : List Block) :
    tail.foldl writeBlock (older.foldl writeBlock empty) =
      (older ++ tail).foldl writeBlock empty := by
  rw [List.foldl_append]

end RTDB.OrderedIndex
