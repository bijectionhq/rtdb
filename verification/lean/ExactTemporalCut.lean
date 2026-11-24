import Std

namespace RTDB.ExactTemporalCut

structure Event where
  position : Nat
  ordinal : Nat
  sqlTime : Int
  deriving DecidableEq, Repr

def admitted (cut : Nat) (cap : Int) (event : Event) : Bool :=
  decide (event.position ≤ cut ∧ event.sqlTime ≤ cap)

def eligible (history : List Event) (cut : Nat) (cap : Int) : List Event :=
  history.filter (admitted cut cap)

theorem later_position_ineligible (cut : Nat) (cap : Int) (event : Event)
    (later : cut < event.position) :
    admitted cut cap event = false := by
  simp [admitted, Nat.not_le_of_lt later]

theorem suffix_outside_cut (later : List Event) (cut : Nat) (cap : Int)
    (outside : ∀ event ∈ later, cut < event.position) :
    eligible later cut cap = [] := by
  induction later with
  | nil => rfl
  | cons event rest ih =>
    have first := later_position_ineligible cut cap event (outside event (by simp))
    have tail := ih (fun other member => outside other (by simp [member]))
    simpa [eligible, first] using tail

theorem exact_cut_suffix_invariance (earlier suffix : List Event) (cut : Nat) (cap : Int)
    (outside : ∀ event ∈ suffix, cut < event.position) :
    eligible (earlier ++ suffix) cut cap = eligible earlier cut cap := by
  have tail := suffix_outside_cut suffix cut cap outside
  change suffix.filter (admitted cut cap) = [] at tail
  simp [eligible, List.filter_append, tail]

-- The traversal receives events in descending (SQL time, position, ordinal) order.
def winner (newestFirst : List Event) (cut : Nat) (cap : Int) : Option Event :=
  (eligible newestFirst cut cap).head?

inductive Result where
  | absent
  | original (event : Event)
  | erased (event : Event)
  deriving DecidableEq, Repr

def release (hidden : Event → Bool) : Option Event → Result
  | none => .absent
  | some event => if hidden event then .erased event else .original event

theorem original_or_erased (hidden : Event → Bool) (event : Event) :
    release hidden (some event) = .original event ∨
      release hidden (some event) = .erased event := by
  cases eq : hidden event <;> simp [release, eq]

theorem newest_prefix_irrelevant (future history : List Event) (cut : Nat) (cap : Int)
    (outside : ∀ event ∈ future, cut < event.position) :
    winner (future ++ history) cut cap = winner history cut cap := by
  have excluded := suffix_outside_cut future cut cap outside
  change future.filter (admitted cut cap) = [] at excluded
  simp [winner, eligible, List.filter_append, excluded]

private def original : Event := ⟨1, 1, 10⟩
private def correction : Event := ⟨2, 1, 15⟩
private def tied : Event := ⟨2, 1, 10⟩

example : 15 ≤ (40 : Int) ∧
    winner [correction, original] 1 20 = some original ∧
    winner [correction, original] 2 20 = some correction := by decide

example : winner [tied, original] 1 20 = some original ∧
    winner [tied, original] 2 20 = some tied := by decide

example : release (· == tied) (winner [tied, original] 2 20) = .erased tied := by decide

#print axioms later_position_ineligible
#print axioms exact_cut_suffix_invariance
#print axioms original_or_erased
#print axioms newest_prefix_irrelevant

end RTDB.ExactTemporalCut
