import Std

namespace RTDB.PublishedCheckpoint

structure Checkpoint where
  block : Nat
  journal : Nat
  boundary : Nat
  deriving DecidableEq

def agreesOrAdvances (anchor candidate : Checkpoint) : Prop :=
  candidate.journal = anchor.journal ∧
    ((candidate.block = anchor.block ∧ candidate.boundary = anchor.boundary) ∨
      (anchor.block < candidate.block ∧ anchor.boundary < candidate.boundary))

theorem an_accepted_checkpoint_keeps_its_journal
    (admitted : agreesOrAdvances anchor candidate) : candidate.journal = anchor.journal := by
  simpa [agreesOrAdvances] using admitted.1

theorem keeping_the_block_keeps_its_boundary
    (admitted : agreesOrAdvances anchor candidate) (sameBlock : anchor.block = candidate.block) :
    anchor.boundary = candidate.boundary := by
  rcases admitted.2 with same | later <;> omega

theorem advancing_the_block_advances_its_boundary
    (admitted : agreesOrAdvances anchor candidate) (laterBlock : anchor.block < candidate.block) :
    anchor.boundary < candidate.boundary := by
  rcases admitted.2 with same | later <;> omega

theorem accepts_transitively (first : agreesOrAdvances original current)
    (next : agreesOrAdvances current candidate) : agreesOrAdvances original candidate := by
  unfold agreesOrAdvances at first next ⊢
  constructor
  · first | exact next.1.trans first.1 | trivial
  · rcases first.2 with sameFirst | laterFirst <;> rcases next.2 with sameNext | laterNext
    · exact Or.inl ⟨by omega, by omega⟩
    · exact Or.inr ⟨by omega, by omega⟩
    · exact Or.inr ⟨by omega, by omega⟩
    · exact Or.inr ⟨by omega, by omega⟩

inductive Recovery (original : Checkpoint) : Checkpoint → Prop
  | admitted : Recovery original original
  | retry (prior : Recovery original current) (advance : agreesOrAdvances current next) : Recovery original next

theorem every_retry_respects_the_original_checkpoint
    (history : Recovery original selected) : agreesOrAdvances original selected := by
  induction history with
  | admitted => simp [agreesOrAdvances]
  | retry _ advance law => exact accepts_transitively law advance

#print axioms an_accepted_checkpoint_keeps_its_journal
#print axioms keeping_the_block_keeps_its_boundary
#print axioms advancing_the_block_advances_its_boundary
#print axioms accepts_transitively
#print axioms every_retry_respects_the_original_checkpoint

end RTDB.PublishedCheckpoint
