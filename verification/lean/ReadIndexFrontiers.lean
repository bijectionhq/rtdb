import Std

namespace RTDB.ReadIndexFrontiers

inductive Reply where
  | observed (heldBeforeCall heldAtReply : Bool)
  | missing (heldBeforeCall : Bool)
  deriving DecidableEq

def priorVotes : List Reply → Nat
  | [] => 0
  | .observed prior _ :: rest | .missing prior :: rest => prior.toNat + priorVotes rest

def observedVotes : List Reply → Nat
  | [] => 0
  | .missing _ :: rest => observedVotes rest
  | .observed _ held :: rest => held.toNat + observedVotes rest

def missingVotes : List Reply → Nat
  | [] => 0
  | .observed _ _ :: rest => missingVotes rest
  | .missing _ :: rest => 1 + missingVotes rest

def upperVotes (replies : List Reply) : Nat :=
  observedVotes replies + missingVotes replies

def Consistent : List Reply → Prop
  | [] => True
  | .observed prior held :: rest => prior.toNat ≤ held.toNat ∧ Consistent rest
  | .missing _ :: rest => Consistent rest

theorem completed_position_is_below_the_read_bound
    (replies : List Reply) (quorum : Nat) (consistent : Consistent replies)
    (completed : quorum ≤ priorVotes replies) : quorum ≤ upperVotes replies := by
  suffices priorVotes replies ≤ upperVotes replies from Nat.le_trans completed this
  clear completed quorum
  induction replies with
  | nil => simp [priorVotes, upperVotes, observedVotes, missingVotes]
  | cons reply rest ih =>
    cases reply with
    | observed prior held =>
      simp only [Consistent] at consistent
      have le := ih consistent.2
      simp only [priorVotes, upperVotes, observedVotes, missingVotes] at le ⊢
      omega
    | missing prior =>
      have le := ih consistent
      cases prior <;>
        simp [priorVotes, upperVotes, observedVotes, missingVotes] at le ⊢ <;> omega

theorem complete_observation_excludes_a_minority_tail
    (replies : List Reply) (quorum : Nat) (complete : missingVotes replies = 0)
    (minority : observedVotes replies < quorum) : upperVotes replies < quorum := by
  simp [upperVotes, complete, minority]

theorem missing_reply_can_hide_a_completed_position :
    let replies : List Reply := [.observed true true, .observed false false, .missing true]
    priorVotes replies = 2 ∧ observedVotes replies = 1 ∧ upperVotes replies = 2 := by decide

#print axioms completed_position_is_below_the_read_bound
#print axioms complete_observation_excludes_a_minority_tail
#print axioms missing_reply_can_hide_a_completed_position

end RTDB.ReadIndexFrontiers
