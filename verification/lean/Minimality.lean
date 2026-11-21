import History

namespace RTDB

theorem single_system_coordinate_cannot_express_fresh_erase :
    ¬ ∃ read : Nat → Nat → Int → Option Nat,
      ∀ horizon system key valid, system ≤ horizon →
        read system key valid = point (admit [correction, original] horizon) system key valid := by
  rintro ⟨read, agrees⟩
  have oldAnswer : point (admit [correction, original] 1) 1 0 3 = some 20 := by decide
  have erasedAnswer : point (admit [correction, original] 2) 1 0 3 = none := by decide
  have old := (agrees 1 1 0 3 (by decide)).trans oldAnswer
  have erased := (agrees 2 1 0 3 (by decide)).trans erasedAnswer
  have impossible : (some 20 : Option Nat) = none := old.symm.trans erased
  simp at impossible

theorem committed_state_alone_cannot_express_read_your_writes :
    ¬ ∃ read : History → Option Nat,
      ∀ history resolved, read history =
        scan (history.length + 1) 0 3 (privateView history resolved) := by
  rintro ⟨read, agrees⟩
  have empty : scan 1 0 3 (privateView [] []) = none := by decide
  have written : scan 1 0 3 (privateView [] original) = some 20 := by decide
  have before := (agrees [] []).trans empty
  have after := (agrees [] original).trans written
  have impossible : (some 20 : Option Nat) = none := after.symm.trans before
  simp at impossible

structure Balances where
  debit : Nat
  credit : Nat
  deriving DecidableEq, Repr

def Balances.total (balances : Balances) : Nat :=
  balances.debit + balances.credit

def transfer (balance : Balances) (amount : Nat) : Balances :=
  ⟨balance.debit - amount, balance.credit + amount⟩

theorem a_whole_transfer_preserves_balance (balance : Balances) (amount : Nat)
    (available : amount ≤ balance.debit) :
    (transfer balance amount).total = balance.total := by
  simp [transfer, Balances.total]
  omega

theorem visible_half_transfer_breaks_balance :
    (Balances.mk 10 0).total ≠ (Balances.mk 7 0).total := by decide

theorem same_commit_operational_order_is_observable :
    point (admit [correction, original] 2) 2 0 3 ≠
      point (admit [correction.reverse, original] 2) 2 0 3 := by decide

#print axioms single_system_coordinate_cannot_express_fresh_erase
#print axioms committed_state_alone_cannot_express_read_your_writes
#print axioms a_whole_transfer_preserves_balance
#print axioms visible_half_transfer_breaks_balance
#print axioms same_commit_operational_order_is_observable

end RTDB
