import Std

namespace RTDB.Multi.ServingFailure

def failedRounds (previous : Nat) (sameEndpoints controlReady pgwireReady : Bool) : Nat :=
  if controlReady && pgwireReady then 0
  else if sameEndpoints then previous + 1 else 1

theorem healthy_resets (previous : Nat) (sameEndpoints : Bool) :
    failedRounds previous sameEndpoints true true = 0 := by
  simp [failedRounds]

theorem changed_endpoints_do_not_inherit_failures (previous : Nat) :
    failedRounds previous false true false = 1 := by
  simp [failedRounds]

def pgwireOnlyFailures (previous : Nat) : Nat → Nat
  | 0 => previous
  | rounds + 1 => failedRounds (pgwireOnlyFailures previous rounds) true true false

theorem repeated_pgwire_failure_reaches_threshold (previous rounds : Nat) :
    rounds ≤ pgwireOnlyFailures previous rounds := by
  induction rounds with
  | zero => exact Nat.zero_le previous
  | succ rounds ih => simpa [pgwireOnlyFailures, failedRounds] using Nat.succ_le_succ ih

#print axioms healthy_resets
#print axioms changed_endpoints_do_not_inherit_failures
#print axioms repeated_pgwire_failure_reaches_threshold

end RTDB.Multi.ServingFailure
