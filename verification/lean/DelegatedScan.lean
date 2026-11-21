import History

namespace RTDB.DelegatedScan

def FragmentReply (history : History) (coordinator worker system key : Nat)
    (valid : Int) (privateEvents : List Event) : Option (Option Nat) :=
  if worker = coordinator ∧ privateEvents = [] then
    some (point (privateEvents ++ admit history worker) system key valid)
  else none

theorem successful_fragment_agrees_with_capture (history : History)
    (coordinator worker system key : Nat) (valid : Int) (privateEvents : List Event)
    (value : Option Nat)
    (served : FragmentReply history coordinator worker system key valid privateEvents = some value) :
    point (admit history coordinator) system key valid = value := by
  have headMatches : worker = coordinator := by
    by_cases same : worker = coordinator
    · exact same
    · simp [FragmentReply, same] at served
  have noOverlay : privateEvents = [] := by
    by_cases empty : privateEvents = []
    · exact empty
    · simp [FragmentReply, empty] at served
  simpa [FragmentReply, headMatches, noOverlay] using served

theorem refusal_falls_back_to_captured_answer (history : History)
    (coordinator worker system key : Nat) (valid : Int) (privateEvents : List Event) :
    (FragmentReply history coordinator worker system key valid privateEvents).getD
      (point (admit history coordinator) system key valid) =
    point (admit history coordinator) system key valid := by
  cases reply : FragmentReply history coordinator worker system key valid privateEvents with
  | none => rfl
  | some value =>
    exact (successful_fragment_agrees_with_capture history coordinator worker system key valid privateEvents value reply).symm

theorem advancing_only_worker_can_change_a_historical_answer :
    point (admit [correction, original] 1) 1 0 3 ≠
    point (admit [correction, original] 2) 1 0 3 := by decide

#print axioms successful_fragment_agrees_with_capture
#print axioms refusal_falls_back_to_captured_answer
#print axioms advancing_only_worker_can_change_a_historical_answer

end RTDB.DelegatedScan
