import Std

namespace RTDB.ProgressWait

inductive Engine where
  | native
  | kotlin
  deriving DecidableEq

inductive Request where
  | transaction (id : Int)
  | source (position : Int)
  | systemTime (time : Int)

inductive Terminal where
  | fault (code : Nat)
  | stopped
  deriving DecidableEq

structure Snapshot where
  transaction : Int
  source : Int
  systemTime : Option Int
  latestResult : Option (Int × Nat)
  terminal : Option Terminal

inductive Decision where
  | waiting
  | reached (result : Option Nat)
  | failed (terminal : Terminal)
  | cancelled
  deriving DecidableEq

def ready (snapshot : Snapshot) : Request → Bool
  | .transaction id => decide (id ≤ snapshot.transaction)
  | .source position => decide (position ≤ snapshot.source)
  | .systemTime time => snapshot.systemTime.any (fun applied => decide (time ≤ applied))

def reached (snapshot : Snapshot) : Request → Decision
  | .transaction id =>
      .reached ((snapshot.latestResult.filter (fun receipt => receipt.1 == id)).map Prod.snd)
  | .source _ | .systemTime _ => .reached none

def observe (engine : Engine) (snapshot : Snapshot) (request : Request) : Decision :=
  if engine = .native ∧ ready snapshot request = true then reached snapshot request
  else match snapshot.terminal with
    | some terminal => .failed terminal
    | none => if ready snapshot request then reached snapshot request else .waiting

structure Subscriptions where
  snapshot : Snapshot
  first : Decision
  second : Decision

def cancelFirst (subscriptions : Subscriptions) : Subscriptions :=
  if subscriptions.first = .waiting then { subscriptions with first := .cancelled }
  else subscriptions

theorem native_wait_can_observe_an_applied_prefix_after_failure (snapshot : Snapshot)
    (request : Request) (available : ready snapshot request = true) :
    observe .native snapshot request = reached snapshot request := by
  simp [observe, available]

theorem kotlin_wait_reports_the_observed_terminal_state (snapshot : Snapshot) (request : Request)
    (terminal : Terminal) (failed : snapshot.terminal = some terminal) :
    observe .kotlin snapshot request = .failed terminal := by
  simp [observe, failed]

theorem an_unmet_wait_observes_the_terminal_state (engine : Engine) (snapshot : Snapshot)
    (request : Request) (terminal : Terminal) (failed : snapshot.terminal = some terminal)
    (unmet : ready snapshot request = false) : observe engine snapshot request = .failed terminal := by
  simp [observe, failed, unmet]

theorem an_older_wait_never_reuses_a_newer_transaction_result (snapshot : Snapshot)
    (requested reported : Int) (value : Nat) (later : requested < reported)
    (latest : snapshot.latestResult = some (reported, value)) :
    reached snapshot (.transaction requested) = .reached none := by
  have different : reported ≠ requested := Int.ne_of_gt later
  simp [reached, latest, different]

theorem cancelling_one_wait_preserves_the_other_and_the_snapshot (subscriptions : Subscriptions)
    (pending : subscriptions.first = .waiting) :
    (cancelFirst subscriptions).first = .cancelled ∧
    (cancelFirst subscriptions).second = subscriptions.second ∧
    (cancelFirst subscriptions).snapshot = subscriptions.snapshot := by
  simp [cancelFirst, pending]

#print axioms native_wait_can_observe_an_applied_prefix_after_failure
#print axioms kotlin_wait_reports_the_observed_terminal_state
#print axioms an_unmet_wait_observes_the_terminal_state
#print axioms an_older_wait_never_reuses_a_newer_transaction_result
#print axioms cancelling_one_wait_preserves_the_other_and_the_snapshot

end RTDB.ProgressWait
