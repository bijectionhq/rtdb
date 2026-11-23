import Std

namespace RTDB.ExplicitSession

inductive Access where
  | unspecified
  | readOnly
  | readWrite

inductive Phase (Basis Op : Type) where
  | pending (readOnly : Bool)
  | reading (basis : Basis)
  | writing (operations : List Op)

inductive Session (Basis Op : Type) where
  | idle
  | active (phase : Phase Basis Op) (failed : Bool)
  | closed

inductive Command (Basis Op : Type) where
  | start (access : Access)
  | query (freshBasis : Basis)
  | dml (operation : Op)
  | fail
  | commit
  | rollback
  | close

inductive Outcome (Basis Op : Type) where
  | done
  | refused
  | read (basis : Basis)
  | submit (operations : List Op)

def initialPhase : Access → Phase Basis Op
  | .unspecified => .pending false
  | .readOnly => .pending true
  | .readWrite => .writing []

def step : Session Basis Op → Command Basis Op → Session Basis Op × Outcome Basis Op
  | _, .close => (.closed, .done)
  | .closed, _ => (.closed, .refused)
  | _, .rollback => (.idle, .done)
  | .idle, .start access => (.active (initialPhase access) false, .done)
  | .idle, .commit => (.idle, .done)
  | .active phase _, .fail => (.active phase true, .done)
  | .active phase true, _ => (.active phase true, .refused)
  | .active (.pending _) false, .query basis => (.active (.reading basis) false, .read basis)
  | .active (.reading pinned) false, .query _ => (.active (.reading pinned) false, .read pinned)
  | .active (.pending false) false, .dml op => (.active (.writing [op]) false, .done)
  | .active (.writing buffered) false, .dml op => (.active (.writing (buffered ++ [op])) false, .done)
  | .active (.writing buffered) false, .commit => (.idle, .submit buffered)
  | .active _ false, .commit => (.idle, .done)
  | session, _ => (session, .refused)

def run (session : Session Basis Op) (commands : List (Command Basis Op)) : Session Basis Op :=
  commands.foldl (fun state cmd => (step state cmd).1) session

inductive KeepsOpen : Command Basis Op → Prop
  | start (access : Access) : KeepsOpen (.start access)
  | query (basis : Basis) : KeepsOpen (.query basis)
  | dml (op : Op) : KeepsOpen (.dml op)
  | fail : KeepsOpen .fail

theorem first_query_resolves_pending_and_returns_the_captured_basis (readOnly : Bool) (basis : Basis) :
    step (.active (.pending readOnly) false : Session Basis Op) (.query basis) =
      (.active (.reading basis) false, .read basis) := by
  cases readOnly <;> rfl

theorem first_dml_resolves_only_an_unspecified_pending_transaction (op : Op) :
    step (.active (.pending false) false : Session Basis Op) (.dml op) =
      (.active (.writing [op]) false, .done) ∧
    step (.active (.pending true) false : Session Basis Op) (.dml op) =
      (.active (.pending true) false, .refused) := by
  exact ⟨rfl, rfl⟩

theorem a_later_query_observes_the_pinned_basis (pinned fresh : Basis) :
    step (.active (.reading pinned) false : Session Basis Op) (.query fresh) =
      (.active (.reading pinned) false, .read pinned) := rfl

theorem pin_persists_through_every_nonterminal_sequence (pinned : Basis) (failed : Bool)
    (commands : List (Command Basis Op)) (inScope : ∀ cmd ∈ commands, KeepsOpen cmd) :
    ∃ finalFailure, run (.active (.reading pinned) failed) commands =
      .active (.reading pinned) finalFailure := by
  induction commands generalizing failed with
  | nil => exact ⟨failed, rfl⟩
  | cons cmd rest ih =>
    have continuation := fun later present => inScope later (List.mem_cons_of_mem cmd present)
    change ∃ finalFailure, run (step (.active (.reading pinned) failed) cmd).1 rest =
      .active (.reading pinned) finalFailure
    have keeps := inScope cmd List.mem_cons_self
    cases keeps <;> cases failed <;> exact ih _ continuation

theorem admitted_dml_preserves_complete_operation_order (buffered operations : List Op) :
    run (.active (.writing buffered) false : Session Basis Op) (operations.map Command.dml) =
      .active (.writing (buffered ++ operations)) false := by
  induction operations generalizing buffered with
  | nil => simp [run]
  | cons first rest ih =>
    change run (.active (.writing (buffered ++ [first])) false) (rest.map Command.dml) = _
    rw [ih]
    simp [List.append_assoc]

theorem writing_commit_submits_the_buffer_once (buffered : List Op) :
    step (.active (.writing buffered) false : Session Basis Op) .commit =
      (.idle, .submit buffered) ∧
    step (.idle : Session Basis Op) .commit = (.idle, .done) := by
  exact ⟨rfl, rfl⟩

theorem rollback_and_close_discard_the_buffer_without_submission (buffered : List Op) (failed : Bool) :
    step (.active (.writing buffered) failed : Session Basis Op) .rollback = (.idle, .done) ∧
    step (.active (.writing buffered) failed : Session Basis Op) .close = (.closed, .done) := by
  exact ⟨rfl, rfl⟩

theorem failed_transactions_reject_queries_dml_and_commit (phase : Phase Basis Op) (fresh : Basis) (op : Op) :
    step (.active phase true) (.query fresh) = (.active phase true, .refused) ∧
    step (.active phase true) (.dml op) = (.active phase true, .refused) ∧
    step (.active phase true) .commit = (.active phase true, .refused) := by
  exact ⟨rfl, rfl, rfl⟩

theorem a_closed_session_remains_closed (commands : List (Command Basis Op)) :
    run .closed commands = .closed := by
  induction commands with
  | nil => rfl
  | cons command rest ih =>
    cases command <;> exact ih

#print axioms first_query_resolves_pending_and_returns_the_captured_basis
#print axioms first_dml_resolves_only_an_unspecified_pending_transaction
#print axioms a_later_query_observes_the_pinned_basis
#print axioms pin_persists_through_every_nonterminal_sequence
#print axioms admitted_dml_preserves_complete_operation_order
#print axioms writing_commit_submits_the_buffer_once
#print axioms rollback_and_close_discard_the_buffer_without_submission
#print axioms failed_transactions_reject_queries_dml_and_commit
#print axioms a_closed_session_remains_closed

end RTDB.ExplicitSession
