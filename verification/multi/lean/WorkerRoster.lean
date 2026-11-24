namespace WorkerRoster

structure State where
  live : Nat
  stopped : Nat
  deriving DecidableEq

def rosterSize (state : State) : Nat :=
  state.live + state.stopped

def initial : State := ⟨0, 0⟩

inductive Step (capacity : Nat) : State → State → Prop where
  | enroll {s} (room : rosterSize s + 1 < capacity) :
      Step capacity s { s with live := s.live + 1 }
  | stop {live stopped} :
      Step capacity ⟨live + 1, stopped⟩ ⟨live, stopped + 1⟩
  | withdrawLive {live stopped} :
      Step capacity ⟨live + 1, stopped⟩ ⟨live, stopped⟩
  | withdrawStopped {live stopped} :
      Step capacity ⟨live, stopped + 1⟩ ⟨live, stopped⟩
  | disposeExpired {s} :
      Step capacity s s

inductive Trace (capacity : Nat) : State → State → Prop where
  | refl (s) : Trace capacity s s
  | snoc : Trace capacity s t → Step capacity t u → Trace capacity s u

theorem step_preserves_headroom (before : rosterSize s < capacity)
    (step : Step capacity s t) : rosterSize t < capacity := by
  cases step <;> simp_all [rosterSize] <;> omega

theorem trace_preserves_headroom (before : rosterSize s < capacity)
    (trace : Trace capacity s t) : rosterSize t < capacity := by
  induction trace with
  | refl => exact before
  | snoc _ step ih => exact step_preserves_headroom ih step

theorem empty_trace_has_headroom (positive : 0 < capacity)
    (trace : Trace capacity initial s) : rosterSize s < capacity :=
  trace_preserves_headroom (by simpa [initial, rosterSize] using positive) trace

theorem stopped_worker_withdrawal_reopens_admission
    (before : rosterSize ⟨live, stopped + 1⟩ < capacity) :
    Step capacity ⟨live, stopped⟩ ⟨live + 1, stopped⟩ := by
  apply Step.enroll
  simp [rosterSize] at before ⊢
  omega

theorem crash_and_cleanup_reach_empty :
    Trace 2 initial ⟨0, 0⟩ :=
  ((Trace.refl initial).snoc (Step.enroll (by decide))).snoc Step.stop
    |>.snoc Step.withdrawStopped |>.snoc Step.disposeExpired

-- The current reducer accepts an enrollment as long as the pre-state is below capacity.
inductive ReducerStep (capacity : Nat) : State → State → Prop where
  | enroll {s} (room : rosterSize s < capacity) :
      ReducerStep capacity s { s with live := s.live + 1 }
  | controlled : Step capacity s t → ReducerStep capacity s t

inductive ReducerTrace (capacity : Nat) : State → State → Prop where
  | refl (s) : ReducerTrace capacity s s
  | snoc : ReducerTrace capacity s t → ReducerStep capacity t u → ReducerTrace capacity s u

theorem crash_without_withdrawal_can_exhaust_roster :
    ∃ s, ReducerTrace 2 initial s ∧ s.live = 0 ∧ rosterSize s = 2 := by
  refine ⟨⟨0, 2⟩, ?_, rfl, rfl⟩
  have first : ReducerTrace 2 initial ⟨1, 0⟩ :=
    .snoc (.refl initial) (.enroll (by decide))
  have stopped : ReducerTrace 2 initial ⟨0, 1⟩ :=
    .snoc first (.controlled .stop)
  have second : ReducerTrace 2 initial ⟨1, 1⟩ :=
    .snoc stopped (.enroll (by decide))
  exact .snoc second (.controlled .stop)

end WorkerRoster
