import History

namespace RTDB.CeilingStudy

abbrev Version := Nat × Option Nat

def versionAt (key : Nat) (valid : Int) (event : Event) : Option Version :=
  if affected key valid event.fact then some (event.commit, valueOf event.fact) else none

def project (view : List Event) (key : Nat) (valid : Int) : List Version :=
  (redact key view).filterMap (versionAt key valid)

def latest (system : Nat) (versions : List Version) : Option Nat :=
  (versions.find? (fun version => decide (version.1 ≤ system))).bind Prod.snd

def below (system : Nat) : Option Nat → Bool
  | none => true
  | some upper => decide (system < upper)

-- A valid coordinate needs only the ceiling covering that coordinate. A delete
-- replaces the ceiling without contributing an emitted value.
def emit (system : Nat) (ceiling : Option Nat) : List Version → List Nat
  | [] => []
  | head :: tail =>
    let later := emit system (some head.1) tail
    if head.1 ≤ system ∧ below system ceiling = true then head.2.toList ++ later else later

def newestFirst (versions : List Version) : Prop :=
  versions.Pairwise (fun newer older => older.1 ≤ newer.1)

theorem projection_refines_point (view : List Event) (system key : Nat) (valid : Int) :
    latest system (project view key valid) = point view system key valid := by
  unfold latest project point
  simp only [List.find?_filterMap]
  generalize redact key view = events
  induction events with
  | nil => rfl
  | cons event tail ih =>
    cases applies : affected key valid event.fact <;> by_cases before : event.commit ≤ system <;>
      simp_all [versionAt]

theorem redact_sublist (view : List Event) (key : Nat) :
    List.Sublist (redact key view) view := by
  induction view with
  | nil => exact List.Sublist.slnil
  | cons event tail ih =>
    rcases event with ⟨commit, fact⟩
    cases fact with
    | put other value span => exact ih.cons_cons _
    | delete other span => exact ih.cons_cons _
    | erase other =>
      by_cases same : key = other
      · simp [redact, same]
      · simpa [redact, same] using ih.cons (⟨commit, .erase other⟩ : Event)

theorem projected_order (view : List Event) (key : Nat) (valid : Int)
    (ordered : view.Pairwise (fun newer older => older.commit ≤ newer.commit)) :
    newestFirst (project view key valid) := by
  unfold newestFirst project
  apply List.pairwise_filterMap.2
  apply (ordered.sublist (redact_sublist view key)).imp
  intro newer older order newerVersion hn olderVersion ho
  unfold versionAt at hn ho
  split at hn
  · split at ho
    · cases hn
      cases ho
      exact order
    · contradiction
  · contradiction

theorem emitted_below_closed_ceiling (system upper : Nat) (versions : List Version)
    (ordered : newestFirst versions) (covered : ∀ version ∈ versions, version.1 ≤ upper)
    (closed : upper ≤ system) :
    emit system (some upper) versions = [] := by
  induction versions generalizing upper with
  | nil => rfl
  | cons head tail ih =>
    have top := covered head (by simp)
    have tailOrder := List.pairwise_cons.1 ordered
    have erased := ih head.1 tailOrder.2 tailOrder.1 (Nat.le_trans top closed)
    simp [emit, below, show ¬ system < upper by omega, erased]

theorem emission_refines_latest (system : Nat) (ceiling : Option Nat) (versions : List Version)
    (ordered : newestFirst versions) (openAtPoint : below system ceiling = true) :
    emit system ceiling versions = (latest system versions).toList := by
  induction versions generalizing ceiling with
  | nil => rfl
  | cons head tail ih =>
    have tailOrder := List.pairwise_cons.1 ordered
    by_cases atOrBefore : head.1 ≤ system
    · have closed := emitted_below_closed_ceiling system head.1 tail tailOrder.2 tailOrder.1 atOrBefore
      simp [emit, latest, atOrBefore, openAtPoint, closed]
    · have stillOpen : below system (some head.1) = true := by
        unfold below
        apply decide_eq_true
        omega
      simp [emit, latest, atOrBefore, ih (some head.1) tailOrder.2 stillOpen]

theorem ceiling_refines_point (view : List Event) (system key : Nat) (valid : Int)
    (ordered : view.Pairwise (fun newer older => older.commit ≤ newer.commit)) :
    emit system none (project view key valid) = (point view system key valid).toList := by
  rw [emission_refines_latest system none _ (projected_order view key valid ordered) rfl,
      projection_refines_point]

theorem latest_below_requested_upper (system upper : Nat) (versions : List Version)
    (inside : system < upper) :
    latest system (versions.filter (fun version => decide (version.1 < upper))) =
      latest system versions := by
  induction versions with
  | nil => rfl
  | cons head tail ih =>
    by_cases inRange : head.1 < upper
    · by_cases now : head.1 ≤ system <;> simp [latest, inRange, now] at ih ⊢ <;> assumption
    · have now : ¬ head.1 ≤ system := by omega
      simpa [latest, inRange, now] using ih

theorem bounded_ceiling_refines_point (view : List Event) (system upper key : Nat) (valid : Int)
    (inside : system < upper)
    (ordered : view.Pairwise (fun newer older => older.commit ≤ newer.commit)) :
    emit system none ((project view key valid).filter (fun version => decide (version.1 < upper))) =
      (point view system key valid).toList := by
  have bounded : newestFirst ((project view key valid).filter (fun version => decide (version.1 < upper))) :=
    (projected_order view key valid ordered).sublist List.filter_sublist
  rw [emission_refines_latest system none _ bounded rfl,
    latest_below_requested_upper system upper _ inside, projection_refines_point]

-- With the order restriction weakened, a later input reopens the ceiling.
theorem unordered_events_can_emit_twice :
    emit 1 none [(1, some 10), (2, some 20), (0, some 30)] = [10, 30] := by decide

#print axioms ceiling_refines_point
#print axioms bounded_ceiling_refines_point
#print axioms unordered_events_can_emit_twice

end RTDB.CeilingStudy
