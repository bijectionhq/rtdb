set_option linter.unusedSectionVars false

namespace RTDB.SharedPlans

/-!
The node's shared plan cache (`rust/crates/query/src/shared_plans.rs`).

Planning reads its key (the SQL source and its `PrepareOptions`) and, adaptively, the answers its catalog gives.
A `Planner` is that computation for one key: each catalog question may depend on the answers before it.
A cached entry keeps the questions and answers its planning saw, and is served again only where the current
catalog gives every one of those answers.
-/

inductive Planner (Q A P : Type) where
  | done : P → Planner Q A P
  | ask : Q → (A → Planner Q A P) → Planner Q A P

section Replay

variable {Q A P : Type}

def run : Planner Q A P → (Q → A) → List (Q × A) × P
  | .done p, _ => ([], p)
  | .ask q next, cat => ((q, cat q) :: (run (next (cat q)) cat).1, (run (next (cat q)) cat).2)

def Replays (cat : Q → A) (trace : List (Q × A)) : Prop := ∀ x ∈ trace, cat x.1 = x.2

def replays [DecidableEq A] (cat : Q → A) : List (Q × A) → Bool
  | [] => true
  | x :: rest => decide (cat x.1 = x.2) && replays cat rest

theorem replays_sound [DecidableEq A] (cat : Q → A) (trace : List (Q × A))
    (h : replays cat trace = true) : Replays cat trace := by
  induction trace with
  | nil => intro x hx; cases hx
  | cons y rest ih =>
    simp only [replays, Bool.and_eq_true, decide_eq_true_eq] at h
    intro x hx
    cases hx with
    | head => exact h.1
    | tail _ hx => exact ih h.2 x hx

theorem replays_complete [DecidableEq A] (cat : Q → A) (trace : List (Q × A))
    (h : Replays cat trace) : replays cat trace = true := by
  induction trace with
  | nil => rfl
  | cons y rest ih =>
    simp only [replays, Bool.and_eq_true, decide_eq_true_eq]
    exact ⟨h y (List.Mem.head _), ih (fun x hx => h x (List.Mem.tail _ hx))⟩

theorem a_planning_replays_against_its_own_catalog (pl : Planner Q A P) (cat : Q → A) :
    Replays cat (run pl cat).1 := by
  induction pl with
  | done p => intro x hx; cases hx
  | ask q next ih =>
    intro x hx
    cases hx with
    | head => rfl
    | tail _ hx => exact ih (cat q) x hx

/-- A catalog that gives every recorded answer leads planning down the same questions to the same plan. -/
theorem a_replayed_planning_is_the_fresh_planning (pl : Planner Q A P) (cat cat' : Q → A)
    (h : Replays cat' (run pl cat).1) : run pl cat' = run pl cat := by
  induction pl with
  | done p => rfl
  | ask q next ih =>
    have hq : cat' q = cat q := h (q, cat q) (List.Mem.head _)
    have ht : Replays cat' (run (next (cat q)) cat).1 := fun x hx => h x (List.Mem.tail _ hx)
    show ((q, cat' q) :: (run (next (cat' q)) cat').1, (run (next (cat' q)) cat').2)
      = ((q, cat q) :: (run (next (cat q)) cat).1, (run (next (cat q)) cat).2)
    rw [hq, ih (cat q) ht]

end Replay

structure Entry (K Q A P : Type) where
  key : K
  trace : List (Q × A)
  plan : P
  bytes : Nat

/-- The entries, least recently used first, and the bytes the cache holds reserved for them. -/
structure Cache (K Q A P : Type) where
  entries : List (Entry K Q A P)
  reserved : Nat

inductive Kind where
  | query
  | explain
  | dml

section Cache

variable {K Q A P : Type} [DecidableEq K] [DecidableEq A]

def findKey (k : K) : List (Entry K Q A P) → Option (Entry K Q A P)
  | [] => none
  | e :: rest => if e.key = k then some e else findKey k rest

def lookup (entries : List (Entry K Q A P)) (k : K) (cat : Q → A) : Option P :=
  match findKey k entries with
  | some e => if replays cat e.trace then some e.plan else none
  | none => none

def used : List (Entry K Q A P) → Nat
  | [] => 0
  | e :: rest => e.bytes + used rest

def Distinct : List (Entry K Q A P) → Prop
  | [] => True
  | e :: rest => (∀ x ∈ rest, x.key ≠ e.key) ∧ Distinct rest

def removeFirst (k : K) : List (Entry K Q A P) → List (Entry K Q A P)
  | [] => []
  | e :: rest => if e.key = k then rest else e :: removeFirst k rest

def removedBytes (k : K) : List (Entry K Q A P) → Nat
  | [] => 0
  | e :: rest => if e.key = k then e.bytes else removedBytes k rest

variable (planner : K → Planner Q A P) (budget capacity : Nat)

/-- Every entry is what planning its key produced against some catalog. -/
def Planned (entries : List (Entry K Q A P)) : Prop :=
  ∀ e ∈ entries, ∃ cat : Q → A, run (planner e.key) cat = (e.trace, e.plan)

def Inv (c : Cache K Q A P) : Prop :=
  c.reserved = used c.entries ∧ c.reserved ≤ budget ∧ c.entries.length ≤ capacity
    ∧ Distinct c.entries ∧ Planned planner c.entries

def fresh (k : K) (cat : Q → A) : P := (run (planner k) cat).2

def evict (c : Cache K Q A P) : Cache K Q A P :=
  match c.entries with
  | [] => c
  | e :: rest => ⟨rest, c.reserved - e.bytes⟩

def removeKey (k : K) (c : Cache K Q A P) : Cache K Q A P :=
  ⟨removeFirst k c.entries, c.reserved - removedBytes k c.entries⟩

/-- Reserves and keeps a fresh planning of `k`, or keeps nothing where the budget, the capacity or an equal key forbids it. -/
def admit (k : K) (cat : Q → A) (bytes : Nat) (c : Cache K Q A P) : Cache K Q A P :=
  if c.reserved + bytes ≤ budget ∧ c.entries.length < capacity ∧ (∀ x ∈ c.entries, x.key ≠ k) then
    ⟨c.entries ++ [⟨k, (run (planner k) cat).1, (run (planner k) cat).2, bytes⟩], c.reserved + bytes⟩
  else c

/-- The plan a request is served, and the cache after it. -/
def serve (c : Cache K Q A P) (kind : Kind) (k : K) (cat : Q → A) (bytes : Nat) : P × Cache K Q A P :=
  match kind with
  | .query =>
    match lookup c.entries k cat with
    | some p => (p, c)
    | none => (fresh planner k cat, admit planner budget capacity k cat bytes (removeKey k c))
  | _ => (fresh planner k cat, c)

inductive Step (K Q A : Type) where
  | serve (kind : Kind) (k : K) (cat : Q → A) (bytes : Nat)
  | evict
  | remove (k : K)

def apply (c : Cache K Q A P) : Step K Q A → Cache K Q A P
  | .serve kind k cat bytes => (serve planner budget capacity c kind k cat bytes).2
  | .evict => evict c
  | .remove k => removeKey k c

def evictAll : Cache K Q A P → Nat → Cache K Q A P
  | c, 0 => c
  | c, n + 1 => evictAll (evict c) n

theorem findKey_found (k : K) (l : List (Entry K Q A P)) (e : Entry K Q A P)
    (h : findKey k l = some e) : e ∈ l ∧ e.key = k := by
  induction l with
  | nil => cases h
  | cons x rest ih =>
    by_cases hx : x.key = k
    · have : some x = some e := by simpa [findKey, hx] using h
      cases this
      exact ⟨List.Mem.head _, hx⟩
    · have : findKey k rest = some e := by simpa [findKey, hx] using h
      exact ⟨List.Mem.tail _ (ih this).1, (ih this).2⟩

theorem findKey_unique (l : List (Entry K Q A P)) (e : Entry K Q A P)
    (hd : Distinct l) (he : e ∈ l) : findKey e.key l = some e := by
  induction l with
  | nil => cases he
  | cons x rest ih =>
    cases he with
    | head => simp [findKey]
    | tail _ he =>
      have hne : ¬ x.key = e.key := fun h => hd.1 e he h.symm
      show (if x.key = e.key then some x else findKey e.key rest) = some e
      rw [if_neg hne]
      exact ih hd.2 he

theorem used_append (l : List (Entry K Q A P)) (e : Entry K Q A P) : used (l ++ [e]) = used l + e.bytes := by
  induction l with
  | nil => simp [used]
  | cons x rest ih => simp only [List.cons_append, used, ih]; omega

theorem distinct_append (l : List (Entry K Q A P)) (e : Entry K Q A P)
    (hd : Distinct l) (hk : ∀ x ∈ l, x.key ≠ e.key) : Distinct (l ++ [e]) := by
  induction l with
  | nil => exact ⟨fun x hx => (by cases hx), trivial⟩
  | cons x rest ih =>
    refine ⟨fun y hy => ?_, ih hd.2 (fun z hz => hk z (List.Mem.tail _ hz))⟩
    rcases List.mem_append.mp hy with hy | hy
    · exact hd.1 y hy
    · have : y = e := List.mem_singleton.mp hy
      subst this
      exact fun h => hk x (List.Mem.head _) h.symm

theorem removeFirst_mem (k : K) (l : List (Entry K Q A P)) (x : Entry K Q A P)
    (h : x ∈ removeFirst k l) : x ∈ l := by
  induction l with
  | nil => cases h
  | cons e rest ih =>
    by_cases he : e.key = k
    · have : x ∈ rest := by simpa [removeFirst, he] using h
      exact List.Mem.tail _ this
    · have : x ∈ e :: removeFirst k rest := by simpa [removeFirst, he] using h
      cases this with
      | head => exact List.Mem.head _
      | tail _ hx => exact List.Mem.tail _ (ih hx)

theorem removeFirst_removes (k : K) (l : List (Entry K Q A P)) (hd : Distinct l) :
    ∀ x ∈ removeFirst k l, x.key ≠ k := by
  induction l with
  | nil => intro x hx; cases hx
  | cons e rest ih =>
    intro x hx
    by_cases he : e.key = k
    · have hx : x ∈ rest := by simpa [removeFirst, he] using hx
      exact he ▸ hd.1 x hx
    · have hx : x ∈ e :: removeFirst k rest := by simpa [removeFirst, he] using hx
      cases hx with
      | head => exact he
      | tail _ hx => exact ih hd.2 x hx

theorem removeFirst_distinct (k : K) (l : List (Entry K Q A P)) (hd : Distinct l) :
    Distinct (removeFirst k l) := by
  induction l with
  | nil => trivial
  | cons e rest ih =>
    by_cases he : e.key = k
    · simpa [removeFirst, he] using hd.2
    · show Distinct (if e.key = k then rest else e :: removeFirst k rest)
      rw [if_neg he]
      exact ⟨fun x hx => hd.1 x (removeFirst_mem k rest x hx), ih hd.2⟩

theorem removeFirst_bytes (k : K) (l : List (Entry K Q A P)) :
    used (removeFirst k l) + removedBytes k l = used l := by
  induction l with
  | nil => rfl
  | cons e rest ih =>
    by_cases he : e.key = k
    · simp only [removeFirst, removedBytes, he, if_true, used]; omega
    · simp only [removeFirst, removedBytes, he, if_false, used]; omega

theorem removeFirst_length (k : K) (l : List (Entry K Q A P)) :
    (removeFirst k l).length ≤ l.length := by
  induction l with
  | nil => exact Nat.le_refl _
  | cons e rest ih =>
    by_cases he : e.key = k
    · simp only [removeFirst, he, if_true, List.length_cons]; omega
    · simp only [removeFirst, he, if_false, List.length_cons]; omega

/-- A plan served from the cache was planned from an equal key and from catalog answers the current catalog gives. -/
theorem a_cached_plan_had_the_same_inputs (entries : List (Entry K Q A P)) (k : K) (cat : Q → A) (p : P)
    (h : lookup entries k cat = some p) :
    ∃ e ∈ entries, e.key = k ∧ Replays cat e.trace ∧ e.plan = p := by
  unfold lookup at h
  split at h
  · rename_i e found
    by_cases hr : replays cat e.trace = true
    · rw [if_pos hr] at h
      cases h
      exact ⟨e, (findKey_found k entries e found).1, (findKey_found k entries e found).2, replays_sound cat e.trace hr, rfl⟩
    · rw [if_neg hr] at h
      cases h
  · cases h

/-- Law 1: the cache serves exactly the plan re-planning would produce. -/
theorem a_cached_plan_is_the_fresh_plan (c : Cache K Q A P) (k : K) (cat : Q → A) (p : P)
    (hi : Inv planner budget capacity c) (h : lookup c.entries k cat = some p) : p = fresh planner k cat := by
  obtain ⟨e, he, hk, hr, hp⟩ := a_cached_plan_had_the_same_inputs c.entries k cat p h
  obtain ⟨cat0, built⟩ := hi.2.2.2.2 e he
  have replayed : Replays cat (run (planner e.key) cat0).1 := by rw [built]; exact hr
  have same := a_replayed_planning_is_the_fresh_planning (planner e.key) cat0 cat replayed
  rw [built] at same
  unfold fresh
  rw [← hk, same, ← hp]

/-- Law 2: once the catalog changes any answer an entry recorded, that entry is not served. -/
theorem a_stale_plan_is_never_served (c : Cache K Q A P) (e : Entry K Q A P) (cat : Q → A)
    (hi : Inv planner budget capacity c) (he : e ∈ c.entries) (changed : ∃ x ∈ e.trace, cat x.1 ≠ x.2) :
    lookup c.entries e.key cat = none := by
  unfold lookup
  rw [findKey_unique c.entries e hi.2.2.2.1 he]
  have hr : ¬ replays cat e.trace = true := by
    intro hr
    obtain ⟨x, hx, hne⟩ := changed
    exact hne (replays_sound cat e.trace hr x hx)
  simp only [hr]
  rfl

theorem every_request_is_served_the_fresh_plan (c : Cache K Q A P) (kind : Kind) (k : K) (cat : Q → A) (bytes : Nat)
    (hi : Inv planner budget capacity c) : (serve planner budget capacity c kind k cat bytes).1 = fresh planner k cat := by
  unfold serve
  split
  · split
    · rename_i p found
      exact a_cached_plan_is_the_fresh_plan planner budget capacity c k cat p hi found
    · rfl
  · rfl

/-- Law 3: EXPLAIN and DML are planned afresh and leave the cache as it was. -/
theorem explain_and_dml_bypass_the_cache (c : Cache K Q A P) (kind : Kind) (k : K) (cat : Q → A) (bytes : Nat)
    (hk : kind ≠ .query) : serve planner budget capacity c kind k cat bytes = (fresh planner k cat, c) := by
  cases kind with
  | query => exact absurd rfl hk
  | explain => rfl
  | dml => rfl

theorem admit_keeps_the_invariant (k : K) (cat : Q → A) (bytes : Nat) (c : Cache K Q A P)
    (hi : Inv planner budget capacity c) : Inv planner budget capacity (admit planner budget capacity k cat bytes c) := by
  unfold admit
  split
  · rename_i guard
    obtain ⟨fits, room, absent⟩ := guard
    obtain ⟨ledger, _, _, hd, hp⟩ := hi
    refine ⟨?_, fits, ?_, distinct_append c.entries _ hd absent, ?_⟩
    · show c.reserved + bytes = used (c.entries ++ _)
      rw [used_append, ledger]
    · show (c.entries ++ _).length ≤ capacity
      simp only [List.length_append, List.length_cons, List.length_nil]
      omega
    · intro x hx
      rcases List.mem_append.mp hx with hx | hx
      · exact hp x hx
      · have : x = _ := List.mem_singleton.mp hx
        subst this
        exact ⟨cat, rfl⟩
  · exact hi

theorem removeKey_keeps_the_invariant (k : K) (c : Cache K Q A P)
    (hi : Inv planner budget capacity c) : Inv planner budget capacity (removeKey k c) := by
  obtain ⟨ledger, fits, room, hd, hp⟩ := hi
  have bytes := removeFirst_bytes k c.entries
  refine ⟨?_, ?_, ?_, removeFirst_distinct k c.entries hd, fun x hx => hp x (removeFirst_mem k c.entries x hx)⟩
  · show c.reserved - removedBytes k c.entries = used (removeFirst k c.entries)
    omega
  · show c.reserved - removedBytes k c.entries ≤ budget
    omega
  · exact Nat.le_trans (removeFirst_length k c.entries) room

theorem evict_keeps_the_invariant (c : Cache K Q A P)
    (hi : Inv planner budget capacity c) : Inv planner budget capacity (evict c) := by
  obtain ⟨ledger, fits, room, hd, hp⟩ := hi
  unfold evict
  split
  · exact ⟨ledger, fits, room, hd, hp⟩
  · rename_i e rest held
    rw [held] at ledger room hd hp
    refine ⟨?_, ?_, ?_, hd.2, fun x hx => hp x (List.Mem.tail _ hx)⟩
    · show c.reserved - e.bytes = used rest
      simp only [used] at ledger
      omega
    · show c.reserved - e.bytes ≤ budget
      omega
    · show rest.length ≤ capacity
      simp only [List.length_cons] at room
      omega

/-- Law 4: every step keeps the reservations equal to the retained entries, within the budget and the capacity. -/
theorem every_step_keeps_the_budget (c : Cache K Q A P) (s : Step K Q A)
    (hi : Inv planner budget capacity c) : Inv planner budget capacity (apply planner budget capacity c s) := by
  cases s with
  | serve kind k cat bytes =>
    show Inv planner budget capacity (serve planner budget capacity c kind k cat bytes).2
    unfold serve
    split
    · split
      · exact hi
      · exact admit_keeps_the_invariant planner budget capacity k cat bytes _
          (removeKey_keeps_the_invariant planner budget capacity k c hi)
    · exact hi
  | evict => exact evict_keeps_the_invariant planner budget capacity c hi
  | remove k => exact removeKey_keeps_the_invariant planner budget capacity k c hi

theorem every_reachable_cache_keeps_the_budget (steps : List (Step K Q A)) :
    Inv planner budget capacity (steps.foldl (apply planner budget capacity) (⟨[], 0⟩ : Cache K Q A P)) := by
  suffices h : ∀ (c : Cache K Q A P), Inv planner budget capacity c →
      Inv planner budget capacity (steps.foldl (apply planner budget capacity) c) from
    h _ ⟨rfl, Nat.zero_le _, Nat.zero_le _, trivial, fun x hx => by cases hx⟩
  induction steps with
  | nil => intro c hc; exact hc
  | cons s rest ih => intro c hc; exact ih _ (every_step_keeps_the_budget planner budget capacity c s hc)

/-- Law 4: evicting every entry returns every reservation. -/
theorem evicting_every_entry_releases_every_reservation (c : Cache K Q A P)
    (hi : Inv planner budget capacity c) : (evictAll c c.entries.length).reserved = 0 := by
  suffices h : ∀ (n : Nat) (c : Cache K Q A P), c.entries.length = n → c.reserved = used c.entries →
      (evictAll c n).reserved = 0 from h _ c rfl hi.1
  intro n
  induction n with
  | zero =>
    intro c hn ledger
    have : c.entries = [] := List.eq_nil_of_length_eq_zero hn
    show c.reserved = 0
    rw [ledger, this]
    rfl
  | succ n ih =>
    intro c hn ledger
    show (evictAll (evict c) n).reserved = 0
    match he : c.entries, hn with
    | e :: rest, hn =>
      have shape : evict c = ⟨rest, c.reserved - e.bytes⟩ := by unfold evict; rw [he]
      rw [shape]
      apply ih
      · simp only [List.length_cons] at hn; exact Nat.succ.inj hn
      · show c.reserved - e.bytes = used rest
        rw [he] at ledger
        simp only [used] at ledger
        omega

/-- Reachability: a query served twice against the same catalog is the second time served from the cache. -/
theorem a_repeated_query_is_served_from_the_cache (k : K) (cat : Q → A) (bytes : Nat)
    (fits : bytes ≤ budget) (room : 0 < capacity) :
    lookup (serve planner budget capacity (⟨[], 0⟩ : Cache K Q A P) .query k cat bytes).2.entries k cat
      = some (fresh planner k cat) := by
  have admitted : admit planner budget capacity k cat bytes (removeKey k (⟨[], 0⟩ : Cache K Q A P))
      = ⟨[⟨k, (run (planner k) cat).1, (run (planner k) cat).2, bytes⟩], 0 + bytes⟩ := by
    unfold admit removeKey
    rw [if_pos ⟨by simpa [removedBytes] using fits, by simpa [removeFirst] using room, fun x hx => by cases hx⟩]
    rfl
  have missed : lookup ([] : List (Entry K Q A P)) k cat = none := rfl
  unfold serve
  simp only [missed, admitted]
  unfold lookup findKey
  simp only [if_true, replays_complete cat _ (a_planning_replays_against_its_own_catalog (planner k) cat)]
  rfl

end Cache

#print axioms a_replayed_planning_is_the_fresh_planning
#print axioms a_cached_plan_had_the_same_inputs
#print axioms a_cached_plan_is_the_fresh_plan
#print axioms a_stale_plan_is_never_served
#print axioms every_request_is_served_the_fresh_plan
#print axioms explain_and_dml_bypass_the_cache
#print axioms every_step_keeps_the_budget
#print axioms every_reachable_cache_keeps_the_budget
#print axioms evicting_every_entry_releases_every_reservation
#print axioms a_repeated_query_is_served_from_the_cache

end RTDB.SharedPlans
