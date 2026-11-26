set_option linter.unusedVariables false

namespace RTDB.JournalCustody

/-!
Logkeeper journal custody in a Multi cell (`rust/crates/directory/src/journal_fence.rs`,
`rust/crates/multi/src/admin/{decommission,release}.rs`).

Journals and deployments are numbered.
Each journal carries a stage and an eligibility epoch, which every stage change moves on.
`seen j e` records that journal `j` accepted references at epoch `e`, so a writer may have observed it there.
A deployment retains a journal (`ref`), may hold it in its serving ensemble (`ens`), and may have dependents on that copy (`deps`):
a fork, a pending recovery, a replay hold, a retained basis, an erasure obligation or an in-flight handoff.
`stored d j` is how many of deployment `d`'s acknowledged entries journal `j` holds, and `checkpoint d` how many a checkpoint covers.

Every step may interleave with any other.
A placement, repair, claim or fork that observed a journal at some earlier epoch lands only while that epoch is current (the fence).
-/

inductive Stage where
  | eligible
  | draining
  | certified
  | decommissioned
  deriving DecidableEq

def Stage.accepting : Stage → Bool
  | .eligible => true
  | .draining => true
  | .certified => false
  | .decommissioned => false

structure State where
  stage : Nat → Stage
  epoch : Nat → Nat
  seen : Nat → Nat → Bool
  ref : Nat → Nat → Bool
  ens : Nat → Nat → Bool
  deps : Nat → Nat → Bool
  stored : Nat → Nat → Nat
  acked : Nat → Nat
  checkpoint : Nat → Nat

def upd {α : Type} (f : Nat → α) (x : Nat) (v : α) : Nat → α := fun y => if y = x then v else f y

def upd2 {α : Type} (f : Nat → Nat → α) (x y : Nat) (v : α) : Nat → Nat → α :=
  fun a b => if a = x ∧ b = y then v else f a b

def initial : State where
  stage := fun _ => .eligible
  epoch := fun _ => 0
  seen := fun _ e => e == 0
  ref := fun _ _ => false
  ens := fun _ _ => false
  deps := fun _ _ => false
  stored := fun _ _ => 0
  acked := fun _ => 0
  checkpoint := fun _ => 0

inductive Step : State → State → Prop
  /-- A write that newly names `j`, planned from an observation of it at epoch `obs`. -/
  | place (s : State) (d j obs : Nat) (hseen : s.seen j obs = true) (hfence : s.epoch j = obs) :
      Step s { s with ref := upd2 s.ref d j true }
  /-- A certified handoff copies the retained history onto a retained journal and adds it to the ensemble. -/
  | join (s : State) (d j : Nat) (href : s.ref d j = true) :
      Step s { s with ens := upd2 s.ens d j true, stored := upd2 s.stored d j (s.acked d) }
  /-- A journal leaves the ensemble while another member holds every acknowledged entry. -/
  | leave (s : State) (d j k : Nat) (hk : k ≠ j) (hens : s.ens d k = true) (hfull : s.acked d ≤ s.stored d k) :
      Step s { s with ens := upd2 s.ens d j false }
  /-- An append is acknowledged once the ensemble holds it. -/
  | append (s : State) (d k : Nat) (hens : s.ens d k = true) :
      Step s { s with acked := upd s.acked d (s.acked d + 1),
                      stored := fun a b => if a = d ∧ s.ens d b = true then s.acked d + 1 else s.stored a b }
  | checkpoint (s : State) (d : Nat) :
      Step s { s with checkpoint := upd s.checkpoint d (s.acked d) }
  | depend (s : State) (d j : Nat) (href : s.ref d j = true) :
      Step s { s with deps := upd2 s.deps d j true }
  | undepend (s : State) (d j : Nat) :
      Step s { s with deps := upd2 s.deps d j false }
  /-- The release retires the copy on the journal and drops its identity from custody. -/
  | release (s : State) (d j : Nat) (href : s.ref d j = true) (hens : s.ens d j = false) (hdeps : s.deps d j = false) :
      Step s { s with ref := upd2 s.ref d j false, stored := upd2 s.stored d j 0 }
  | dispose (s : State) (d : Nat) (hdeps : ∀ j, s.deps d j = false) :
      Step s { s with ref := fun a b => if a = d then false else s.ref a b,
                      ens := fun a b => if a = d then false else s.ens a b,
                      acked := upd s.acked d 0, checkpoint := upd s.checkpoint d 0 }
  | drain (s : State) (j : Nat) (h : s.stage j = .eligible) :
      Step s { s with stage := upd s.stage j .draining, epoch := upd s.epoch j (s.epoch j + 1),
                      seen := upd2 s.seen j (s.epoch j + 1) true }
  | certify (s : State) (j : Nat) (h : s.stage j = .draining) (hscan : ∀ d, s.ref d j = false) :
      Step s { s with stage := upd s.stage j .certified, epoch := upd s.epoch j (s.epoch j + 1) }
  | withdraw (s : State) (j : Nat) (h : s.stage j = .certified) :
      Step s { s with stage := upd s.stage j .draining, epoch := upd s.epoch j (s.epoch j + 1),
                      seen := upd2 s.seen j (s.epoch j + 1) true }
  | decommission (s : State) (j : Nat) (h : s.stage j = .certified) (hscan : ∀ d, s.ref d j = false) :
      Step s { s with stage := upd s.stage j .decommissioned, epoch := upd s.epoch j (s.epoch j + 1) }

inductive Reachable : State → Prop
  | init : Reachable initial
  | step {s t : State} : Reachable s → Step s t → Reachable t

/-- Every acknowledged entry is in the checkpoint or on a member of the serving ensemble. -/
def Durable (s : State) : Prop :=
  ∀ d n, n < s.acked d → n < s.checkpoint d ∨ ∃ k, s.ens d k = true ∧ n < s.stored d k

structure Inv (s : State) : Prop where
  seenPast : ∀ j e, s.seen j e = true → e ≤ s.epoch j
  seenAccepting : ∀ j, s.seen j (s.epoch j) = true → (s.stage j).accepting = true
  retiringUnreferenced : ∀ d j, (s.stage j).accepting = false → s.ref d j = false
  dependentsRetained : ∀ d j, s.deps d j = true → s.ref d j = true
  ensembleRetained : ∀ d j, s.ens d j = true → s.ref d j = true
  durable : Durable s

theorem initial_inv : Inv initial where
  seenPast := by
    intro j e h
    simp [initial] at h
    simp [initial, h]
  seenAccepting := by intro j _; rfl
  retiringUnreferenced := by intro d j _; rfl
  dependentsRetained := by intro d j h; simp [initial] at h
  ensembleRetained := by intro d j h; simp [initial] at h
  durable := by intro d n h; simp [initial] at h

theorem step_inv {s t : State} (step : Step s t) (hi : Inv s) : Inv t := by
  obtain ⟨i1, i2, i3, i4, i5, i6⟩ := hi
  cases step with
  | place d j obs hseen hfence =>
    refine ⟨i1, i2, ?_, ?_, ?_, i6⟩
    · intro a b hb
      simp only [upd2]
      by_cases h : a = d ∧ b = j
      · obtain ⟨_, rfl⟩ := h
        have := i2 b (hfence ▸ hseen)
        simp_all
      · simp only [h, ite_false]; exact i3 a b hb
    · intro a b hb
      simp only [upd2]
      by_cases h : a = d ∧ b = j
      · simp [h]
      · simp only [h, ite_false]; exact i4 a b hb
    · intro a b hb
      simp only [upd2]
      by_cases h : a = d ∧ b = j
      · simp [h]
      · simp only [h, ite_false]; exact i5 a b hb
  | join d j href =>
    refine ⟨i1, i2, i3, i4, ?_, ?_⟩
    · intro a b hb
      simp only [upd2] at hb
      by_cases h : a = d ∧ b = j
      · obtain ⟨rfl, rfl⟩ := h; exact href
      · simp only [h, ite_false] at hb; exact i5 a b hb
    · intro a n hn
      rcases i6 a n hn with hc | ⟨k, hk, hs⟩
      · exact Or.inl hc
      · by_cases h : a = d ∧ k = j
        · obtain ⟨rfl, rfl⟩ := h
          exact Or.inr ⟨k, by simp [upd2], by simp [upd2]; exact hn⟩
        · exact Or.inr ⟨k, by simp only [upd2, h, ite_false]; exact hk, by simp only [upd2, h, ite_false]; exact hs⟩
  | leave d j k hk hens hfull =>
    refine ⟨i1, i2, i3, i4, ?_, ?_⟩
    · intro a b hb
      simp only [upd2] at hb
      by_cases h : a = d ∧ b = j
      · simp [h] at hb
      · simp only [h, ite_false] at hb; exact i5 a b hb
    · intro a n hn
      by_cases ha : a = d
      · subst ha
        exact Or.inr ⟨k, by simp [upd2, hk, hens], by have hn' : n < s.acked a := hn; show n < s.stored a k; omega⟩
      · rcases i6 a n hn with hc | ⟨m, hm, hs⟩
        · exact Or.inl hc
        · exact Or.inr ⟨m, by simp [upd2, ha, hm], hs⟩
  | append d k hens =>
    refine ⟨i1, i2, i3, i4, i5, ?_⟩
    intro a n hn
    by_cases ha : a = d
    · subst ha
      refine Or.inr ⟨k, hens, ?_⟩
      simp only [upd] at hn
      simp [hens]
      simp at hn
      omega
    · simp only [upd, ha, ite_false] at hn
      rcases i6 a n hn with hc | ⟨m, hm, hs⟩
      · exact Or.inl hc
      · exact Or.inr ⟨m, hm, by simp [ha, hs]⟩
  | checkpoint d =>
    refine ⟨i1, i2, i3, i4, i5, ?_⟩
    intro a n hn
    by_cases ha : a = d
    · subst ha; exact Or.inl (by simp [upd]; exact hn)
    · rcases i6 a n hn with hc | hr
      · exact Or.inl (by simp [upd, ha]; exact hc)
      · exact Or.inr hr
  | depend d j href =>
    refine ⟨i1, i2, i3, ?_, i5, i6⟩
    intro a b hb
    simp only [upd2] at hb
    by_cases h : a = d ∧ b = j
    · obtain ⟨rfl, rfl⟩ := h; exact href
    · simp only [h, ite_false] at hb; exact i4 a b hb
  | undepend d j =>
    refine ⟨i1, i2, i3, ?_, i5, i6⟩
    intro a b hb
    simp only [upd2] at hb
    by_cases h : a = d ∧ b = j
    · simp [h] at hb
    · simp only [h, ite_false] at hb; exact i4 a b hb
  | release d j href hens hdeps =>
    refine ⟨i1, i2, ?_, ?_, ?_, ?_⟩
    · intro a b hb
      simp only [upd2]
      by_cases h : a = d ∧ b = j
      · simp [h]
      · simp only [h, ite_false]; exact i3 a b hb
    · intro a b hb
      simp only [upd2]
      by_cases h : a = d ∧ b = j
      · obtain ⟨rfl, rfl⟩ := h; simp_all
      · simp only [h, ite_false]; exact i4 a b hb
    · intro a b hb
      simp only [upd2]
      by_cases h : a = d ∧ b = j
      · obtain ⟨rfl, rfl⟩ := h; simp_all
      · simp only [h, ite_false]; exact i5 a b hb
    · intro a n hn
      rcases i6 a n hn with hc | ⟨k, hk, hs⟩
      · exact Or.inl hc
      · by_cases h : a = d ∧ k = j
        · obtain ⟨rfl, rfl⟩ := h; simp_all
        · exact Or.inr ⟨k, hk, by simp only [upd2, h, ite_false]; exact hs⟩
  | dispose d hdeps =>
    refine ⟨i1, i2, ?_, ?_, ?_, ?_⟩
    · intro a b hb
      by_cases ha : a = d
      · simp [ha]
      · simp only [ha, ite_false]; exact i3 a b hb
    · intro a b hb
      by_cases ha : a = d
      · subst ha; simp_all
      · simp only [ha, ite_false]; exact i4 a b hb
    · intro a b hb
      by_cases ha : a = d
      · simp [ha] at hb
      · simp only [ha, ite_false] at hb ⊢; exact i5 a b hb
    · intro a n hn
      by_cases ha : a = d
      · subst ha; simp [upd] at hn
      · simp only [upd, ha, ite_false] at hn ⊢
        rcases i6 a n hn with hc | ⟨k, hk, hs⟩
        · exact Or.inl hc
        · exact Or.inr ⟨k, by simp [ha, hk], hs⟩
  | drain j h =>
    refine ⟨?_, ?_, ?_, i4, i5, i6⟩
    · intro b e he
      simp only [upd2, upd] at he ⊢
      by_cases hb : b = j
      · subst hb
        by_cases hee : e = s.epoch b + 1
        · simp [hee]
        · simp [hee] at he; have := i1 b e he; simp; omega
      · simp [hb] at he ⊢; exact i1 b e he
    · intro b hb
      simp only [upd2, upd] at hb ⊢
      by_cases hbj : b = j
      · simp [hbj, Stage.accepting]
      · simp [hbj] at hb ⊢; exact i2 b hb
    · intro a b hb
      simp only [upd] at hb
      by_cases hbj : b = j
      · simp [hbj, Stage.accepting] at hb
      · simp only [hbj, ite_false] at hb; exact i3 a b hb
  | certify j h hscan =>
    refine ⟨?_, ?_, ?_, i4, i5, i6⟩
    · intro b e he
      simp only [upd]
      by_cases hb : b = j
      · subst hb; have := i1 b e he; simp; omega
      · simp [hb]; exact i1 b e he
    · intro b hb
      simp only [upd] at hb ⊢
      by_cases hbj : b = j
      · subst hbj
        simp at hb
        have := i1 b _ hb
        omega
      · simp [hbj] at hb ⊢; exact i2 b hb
    · intro a b hb
      simp only [upd] at hb
      by_cases hbj : b = j
      · subst hbj; exact hscan a
      · simp only [hbj, ite_false] at hb; exact i3 a b hb
  | withdraw j h =>
    refine ⟨?_, ?_, ?_, i4, i5, i6⟩
    · intro b e he
      simp only [upd2, upd] at he ⊢
      by_cases hb : b = j
      · subst hb
        by_cases hee : e = s.epoch b + 1
        · simp [hee]
        · simp [hee] at he; have := i1 b e he; simp; omega
      · simp [hb] at he ⊢; exact i1 b e he
    · intro b hb
      simp only [upd2, upd] at hb ⊢
      by_cases hbj : b = j
      · simp [hbj, Stage.accepting]
      · simp [hbj] at hb ⊢; exact i2 b hb
    · intro a b hb
      simp only [upd] at hb
      by_cases hbj : b = j
      · simp [hbj, Stage.accepting] at hb
      · simp only [hbj, ite_false] at hb; exact i3 a b hb
  | decommission j h hscan =>
    refine ⟨?_, ?_, ?_, i4, i5, i6⟩
    · intro b e he
      simp only [upd]
      by_cases hb : b = j
      · subst hb; have := i1 b e he; simp; omega
      · simp [hb]; exact i1 b e he
    · intro b hb
      simp only [upd] at hb ⊢
      by_cases hbj : b = j
      · subst hbj
        simp at hb
        have := i1 b _ hb
        omega
      · simp [hbj] at hb ⊢; exact i2 b hb
    · intro a b hb
      simp only [upd] at hb
      by_cases hbj : b = j
      · subst hbj; exact hscan a
      · simp only [hbj, ite_false] at hb; exact i3 a b hb

theorem reachable_inv {s : State} (h : Reachable s) : Inv s := by
  induction h with
  | init => exact initial_inv
  | step _ step ih => exact step_inv step ih

/-- (a) Fence soundness: whatever stale observations placements land on, no deployment retains a certified journal. -/
theorem no_deployment_retains_a_certified_journal {s : State} (h : Reachable s) (d j : Nat)
    (hc : s.stage j = .certified) : s.ref d j = false :=
  (reachable_inv h).retiringUnreferenced d j (by simp [hc, Stage.accepting])

/-- (a) No deployment references a decommissioned logkeeper, under arbitrary interleavings. -/
theorem no_deployment_references_a_decommissioned_logkeeper {s : State} (h : Reachable s) (d j : Nat)
    (hc : s.stage j = .decommissioned) : s.ref d j = false ∧ s.ens d j = false ∧ s.deps d j = false := by
  have hi := reachable_inv h
  have href := hi.retiringUnreferenced d j (by simp [hc, Stage.accepting])
  refine ⟨href, ?_, ?_⟩
  · cases he : s.ens d j
    · rfl
    · have := hi.ensembleRetained d j he; simp_all
  · cases hd : s.deps d j
    · rfl
    · have := hi.dependentsRetained d j hd; simp_all

/-- (b) A copy is released only when nothing depends on it and it no longer serves. -/
theorem a_release_has_no_dependents {s t : State} {d j : Nat} (hs : Reachable s)
    (step : Step s t) (released : s.ref d j = true ∧ t.ref d j = false) :
    s.deps d j = false ∧ s.ens d j = false ∨ (∀ j', t.ref d j' = false) := by
  cases step with
  | release d' j' href hens hdeps =>
    by_cases h : d = d' ∧ j = j'
    · obtain ⟨rfl, rfl⟩ := h; exact Or.inl ⟨hdeps, hens⟩
    · have := released.2; simp [upd2, h, released.1] at this
  | dispose d' hdeps =>
    by_cases h : d = d'
    · subst h; exact Or.inr (fun j' => by simp)
    · have := released.2; simp [h, released.1] at this
  | place d' j' obs _ _ =>
    have := released.2; simp only [upd2] at this
    by_cases h : d = d' ∧ j = j'
    · simp [h] at this
    · simp [h, released.1] at this
  | join => have := released.2; simp_all
  | leave => have := released.2; simp_all
  | append => have := released.2; simp_all
  | checkpoint => have := released.2; simp_all
  | depend => have := released.2; simp_all
  | undepend => have := released.2; simp_all
  | drain => have := released.2; simp_all
  | certify => have := released.2; simp_all
  | withdraw => have := released.2; simp_all
  | decommission => have := released.2; simp_all

/-- (b) After any release, nothing depends on a copy that is no longer retained. -/
theorem nothing_depends_on_a_released_copy {s : State} (h : Reachable s) (d j : Nat)
    (hr : s.ref d j = false) : s.deps d j = false := by
  cases hd : s.deps d j
  · rfl
  · have := (reachable_inv h).dependentsRetained d j hd; simp_all

/-- (b) Every acknowledged entry stays durable in the checkpoint or the serving ensemble, across releases. -/
theorem every_acknowledged_entry_stays_durable {s : State} (h : Reachable s) : Durable s :=
  (reachable_inv h).durable

/-- (c) A decommission lands only while no deployment retains the journal. -/
theorem decommission_requires_no_custody {s t : State} {j : Nat} (hs : Reachable s) (step : Step s t)
    (hd : t.stage j = .decommissioned) (hb : s.stage j ≠ .decommissioned) : ∀ d, s.ref d j = false := by
  intro d
  exact (reachable_inv hs).retiringUnreferenced d j (by
    cases step with
    | decommission j' hc _ =>
      by_cases hj : j = j'
      · subst hj; simp [hc, Stage.accepting]
      · simp [upd, hj] at hd; exact absurd hd hb
    | drain j' _ => by_cases hj : j = j' <;> simp_all [upd]
    | certify j' _ _ => by_cases hj : j = j' <;> simp_all [upd]
    | withdraw j' _ => by_cases hj : j = j' <;> simp_all [upd]
    | _ => exact absurd hd hb)

/-- Reachability: a journal serves a deployment, is drained and evacuated, its copy is released, and it is
decommissioned while the deployment keeps its acknowledged entry on the remaining journal. -/
theorem an_evacuated_journal_is_released_and_decommissioned :
    ∃ s, Reachable s ∧ s.stage 0 = .decommissioned ∧ s.acked 0 = 1 ∧ s.ens 0 1 = true ∧ 0 < s.stored 0 1 := by
  let s1 : State := { initial with ref := upd2 initial.ref 0 0 true }
  let s2 : State := { s1 with ens := upd2 s1.ens 0 0 true, stored := upd2 s1.stored 0 0 (s1.acked 0) }
  let s3 : State := { s2 with acked := upd s2.acked 0 (s2.acked 0 + 1),
                              stored := fun a b => if a = 0 ∧ s2.ens 0 b = true then s2.acked 0 + 1 else s2.stored a b }
  let s4 : State := { s3 with ref := upd2 s3.ref 0 1 true }
  let s5 : State := { s4 with ens := upd2 s4.ens 0 1 true, stored := upd2 s4.stored 0 1 (s4.acked 0) }
  let s6 : State := { s5 with stage := upd s5.stage 0 .draining, epoch := upd s5.epoch 0 (s5.epoch 0 + 1),
                              seen := upd2 s5.seen 0 (s5.epoch 0 + 1) true }
  let s7 : State := { s6 with ens := upd2 s6.ens 0 0 false }
  let s8 : State := { s7 with ref := upd2 s7.ref 0 0 false, stored := upd2 s7.stored 0 0 0 }
  let s9 : State := { s8 with stage := upd s8.stage 0 .certified, epoch := upd s8.epoch 0 (s8.epoch 0 + 1) }
  let s10 : State := { s9 with stage := upd s9.stage 0 .decommissioned, epoch := upd s9.epoch 0 (s9.epoch 0 + 1) }
  have r1 : Reachable s1 := .step .init (.place initial 0 0 0 (by rfl) (by rfl))
  have r2 : Reachable s2 := .step r1 (.join s1 0 0 (by simp [s1, upd2]))
  have r3 : Reachable s3 := .step r2 (.append s2 0 0 (by simp [s2, upd2]))
  have r4 : Reachable s4 := .step r3 (.place s3 0 1 0 (by rfl) (by rfl))
  have r5 : Reachable s5 := .step r4 (.join s4 0 1 (by simp [s4, upd2]))
  have r6 : Reachable s6 := .step r5 (.drain s5 0 (by rfl))
  have r7 : Reachable s7 := .step r6 (.leave s6 0 0 1 (by decide)
    (by simp [s6, s5, upd2]) (by simp [s6, s5, s4, s3, s2, s1, upd2, upd, initial]))
  have r8 : Reachable s8 := .step r7 (.release s7 0 0 (by simp [s7, s6, s5, s4, s3, s2, s1, upd2])
    (by simp [s7, upd2]) (by rfl))
  have r9 : Reachable s9 := .step r8 (.certify s8 0 (by simp [s8, s7, s6, upd])
    (by intro d; by_cases hd : d = 0 <;> simp [s8, s7, s6, s5, s4, s3, s2, s1, upd2, initial, hd]))
  have r10 : Reachable s10 := .step r9 (.decommission s9 0 (by simp [s9, upd])
    (by intro d; by_cases hd : d = 0 <;> simp [s9, s8, s7, s6, s5, s4, s3, s2, s1, upd2, initial, hd]))
  exact ⟨s10, r10, by simp [s10, upd], by simp [s10, s9, s8, s7, s6, s5, s4, s3, s2, s1, upd, initial],
    by simp [s10, s9, s8, s7, s6, s5, upd2], by simp [s10, s9, s8, s7, s6, s5, s4, s3, s2, s1, upd2, upd, initial]⟩

#print axioms no_deployment_retains_a_certified_journal
#print axioms no_deployment_references_a_decommissioned_logkeeper
#print axioms a_release_has_no_dependents
#print axioms nothing_depends_on_a_released_copy
#print axioms every_acknowledged_entry_stays_durable
#print axioms decommission_requires_no_custody
#print axioms an_evacuated_journal_is_released_and_decommissioned

end RTDB.JournalCustody
