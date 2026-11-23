import OrderedCapacity

namespace RTDB.Recovery.OrderedJournal

/- The chosen-command layer of one placement. Digest names model exact
identity, and the `chosen` projection is the logical history even after the
physical suffix has been trimmed. A true quorum flag stands for a reachable
durable majority implementing the chosen-command interface. It does not certify
the ballot algorithm, disk barriers, or provider transport. -/

structure Cut where
  next : Nat
  digest : Nat
  deriving DecidableEq

structure Checkpoint where
  cut : Cut
  root : Nat
  provider : Nat
  allocation : Nat
  deriving DecidableEq

structure Attempt where
  issuer : Nat
  nonce : Nat
  expiry : Nat
  deriving DecidableEq

structure Receipt where
  payload : Nat
  cut : Cut
  deriving DecidableEq

structure Hold where
  enrollment : Nat
  nonce : Nat
  base : Checkpoint
  first : Nat
  through : Cut
  activation : Bool
  deriving DecidableEq

structure Preparation where
  base : Checkpoint
  target : Checkpoint
  staged : Bool
  deriving DecidableEq

structure State where
  cohort : Nat
  seal : Option Nat
  writer : Nat
  custodian : Nat
  chosen : Nat → Option Cut
  head : Cut
  floor : Nat
  published : Checkpoint
  preparing : Option Preparation
  closed : List Checkpoint
  fenced : List Checkpoint
  disposed : List Checkpoint
  holdBook : List Hold
  receipts : Attempt → Option Receipt
  readFloor : Nat
  observed : Nat
  quorum : Bool
  provider : Bool

def update (f : α → β) [DecidableEq α] (key : α) (value : β) : α → β :=
  fun entry => if entry = key then value else f entry

def Current (s : State) (cohort writer : Nat) : Prop :=
  cohort = s.cohort ∧ writer = s.writer ∧ s.seal = none

def CanServe (s : State) (hold : Hold) (applied : Cut) : Prop :=
  hold ∈ s.holdBook ∧ s.chosen applied.next = some applied ∧ hold.through.next ≤ applied.next

def Reissue (s : State) (attempt : Attempt) (payload : Nat) : Option Receipt :=
  (s.receipts attempt).bind fun r => if r.payload = payload then some r else none

def CanMaterialize (s : State) (target : Checkpoint) (provider : Nat) : Prop :=
  s.provider = true ∧ target.provider = provider ∧ target ∉ s.fenced

def Invariant (s : State) : Prop :=
  s.floor ≤ s.published.cut.next ∧ s.published.cut.next ≤ s.head.next ∧
  s.chosen s.published.cut.next = some s.published.cut ∧
  ∀ h ∈ s.holdBook, h.first = h.through.next ∨ s.floor ≤ h.first

def seedCut : Cut := ⟨0, 0⟩

def seed : State where
  cohort := 1
  seal := none
  writer := 0
  custodian := 1
  chosen := fun n => if n = 0 then some seedCut else none
  head := seedCut
  floor := 0
  published := ⟨seedCut, 0, 0, 0⟩
  preparing := none
  closed := []
  fenced := []
  disposed := []
  holdBook := []
  receipts := fun _ => none
  readFloor := 0
  observed := 0
  quorum := true
  provider := true

def hold (s : State) (enrollment nonce : Nat) (activation : Bool) : Hold :=
  ⟨enrollment, nonce, s.published, s.published.cut.next, s.head, activation⟩

def ReaderCount (s : State) : Nat := s.holdBook.countP (fun h => !h.activation)

def ActivationCount (s : State) : Nat := s.holdBook.countP (fun h => h.activation)

inductive Step (readerSlots activationSlots : Nat) : State → State → Prop where
  | append (s : State) (cohort writer : Nat) (attempt : Attempt) (receipt : Receipt)
      (auth : Current s cohort writer) (quorum : s.quorum = true)
      (issuer : attempt.issuer = writer) (fresh : s.receipts attempt = none)
      (next : receipt.cut.next = s.head.next + 1) :
      Step readerSlots activationSlots s
        {s with head := receipt.cut, chosen := update s.chosen receipt.cut.next (some receipt.cut),
          receipts := update s.receipts attempt (some receipt)}
  | expire (s : State) (observed : Nat) (forward : s.observed ≤ observed) :
      Step readerSlots activationSlots s
        {s with observed := observed,
          receipts := fun attempt => if attempt.expiry ≤ observed then none else s.receipts attempt}
  | grant (s : State) (cohort expected nonce : Nat) (auth : Current s cohort expected)
      (quorum : s.quorum = true) (room : ActivationCount s < activationSlots) :
      Step readerSlots activationSlots s
        {s with writer := s.writer + 1, readFloor := 0,
          holdBook := hold s (s.writer + 1) nonce true :: s.holdBook}
  | barrier (s : State) (cohort writer nonce : Nat) (auth : Current s cohort writer)
      (quorum : s.quorum = true) (room : ReaderCount s < readerSlots)
      (next : nonce = s.readFloor + 1) :
      Step readerSlots activationSlots s
        {s with readFloor := nonce, holdBook := hold s writer nonce false :: s.holdBook}
  | abandon (s : State) (cohort writer nonce : Nat) (auth : Current s cohort writer)
      (quorum : s.quorum = true) (next : nonce = s.readFloor + 1) :
      Step readerSlots activationSlots s {s with readFloor := nonce}
  | closeHold (s : State) (closing : Hold) (owner : Nat) (quorum : s.quorum = true)
      (matches : closing.enrollment = owner) (present : closing ∈ s.holdBook) :
      Step readerSlots activationSlots s {s with holdBook := s.holdBook.filter (fun entry => decide (entry ≠ closing))}
  | prepare (s : State) (custodian : Nat) (base target : Checkpoint)
      (authorized : custodian = s.custodian) (quorum : s.quorum = true)
      (empty : s.preparing = none) (matches : s.chosen target.cut.next = some target.cut)
      (forward : s.published.cut.next < target.cut.next) (within : target.cut.next ≤ s.head.next)
      (baseOpen : base ∉ s.closed) (targetOpen : target ∉ s.closed) :
      Step readerSlots activationSlots s {s with preparing := some ⟨base, target, false⟩}
  | copied (s : State) (base target : Checkpoint) (provider : Nat)
      (preparing : s.preparing = some ⟨base, target, false⟩)
      (evidence : CanMaterialize s target provider) :
      Step readerSlots activationSlots s {s with preparing := some ⟨base, target, true⟩}
  | publish (s : State) (custodian : Nat) (base target : Checkpoint)
      (authorized : custodian = s.custodian) (quorum : s.quorum = true)
      (ready : s.preparing = some ⟨base, target, true⟩)
      (matches : s.chosen target.cut.next = some target.cut)
      (forward : s.published.cut.next < target.cut.next) (within : target.cut.next ≤ s.head.next) :
      Step readerSlots activationSlots s {s with published := target, preparing := none}
  | trim (s : State) (custodian : Nat) (selected : Checkpoint) (through : Cut)
      (authorized : custodian = s.custodian) (quorum : s.quorum = true)
      (selection : selected = s.published)
      (matches : s.chosen through.next = some through) (forward : s.floor ≤ through.next)
      (covered : through.next ≤ s.published.cut.next)
      (unheld : ∀ h ∈ s.holdBook, h.first = h.through.next ∨ through.next ≤ h.first) :
      Step readerSlots activationSlots s {s with floor := through.next}
  | close (s : State) (old : Checkpoint) (custodian : Nat)
      (authorized : custodian = s.custodian) (quorum : s.quorum = true) (unselected : old ≠ s.published)
      (unheld : ∀ h ∈ s.holdBook, h.base ≠ old)
      (unprepared : ∀ preparing, s.preparing = some preparing → preparing.base ≠ old ∧ preparing.target ≠ old) :
      Step readerSlots activationSlots s {s with closed := old :: s.closed}
  | fence (s : State) (old : Checkpoint) (provider : Nat) (closed : old ∈ s.closed)
      (exactProvider : old.provider = provider) (reachable : s.provider = true) :
      Step readerSlots activationSlots s {s with fenced := old :: s.fenced}
  | settle (s : State) (old : Checkpoint) (provider custodian : Nat) (fenced : old ∈ s.fenced)
      (authorized : custodian = s.custodian) (exactProvider : old.provider = provider) (reachable : s.provider = true)
      (quorum : s.quorum = true) :
      Step readerSlots activationSlots s {s with disposed := old :: s.disposed}
  | seal (s : State) (successor : Nat) (active : s.seal = none) (quorum : s.quorum = true)
      (successorDistinct : successor ≠ s.cohort) (funded : Bool) (ticket : funded = true) :
      Step readerSlots activationSlots s {s with seal := some successor}
  | install (s : State) (successor : Nat) (matches : s.seal = some successor)
      (prepared : Bool) (ticket : prepared = true) :
      Step readerSlots activationSlots s {s with cohort := successor, seal := none}
  | lostReply (s : State) : Step readerSlots activationSlots s s
  | quorumAvailability (s : State) (reachable : Bool) :
      Step readerSlots activationSlots s {s with quorum := reachable}
  | providerAvailability (s : State) (reachable : Bool) :
      Step readerSlots activationSlots s {s with provider := reachable}

inductive Trace (readerSlots activationSlots : Nat) : State → State → Prop where
  | nil (s : State) : Trace readerSlots activationSlots s s
  | cons : Step readerSlots activationSlots s t → Trace readerSlots activationSlots t u →
      Trace readerSlots activationSlots s u

theorem step_preserves_checkpoint_and_holds (before : Invariant s)
    (step : Step readerSlots activationSlots s t) : Invariant t := by
  obtain ⟨covered, head, selected, held⟩ := before
  cases step <;> try exact ⟨covered, head, selected, held⟩
  case append cohort writer attempt receipt auth quorum issuer fresh next =>
    have different : s.published.cut.next ≠ receipt.cut.next := by omega
    refine ⟨covered, ?_, ?_, held⟩
    · change s.published.cut.next ≤ receipt.cut.next
      omega
    · simpa [update, different] using selected
  case grant =>
    refine ⟨covered, head, selected, ?_⟩
    intro h member
    rcases List.mem_cons.mp member with rfl | old
    · exact Or.inr covered
    · exact held h old
  case barrier =>
    refine ⟨covered, head, selected, ?_⟩
    intro h member
    rcases List.mem_cons.mp member with rfl | old
    · exact Or.inr covered
    · exact held h old
  case closeHold =>
    exact ⟨covered, head, selected, fun h member => held h (List.mem_filter.mp member).1⟩
  case publish custodian base target authorized quorum ready matches forward within =>
    exact ⟨Nat.le_trans covered (Nat.le_of_lt forward), within, matches, held⟩
  case trim custodian selectedRoot through authorized quorum selection matches forward current pins =>
    exact ⟨current, head, selected, pins⟩

theorem protected_through_trace (before : Invariant s)
    (path : Trace readerSlots activationSlots s t) : Invariant t := by
  revert before
  induction path with
  | nil => exact id
  | cons step _ rest =>
      intro before
      exact rest (step_preserves_checkpoint_and_holds before step)

theorem reached_floor_has_coverage (path : Trace readerSlots activationSlots seed s) :
    s.floor ≤ s.published.cut.next ∧ s.chosen s.published.cut.next = some s.published.cut := by
  have initial : Invariant seed := by simp [Invariant, seed, seedCut]
  obtain ⟨floor, _, chosen, _⟩ := protected_through_trace initial path
  exact ⟨floor, chosen⟩

theorem live_hold_prevents_crossing_its_start (ok : Invariant s) (member : h ∈ s.holdBook)
    (nonempty : h.first ≠ h.through.next) : s.floor ≤ h.first :=
  Or.resolve_left (ok.2.2.2 h member) nonempty

theorem a_fresh_reader_preserves_activation_room (s : State) (writer nonce : Nat) :
    ActivationCount {s with holdBook := hold s writer nonce false :: s.holdBook} =
      ActivationCount s := by
  simp [ActivationCount, List.countP_cons, hold]

theorem activated_slot_survives_reader_pressure (s : State) (expected cohort nonce : Nat)
    (auth : Current s cohort expected) (quorum : s.quorum = true)
    (room : ActivationCount s < activationSlots) :
    ∃ t, Step readerSlots activationSlots s t ∧ t.writer = s.writer + 1 :=
  ⟨_, Step.grant s cohort expected nonce auth quorum room, rfl⟩

theorem fresh_nonce_cannot_reopen (s : State) (nonce : Nat) (spent : nonce ≤ s.readFloor) :
    nonce ≠ s.readFloor + 1 := by omega

theorem previous_writer_cannot_act (s : State) (cohort : Nat) :
    ¬Current {s with writer := s.writer + 1} cohort s.writer := by
  intro active
  have impossible : s.writer = s.writer + 1 := active.2.1
  omega

theorem sealed_predecessor_cannot_act (s : State) (successor : Nat) :
    ¬Current {s with seal := some successor} s.cohort s.writer := by
  simp [Current]

theorem installed_successor_fences_the_old_cohort (s : State) (successor : Nat)
    (different : successor ≠ s.cohort) :
    ¬Current {s with cohort := successor, seal := none} s.cohort s.writer := by
  intro old
  exact different old.1.symm

theorem successor_cannot_change_original_receipt (s : State) (successor : Nat)
    (attempt : Attempt) (payload : Nat) :
    Reissue {s with cohort := successor, seal := none} attempt payload = Reissue s attempt payload := rfl

theorem repeated_payload_returns_exact_receipt (present : s.receipts attempt = some r) :
    Reissue s attempt r.payload = some r := by simp [Reissue, present]

theorem rival_payload_cannot_reissue (present : s.receipts attempt = some r) (rival : payload ≠ r.payload) :
    Reissue s attempt payload = none := by
  simp [Reissue, present, Ne.symm rival]

theorem trim_preserves_receipt (s : State) (through : Cut) (attempt : Attempt) (payload : Nat) :
    Reissue {s with floor := through.next} attempt payload = Reissue s attempt payload := rfl

theorem expiry_clock_is_monotone (step : Step readerSlots activationSlots s t) :
    s.observed ≤ t.observed := by
  cases step <;> first | assumption | exact Nat.le_refl _

theorem expiry_clock_is_monotone_on_trace (path : Trace readerSlots activationSlots s t) :
    s.observed ≤ t.observed := by
  induction path with
  | nil => exact Nat.le_refl _
  | cons first _ rest => exact Nat.le_trans (expiry_clock_is_monotone first) rest

theorem live_receipt_survives_step (retained : s.receipts attempt = some r)
    (step : Step readerSlots activationSlots s t) (live : t.observed < attempt.expiry) :
    t.receipts attempt = some r := by
  cases step <;> try exact retained
  case append actor writer freshAttempt receipt auth quorum issuer fresh next =>
    by_cases same : attempt = freshAttempt
    · have contradiction := retained
      rw [same, fresh] at contradiction
      contradiction
    · simpa [update, same] using retained
  case expire observed forward =>
    simpa [Nat.not_le.mpr live] using retained

theorem live_receipt_survives_recovery (retained : s.receipts attempt = some r)
    (path : Trace readerSlots activationSlots s t) (live : t.observed < attempt.expiry) :
    t.receipts attempt = some r := by
  revert retained live
  induction path with
  | nil => exact fun retained _ => retained
  | cons first tail rest =>
    intro retained live
    have oneLive := Nat.lt_of_le_of_lt (expiry_clock_is_monotone_on_trace tail) live
    exact rest (live_receipt_survives_step retained first oneLive) live

theorem read_cannot_serve_an_unapplied_barrier (served : CanServe s h applied) :
    h.through.next ≤ applied.next := served.2.2

theorem read_cannot_serve_same_position_with_alien_digest (served : CanServe s h applied)
    (matches : s.chosen expected.next = some expected) (samePosition : applied.next = expected.next) :
    applied = expected := by
  have actual := served.2.1
  rw [samePosition, matches] at actual
  exact (Option.some.inj actual).symm

theorem late_provider_write_after_fence_is_refused (s : State) (old : Checkpoint) (provider : Nat) :
    ¬CanMaterialize {s with fenced := old :: s.fenced} old provider := by
  simp [CanMaterialize]

def AllDisposedFenced (s : State) : Prop :=
  ∀ old ∈ s.disposed, old ∈ s.fenced

theorem disposal_keeps_its_fence (before : AllDisposedFenced s)
    (step : Step readerSlots activationSlots s t) : AllDisposedFenced t := by
  cases step <;> try exact before
  case fence old provider closed exactProvider reachable =>
    intro root member
    exact List.mem_cons_of_mem old (before root member)
  case settle old provider custodian fenced authorized exactProvider reachable quorum =>
    intro root member
    rcases List.mem_cons.mp member with rfl | previous
    · exact fenced
    · exact before root previous

theorem disposed_checkpoint_cannot_rematerialize (path : Trace readerSlots activationSlots seed s)
    (disposed : old ∈ s.disposed) : ¬CanMaterialize s old provider := by
  have initial : AllDisposedFenced seed := by simp [AllDisposedFenced, seed]
  have preserves : ∀ start finish, Trace readerSlots activationSlots start finish →
      AllDisposedFenced start → AllDisposedFenced finish := by
    intro start finish path
    induction path with
    | nil => exact id
    | cons first _ rest =>
      intro sound
      exact rest (disposal_keeps_its_fence sound first)
  intro materialized
  exact materialized.2.2 (preserves seed s path initial old disposed)

/-- The journal authorizes a trim at an exact chosen cut. The physical
accounting adapter supplies the ordinary and maintenance bytes represented by
that same removed interval. In the post-trim state a custodian can fund a
positive ordinary window while an unexpired receipt remains identical. -/
theorem protected_trim_can_fund_without_rewriting_receipts
    (s : State) (through : Cut) (attempt : Attempt) (payload : Nat)
    (trimStep : Step readerSlots activationSlots s {s with floor := through.next})
    (before : Invariant s)
    (usage : OrderedCapacity.State) (budget : OrderedCapacity.Budget)
    (cost : OrderedCapacity.Cost) (data maintenance : Nat)
    (within : OrderedCapacity.Within budget usage)
    (published : usage.phase = .published) (quorum : usage.quorum = true)
    (worker : usage.worker = true) (released : usage.held = false)
    (coversData : data ≤ usage.data) (coversMaintenance : maintenance ≤ usage.maintenance)
    (freesData : 0 < data) :
    ∃ journal capacity,
      Step readerSlots activationSlots s journal ∧ Invariant journal ∧
      OrderedCapacity.Trace budget cost usage capacity ∧ 0 < capacity.unspent ∧
      Reissue journal attempt payload = Reissue s attempt payload := by
  let journal := {s with floor := through.next}
  let cutUsage := OrderedCapacity.trim usage data maintenance
  let funded := {cutUsage with unspent := data}
  have fits : cutUsage.data + data ≤ budget.data := by
    have paid := within.1
    change usage.data - data + data ≤ budget.data
    omega
  refine ⟨journal, funded, trimStep, step_preserves_checkpoint_and_holds before trimStep, ?_,
    freesData, rfl⟩
  exact OrderedCapacity.Trace.cons
    (OrderedCapacity.Step.trim usage data maintenance published quorum worker released
      coversData coversMaintenance)
    (OrderedCapacity.Trace.cons
      (OrderedCapacity.Step.fund cutUsage data quorum worker fits) (OrderedCapacity.Trace.nil _))

#print axioms reached_floor_has_coverage
#print axioms activated_slot_survives_reader_pressure
#print axioms live_receipt_survives_recovery
#print axioms read_cannot_serve_same_position_with_alien_digest
#print axioms disposed_checkpoint_cannot_rematerialize
#print axioms protected_trim_can_fund_without_rewriting_receipts

end RTDB.Recovery.OrderedJournal
