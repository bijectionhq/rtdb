import RecoveryComposition
import NativeAppend
import NativeAcknowledgement
import FencedTermAdmission

set_option warningAsError true

namespace RTDB.Recovery.ReplicationDriver

open Logkeeper
open Composition (projectAttempt projectReceipt)

structure Owner where
  grant : Nat
  term : Int
  sequence : Int

structure Flight where
  owner : Owner
  tx : Nat
  fingerprint : List UInt8
  guarded : Bool

def Flight.identity (flight : Flight) : NativeRecordIdentity :=
  ⟨flight.owner.term, some flight.owner.sequence, some flight.fingerprint⟩

structure Entry where
  id : Nat
  flight : Flight
  record : NativeAppend.ChosenRecord
  decisionPosition : Nat

def Entry.append (entry : Entry) : Protocol.Append := NativeAppend.nativeAppend entry.record

inductive Channel where
  | unused | waiting | spent
  deriving DecidableEq, Repr

inductive ApplyPhase where
  | read | applying | published | routed
  deriving DecidableEq

structure Application where
  entry : Entry
  recipient : Option Nat
  phase : ApplyPhase
  observed : NativeRecordIdentity
  resolved : Bool
  exact : ExactReadback entry.flight.identity observed resolved = true

structure Local where
  cursor : Nat := 0
  highest : Int := 0
  emptyRead : Bool := false
  observedGrant : Option Nat := none
  owner : Option Owner := none
  queue : List (Nat × Nat) := []
  enqueueing : Option Nat := none
  guardedReceipt : Option Nat := none
  returned : Nat → Bool := fun _ => false
  halted : Bool := false
  application : Option Application := none
  channels : Nat → Channel := fun _ => .unused

structure State where
  journal : OrderedJournal.State := OrderedJournal.seed
  entries : Nat → Option Entry := fun _ => none
  flights : Nat → Option Flight := fun _ => none
  activationSlots : Nat := 1024
  worker : Local := {}

inductive LocalAction where
  | emptyRead | observeGrant (grant : Nat) | promote (term : Int)
  | enqueue (id tx : Nat)
  | appendReturned (id : Nat)
  | durable (id next : Nat)
  | providerFault (id : Nat)
  | readBack (identity : NativeRecordIdentity) (resolved : Bool)
  | replay (identity : NativeRecordIdentity)
  | applyBegin | applyFinished (tx : Nat) | applyFault | acknowledge (id tx : Nat) | advance
  | termLoss | crash

inductive Action where
  | worker (action : LocalAction)
  | dispatch (id : Nat) (term sequence : Int) (fingerprint : List UInt8) (guarded : Bool)
  | choose (id decisionPosition : Nat) (placement : Protocol.Placement)
      (requestDigest outcomeDigest cutDigest expiry expiryDigest charge : Nat) (chargeClass : Protocol.ChargeClass)
  | replace (nonce : Nat)
  | release (hold : OrderedJournal.Hold) (owner : Nat)
  | expire (observed : Nat)
  | quorum (available : Bool)
  | provider (available : Bool)

inductive Reject where
  | emptyRead | grant | promotion | enqueue | dispatch | providerReturn | durability
  | staleWriter | choose | replacement | release | expiry | readOrder | readbackIdentity
  | applyOrder | appliedOutcome | acknowledgeAfterApply | routeOnce
  deriving DecidableEq, Repr

structure LocalEffect where
  state : Local
  acknowledgement : Option (Nat × Entry) := none

structure Effect where
  state : State
  acknowledgement : Option (Nat × Entry) := none

private def require (ok : Bool) (reason : Reject) : Except Reject Unit :=
  if ok then .ok () else .error reason

private def find (value : Option α) (reason : Reject) : Except Reject α :=
  match value with
  | some item => .ok item
  | none => .error reason

private theorem require_bind (ok : Bool) (reason : Reject) (next : Unit → Except Reject α) :
    (require ok reason >>= next) = (if ok then next () else .error reason) := by
  cases ok <;> rfl

private theorem find_bind (value : Option α) (reason : Reject) (next : α → Except Reject β) :
    (find value reason >>= next) = (match value with
      | some item => next item
      | none => .error reason) := by
  cases value <;> rfl

private def close (channels : Nat → Channel) (id : Nat) : Nat → Channel :=
  OrderedJournal.update channels id .spent

private def retire (channels : Nat → Channel) : Nat → Channel :=
  fun id => if channels id = .waiting then .spent else channels id

private def readRecord (s : State) (identity : NativeRecordIdentity) (resolved own : Bool) :
    Except Reject LocalEffect := do
  let l := s.worker
  require (l.application.isNone && !l.halted && s.journal.provider) .readOrder
  let entry ← find (s.entries (l.cursor + 1)) .readOrder
  let isOwn := own_readback_term (l.owner.map (·.term)) entry.flight.owner.term
  require (own == isOwn && (own || l.owner.isNone) && decide (l.highest ≤ entry.flight.owner.term)) .readOrder
  require (own_readback_term (some entry.flight.owner.term) identity.term &&
    matching_sequence entry.flight.identity.sequence identity.sequence) .readOrder
  if exact : ExactReadback entry.flight.identity identity resolved then
    return ⟨{l with
      application := some ⟨entry, if own then some entry.id else none, .read, identity, resolved, exact⟩
      highest := entry.flight.owner.term
      emptyRead := false}, none⟩
  else if own then
    return ⟨{l with halted := true, channels := close l.channels entry.id}, none⟩
  else
    throw .readbackIdentity

def localStep (s : State) (action : LocalAction) : Except Reject LocalEffect := do
  let l := s.worker
  match action with
  | .emptyRead =>
    require (l.cursor == s.journal.head.next && l.application.isNone &&
      !l.halted && s.journal.provider) .emptyRead
    return ⟨{l with emptyRead := true}, none⟩
  | .observeGrant grant =>
    require (l.emptyRead && grant == s.journal.writer && s.journal.quorum) .grant
    return ⟨{l with observedGrant := some grant}, none⟩
  | .promote term =>
    let grant ← find l.observedGrant .promotion
    require (l.emptyRead && !l.halted &&
      FencedTermAdmission.fenced_promotion 0 true true l.owner.isSome true l.highest ==
        some (term, 0)) .promotion
    return ⟨{l with owner := some ⟨grant, term, 0⟩, observedGrant := none, emptyRead := false}, none⟩
  | .enqueue id tx =>
    require (l.owner.isSome && !l.halted && decide (l.channels id = .unused)) .enqueue
    return ⟨{l with queue := l.queue ++ [(id, tx)], channels := OrderedJournal.update l.channels id .waiting}, none⟩
  | .appendReturned id =>
    require (l.enqueueing == some id) .providerReturn
    return ⟨{l with enqueueing := none, returned := OrderedJournal.update l.returned id true}, none⟩
  | .durable id next =>
    let entry ← find (s.entries next) .durability
    require (l.returned id && entry.id == id) .durability
    return ⟨{l with
      guardedReceipt := l.guardedReceipt.filter (· != id)
      returned := OrderedJournal.update l.returned id false}, none⟩
  | .providerFault id =>
    require (l.enqueueing == some id || l.returned id) .providerReturn
    return ⟨{l with
      enqueueing := none
      guardedReceipt := none
      returned := OrderedJournal.update l.returned id false
      halted := true
      channels := close l.channels id}, none⟩
  | .readBack identity resolved => readRecord s identity resolved true
  | .replay identity => readRecord s identity true false
  | .applyBegin =>
    let a ← find l.application .applyOrder
    require (decide (a.phase = .read)) .applyOrder
    return ⟨{l with application := some {a with phase := .applying}}, none⟩
  | .applyFinished tx =>
    let a ← find l.application .applyOrder
    require (decide (a.phase = .applying)) .applyOrder
    require (a.entry.flight.tx == tx) .appliedOutcome
    return ⟨{l with application := some {a with phase := .published}}, none⟩
  | .applyFault =>
    let a ← find l.application .applyOrder
    require (decide (a.phase = .applying)) .applyOrder
    return ⟨{l with
      halted := true
      application := none
      channels := a.recipient.elim l.channels (close l.channels)}, none⟩
  | .acknowledge id tx =>
    let a ← find l.application .acknowledgeAfterApply
    require (decide (a.phase = .published ∨ a.phase = .routed) && a.recipient == some id)
      .acknowledgeAfterApply
    require (decide (l.channels id = .waiting)) .routeOnce
    require (a.entry.flight.tx == tx) .appliedOutcome
    return ⟨{l with application := some {a with phase := .routed}, channels := close l.channels id},
      some (id, a.entry)⟩
  | .advance =>
    let a ← find l.application .applyOrder
    require (decide (a.phase = .published ∨ a.phase = .routed) &&
      (a.recipient.elim true fun id => decide (l.channels id = .spent))) .applyOrder
    return ⟨{l with application := none, cursor := l.cursor + 1}, none⟩
  | .termLoss =>
    return ⟨{l with
      owner := none
      observedGrant := none
      emptyRead := false
      queue := []
      enqueueing := none
      guardedReceipt := none
      returned := fun _ => false
      channels := retire l.channels}, none⟩
  | .crash =>
    return ⟨{channels := retire l.channels}, none⟩

private def extendsTerm (s : State) (flight : Flight) : Bool :=
  match s.entries s.journal.head.next with
  | none => flight.owner.sequence == 0
  | some last =>
    if flight.owner.term == last.flight.owner.term then
      flight.owner.grant == last.flight.owner.grant && flight.owner.sequence == last.flight.owner.sequence + 1
    else
      decide (last.flight.owner.term < flight.owner.term) && flight.owner.sequence == 0

def installChoice (s : State) (entry : Entry) : State :=
  let attempt := projectAttempt entry.append.receipt
  let receipt := projectReceipt entry.append.receipt
  {s with
    journal := {s.journal with
      head := receipt.cut
      chosen := OrderedJournal.update s.journal.chosen receipt.cut.next (some receipt.cut)
      receipts := OrderedJournal.update s.journal.receipts attempt (some receipt)}
    entries := OrderedJournal.update s.entries receipt.cut.next (some entry)
    flights := OrderedJournal.update s.flights entry.id none}

private def choose (s : State) (id position : Nat) (placement : Protocol.Placement)
    (request outcome digest expiry expiryDigest charge : Nat) (chargeClass : Protocol.ChargeClass) :
    Except Reject Entry :=
  match s.flights id with
  | none => .error .choose
  | some flight =>
    if flight.owner.grant == s.journal.writer && s.journal.successorSeal.isNone then
      let record : NativeAppend.ChosenRecord :=
        { resultPosition := position + 1
          attempt := ⟨placement, flight.owner.grant, id⟩
          requestFingerprint := request
          outcomeFingerprint := outcome
          throughNext := s.journal.head.next + 1
          throughDigest := digest
          expiryNext := expiry
          expiryDigest := expiryDigest
          chargeClass := chargeClass
          byteStart := 0
          byteEnd := charge }
      let entry : Entry := ⟨id, flight, record, position⟩
      let positionAdvances := (s.entries s.journal.head.next).elim true fun last =>
        decide (last.decisionPosition < position)
      if s.journal.quorum && (s.journal.receipts (projectAttempt entry.append.receipt)).isNone &&
          extendsTerm s flight && positionAdvances then
        .ok entry
      else .error .choose
    else .error .staleWriter

def step (s : State) (action : Action) : Except Reject Effect := do
  let j := s.journal
  let l := s.worker
  match action with
  | .worker action =>
    let result ← localStep s action
    return ⟨{s with worker := result.state}, result.acknowledgement⟩
  | .dispatch id term sequence fingerprint guarded =>
    let owner ← find l.owner .dispatch
    let (nextId, tx) ← find l.queue.head? .dispatch
    require (!l.halted && l.enqueueing.isNone && l.guardedReceipt.isNone &&
      nextId == id && owner.term == term && owner.sequence == sequence &&
      (FencedTermAdmission.next_term sequence).isSome && (s.flights id).isNone) .dispatch
    return ⟨{s with
      flights := OrderedJournal.update s.flights id (some ⟨owner, tx, fingerprint, guarded⟩)
      worker := {l with
        owner := some {owner with sequence := sequence + 1}
        queue := l.queue.drop 1
        enqueueing := some id
        guardedReceipt := if guarded then some id else none}}, none⟩
  | .choose id position placement request outcome digest expiry expiryDigest charge chargeClass =>
    let entry ← choose s id position placement request outcome digest expiry expiryDigest charge chargeClass
    return ⟨installChoice s entry, none⟩
  | .replace nonce =>
    require (j.quorum && j.successorSeal.isNone &&
      decide (OrderedJournal.ActivationCount j < s.activationSlots)) .replacement
    return ⟨{s with journal := {j with
      writer := j.writer + 1
      holdBook := OrderedJournal.hold j (j.writer + 1) nonce true :: j.holdBook}}, none⟩
  | .release hold owner =>
    require (j.quorum && hold.enrollment == owner && decide (hold ∈ j.holdBook)) .release
    return ⟨{s with journal := {j with holdBook := j.holdBook.filter (fun entry => decide (entry ≠ hold))}}, none⟩
  | .expire observed =>
    require (decide (j.observed ≤ observed)) .expiry
    return ⟨{s with journal := {j with
      observed := observed
      receipts := fun attempt => if attempt.expiry ≤ observed then none else j.receipts attempt}}, none⟩
  | .quorum available => return ⟨{s with journal := {j with quorum := available}}, none⟩
  | .provider available => return ⟨{s with journal := {j with provider := available}}, none⟩

private theorem choose_sound {id : Nat}
    (ran : choose s id position placement request outcome digest expiry expiryDigest charge cls = .ok entry) :
    OrderedJournal.Current s.journal s.journal.cohort entry.flight.owner.grant ∧
    s.journal.quorum = true ∧
    (projectAttempt entry.append.receipt).issuer = entry.flight.owner.grant ∧
    s.journal.receipts (projectAttempt entry.append.receipt) = none ∧
    (projectReceipt entry.append.receipt).cut.next = s.journal.head.next + 1 ∧
    entry.append.receipt.cut.placement = entry.append.receipt.attempt.placement ∧
    entry.append.receipt.expiresAt.placement = entry.append.receipt.attempt.placement := by
  unfold choose at ran
  split at ran <;> try contradiction
  split at ran <;> try contradiction
  rename_i auth
  dsimp only at ran
  split at ran <;> try contradiction
  rename_i allowed
  injection ran with same
  subst entry
  simp only [Bool.and_eq_true, beq_iff_eq, Option.isNone_iff_eq_none] at auth allowed
  exact ⟨⟨rfl, auth.1, auth.2⟩, allowed.1.1.1, rfl, allowed.1.1.2, rfl, rfl, rfl⟩

private theorem choose_projects {id : Nat}
    (ran : choose s id position placement request outcome digest expiry expiryDigest charge cls = .ok entry) :
    OrderedJournal.Step readers s.activationSlots s.journal (installChoice s entry).journal := by
  obtain ⟨current, quorum, issuer, fresh, next, _, _⟩ := choose_sound ran
  exact .append s.journal s.journal.cohort entry.flight.owner.grant _ _ current quorum issuer fresh next

theorem current_writer_at_choice {id : Nat}
    (ran : step s (.choose id position placement request outcome digest expiry expiryDigest charge cls) =
      .ok effect) :
    ∃ entry, (effect.state.entries (s.journal.head.next + 1)) = some entry ∧
      OrderedJournal.Current s.journal s.journal.cohort entry.flight.owner.grant := by
  cases chosen : choose s id position placement request outcome digest expiry expiryDigest charge cls with
  | error => simp [step, chosen, bind, Except.bind] at ran
  | ok entry =>
    simp only [step, chosen, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at ran
    subst effect
    obtain ⟨current, _, _, _, next, _, _⟩ := choose_sound chosen
    exact ⟨entry, by simp [installChoice, OrderedJournal.update, next], current⟩

private theorem local_result
    (ran : step s (.worker action) = .ok effect) :
    ∃ result, localStep s action = .ok result ∧ effect = ⟨{s with worker := result.state}, result.acknowledgement⟩ := by
  cases result : localStep s action <;> simp_all [step, bind, Except.bind, pure, Except.pure]

theorem step_refines
    (ran : step s action = .ok effect) :
    effect.state.activationSlots = s.activationSlots ∧
      (effect.state.journal = s.journal ∨
        OrderedJournal.Step readers s.activationSlots s.journal effect.state.journal) := by
  cases action with
  | worker action =>
    obtain ⟨_, _, rfl⟩ := local_result ran
    exact ⟨rfl, .inl rfl⟩
  | dispatch =>
    simp only [step, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst effect) | split at ran)
    exact ⟨rfl, .inl rfl⟩
  | choose id position placement request outcome digest expiry expiryDigest charge cls =>
    cases chosen : choose s id position placement request outcome digest expiry expiryDigest charge cls with
    | error => simp [step, chosen, bind, Except.bind] at ran
    | ok entry =>
      simp only [step, chosen, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at ran
      subst effect
      exact ⟨rfl, .inr (choose_projects chosen)⟩
  | replace nonce =>
    simp only [step, require_bind, pure, Except.pure] at ran
    split at ran <;> try contradiction
    rename_i ready
    injection ran with same
    subst effect
    simp only [Bool.and_eq_true, Option.isNone_iff_eq_none, decide_eq_true_eq] at ready
    exact ⟨rfl, .inr (.grant s.journal s.journal.cohort s.journal.writer nonce
      ⟨rfl, rfl, ready.1.2⟩ ready.1.1 ready.2)⟩
  | release hold owner =>
    simp only [step, require_bind, pure, Except.pure] at ran
    split at ran <;> try contradiction
    rename_i ready
    injection ran with same
    subst effect
    simp only [Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at ready
    exact ⟨rfl, .inr (.closeHold s.journal hold owner ready.1.1 ready.1.2 ready.2)⟩
  | expire observed =>
    simp only [step, require_bind, pure, Except.pure] at ran
    split at ran <;> try contradiction
    rename_i forward
    injection ran with same
    subst effect
    exact ⟨rfl, .inr (.expire _ _ (of_decide_eq_true forward))⟩
  | quorum available =>
    injection ran with same
    subst effect
    exact ⟨rfl, .inr (.quorumAvailability _ _)⟩
  | provider available =>
    injection ran with same
    subst effect
    exact ⟨rfl, .inr (.providerAvailability _ _)⟩

inductive Run : State → List Action → State → Prop where
  | nil (s : State) : Run s [] s
  | cons : step s action = .ok effect → Run effect.state rest t → Run s (action :: rest) t

theorem run_refines (path : Run s actions t) :
    OrderedJournal.Trace readers s.activationSlots s.journal t.journal := by
  induction path with
  | nil => exact .nil _
  | cons ran _ ih =>
    obtain ⟨limits, same | advances⟩ := step_refines (readers := readers) ran
    · rwa [limits, same] at ih
    · exact .cons advances (limits ▸ ih)

def Projected (j : OrderedJournal.State) (entry : Entry) : Prop :=
  j.receipts (projectAttempt entry.append.receipt) = some (projectReceipt entry.append.receipt) ∧
    entry.append.receipt.cut.placement = entry.append.receipt.attempt.placement ∧
    entry.append.receipt.expiresAt.placement = entry.append.receipt.attempt.placement

def Certificates (ledger : Composition.Ledger Voter) (s : State) : Prop :=
  ∀ next entry, s.entries next = some entry →
    ∃ cert : Protocol.Seal ledger, cert.decisions[entry.decisionPosition]? = some (.append entry.append)

def Covered (s : State) : Prop :=
  ∀ next entry, s.entries next = some entry →
    ∃ origin, Projected origin entry ∧ OrderedJournal.Trace 0 s.activationSlots origin s.journal

def Admitted (s : State) : Prop :=
  ∀ a, s.worker.application = some a → ∃ next, s.entries next = some a.entry

private theorem trace_snoc (path : OrderedJournal.Trace readers slots s t)
    (last : OrderedJournal.Step readers slots t u) :
    OrderedJournal.Trace readers slots s u := by
  induction path with
  | nil => exact .cons last (.nil _)
  | cons first _ rest => exact .cons first (rest last)

private theorem cover_through (ran : step s action = .ok effect)
    (before : Covered s) (present : s.entries next = some entry) :
    ∃ origin, Projected origin entry ∧ OrderedJournal.Trace 0 effect.state.activationSlots origin effect.state.journal := by
  obtain ⟨origin, projected, path⟩ := before next entry present
  obtain ⟨limits, same | advances⟩ := step_refines (readers := 0) ran
  · exact ⟨origin, projected, by simpa only [limits, same] using path⟩
  · exact ⟨origin, projected, limits.symm ▸ trace_snoc path advances⟩

private theorem choice_covers {id : Nat}
    (ran : step s (.choose id position placement request outcome digest expiry expiryDigest charge cls) =
      .ok ⟨installChoice s entry, none⟩)
    (chosen : choose s id position placement request outcome digest expiry expiryDigest charge cls = .ok entry)
    (before : Covered s) : Covered (installChoice s entry) := by
  obtain ⟨_, _, _, _, _, placement, expiryPlacement⟩ := choose_sound chosen
  intro next actual present
  by_cases same : next = (projectReceipt entry.append.receipt).cut.next
  · have equal : entry = actual := by simpa [installChoice, OrderedJournal.update, same] using present
    subst actual
    exact ⟨_, ⟨by simp [installChoice, OrderedJournal.update], placement, expiryPlacement⟩, .nil _⟩
  · exact cover_through ran before (by simpa [installChoice, OrderedJournal.update, same] using present)

private theorem readRecord_admitted (ran : readRecord s identity resolved own = .ok result)
    (before : Admitted s) (active : result.state.application = some a) :
    ∃ next, s.entries next = some a.entry := by
  simp only [readRecord, find_bind, require_bind, pure, Except.pure] at ran
  repeat' (first | contradiction | (injection ran with same; subst result) | split at ran)
  · simp only [Option.some.injEq] at active
    subst a
    exact ⟨_, by assumption⟩
  · exact before _ active

private theorem worker_admitted (ran : localStep s action = .ok result)
    (before : Admitted s) (active : result.state.application = some a) :
    ∃ next, s.entries next = some a.entry := by
  cases action with
  | readBack => exact readRecord_admitted ran before active
  | replay => exact readRecord_admitted ran before active
  | _ =>
    simp only [localStep, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst result) | split at ran)
    all_goals
      first
      | contradiction
      | exact before _ active
      | (simp only [Option.some.injEq] at active; subst a
         have origin := before _ (by assumption)
         exact origin)

private theorem choose_preserves_entries {id : Nat}
    (ran : choose s id position placement request outcome digest expiry expiryDigest charge cls = .ok entry)
    (within : ∀ next, (s.entries next).isSome → next ≤ s.journal.head.next)
    (present : s.entries next = some old) :
    (installChoice s entry).entries next = some old := by
  have bound := within next (by simp [present])
  have fresh := (choose_sound ran).2.2.2.2.1
  have different : next ≠ (projectReceipt entry.append.receipt).cut.next := by omega
  simpa [installChoice, OrderedJournal.update, different] using present

def Within (s : State) : Prop :=
  ∀ next, (s.entries next).isSome → next ≤ s.journal.head.next

theorem preserves_projection (ran : step s action = .ok effect)
    (within : Within s) (covered : Covered s) (admitted : Admitted s) :
    Within effect.state ∧ Covered effect.state ∧ Admitted effect.state := by
  cases action with
  | worker action =>
    obtain ⟨result, transition, rfl⟩ := local_result ran
    exact ⟨within, covered, fun _ active => worker_admitted (s := s) (result := result) transition admitted active⟩
  | dispatch =>
    simp only [step, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst effect) | split at ran)
    exact ⟨within, covered, admitted⟩
  | choose id position placement request outcome digest expiry expiryDigest charge cls =>
    cases chosen : choose s id position placement request outcome digest expiry expiryDigest charge cls with
    | error => simp [step, chosen, bind, Except.bind] at ran
    | ok entry =>
      simp only [step, chosen, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at ran
      subst effect
      have fresh := (choose_sound chosen).2.2.2.2.1
      refine ⟨?_, choice_covers (by simp [step, chosen, bind, Except.bind, pure, Except.pure]) chosen covered, ?_⟩
      · intro next present
        change next ≤ (projectReceipt entry.append.receipt).cut.next
        rw [fresh]
        by_cases same : next = (projectReceipt entry.append.receipt).cut.next
        · omega
        · have prior := within next (by simpa [installChoice, OrderedJournal.update, same] using present)
          omega
      · intro a active
        obtain ⟨next, present⟩ := admitted a active
        exact ⟨next, choose_preserves_entries chosen within present⟩
  | replace | release | expire =>
    have transition := ran
    simp only [step, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst effect) | split at ran)
    all_goals exact ⟨within, fun _ _ present => cover_through (s := s) transition covered present, admitted⟩
  | quorum | provider =>
    have transition := ran
    injection ran with same
    subst effect
    exact ⟨within, fun _ _ present => cover_through (s := s) transition covered present, admitted⟩

theorem run_projection (path : Run s actions t)
    (within : Within s) (covered : Covered s) (admitted : Admitted s) :
    Within t ∧ Covered t ∧ Admitted t := by
  induction path with
  | nil => exact ⟨within, covered, admitted⟩
  | cons ran _ ih =>
    obtain ⟨nextWithin, nextCovered, nextAdmitted⟩ := preserves_projection ran within covered admitted
    exact ih nextWithin nextCovered nextAdmitted

theorem seed_projection (path : Run {} actions s) :
    Within s ∧ Covered s ∧ Admitted s :=
  run_projection path (by intro _ present; contradiction) (by intro _ _ present; contradiction)
    (by intro _ active; contradiction)

private theorem readRecord_silent (ran : readRecord s identity resolved own = .ok result) :
    result.acknowledgement = none := by
  simp only [readRecord, find_bind, require_bind, pure, Except.pure] at ran
  repeat' (first | contradiction | (injection ran with same; subst result) | split at ran)
  all_goals rfl

theorem acknowledge_after_apply {id : Nat}
    (ran : localStep s (.acknowledge id tx) = .ok result) :
    ∃ a, s.worker.application = some a ∧
      (a.phase = .published ∨ a.phase = .routed) ∧ a.recipient = some id ∧
      s.worker.channels id = .waiting ∧ result.state.channels id = .spent ∧
      nativeAcknowledgement a.entry.flight.identity
        (.readBack a.observed a.resolved () (some (.ok a.entry.flight.tx))) = some (.ok tx) ∧
      result.acknowledgement = some (id, a.entry) := by
  simp only [localStep, find_bind, require_bind, pure, Except.pure] at ran
  split at ran <;> try contradiction
  rename_i a selected
  split at ran <;> try contradiction
  rename_i published
  split at ran <;> try contradiction
  rename_i waiting
  split at ran <;> try contradiction
  rename_i outcome
  injection ran with same
  subst result
  simp only [Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at published waiting outcome
  refine ⟨a, selected, published.1, published.2, waiting, by simp [close, OrderedJournal.update], ?_, rfl⟩
  exact (native_acknowledgement_refines_applied_record _ () _).2 ⟨a.exact, by simp [outcome]⟩

private theorem local_routes {id : Nat}
    (ran : localStep s action = .ok result) (sent : result.acknowledgement = some (id, entry)) :
    ∃ tx, action = .acknowledge id tx := by
  cases action with
  | readBack => simp [readRecord_silent ran] at sent
  | replay => simp [readRecord_silent ran] at sent
  | acknowledge recipient tx =>
    obtain ⟨a, _, _, _, _, _, _, outcome⟩ := acknowledge_after_apply ran
    simp only [outcome, Option.some.injEq, Prod.mk.injEq] at sent
    exact ⟨tx, by rw [sent.1]⟩
  | _ =>
    simp only [localStep, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst result) | split at ran)
    all_goals contradiction

theorem routes_from_application {id : Nat} (ran : step s action = .ok effect)
    (sent : effect.acknowledgement = some (id, entry)) :
    ∃ a, s.worker.application = some a ∧ a.entry = entry ∧
      s.worker.channels id = .waiting ∧ effect.state.worker.channels id = .spent := by
  cases action with
  | worker action =>
    obtain ⟨result, transition, rfl⟩ := local_result ran
    obtain ⟨tx, rfl⟩ := local_routes transition sent
    obtain ⟨a, active, _, _, waiting, spent, _, outcome⟩ := acknowledge_after_apply transition
    have exactEntry : a.entry = entry := by simpa [outcome] using sent
    exact ⟨a, active, exactEntry, waiting, spent⟩
  | dispatch | replace | release | expire =>
    simp only [step, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst effect) | split at ran)
    all_goals contradiction
  | choose id position placement request outcome digest expiry expiryDigest charge cls =>
    cases chosen : choose s id position placement request outcome digest expiry expiryDigest charge cls with
    | error => simp [step, chosen, bind, Except.bind] at ran
    | ok entry =>
      simp only [step, chosen, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at ran
      subst effect
      contradiction
  | quorum | provider =>
    injection ran with same
    subst effect
    contradiction

private theorem close_preserves_spent {id : Nat} {channels : Nat → Channel}
    (spent : channels id = .spent) (other : Nat) : close channels other id = .spent := by
  simp [close, OrderedJournal.update, spent]

private theorem readRecord_preserves_spent {id : Nat}
    (ran : readRecord s identity resolved own = .ok result) (spent : s.worker.channels id = .spent) :
    result.state.channels id = .spent := by
  simp only [readRecord, find_bind, require_bind, pure, Except.pure] at ran
  repeat' (first | contradiction | (injection ran with same; subst result) | split at ran)
  all_goals first | exact spent | exact close_preserves_spent spent _

private theorem local_preserves_spent {id : Nat}
    (ran : localStep s action = .ok result) (spent : s.worker.channels id = .spent) :
    result.state.channels id = .spent := by
  cases action with
  | readBack => exact readRecord_preserves_spent ran spent
  | replay => exact readRecord_preserves_spent ran spent
  | enqueue other tx =>
    simp only [localStep, require_bind, pure, Except.pure] at ran
    split at ran <;> try contradiction
    rename_i eligible
    injection ran with same
    subst result
    simp only [Bool.and_eq_true, decide_eq_true_eq] at eligible
    have distinct : id ≠ other := by
      intro equal
      rw [← equal, spent] at eligible
      exact Channel.noConfusion eligible.2
    simp [OrderedJournal.update, distinct, spent]
  | applyFault =>
    simp only [localStep, find_bind, require_bind, pure, Except.pure] at ran
    split at ran <;> try contradiction
    rename_i active _
    split at ran <;> try contradiction
    injection ran with same
    subst result
    cases active.recipient with
    | none => exact spent
    | some recipient => exact close_preserves_spent spent recipient
  | _ =>
    simp only [localStep, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst result) | split at ran)
    all_goals first | exact spent | exact close_preserves_spent spent _ | simp [retire, spent]

theorem step_preserves_spent {id : Nat} (ran : step s action = .ok effect)
    (spent : s.worker.channels id = .spent) : effect.state.worker.channels id = .spent := by
  cases action with
  | worker action =>
    obtain ⟨result, transition, rfl⟩ := local_result ran
    exact local_preserves_spent (s := s) (result := result) transition spent
  | dispatch | replace | release | expire =>
    simp only [step, find_bind, require_bind, pure, Except.pure] at ran
    repeat' (first | contradiction | (injection ran with same; subst effect) | split at ran)
    exact spent
  | choose id position placement request outcome digest expiry expiryDigest charge cls =>
    cases chosen : choose s id position placement request outcome digest expiry expiryDigest charge cls with
    | error => simp [step, chosen, bind, Except.bind] at ran
    | ok entry =>
      simp only [step, chosen, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at ran
      subst effect
      exact spent
  | quorum | provider =>
    injection ran with same
    subst effect
    exact spent

theorem run_preserves_spent {id : Nat} (path : Run s actions t) (spent : s.worker.channels id = .spent) :
    t.worker.channels id = .spent := by
  induction path with
  | nil => exact spent
  | cons ran _ ih => exact ih (step_preserves_spent ran spent)

theorem route_once {id : Nat} (first : step s action = .ok sent) (firstRoute : sent.acknowledgement = some (id, entry))
    (later : Run sent.state actions t) (second : step t action' = .ok effect) :
    effect.acknowledgement ≠ some (id, entry') := by
  intro repeated
  obtain ⟨_, _, _, _, spent⟩ := routes_from_application first firstRoute
  obtain ⟨_, _, _, waiting, _⟩ := routes_from_application second repeated
  rw [run_preserves_spent later spent] at waiting
  contradiction

def Entry.frame (entry : Entry) (eventFor : Nat → Tx) : NativeAppend.Frame Tx :=
  ⟨entry.flight.owner.term, some entry.flight.owner.sequence, some (.resolvedTx (eventFor entry.flight.tx))⟩

theorem acknowledgement_projects_for_PX1 {id : Nat} (path : Run {} actions s)
    (ran : step s action = .ok effect) (sent : effect.acknowledgement = some (id, entry))
    (certificates : Certificates ledger s)
    (opened : source.frameFor entry.append = some (entry.frame eventFor)) :
    ∃ origin, ∃ initial : Composition.RecordedAppend ledger origin entry.decisionPosition,
      OrderedJournal.Trace 0 s.activationSlots origin s.journal ∧ initial.record = entry.append ∧
        Nonempty (NativeAppend.Binding source initial.record (eventFor entry.flight.tx)) := by
  obtain ⟨_, covered, admitted⟩ := seed_projection path
  obtain ⟨a, active, rfl, _, _⟩ := routes_from_application ran sent
  obtain ⟨next, present⟩ := admitted a active
  obtain ⟨origin, ⟨receipt, placement, expiryPlacement⟩, journalPath⟩ := covered next a.entry present
  obtain ⟨cert, selected⟩ := certificates next a.entry present
  exact ⟨origin, ⟨cert, a.entry.append, selected, placement, expiryPlacement, receipt⟩, journalPath, rfl,
    NativeAppend.resolved_frame_has_binding opened⟩

#print axioms current_writer_at_choice
#print axioms step_refines
#print axioms run_refines
#print axioms acknowledge_after_apply
#print axioms route_once
#print axioms acknowledgement_projects_for_PX1

end RTDB.Recovery.ReplicationDriver
