import Std

namespace RTDB.Recovery.OrderedCapacity

/- One exact recovery interval, with the ordinary write window exhausted. The
maintenance costs include the physical framing of any activation record and of
both the block boundary and upload records. `trim` receives the byte counts
covered by its checkpoint from the ordered journal. The state here abstracts
from their identities, which the ordered journal model tracks separately. -/

structure Budget where
  data : Nat
  reserve : Nat

structure Cost where
  activation : Nat
  boundary : Nat
  replacement : Nat
  uploaded : Nat

def Cost.total (cost : Cost) : Nat :=
  cost.activation + cost.boundary + cost.replacement + cost.uploaded

inductive Phase where
  | idle | activated | bounded | uploaded | published
  deriving DecidableEq

structure State where
  data : Nat
  unspent : Nat
  maintenance : Nat
  phase : Phase
  quorum : Bool
  worker : Bool
  provider : Bool
  held : Bool

def Within (budget : Budget) (s : State) : Prop :=
  s.data + s.unspent ≤ budget.data ∧ s.maintenance ≤ budget.reserve

def activate (s : State) (cost : Cost) : State :=
  {s with maintenance := s.maintenance + cost.activation, phase := .activated}

def bound (s : State) (cost : Cost) : State :=
  {s with maintenance := s.maintenance + cost.boundary, phase := .bounded}

def replaceWorker (s : State) (cost : Cost) : State :=
  {s with worker := true, maintenance := s.maintenance + cost.replacement}

def upload (s : State) (cost : Cost) : State :=
  {s with maintenance := s.maintenance + cost.uploaded, phase := .uploaded}

def publish (s : State) : State := {s with phase := .published}

def trim (s : State) (data maintenance : Nat) : State :=
  {s with data := s.data - data, maintenance := s.maintenance - maintenance, phase := .idle}

inductive Step (budget : Budget) (cost : Cost) : State → State → Prop where
  | ordinary (s : State) (bytes : Nat) (quorum : s.quorum = true)
      (worker : s.worker = true) (funded : bytes ≤ s.unspent) :
      Step budget cost s {s with data := s.data + bytes, unspent := s.unspent - bytes}
  | fund (s : State) (bytes : Nat) (quorum : s.quorum = true)
      (worker : s.worker = true) (funded : s.data + bytes ≤ budget.data) :
      Step budget cost s {s with unspent := bytes}
  | claim (s : State) (idle : s.phase = .idle) (quorum : s.quorum = true)
      (worker : s.worker = true) (funded : s.maintenance + cost.activation ≤ budget.reserve) :
      Step budget cost s (activate s cost)
  | boundary (s : State) (active : s.phase = .activated) (quorum : s.quorum = true)
      (worker : s.worker = true) (funded : s.maintenance + cost.boundary ≤ budget.reserve) :
      Step budget cost s (bound s cost)
  | replace (s : State) (bounded : s.phase = .bounded) (quorum : s.quorum = true)
      (unavailable : s.worker = false) (funded : s.maintenance + cost.replacement ≤ budget.reserve) :
      Step budget cost s (replaceWorker s cost)
  | upload (s : State) (bounded : s.phase = .bounded) (quorum : s.quorum = true)
      (worker : s.worker = true) (provider : s.provider = true)
      (funded : s.maintenance + cost.uploaded ≤ budget.reserve) :
      Step budget cost s (upload s cost)
  | publish (s : State) (uploaded : s.phase = .uploaded) (quorum : s.quorum = true)
      (worker : s.worker = true) (provider : s.provider = true) :
      Step budget cost s (publish s)
  | trim (s : State) (data maintenance : Nat) (published : s.phase = .published)
      (quorum : s.quorum = true) (worker : s.worker = true) (released : s.held = false)
      (coversData : data ≤ s.data) (coversMaintenance : maintenance ≤ s.maintenance) :
      Step budget cost s (trim s data maintenance)
  | lostReply (s : State) : Step budget cost s s
  | workerAvailability (s : State) (available : Bool) :
      Step budget cost s {s with worker := available}
  | quorumAvailability (s : State) (available : Bool) :
      Step budget cost s {s with quorum := available}
  | providerAvailability (s : State) (available : Bool) :
      Step budget cost s {s with provider := available}

inductive Trace (budget : Budget) (cost : Cost) : State → State → Prop where
  | nil (s : State) : Trace budget cost s s
  | cons : Step budget cost s t → Trace budget cost t u → Trace budget cost s u

theorem step_preserves_budget (before : Within budget s) (step : Step budget cost s t) :
    Within budget t := by
  rcases before with ⟨data, maintenance⟩
  cases step <;> simp_all [Within, activate, bound, replaceWorker, upload, publish, trim] <;> omega

theorem trace_preserves_budget (before : Within budget s) (path : Trace budget cost s t) :
    Within budget t := by
  revert before
  induction path with
  | nil => exact id
  | cons step _ rest =>
      intro before
      exact rest (step_preserves_budget before step)

theorem paid_physical_capacity (sizes : budget.data + budget.reserve ≤ paid)
    (before : Within budget s) (path : Trace budget cost s t) : t.data + t.maintenance ≤ paid := by
  obtain ⟨data, maintenance⟩ := trace_preserves_budget before path
  omega

/-- Existence of a finite recovery schedule. Availability and release are
premises for the interval on this schedule, rather than claims of fairness. A
worker may lose the boundary reply and disappear during that interval. The
checkpoint covers the initial term and boundary records. The later term and
upload records remain charged until a subsequent checkpoint. -/
theorem recovery_at_full_data_window (s : State) (budget : Budget) (cost : Cost)
    (idle : s.phase = .idle) (quorum : s.quorum = true) (worker : s.worker = true)
    (provider : s.provider = true) (released : s.held = false)
    (full : s.data = budget.data) (nonempty : 0 < s.data)
    (fits : s.maintenance + cost.total ≤ budget.reserve) :
    ∃ t, Trace budget cost s t ∧ t.data < budget.data ∧
      t.unspent = budget.data ∧ t.maintenance = cost.replacement + cost.uploaded := by
  let a := activate s cost
  let b := bound a cost
  let down := {b with worker := false}
  let up := replaceWorker down cost
  let c := upload up cost
  let p := publish c
  let tail := cost.replacement + cost.uploaded
  let reclaimed := p.maintenance - tail
  let t := trim p p.data reclaimed
  refine ⟨{t with unspent := budget.data}, ?_, ?_, rfl, ?_⟩
  · apply Trace.cons (Step.claim s idle quorum worker (by unfold Cost.total at fits; omega))
    apply Trace.cons (Step.boundary a rfl quorum worker (by
      dsimp [a, activate]
      unfold Cost.total at fits
      omega))
    apply Trace.cons (Step.lostReply b)
    apply Trace.cons (Step.workerAvailability b false)
    apply Trace.cons (Step.replace down rfl quorum rfl (by
      dsimp [down, b, a, bound, activate]
      unfold Cost.total at fits
      omega))
    apply Trace.cons (Step.upload up rfl quorum rfl provider (by
      dsimp [up, down, b, a, replaceWorker, bound, activate]
      unfold Cost.total at fits
      omega))
    apply Trace.cons (Step.publish c rfl quorum rfl provider)
    apply Trace.cons (Step.trim p p.data reclaimed rfl quorum rfl released
      (Nat.le_refl _) (Nat.sub_le _ _))
    exact Trace.cons (Step.fund t budget.data quorum rfl (by simp [t, trim])) (Trace.nil _)
  · simpa [t, trim] using (show 0 < budget.data by omega)
  · change p.maintenance - (p.maintenance - tail) = tail
    have retained : tail ≤ p.maintenance := by
      change cost.replacement + cost.uploaded ≤
        s.maintenance + cost.activation + cost.boundary + cost.replacement + cost.uploaded
      omega
    omega

/-- Filling the single data window with no separate reserve can leave an idle
recovery interval unable to start, even with its worker and provider available. -/
def exhausted : State := ⟨1, 0, 0, .idle, true, true, true, false⟩

theorem ordinary_tail_cannot_fund_its_own_activation (s : State)
    (step : Step ⟨1, 0⟩ ⟨1, 1, 1, 1⟩ exhausted s) : s.phase = .idle ∧ s.data = 1 := by
  cases step <;> simp_all [exhausted] <;> omega

/-- A pin on the covered data blocks trim even when the checkpoint is published.
A caller cannot turn logical publication into physical reuse by retrying trim. -/
theorem pinned_publication_cannot_trim (s t : State) (held : s.held = true)
    (published : s.phase = .published) (step : Step budget cost s t) : t.data ≥ s.data := by
  cases step <;> simp_all <;> omega

#print axioms paid_physical_capacity
#print axioms recovery_at_full_data_window
#print axioms ordinary_tail_cannot_fund_its_own_activation
#print axioms pinned_publication_cannot_trim

end RTDB.Recovery.OrderedCapacity
