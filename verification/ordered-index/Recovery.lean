import Temporal
import Buckets
import DistinctPage
import OrderedRead

set_option autoImplicit false

namespace RTDB.OrderedIndex

variable {K : Type}

/-- Meaning of a decoded, complete checkpoint. The source is the exact ordered
prefix. Physical pages and their references are modeled separately. -/
structure Checkpoint (K : Type) where
  source : List (Event K)
  root : Nat → List (Span K)
  agrees : ∀ systemAt validAt, seekSpan validAt (root systemAt) = tableAt source systemAt validAt

def extend (checkpoint : Checkpoint K) (tail : List (Event K)) : Checkpoint K where
  source := checkpoint.source ++ tail
  root := fun systemAt => tail.foldl (edits systemAt) (checkpoint.root systemAt)
  agrees := by
    intro systemAt validAt
    rw [replay_refines_table_from, checkpoint.agrees]
    simp [tableAt, List.foldl_append]

def build (events : List (Event K)) : Checkpoint K where
  source := events
  root := fun systemAt => events.foldl (edits systemAt) []
  agrees := replay_refines_table events

theorem extend_preserves_basis (checkpoint : Checkpoint K) (tail : List (Event K))
    (systemAt : Nat) (validAt : Int) :
    seekSpan validAt ((extend checkpoint tail).root systemAt) =
      seekSpan validAt ((build (checkpoint.source ++ tail)).root systemAt) := by
  rw [(extend checkpoint tail).agrees, (build (checkpoint.source ++ tail)).agrees]
  rfl

/-- A storage reader offers a checkpoint only after decoding a complete
authoritative prefix, and retains its continuation from the same capture. -/
inductive Coverage (capture : List (Event K)) where
  | unavailable
  | usable (before : Checkpoint K) (tail : List (Event K))
      (exact : before.source ++ tail = capture)

def serve (capture : List (Event K)) (coverage : Coverage capture)
    (systemAt : Nat) (validAt : Int) : Option K :=
  match coverage with
  | .unavailable => tableAt capture systemAt validAt
  | .usable before tail _ => seekSpan validAt ((extend before tail).root systemAt)

theorem serve_exact (capture : List (Event K)) (coverage : Coverage capture)
    (systemAt : Nat) (validAt : Int) :
    serve capture coverage systemAt validAt = tableAt capture systemAt validAt := by
  cases coverage with
  | unavailable => rfl
  | usable before tail exactPrefix =>
    rw [serve, (extend before tail).agrees]
    exact congrArg (fun events => tableAt events systemAt validAt) exactPrefix

/-- The result is independent of physical compaction, abandoned uploads,
and loss of a checkpoint if coverage uses the captured log. -/
theorem serving_independent (capture : List (Event K)) (left right : Coverage capture)
    (systemAt : Nat) (validAt : Int) :
    serve capture left systemAt validAt = serve capture right systemAt validAt := by
  rw [serve_exact, serve_exact]


/-- An ordered route and an authoritative scan may traverse different duplicate
runs. Once their authorized candidate sets agree, exact replay preserves the
page, its lookahead, and the ordered keys inspected to register the read. -/
theorem ordered_route_preserves_page_and_witness
    {V : Type} (capture : Nat → List (Event K))
    (coverage : (key : Nat) → Coverage (capture key))
    (project : Nat → K → Option V) (native scanned : List Nat)
    (nativeSorted : native.Pairwise (· ≤ ·)) (scannedSorted : scanned.Pairwise (· ≤ ·))
    (sameKeys : ∀ key, key ∈ native ↔ key ∈ scanned)
    (systemAt : Nat) (validAt : Int) (limit : Nat) :
    let nativeKeys := distinctRuns none native
    let scanKeys := distinctRuns none scanned
    let index key := (serve (capture key) (coverage key) systemAt validAt).bind (project key)
    let scan key := (tableAt (capture key) systemAt validAt).bind (project key)
    observation (nativeKeys.filterMap index) limit =
      observation (scanKeys.filterMap scan) limit
      ∧ inspected index (limit + 1) nativeKeys = inspected scan (limit + 1) scanKeys := by
  dsimp
  have keys := distinct_runs_refine_membership native scanned nativeSorted scannedSorted sameKeys
  have rows : (fun key => (serve (capture key) (coverage key) systemAt validAt).bind (project key)) =
      (fun key => (tableAt (capture key) systemAt validAt).bind (project key)) := by
    funext key
    rw [serve_exact]
  rw [keys, rows]
  exact ⟨rfl, rfl⟩

def keysAtBuckets (buckets : Span K → List Bucket) (point : Nat) (spans : List (Span K)) : List K :=
  spans.flatMap fun span => List.replicate (hits point (buckets span)) span.key

private theorem keysAtBuckets_refines_spans (buckets : Span K → List Bucket)
    (point : Nat) (validAt : Int) (spans : List (Span K))
    (exact : ∀ span ∈ spans, hits point (buckets span) =
      if span.valid.contains validAt then 1 else 0) :
    let keys := keysAtBuckets buckets point spans
    keys.head? = seekSpan validAt spans ∧ keys.length = hitsSpans validAt spans := by
  induction spans with
  | nil => simp [keysAtBuckets, seekSpan, hitsSpans]
  | cons span rest ih =>
    have head := exact span (by simp)
    obtain ⟨restHead, restLength⟩ := ih (fun row member => exact row (by simp [member]))
    change (List.replicate (hits point (buckets span)) span.key ++
      keysAtBuckets buckets point rest).head? = seekSpan validAt (span :: rest) ∧
      (List.replicate (hits point (buckets span)) span.key ++
        keysAtBuckets buckets point rest).length = hitsSpans validAt (span :: rest)
    by_cases contains : span.valid.contains validAt <;>
      simp [hitsSpans, seekSpan, head, contains, restHead, restLength, Nat.add_comm]

/-- Exact tiling and temporal projection give one matching indexed tuple per
visible IID, with the same point value as the original captured events. -/
theorem bucket_postings_refine_table (buckets : Span K → List Bucket)
    (events : List (Event K)) (systemAt : Nat) (validAt : Int) (point : Nat)
    (ordered : ∀ event ∈ events, event.valid.lower ≤ event.valid.upper)
    (exact : ∀ span ∈ events.foldl (edits systemAt) [], hits point (buckets span) =
      if span.valid.contains validAt then 1 else 0) :
    let keys := keysAtBuckets buckets point (events.foldl (edits systemAt) [])
    keys.head? = tableAt events systemAt validAt ∧ keys.length ≤ 1 := by
  obtain ⟨value, count⟩ := keysAtBuckets_refines_spans buckets point validAt _ exact
  exact ⟨value.trans (replay_refines_table events systemAt validAt),
    count ▸ replay_keeps_one events systemAt validAt [] (by simp [hitsSpans]) ordered⟩

def weight {A : Type} (rows : A → Nat) : List A → Nat
  | [] => 0
  | block :: blocks => rows block + weight rows blocks

theorem sublist_weight_le {A : Type} (rows : A → Nat) {actual expected : List A}
    (identity : actual.Sublist expected) :
    weight rows actual ≤ weight rows expected := by
  induction identity with
  | slnil => simp [weight]
  | cons block _ ih => simp only [weight]; omega
  | cons_cons block _ ih => simp only [weight]; omega

/-- A count closes coverage after the input loader establishes ordered inclusion
and every omitted block would account for a positive number of events. -/
theorem count_completes_ordered_inclusion {A : Type} (rows : A → Nat)
    {actual expected : List A} (identity : actual.Sublist expected)
    (nonempty : ∀ block ∈ expected, 0 < rows block)
    (count : weight rows actual = weight rows expected) :
    actual = expected := by
  induction identity with
  | slnil => rfl
  | cons block sub _ =>
    have hl := sublist_weight_le rows sub
    have ht := nonempty block (by simp)
    simp only [weight] at count
    omega
  | cons_cons block _ ih =>
    apply congrArg (List.cons block)
    apply ih
    · exact fun tailBlock h => nonempty tailBlock (by simp [h])
    · simp only [weight] at count
      omega

theorem count_alone_loses_a_retraction :
    let put : Event Nat := ⟨1, ⟨0, 1⟩, .put 1⟩
    let del : Event Nat := ⟨2, ⟨0, 1⟩, .delete⟩
    [put, put].length = [put, del].length ∧
      tableAt [put, put] 2 0 ≠ tableAt [put, del] 2 0 := by
  decide

/-- Replay can exclude every event strictly before the latest erase. Events
at the erase's system time keep their original operation order. -/
theorem scrubbed_replay_preserves_points (front sameRevision tail : List (Event K))
    (cut : Nat) (valid : Interval) (systemAt : Nat) (validAt : Int)
    (earlier : ∀ event ∈ front, event.system < cut)
    (same : ∀ event ∈ sameRevision, event.system = cut)
    (later : ∀ event ∈ tail, cut ≤ event.system) :
    let events := (front ++ sameRevision) ++ ⟨cut, valid, .erase⟩ :: tail
    tableAt (events.filter (fun event => decide (cut ≤ event.system))) systemAt validAt =
      tableAt events systemAt validAt := by
  have oldEmpty : front.filter (fun event => decide (cut ≤ event.system)) = [] := by
    induction front with
    | nil => rfl
    | cons event rest ih =>
      have old := Nat.not_le_of_gt (earlier event (by simp))
      simp [old, ih (fun row hr => earlier row (by simp [hr]))]
  have tailKept : tail.filter (fun event => decide (cut ≤ event.system)) = tail := by
    induction tail with
    | nil => rfl
    | cons event rest ih =>
      simp [later event (by simp), ih (fun row hr => later row (by simp [hr]))]
  have sameKept : sameRevision.filter (fun event => decide (cut ≤ event.system)) = sameRevision := by
    induction sameRevision with
    | nil => rfl
    | cons event rest ih =>
      simp [same event (by simp), ih (fun row hr => same row (by simp [hr]))]
  simp [List.filter_append, oldEmpty, sameKept, tailKept, -List.append_assoc, erase_cuts_history]

structure AdoptedInput (File : Type) where
  block : Nat
  file : File
  bytes : Nat
  current : Bool

def coverPrimary {File : Type} (through : Nat) (input : AdoptedInput File) : AdoptedInput File :=
  if input.block ≤ through then { input with current := false } else input

def exactInputs {File : Type} (after through : Nat) (catalog : List (AdoptedInput File)) :
    List (Nat × File × Nat) :=
  (catalog.filter fun input => decide (after < input.block ∧ input.block ≤ through)).map
    fun input => (input.block, input.file, input.bytes)

/-- Primary compaction changes current scan eligibility while preserving
every adopted source identity, byte length and position for exact replay. -/
theorem compact_primary_preserves_index_inputs {File : Type} (after through compacted : Nat)
    (catalog : List (AdoptedInput File)) :
    exactInputs after through (catalog.map (coverPrimary compacted)) =
      exactInputs after through catalog := by
  induction catalog with
  | nil => rfl
  | cons input catalog ih =>
    have tail := ih
    simp [exactInputs] at tail
    simp only [exactInputs, List.map_cons, List.filter_cons]
    by_cases covered : input.block ≤ compacted <;>
      by_cases inside : after < input.block ∧ input.block ≤ through <;>
      simp [coverPrimary, covered, inside, tail]

end RTDB.OrderedIndex
