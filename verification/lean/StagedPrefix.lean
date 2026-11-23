import History

namespace RTDB.StagedPrefix

structure Stage where
  predecessor : Option Nat
  transaction : Nat
  block : Nat
  rows : Batch

structure Snapshot where
  head : Option Nat
  block : Nat
  history : History

def overlay (snapshot : Snapshot) (stage : Stage) : Option Snapshot :=
  if stage.predecessor = snapshot.head ∧ stage.block = snapshot.block then
    some ⟨some stage.transaction, snapshot.block, stage.rows :: snapshot.history⟩
  else none

def overlayPrefix (snapshot : Snapshot) : List Stage → Option Snapshot
  | [] => some snapshot
  | next :: rest => (overlay snapshot next).bind (fun advanced => overlayPrefix advanced rest)

def linked (block : Nat) (predecessor : Option Nat) : List Stage → Prop
  | [] => True
  | next :: rest => next.predecessor = predecessor ∧ next.block = block ∧
    linked block (some next.transaction) rest

theorem success_admits_exactly_the_linked_stage_chain (durable : Snapshot) (queued : List Stage) :
    (overlayPrefix durable queued).isSome = true ↔ linked durable.block durable.head queued := by
  induction queued generalizing durable with
  | nil => simp [overlayPrefix, linked]
  | cons next rest previous =>
      by_cases correct : next.predecessor = durable.head ∧ next.block = durable.block
      · simp [overlayPrefix, overlay, correct, linked]
        exact previous _
      · have invalid : ¬(next.predecessor = durable.head ∧ next.block = durable.block ∧
            linked durable.block (some next.transaction) rest) := by
          intro impossible
          exact correct ⟨impossible.1, impossible.2.1⟩
        simp [overlayPrefix, overlay, correct, linked, invalid]

theorem resolver_reads_the_newest_queued_batches_then_durable
    (durable merged : Snapshot) (queued : List Stage)
    (admitted : overlayPrefix durable queued = some merged) :
    merged.history = queued.reverse.map Stage.rows ++ durable.history := by
  induction queued generalizing durable with
  | nil =>
      simp [overlayPrefix] at admitted
      subst merged
      simp
  | cons first rest previous =>
      unfold overlayPrefix overlay at admitted
      split at admitted
      · simp only [Option.bind_some] at admitted
        rw [previous _ admitted]
        simp [List.reverse_cons, List.map_append]
      · contradiction

theorem moving_the_head_to_durable_preserves_the_resolver_view
    (durable published : Snapshot) (next : Stage) (pending : List Stage)
    (applied : overlay durable next = some published) :
    overlayPrefix durable (next :: pending) = overlayPrefix published pending := by
  simp [overlayPrefix, applied]

theorem publishing_preserves_the_resolvers_bitemporal_answer
    (durable published : Snapshot) (next : Stage) (pending : List Stage)
    (applied : overlay durable next = some published)
    (system key : Nat) (valid : Int) :
    ((overlayPrefix durable (next :: pending)).map
      (fun view => point (events view.history) system key valid)) =
    ((overlayPrefix published pending).map
      (fun view => point (events view.history) system key valid)) := by
  rw [moving_the_head_to_durable_preserves_the_resolver_view _ _ _ _ applied]

theorem empty_visible_batch_still_advances_the_head (durable : Snapshot) (abort : Stage)
    (preceding : abort.predecessor = durable.head ∧ abort.block = durable.block)
    (empty : abort.rows = []) :
    overlay durable abort = some ⟨some abort.transaction, durable.block, [] :: durable.history⟩ := by
  simp [overlay, preceding, empty]

theorem reversed_predecessor_is_refused
    (durable : Snapshot) (first second : Stage) (remaining : List Stage)
    (followsFirst : second.predecessor = some first.transaction)
    (different : some first.transaction ≠ durable.head) :
    overlayPrefix durable (second :: first :: remaining) = none := by
  simp [overlayPrefix, overlay, followsFirst, different]

structure Pipeline where
  applied : Snapshot
  pending : List Stage

def stage (pipeline : Pipeline) (next : Stage) : Option Pipeline :=
  match overlayPrefix pipeline.applied (pipeline.pending ++ [next]) with
  | none => none
  | some _ => some ⟨pipeline.applied, pipeline.pending ++ [next]⟩

def discard (pipeline : Pipeline) : Pipeline := ⟨pipeline.applied, []⟩

def resolve (pipeline : Pipeline) : Option Snapshot :=
  overlayPrefix pipeline.applied pipeline.pending

theorem staging_preserves_the_external_snapshot
    (pipeline staged : Pipeline) (next : Stage) (accepted : stage pipeline next = some staged) :
    staged.applied = pipeline.applied := by
  unfold stage at accepted
  split at accepted
  · contradiction
  · simpa using congrArg (Option.map Pipeline.applied) accepted.symm

theorem discarding_the_queue_preserves_the_applied_prefix (pipeline : Pipeline) :
    resolve (discard pipeline) = some pipeline.applied := by
  rfl

#print axioms success_admits_exactly_the_linked_stage_chain
#print axioms resolver_reads_the_newest_queued_batches_then_durable
#print axioms moving_the_head_to_durable_preserves_the_resolver_view
#print axioms publishing_preserves_the_resolvers_bitemporal_answer
#print axioms empty_visible_batch_still_advances_the_head
#print axioms reversed_predecessor_is_refused
#print axioms staging_preserves_the_external_snapshot
#print axioms discarding_the_queue_preserves_the_applied_prefix

end RTDB.StagedPrefix
