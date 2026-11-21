import RepairPrefix

namespace RTDB.Recovery.JournalIdentity

structure Slot where
  stream : Nat
  generation : Nat
  epoch : Nat
  offset : Nat

structure Record (Value : Type) where
  slot : Slot
  payload : Value

structure Holding (Value : Type) where
  replica : Nat
  record : Record Value

structure State (Value : Type) where
  originated : List (Record Value)
  held : List (Holding Value)

def SameRecordPerSlot (records : List (Record Value)) : Prop :=
  ∀ first ∈ records, ∀ second ∈ records, first.slot = second.slot → first = second

def Consistent (state : State Value) : Prop :=
  SameRecordPerSlot state.originated ∧ ∀ copy ∈ state.held, copy.record ∈ state.originated

inductive Step : State Value → State Value → Prop
  | originate (state : State Value) (location : Nat) (record : Record Value)
      (fresh : ∀ old ∈ state.originated, old.slot ≠ record.slot) :
      Step state ⟨record :: state.originated, ⟨location, record⟩ :: state.held⟩
  | copy (state : State Value) (location : Nat) (source : Holding Value) (present : source ∈ state.held) :
      Step state { state with held := ⟨location, source.record⟩ :: state.held }
  | discard (state : State Value) (keep : Holding Value → Bool) :
      Step state { state with held := state.held.filter keep }

theorem step_preserves_identity (previous updated : State Value)
    (agrees : Consistent previous) (transition : Step previous updated) : Consistent updated := by
  obtain ⟨unique, provenanced⟩ := agrees
  cases transition with
  | originate location record fresh =>
    constructor
    · intro first firstPresent second secondPresent sameSlot
      rcases List.mem_cons.mp firstPresent with firstIsNew | firstIsOld
      · subst first
        rcases List.mem_cons.mp secondPresent with secondIsNew | secondIsOld
        · exact secondIsNew.symm
        · exact (fresh second secondIsOld sameSlot.symm).elim
      · rcases List.mem_cons.mp secondPresent with secondIsNew | secondIsOld
        · subst second
          exact (fresh first firstIsOld sameSlot).elim
        · exact unique first firstIsOld second secondIsOld sameSlot
    · intro copy present
      rcases List.mem_cons.mp present with isNew | isOld
      · subst copy
        exact List.mem_cons_self ..
      · exact List.mem_cons_of_mem _ (provenanced copy isOld)
  | copy location source present =>
    refine ⟨unique, ?_⟩
    intro item member
    rcases List.mem_cons.mp member with isNew | isOld
    · subst item
      exact provenanced source present
    · exact provenanced item isOld
  | discard keep =>
    exact ⟨unique, fun item member => provenanced item (List.mem_filter.mp member).1⟩

inductive Reachable : State Value → Prop
  | start : Reachable ⟨[], []⟩
  | next (before after : State Value) : Reachable before → Step before after → Reachable after

theorem reachable_is_consistent (state : State Value) (history : Reachable state) : Consistent state := by
  induction history with
  | start => simp [Consistent, SameRecordPerSlot]
  | next before after _ transition ih => exact step_preserves_identity before after ih transition

theorem reachable_journals_agree_on_slot (state : State Value) (history : Reachable state)
    (first second : Holding Value) (firstPresent : first ∈ state.held) (secondPresent : second ∈ state.held)
    (sameSlot : first.record.slot = second.record.slot) : first.record = second.record := by
  have agrees := reachable_is_consistent state history
  exact agrees.1 first.record (agrees.2 first firstPresent) second.record (agrees.2 second secondPresent) sameSlot

def RetainedView (state : State Value) (stream generation start replica : Nat)
    (entries : List (RetainedEntry Value)) : Prop :=
  ∀ index (present : index < entries.length), ∃ holding ∈ state.held,
    holding.replica = replica ∧
    holding.record.slot = ⟨stream, generation, entries[index].epoch, start + index⟩ ∧
    holding.record.payload = entries[index].record

theorem retained_views_agree_on_matching_epochs (state : State Value) (history : Reachable state)
    (server winner : List (RetainedEntry Value)) (stream generation start target source : Nat)
    (serverView : RetainedView state stream generation start target server)
    (winnerView : RetainedView state stream generation start source winner) : SameSlotIdentity server winner := by
  intro index targetHas sourceHas sameEpoch
  obtain ⟨targetEntry, targetPresent, _, targetSlot, targetPayload⟩ := serverView index targetHas
  obtain ⟨sourceEntry, sourcePresent, _, sourceSlot, sourcePayload⟩ := winnerView index sourceHas
  rw [sameEpoch] at targetSlot
  have sameRecord := reachable_journals_agree_on_slot state history targetEntry sourceEntry
    targetPresent sourcePresent (targetSlot.trans sourceSlot.symm)
  have samePayload := targetPayload.symm.trans ((congrArg Record.payload sameRecord).trans sourcePayload)
  cases targetStored : server[index] <;> cases sourceStored : winner[index]
  simp_all

theorem repair_preserves_selected_retained_view (state : State Value) (history : Reachable state)
    (server winner : List (RetainedEntry Value)) (serverRuns winnerRuns : List EpochRun)
    (stream generation start target source count : Nat)
    (serverView : RetainedView state stream generation start target server)
    (winnerView : RetainedView state stream generation start source winner)
    (serverCovers : count ≤ server.length) (winnerCovers : count ≤ winner.length)
    (serverIndex : RunsIndex start serverRuns server) (winnerIndex : RunsIndex start winnerRuns winner)
    (agree : ∀ lsn, start ≤ lsn → lsn < start + count → IsBoundary serverRuns winnerRuns start lsn →
      epochAt lsn serverRuns = epochAt lsn winnerRuns) :
    server.take count ++ winner.drop count = winner := by
  have identity := retained_views_agree_on_matching_epochs state history server winner stream generation start
    target source serverView winnerView
  exact overwrite_from_matching_boundary_reconstructs_winner server winner serverRuns winnerRuns start count
    serverCovers winnerCovers serverIndex winnerIndex identity agree

#print axioms step_preserves_identity
#print axioms reachable_journals_agree_on_slot
#print axioms repair_preserves_selected_retained_view

end RTDB.Recovery.JournalIdentity
