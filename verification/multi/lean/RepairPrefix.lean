import RepairBoundary

namespace RTDB.Recovery

structure RetainedEntry (Record : Type) where
  epoch : Nat
  record : Record

def RunsIndex (start : Nat) (runs : List EpochRun) (entries : List (RetainedEntry Record)) : Prop :=
  ∀ index (present : index < entries.length), epochAt (start + index) runs = some entries[index].epoch

def SameSlotIdentity (server winner : List (RetainedEntry Record)) : Prop :=
  ∀ index (onServer : index < server.length) (onWinner : index < winner.length),
    server[index].epoch = winner[index].epoch → server[index] = winner[index]

theorem retained_prefix_from_matching_boundaries (server winner : List (RetainedEntry Record))
    (serverRuns winnerRuns : List EpochRun) (start count : Nat)
    (serverCovers : count ≤ server.length) (winnerCovers : count ≤ winner.length)
    (serverIndex : RunsIndex start serverRuns server) (winnerIndex : RunsIndex start winnerRuns winner)
    (identity : SameSlotIdentity server winner)
    (agree : ∀ lsn, start ≤ lsn → lsn < start + count → IsBoundary serverRuns winnerRuns start lsn →
      epochAt lsn serverRuns = epochAt lsn winnerRuns) :
    server.take count = winner.take count := by
  apply List.ext_getElem
  · simp [List.length_take_of_le serverCovers, List.length_take_of_le winnerCovers]
  · intro index withinServer withinWinner
    have inPrefix : index < count := by simpa [List.length_take_of_le serverCovers] using withinServer
    have serverHas : index < server.length := by omega
    have winnerHas : index < winner.length := by omega
    have epochs := matching_boundaries_imply_matching_prefix serverRuns winnerRuns start (start + count)
      agree (start + index) (by omega) (by omega)
    rw [serverIndex index serverHas, winnerIndex index winnerHas] at epochs
    simpa using identity index serverHas winnerHas (Option.some.inj epochs)

theorem overwrite_from_matching_boundary_reconstructs_winner (server winner : List (RetainedEntry Record))
    (serverRuns winnerRuns : List EpochRun) (start count : Nat)
    (serverCovers : count ≤ server.length) (winnerCovers : count ≤ winner.length)
    (serverIndex : RunsIndex start serverRuns server) (winnerIndex : RunsIndex start winnerRuns winner)
    (identity : SameSlotIdentity server winner)
    (agree : ∀ lsn, start ≤ lsn → lsn < start + count → IsBoundary serverRuns winnerRuns start lsn →
      epochAt lsn serverRuns = epochAt lsn winnerRuns) :
    server.take count ++ winner.drop count = winner := by
  rw [retained_prefix_from_matching_boundaries server winner serverRuns winnerRuns start count
    serverCovers winnerCovers serverIndex winnerIndex identity agree, List.take_append_drop]

theorem equal_epoch_labels_can_hide_distinct_records :
    let runs : List EpochRun := [⟨0, 1⟩]
    let server : List (RetainedEntry Bool) := [⟨1, false⟩]
    let winner : List (RetainedEntry Bool) := [⟨1, true⟩]
    RunsIndex 0 runs server ∧ RunsIndex 0 runs winner ∧ server.take 1 ++ winner.drop 1 ≠ winner := by
  simp only [RunsIndex, List.length_cons, List.length_nil, Nat.zero_add]
  refine ⟨?_, ?_, ?_⟩
  · intro index present
    have atStart : index = 0 := by omega
    subst index
    rfl
  · intro index present
    have atStart : index = 0 := by omega
    subst index
    rfl
  · intro same
    have records := congrArg (fun items => (items.head?).map RetainedEntry.record) same
    contradiction

#print axioms retained_prefix_from_matching_boundaries
#print axioms overwrite_from_matching_boundary_reconstructs_winner
#print axioms equal_epoch_labels_can_hide_distinct_records

end RTDB.Recovery
