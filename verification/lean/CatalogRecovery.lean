import Std

namespace RTDB.CatalogRecovery

inductive Phase where
  | nascent
  | live
  | garbage
  deriving DecidableEq

structure Historical where
  block : Nat
  recency : Nat
  phase : Phase
  deriving DecidableEq

def pendingEntry (trie : Historical) : Option (Nat × Nat) :=
  if trie.phase = .nascent then some (trie.block, trie.recency) else none

def restorePending (catalog : List Historical) : List (Nat × Nat) :=
  catalog.filterMap pendingEntry

def promote (atBlock : Nat) (trie : Historical) : Historical :=
  if trie.block = atBlock ∧ trie.phase = .nascent then { trie with phase := .live } else trie

def admitCurrent (catalog : List Historical) (atBlock : Nat) : List Historical :=
  let pending := restorePending catalog
  catalog.map (fun trie =>
    if (atBlock, trie.recency) ∈ pending then promote atBlock trie else trie)

theorem restored_pending_is_exact (catalog : List Historical) (block recency : Nat) :
    (block, recency) ∈ restorePending catalog ↔
      ∃ trie ∈ catalog, trie.phase = .nascent ∧ trie.block = block ∧ trie.recency = recency := by
  unfold restorePending
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨trie, held, indexed⟩
    by_cases waiting : trie.phase = .nascent
    · have identity : trie.block = block ∧ trie.recency = recency := by
        simpa [pendingEntry, waiting] using indexed
      exact ⟨trie, held, waiting, identity⟩
    · simp [pendingEntry, waiting] at indexed
  · rintro ⟨trie, held, waiting, atBlock, atRecency⟩
    exact ⟨trie, held, by simp [pendingEntry, waiting, atBlock, atRecency]⟩

theorem current_after_restore_matches_catalog_scan (catalog : List Historical) (block : Nat) :
    admitCurrent catalog block = catalog.map (promote block) := by
  unfold admitCurrent
  apply List.map_congr_left
  intro trie held
  by_cases eligible : trie.block = block ∧ trie.phase = .nascent
  · have indexed : (block, trie.recency) ∈ restorePending catalog :=
      (restored_pending_is_exact _ _ _).2 ⟨trie, held, eligible.2, eligible.1, rfl⟩
    simp [indexed]
  · by_cases indexed : (block, trie.recency) ∈ restorePending catalog
    · simp [indexed]
    · simp [indexed, promote, eligible]

def applyCurrents (step : List Historical → Nat → List Historical) :
    List Historical → List Nat → List Historical
  | catalog, [] => catalog
  | catalog, block :: tail => applyCurrents step (step catalog block) tail

theorem any_current_suffix_matches_catalog_scan (catalog : List Historical) (blocks : List Nat) :
    applyCurrents admitCurrent catalog blocks =
      applyCurrents (fun held block => held.map (promote block)) catalog blocks := by
  induction blocks generalizing catalog with
  | nil => rfl
  | cons first tail ih =>
    simp only [applyCurrents, current_after_restore_matches_catalog_scan]
    exact ih _

theorem an_exact_block_promotes_both_recencies :
    admitCurrent [⟨0, 1, .nascent⟩, ⟨0, 2, .nascent⟩, ⟨1, 1, .nascent⟩] 0 =
      [⟨0, 1, .live⟩, ⟨0, 2, .live⟩, ⟨1, 1, .nascent⟩] := by decide

#print axioms restored_pending_is_exact
#print axioms current_after_restore_matches_catalog_scan
#print axioms any_current_suffix_matches_catalog_scan
#print axioms an_exact_block_promotes_both_recencies

end RTDB.CatalogRecovery
