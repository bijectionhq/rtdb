import Std

namespace RTDB.CheckpointReplayClosure

inductive TrieState where
  | live
  | nascent
  | garbage
  deriving DecidableEq

structure Trie (File : Type) where
  state : TrieState
  level : Nat
  data : File
  metadata : File
  artefacts : List File

def payloadRequired {File : Type} (trie : Trie File) : Prop :=
  trie.state ≠ .garbage ∨ trie.level = 0

instance {File : Type} (trie : Trie File) : Decidable (payloadRequired trie) :=
  inferInstanceAs (Decidable (trie.state ≠ .garbage ∨ trie.level = 0))

def trieFiles {File : Type} (trie : Trie File) : List File :=
  (if payloadRequired trie then [trie.data, trie.metadata] else []) ++
    (if trie.state ≠ .garbage then trie.artefacts else [])

def closure {File : Type} [BEq File] (tries : List (Trie File)) : List File :=
  (tries.flatMap trieFiles).eraseDups

theorem a_required_payload_keeps_both_files {File : Type} [BEq File] [LawfulBEq File]
    {tries : List (Trie File)} {trie : Trie File} (recorded : trie ∈ tries)
    (includePayload : payloadRequired trie) :
    trie.data ∈ closure tries ∧ trie.metadata ∈ closure tries := by
  constructor
  · apply List.mem_eraseDups.mpr
    apply List.mem_flatMap.mpr
    exact ⟨trie, recorded, by simp [trieFiles, includePayload]⟩
  · apply List.mem_eraseDups.mpr
    apply List.mem_flatMap.mpr
    exact ⟨trie, recorded, by simp [trieFiles, includePayload]⟩

theorem a_retained_input_keeps_both_files {File : Type} [BEq File] [LawfulBEq File]
    {tries : List (Trie File)} {trie : Trie File} (recorded : trie ∈ tries) (l0 : trie.level = 0) :
    trie.data ∈ closure tries ∧ trie.metadata ∈ closure tries :=
  a_required_payload_keeps_both_files recorded (Or.inr l0)

theorem a_usable_member_keeps_its_artefact {File : Type} [BEq File] [LawfulBEq File]
    {tries : List (Trie File)} {trie : Trie File} {file : File} (recorded : trie ∈ tries)
    (usable : trie.state ≠ .garbage) (attached : file ∈ trie.artefacts) : file ∈ closure tries := by
  apply List.mem_eraseDups.mpr
  apply List.mem_flatMap.mpr
  refine ⟨trie, recorded, ?_⟩
  exact List.mem_append_right _ (by simpa [usable] using attached)

theorem closure_contains_only_required_files {File : Type} [BEq File] [LawfulBEq File]
    {tries : List (Trie File)} {file : File} (found : file ∈ closure tries) :
    ∃ trie ∈ tries,
      (payloadRequired trie ∧ (file = trie.data ∨ file = trie.metadata)) ∨
      (trie.state ≠ .garbage ∧ file ∈ trie.artefacts) := by
  obtain ⟨trie, recorded, listed⟩ := List.mem_flatMap.mp (List.mem_eraseDups.mp found)
  refine ⟨trie, recorded, ?_⟩
  by_cases garbage : trie.state = .garbage <;> by_cases retained : trie.level = 0 <;>
    simp [trieFiles, payloadRequired, garbage, retained, or_assoc] at listed ⊢
  all_goals
    first
    | exact listed
    | rcases listed with data | metadata
      · exact Or.inl data
      · exact Or.inr (Or.inl metadata)

#print axioms a_required_payload_keeps_both_files
#print axioms a_retained_input_keeps_both_files
#print axioms a_usable_member_keeps_its_artefact
#print axioms closure_contains_only_required_files

end RTDB.CheckpointReplayClosure
