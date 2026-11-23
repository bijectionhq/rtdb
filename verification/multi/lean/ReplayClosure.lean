/-
Typed recovery closure of RTDB replica replay.

The model transcribes the decoded `ReplicaBody` branches of
`rust/crates/replication/src/driver.rs` (`apply_admitted`) and the checkpoint
closure of `rust/crates/multi/src/archive.rs`. It was audited at RTDB
`f3be4137` and re-inspected at `fd3d6dcb32`, where the branch set and read
guards are unchanged and an accepted upload additionally carries
`catalog_sha256`. Ported from Logkeeper design Study 110.

Premises: the protobuf decoder, canonical keys, admission witness and
digest checks supply the typed bodies, and each adopted root is the
certified catalog bound in the selection. Table paths are the slugs named by
catalog entries. `ResolvedTx` reads no recovery dependency: its search delta
is inline and a failed install becomes a serving gap
(`search/src/runtime.rs:420-435`). The model covers follower replay only. A
promoting leader's recovery confirmation also reads files of historical
catalogs (`replication/src/blocks.rs:421-436`); installed journals remove
that path by deriving the term from the Logkeeper grant (design A459).
The production enumeration is `rtdb_replication::replay_closure`.
-/

set_option warningAsError true

namespace RTDB.Recovery.ReplayClosure

inductive TrieState where
  | nascent | live | garbage
  deriving DecidableEq

structure Trie where
  key : Nat
  level : Nat
  state : TrieState
  artefacts : List Nat
  deriving DecidableEq

-- `path` stands for the table's slug as named by its catalog entry, including any
-- collision suffix. Tries in the log map to keys only through those catalogs.
structure Table where
  path : Nat
  partitions : List (List Trie)
  legacyTries : List Trie
  searchRuns : List Nat
  storesExpiringTransactionMetadata : Bool
  deriving DecidableEq

structure Checkpoint where
  root : Nat
  tables : List Table
  deriving DecidableEq

-- `root` is the block index that names the root key `block_key(root)`.
inductive Key where
  | root (root : Nat)
  | tableBlock (root path : Nat)
  | trieData (table trie : Nat)
  | trieMetadata (table trie : Nat)
  | artefact (path : Nat)
  | searchRun (table run : Nat)
  | tailPayload (path : Nat)
  deriving DecidableEq

structure Storage where
  version : Nat
  epoch : Nat
  deriving DecidableEq

abbrev Added := List (Nat × Trie)

-- The decoded replica bodies of RTDB's `ReplicaBody`, other than the oversized proxy.
inductive Body where
  | resolvedTx
  | noOp
  | blockBoundary (index : Nat)
  | blockUploaded (storage : Storage) (index : Nat) (tries : Added)
  | triesAdded (storage : Storage) (tries : Added)
  | triesDeleted
  | tablesRetired
  | artefactsAdded (storage : Storage) (paths : List Nat)
  | artefactsRemoved
  deriving DecidableEq

-- A nested oversized body is rejected by the decoder, so it is unrepresentable here.
inductive Record where
  | inline (body : Body)
  | oversized (payload : Nat) (body : Body)
  deriving DecidableEq

def Record.body : Record → Body
  | .inline body => body
  | .oversized _ body => body

structure Selection where
  checkpoint : Option Checkpoint
  storage : Storage
  catalogs : List (Nat × Checkpoint)
  suffix : List Record
  deriving DecidableEq

def TrieRead (table : Nat) (trie : Trie) (key : Key) : Prop :=
  ((trie.state ≠ .garbage ∨ trie.level = 0) ∧
    (key = .trieData table trie.key ∨ key = .trieMetadata table trie.key)) ∨
  (trie.state ≠ .garbage ∧ ∃ path ∈ trie.artefacts, key = .artefact path)

def TableTrie (table : Table) (trie : Trie) : Prop :=
  (table.partitions = [] ∧ trie ∈ table.legacyTries) ∨
    (table.partitions ≠ [] ∧ ∃ partition ∈ table.partitions, trie ∈ partition)

def tableTries (table : Table) : List Trie :=
  if table.partitions.isEmpty then table.legacyTries else table.partitions.flatten

theorem table_decoding_uses_either_partition_details_or_legacy_flat_details
    (table : Table) (trie : Trie) :
    trie ∈ tableTries table ↔ TableTrie table trie := by
  cases table with
  | mk path partitions legacy runs expires =>
    cases partitions <;> simp [tableTries, TableTrie]

def Selection.firstBlock (selected : Selection) : Nat :=
  (selected.checkpoint.map (fun checkpoint => checkpoint.root + 1)).getD 0

def RetainedTableTrie (table : Table) (trie : Trie) : Prop :=
  TableTrie table trie ∧ (table.storesExpiringTransactionMetadata = false ∨ trie.state ≠ .garbage)

def replayTries (table : Table) : List Trie :=
  (tableTries table).filter fun trie => !(table.storesExpiringTransactionMetadata && trie.state == .garbage)

theorem replay_trie_selection_matches_retention (table : Table) (trie : Trie) :
    trie ∈ replayTries table ↔ RetainedTableTrie table trie := by
  simp [replayTries, RetainedTableTrie, table_decoding_uses_either_partition_details_or_legacy_flat_details]

def BaseRead (base : Checkpoint) (key : Key) : Prop :=
  key = .root base.root ∨
    ∃ table ∈ base.tables, key = .tableBlock base.root table.path ∨
      (∃ trie, RetainedTableTrie table trie ∧ TrieRead table.path trie key) ∨
      ∃ run ∈ table.searchRuns, Key.searchRun table.path run = key

def AddedRead (tries : Added) (key : Key) : Prop :=
  ∃ added ∈ tries, TrieRead added.1 added.2 key

-- Reads and later references made by an applied body, stated per RTDB driver branch.
def BodyRead (selected : Selection) : Body → Key → Prop
  | .blockUploaded storage index tries, key =>
      storage = selected.storage ∧ selected.firstBlock ≤ index ∧
        (key = .root index ∨ (∃ catalog, selected.catalogs.lookup index = some catalog ∧ BaseRead catalog key) ∨
          AddedRead tries key)
  | .triesAdded storage tries, key => storage = selected.storage ∧ AddedRead tries key
  | .artefactsAdded storage paths, key => storage = selected.storage ∧ ∃ path ∈ paths, key = .artefact path
  | _, _ => False

def TailRead (selected : Selection) (key : Key) : Prop :=
  ∃ record ∈ selected.suffix,
    (∃ payload body, record = .oversized payload body ∧ key = .tailPayload payload) ∨
      BodyRead selected record.body key

def ReplayRead (selected : Selection) (key : Key) : Prop :=
  (∃ base, selected.checkpoint = some base ∧ BaseRead base key) ∨ TailRead selected key

def trieKeys (table : Nat) (trie : Trie) : List Key :=
  let files := [Key.trieData table trie.key, Key.trieMetadata table trie.key]
  match trie.state with
  | .garbage => if trie.level = 0 then files else []
  | .live | .nascent => files ++ trie.artefacts.map Key.artefact

def checkpointTrieFiles (table : Table) : List Key :=
  (replayTries table).flatMap (trieKeys table.path) ++ table.searchRuns.map (Key.searchRun table.path)

def baseKeys (base : Checkpoint) : List Key :=
  .root base.root :: base.tables.flatMap (fun table =>
    .tableBlock base.root table.path :: checkpointTrieFiles table)

def addedKeys (tries : Added) : List Key :=
  tries.flatMap (fun added => trieKeys added.1 added.2)

def catalogKeys (selected : Selection) (index : Nat) : List Key :=
  match selected.catalogs.lookup index with
  | some catalog => baseKeys catalog
  | none => []

def bodyKeys (selected : Selection) : Body → List Key
  | .blockUploaded storage index tries =>
      if storage = selected.storage ∧ selected.firstBlock ≤ index then
        .root index :: catalogKeys selected index ++ addedKeys tries
      else []
  | .triesAdded storage tries => if storage = selected.storage then addedKeys tries else []
  | .artefactsAdded storage paths => if storage = selected.storage then paths.map Key.artefact else []
  | _ => []

def payloadKeys : Record → List Key
  | .inline _ => []
  | .oversized payload _ => [.tailPayload payload]

def recordKeys (selected : Selection) (record : Record) : List Key :=
  payloadKeys record ++ bodyKeys selected record.body

def tailKeys (selected : Selection) : List Key :=
  selected.suffix.flatMap (recordKeys selected)

def recoveryKeys (selected : Selection) : List Key :=
  (selected.checkpoint.toList.flatMap baseKeys) ++ tailKeys selected

theorem enumerated_trie_keys_match_the_declared_reads (table : Nat) (trie : Trie) (key : Key) :
    key ∈ trieKeys table trie ↔ TrieRead table trie key := by
  cases trie with
  | mk id level state artefacts =>
    cases state with
    | nascent => simp [trieKeys, TrieRead, eq_comm, or_assoc]
    | live => simp [trieKeys, TrieRead, eq_comm, or_assoc]
    | garbage =>
      by_cases retained : level = 0 <;> simp [trieKeys, TrieRead, retained]

theorem checkpoint_enumeration_matches_every_typed_base_read (base : Checkpoint) (key : Key) :
    key ∈ baseKeys base ↔ BaseRead base key := by
  simp [baseKeys, BaseRead, checkpointTrieFiles, replay_trie_selection_matches_retention,
    enumerated_trie_keys_match_the_declared_reads]

theorem added_trie_enumeration_matches_the_declared_reads (tries : Added) (key : Key) :
    key ∈ addedKeys tries ↔ AddedRead tries key := by
  simp [addedKeys, AddedRead, enumerated_trie_keys_match_the_declared_reads]

theorem bound_catalog_enumeration_matches_its_reads (selected : Selection) (index : Nat) (key : Key) :
    key ∈ catalogKeys selected index ↔
      ∃ catalog, selected.catalogs.lookup index = some catalog ∧ BaseRead catalog key := by
  unfold catalogKeys
  cases selected.catalogs.lookup index with
  | none => simp
  | some catalog => simp [checkpoint_enumeration_matches_every_typed_base_read]

theorem body_enumeration_matches_each_driver_branch (selected : Selection) (body : Body) (key : Key) :
    key ∈ bodyKeys selected body ↔ BodyRead selected body key := by
  cases body with
  | blockUploaded storage index tries =>
    by_cases applies : storage = selected.storage ∧ selected.firstBlock ≤ index
    · simp [bodyKeys, BodyRead, applies, bound_catalog_enumeration_matches_its_reads,
        added_trie_enumeration_matches_the_declared_reads]
    · simp only [bodyKeys, BodyRead, ite_eq_right_of_eq_false _ _ (eq_false applies), List.not_mem_nil, false_iff]
      exact fun ⟨same, later, _⟩ => applies ⟨same, later⟩
  | triesAdded storage tries =>
    by_cases same : storage = selected.storage <;>
      simp [bodyKeys, BodyRead, same, added_trie_enumeration_matches_the_declared_reads]
  | artefactsAdded storage paths =>
    by_cases same : storage = selected.storage <;> simp [bodyKeys, BodyRead, same, eq_comm]
  | _ => simp [bodyKeys, BodyRead]

theorem chosen_suffix_enumeration_keeps_payloads_and_decoded_bodies (selected : Selection) (key : Key) :
    key ∈ tailKeys selected ↔ TailRead selected key := by
  simp only [tailKeys, TailRead, List.mem_flatMap, recordKeys, List.mem_append,
    body_enumeration_matches_each_driver_branch]
  constructor
  · rintro ⟨record, member, found⟩
    refine ⟨record, member, ?_⟩
    rcases found with outer | decoded
    · cases record with
      | inline body => simp [payloadKeys] at outer
      | oversized payload body => exact .inl ⟨payload, body, rfl, by simpa [payloadKeys] using outer⟩
    · exact .inr decoded
  · rintro ⟨record, member, found⟩
    refine ⟨record, member, ?_⟩
    rcases found with ⟨payload, body, rfl, rfl⟩ | decoded
    · exact .inl (by simp [payloadKeys])
    · exact .inr decoded

theorem enumeration_covers_every_base_and_suffix_read (selected : Selection) (key : Key) :
    key ∈ recoveryKeys selected ↔ ReplayRead selected key := by
  simp [recoveryKeys, ReplayRead, checkpoint_enumeration_matches_every_typed_base_read,
    chosen_suffix_enumeration_keeps_payloads_and_decoded_bodies]

-- The replay driver after term admission. `live` is the open block, one past the installed root.
structure Replay where
  live : Nat
  pending : Bool
  held : List Body
  work : List Body
  remaining : List Record
  demands : List Key

def Replay.start (selected : Selection) : Replay :=
  ⟨selected.firstBlock, false, [], [], selected.suffix, []⟩

-- Demand-producing steps carry RTDB's exact guards; skipping and holding are left unrestricted.
inductive Step (selected : Selection) : Replay → Replay → Prop where
  | admit (state : Replay) (record : Record) (rest : List Record) :
      state.work = [] → state.remaining = record :: rest →
      Step selected state ({ state with
        work := [record.body], remaining := rest, demands := state.demands ++ payloadKeys record })
  | fence (state : Replay) (record : Record) (rest : List Record) :
      state.work = [] → state.remaining = record :: rest →
      Step selected state ({ state with
        remaining := rest })
  | adopt (state : Replay) (tries : Added) (rest : List Body) :
      state.pending = true → state.work = .blockUploaded selected.storage state.live tries :: rest →
      Step selected state ({ state with
        live := state.live + 1, pending := false, held := [], work := state.held ++ rest, demands := state.demands ++ (.root state.live :: catalogKeys selected state.live ++ addedKeys tries) })
  | hold (state : Replay) (body : Body) (rest : List Body) :
      state.pending = true → state.work = body :: rest →
      Step selected state ({ state with
        held := state.held ++ [body], work := rest })
  | cut (state : Replay) (rest : List Body) :
      state.pending = false → state.work = .blockBoundary state.live :: rest →
      Step selected state ({ state with
        pending := true, work := rest })
  | addTries (state : Replay) (tries : Added) (rest : List Body) :
      state.pending = false → state.work = .triesAdded selected.storage tries :: rest →
      Step selected state ({ state with
        work := rest, demands := state.demands ++ addedKeys tries })
  | addArtefacts (state : Replay) (paths : List Nat) (rest : List Body) :
      state.pending = false → state.work = .artefactsAdded selected.storage paths :: rest →
      Step selected state ({ state with
        work := rest, demands := state.demands ++ paths.map Key.artefact })
  | skip (state : Replay) (body : Body) (rest : List Body) :
      state.work = body :: rest →
      Step selected state ({ state with
        work := rest })

inductive Reaches (selected : Selection) : Replay → Prop where
  | start : Reaches selected (Replay.start selected)
  | step {state next : Replay} : Reaches selected state → Step selected state next → Reaches selected next

def Covered (selected : Selection) (state : Replay) : Prop :=
  (∀ body ∈ state.work ++ state.held, ∃ record ∈ selected.suffix, record.body = body) ∧
    (∀ record ∈ state.remaining, record ∈ selected.suffix) ∧
    selected.firstBlock ≤ state.live ∧
    ∀ key ∈ state.demands, key ∈ tailKeys selected

theorem payload_keys_are_tail_keys (selected : Selection) (record : Record)
    (member : record ∈ selected.suffix) (key : Key) (found : key ∈ payloadKeys record) :
    key ∈ tailKeys selected := by
  apply (chosen_suffix_enumeration_keeps_payloads_and_decoded_bodies selected key).mpr
  refine ⟨record, member, .inl ?_⟩
  cases record with
  | inline body => simp [payloadKeys] at found
  | oversized payload body => exact ⟨payload, body, rfl, by simpa [payloadKeys] using found⟩

theorem body_reads_are_tail_keys (selected : Selection) (body : Body)
    (source : ∃ record ∈ selected.suffix, record.body = body) (key : Key) (reads : BodyRead selected body key) :
    key ∈ tailKeys selected := by
  obtain ⟨record, member, rfl⟩ := source
  exact (chosen_suffix_enumeration_keeps_payloads_and_decoded_bodies selected key).mpr ⟨record, member, .inr reads⟩

theorem a_replay_step_preserves_coverage (selected : Selection) {state next : Replay}
    (covered : Covered selected state) (step : Step selected state next) : Covered selected next := by
  obtain ⟨bodies, records, later, demanded⟩ := covered
  cases step with
  | admit record rest idle queued =>
    have member : record ∈ selected.suffix := records record (by simp [queued])
    refine ⟨?_, fun r found => records r (by simp [queued, found]), later, ?_⟩
    · intro body found
      simp only [List.mem_append, List.mem_singleton] at found
      rcases found with rfl | held
      · exact ⟨record, member, rfl⟩
      · exact bodies body (by simp [held])
    · intro key found
      rcases List.mem_append.mp found with old | outer
      · exact demanded key old
      · exact payload_keys_are_tail_keys selected record member key outer
  | fence record rest idle queued =>
    exact ⟨bodies, fun r found => records r (by simp [queued, found]), later, demanded⟩
  | adopt tries rest _ queued =>
    have source := bodies (.blockUploaded selected.storage state.live tries) (by simp [queued])
    refine ⟨?_, records, by simp only; omega, ?_⟩
    · intro body found
      simp only [List.append_nil, List.mem_append] at found
      rcases found with held | rest'
      · exact bodies body (by simp [held])
      · exact bodies body (by simp [queued, rest'])
    · intro key found
      rcases List.mem_append.mp found with old | fresh
      · exact demanded key old
      · apply body_reads_are_tail_keys selected _ source key
        refine ⟨rfl, later, ?_⟩
        simp only [List.mem_append, List.mem_cons] at fresh
        rcases fresh with (root | catalog) | added
        · exact .inl root
        · exact .inr (.inl ((bound_catalog_enumeration_matches_its_reads selected state.live key).mp catalog))
        · exact .inr (.inr ((added_trie_enumeration_matches_the_declared_reads tries key).mp added))
  | hold body rest _ queued =>
    refine ⟨?_, records, later, demanded⟩
    intro b found
    exact bodies b (by simp only [List.mem_append, List.mem_singleton] at found ⊢; rcases found with h | h | h <;> simp_all)
  | cut rest _ queued =>
    exact ⟨fun b found => bodies b (by simp_all), records, later, demanded⟩
  | addTries tries rest _ queued =>
    have source := bodies (.triesAdded selected.storage tries) (by simp [queued])
    refine ⟨fun b found => bodies b (by simp_all), records, later, ?_⟩
    intro key found
    rcases List.mem_append.mp found with old | fresh
    · exact demanded key old
    · exact body_reads_are_tail_keys selected _ source key
        ⟨rfl, (added_trie_enumeration_matches_the_declared_reads tries key).mp fresh⟩
  | addArtefacts paths rest _ queued =>
    have source := bodies (.artefactsAdded selected.storage paths) (by simp [queued])
    refine ⟨fun b found => bodies b (by simp_all), records, later, ?_⟩
    intro key found
    rcases List.mem_append.mp found with old | fresh
    · exact demanded key old
    · obtain ⟨path, listed, named⟩ := List.mem_map.mp fresh
      exact body_reads_are_tail_keys selected _ source key ⟨rfl, path, listed, named.symm⟩
  | skip body rest queued =>
    exact ⟨fun b found => bodies b (by simp_all), records, later, demanded⟩

theorem every_replay_read_is_in_the_typed_tail_enumeration (selected : Selection) {state : Replay}
    (reached : Reaches selected state) : ∀ key ∈ state.demands, key ∈ tailKeys selected := by
  have covered : Covered selected state := by
    induction reached with
    | start => exact ⟨by simp [Replay.start], by simp [Replay.start], by simp [Replay.start], by simp [Replay.start]⟩
    | step _ step ih => exact a_replay_step_preserves_coverage selected ih step
  exact covered.2.2.2

-- Witnesses that the previous payload-only tail enumeration missed reachable reads.
def uploadStorage : Storage := ⟨1, 0⟩

def adoptedCatalog : Checkpoint := ⟨2, [⟨7, [[⟨5, 0, .live, []⟩]], [], [], false⟩]⟩

def tailWith (records : List Record) : Selection :=
  ⟨some ⟨1, []⟩, uploadStorage, [(2, adoptedCatalog)], records⟩

theorem an_inline_upload_reads_its_adopted_catalog_and_table_block :
    ∃ state, Reaches (tailWith [.inline (.blockBoundary 2), .inline (.blockUploaded uploadStorage 2 [])]) state ∧
      Key.tableBlock 2 7 ∈ state.demands ∧ Key.trieData 7 5 ∈ state.demands ∧
      Key.tableBlock 2 7 ∉ (tailWith [.inline (.blockBoundary 2),
        .inline (.blockUploaded uploadStorage 2 [])]).suffix.flatMap payloadKeys :=
  ⟨_, .step (.step (.step (.step .start (.admit _ _ _ rfl rfl)) (.cut _ _ rfl rfl))
    (.admit _ _ _ rfl rfl)) (.adopt _ _ _ rfl rfl), by decide, by decide, by decide⟩

theorem an_oversized_upload_reads_its_payload_and_its_decoded_catalog :
    ∃ state, Reaches (tailWith [.inline (.blockBoundary 2), .oversized 9 (.blockUploaded uploadStorage 2 [])]) state ∧
      Key.tailPayload 9 ∈ state.demands ∧ Key.root 2 ∈ state.demands ∧ Key.tableBlock 2 7 ∈ state.demands ∧
      Key.tableBlock 2 7 ∉ (tailWith [.inline (.blockBoundary 2),
        .oversized 9 (.blockUploaded uploadStorage 2 [])]).suffix.flatMap payloadKeys :=
  ⟨_, .step (.step (.step (.step .start (.admit _ _ _ rfl rfl)) (.cut _ _ rfl rfl))
    (.admit _ _ _ rfl rfl)) (.adopt _ _ _ rfl rfl), by decide, by decide, by decide, by decide⟩

theorem a_stale_decoded_body_still_needs_its_external_payload :
    ∃ state, Reaches (tailWith [.oversized 9 (.blockUploaded uploadStorage 1 [])]) state ∧
      Key.tailPayload 9 ∈ state.demands ∧
      bodyKeys (tailWith [.oversized 9 (.blockUploaded uploadStorage 1 [])])
        (.blockUploaded uploadStorage 1 []) = [] :=
  ⟨_, .step (.step .start (.admit _ _ _ rfl rfl)) (.skip _ _ _ rfl), by decide, by decide⟩

-- The identifiers stand for the paths in RTDB's replay_closure fixture.
def replayFixture : Table :=
  ⟨16384, [[⟨1, 0, .garbage, [50]⟩, ⟨2, 1, .garbage, [50]⟩, ⟨3, 2, .live, [40, 50]⟩,
    ⟨4, 2, .nascent, [40]⟩]], [⟨2, 1, .live, [50]⟩], [60], false⟩

theorem rtdb_replay_fixture_requires_retained_l0_nascent_group_artefacts_and_search_runs :
    baseKeys ⟨2, [replayFixture]⟩ =
      [.root 2, .tableBlock 2 16384, .trieData 16384 1, .trieMetadata 16384 1,
      .trieData 16384 3, .trieMetadata 16384 3, .artefact 40, .artefact 50,
      .trieData 16384 4, .trieMetadata 16384 4, .artefact 40, .searchRun 16384 60] := by
  decide

theorem expiring_receipt_garbage_l0_is_outside_the_checkpoint :
    Key.trieData 16384 1 ∉ checkpointTrieFiles { replayFixture with storesExpiringTransactionMetadata := true } := by
  decide

theorem a_suffix_without_a_checkpoint_can_adopt_block_zero :
    let root : Checkpoint := ⟨0, []⟩
    Key.root 0 ∈ tailKeys ⟨none, uploadStorage, [(0, root)], [.inline (.blockUploaded uploadStorage 0 [])]⟩ := by
  decide

#print axioms expiring_receipt_garbage_l0_is_outside_the_checkpoint
#print axioms a_suffix_without_a_checkpoint_can_adopt_block_zero
#print axioms replay_trie_selection_matches_retention
#print axioms table_decoding_uses_either_partition_details_or_legacy_flat_details
#print axioms enumerated_trie_keys_match_the_declared_reads
#print axioms checkpoint_enumeration_matches_every_typed_base_read
#print axioms added_trie_enumeration_matches_the_declared_reads
#print axioms bound_catalog_enumeration_matches_its_reads
#print axioms body_enumeration_matches_each_driver_branch
#print axioms chosen_suffix_enumeration_keeps_payloads_and_decoded_bodies
#print axioms enumeration_covers_every_base_and_suffix_read
#print axioms payload_keys_are_tail_keys
#print axioms body_reads_are_tail_keys
#print axioms a_replay_step_preserves_coverage
#print axioms every_replay_read_is_in_the_typed_tail_enumeration
#print axioms an_inline_upload_reads_its_adopted_catalog_and_table_block
#print axioms an_oversized_upload_reads_its_payload_and_its_decoded_catalog
#print axioms a_stale_decoded_body_still_needs_its_external_payload
#print axioms rtdb_replay_fixture_requires_retained_l0_nascent_group_artefacts_and_search_runs

end RTDB.Recovery.ReplayClosure
