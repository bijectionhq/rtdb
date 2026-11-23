import Std

namespace RTDB.TrieWirePaths

inductive Bucket where
  | zero | one | two | three
  deriving DecidableEq, Repr

abbrev Path := List Bucket

inductive Node (Row : Type) where
  | absent
  | leaf (path : Path) (rows : List Row)
  | branch (path : Path) (zero one two three : Node Row)

inductive WireNode (Row : Type) where
  | absent
  | leaf (rows : List Row)
  | branch (zero one two three : WireNode Row)

def encode : Node Row → WireNode Row
  | .absent => .absent
  | .leaf _ rows => .leaf rows
  | .branch _ zero one two three => .branch (encode zero) (encode one) (encode two) (encode three)

def decode (path : Path) : WireNode Row → Node Row
  | .absent => .absent
  | .leaf rows => .leaf path rows
  | .branch zero one two three =>
      .branch path (decode (path ++ [.zero]) zero) (decode (path ++ [.one]) one)
        (decode (path ++ [.two]) two) (decode (path ++ [.three]) three)

inductive WellPlaced : Path → Node Row → Prop where
  | absent (path : Path) : WellPlaced path .absent
  | leaf (path : Path) (rows : List Row) : WellPlaced path (.leaf path rows)
  | branch (path : Path) {zero one two three : Node Row} :
      WellPlaced (path ++ [.zero]) zero → WellPlaced (path ++ [.one]) one →
      WellPlaced (path ++ [.two]) two → WellPlaced (path ++ [.three]) three →
      WellPlaced path (.branch path zero one two three)

theorem decoded_nodes_recover_every_path_and_row {path : Path} {node : Node Row}
    (placed : WellPlaced path node) : decode path (encode node) = node := by
  induction placed <;> simp_all [encode, decode]

theorem canonical_wire_round_trip (path : Path) (wire : WireNode Row) :
    encode (decode path wire) = wire := by
  induction wire generalizing path <;> simp_all [encode, decode]

theorem decoding_reconstructs_a_well_placed_tree (path : Path) (wire : WireNode Row) :
    WellPlaced path (decode path wire) := by
  induction wire generalizing path with
  | absent => exact .absent path
  | leaf rows => exact .leaf path rows
  | branch zero one two three hz ho ht hth =>
      exact .branch path (hz _) (ho _) (ht _) (hth _)

structure WireTrie (Row : Type) where
  rootPath : Option Path
  root : WireNode Row

def toWire (rootPath : Path) (root : Node Row) : WireTrie Row :=
  ⟨if rootPath.isEmpty then none else some rootPath, encode root⟩

def fromWire (trie : WireTrie Row) : Node Row :=
  decode (trie.rootPath.getD []) trie.root

theorem restored_wire_recovers_the_root_and_every_bucket {root : Path} {node : Node Row}
    (placed : WellPlaced root node) : fromWire (toWire root node) = node := by
  cases root <;> simpa [fromWire, toWire] using decoded_nodes_recover_every_path_and_row placed

theorem legacy_rootless_wire_uses_the_empty_path (root : WireNode Row) :
    fromWire ⟨none, root⟩ = decode [] root := rfl

def leafRanges : Node Row → List (Path × List Row)
  | .absent => []
  | .leaf path rows => [(path, rows)]
  | .branch _ zero one two three => leafRanges zero ++ leafRanges one ++ leafRanges two ++ leafRanges three

def select (query : Path) (node : Node Row) : List Row :=
  ((leafRanges node).filter fun (path, _) =>
    query.isPrefixOf path || path.isPrefixOf query).flatMap Prod.snd

theorem serialized_selection_preserves_order_and_multiplicity {root : Path} {node : Node Row}
    (placed : WellPlaced root node) (query : Path) :
    select query (fromWire (toWire root node)) = select query node := by
  rw [restored_wire_recovers_the_root_and_every_bucket placed]

#print axioms decoded_nodes_recover_every_path_and_row
#print axioms canonical_wire_round_trip
#print axioms decoding_reconstructs_a_well_placed_tree
#print axioms restored_wire_recovers_the_root_and_every_bucket
#print axioms legacy_rootless_wire_uses_the_empty_path
#print axioms serialized_selection_preserves_order_and_multiplicity

end RTDB.TrieWirePaths
