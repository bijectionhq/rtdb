# Typed replay closure refinement

For each successful enumeration and every object key, membership in the Rust set is equivalent to membership in the corresponding Lean list after projecting its keys to canonical object paths.
The proof checks arbitrary finite catalogs and suffixes, with lengths representable by the Rust collections.
Order and duplicate occurrences have no effect on membership.

The area imports the algorithm macros expanded by the shipped functions in [replication](../../../rust/crates/replication/src/replay_closure.rs) and [format](../../../rust/crates/format/src/catalog.rs).
The normal build uses Rust iterators and awaits the two storage reads.
Verus checks the same branch and insertion code with indexed iterator adapters, loop invariants, and synchronous contracts for the results of those reads.
The mutations edit the shared runtime source.

## Lean correspondence

The definitions are in [`RTDB.Recovery.ReplayClosure`](../../multi/lean/ReplayClosure.lean).
The Rust spec uses sets of path identities for the images of the corresponding Lean lists.

| Lean definition | Shipped enumeration and Verus correspondence |
| --- | --- |
| `trieKeys` | `TrieDetails::checkpoint_files` with `Replay` emits the data and metadata pair for live, nascent and retained garbage L0 tries, and recorded artefacts for live and nascent tries. |
| `tableTries`, `replayTries`, `checkpointTrieFiles` | `TableBlock::checkpoint_trie_files` uses partition details when partitions exist, filters garbage from expiring transaction-metadata tables, and includes active `searchRuns`. |
| `baseKeys` | `checkpoint_dependencies` includes the root and every corresponding table block and `checkpointTrieFiles` result. |
| `addedKeys` | `added_trie_keys` expands each logged trie to the recorded slugs of its table in the selected and adopted catalogs. |
| `catalogKeys`, `bodyKeys` | The upload scan checks the storage coordinates and installed block, enumerates each adopted catalog once, and checks its block index. The `bodyFileKeys` pass adds the upload's tries, `TriesAdded`, or usable references from `ArtefactsAdded` for the same storage. |
| `payloadKeys`, `Record.body`, `recordKeys`, `tailKeys` | `tail_dependencies` keeps an oversized payload key and sends its decoded body through the same two passes as an inline body. `partition_tail_keys` equates the three accumulation passes to the union over `recordKeys`. |

An upload's explicit Lean root key is already in the Rust contribution from `baseKeys`, since a successfully adopted catalog has the requested block index.
An absent checkpoint projects to `Selection.firstBlock = 0`, matching Rust's `-1` sentinel for the installed block.
The Lean `Added` and `artefactsAdded` values contain the results of resolving table names to all catalog slugs and projecting the usable artefact entries to paths.
Storage version and epoch preserve equality across the projection.

## Contracts and boundaries

The [Prost contract](../foreign_contracts/prost.md) supplies the typed replica body for the exact fetched bytes after the protobuf or extension refusal framing.
The store contracts supply the exact payload for its key and the catalog and table blocks returned by the first load of an adopted index.
The proof checks dependency membership on successful enumeration.
Digest validation, term admission, I/O progress, and error completeness remain runtime responsibilities.

The adapters in `inputs.rs` state the standard collection operations and the existing Rust input projections for parsed names, table registries, the trie-details iterator, and canonical key constructors.
The artefact projection uses the current catalog resolver.
It resolves logical search publications to their run file and contributes no file for a retire-only record.
Malformed records can reject enumeration before a set is returned.
Canonical path values include the recorded table slug, and identical physical paths have identical projected key identities even when several typed references name them.
The proof checks registry accumulation, dependency traversal, and set membership after those projections.
The correspondence to Lean is stated here and checked against its membership definitions case by case.
The translation between the two proof languages is reviewed by source inspection.

## Controls

`area.json` declares independent source mutations that omit the adopted catalog, the decoded oversized body, added tries, active search runs, and added artefacts.
`verify_typed_replay_closure_lean` in the study runner checks the reference with 19 axiom audits and rejects mutations that keep expired receipt tries or skip block zero for a checkpointless suffix.
The reference includes reachable witnesses for inline and oversized adoption and for a stale decoded body that still needs its external payload.
