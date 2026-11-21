# R5 · Recovery roots as holds; collectable L0; reset from a root

## Goal

Make recovery depend on an explicit, held root instead of on files that are simply never deleted.
After R5, L0 files are ordinary garbage once compaction supersedes them (as in XTDB), recovery and compactor reset start from a recovery root plus the log suffix, and nothing in the store is a permanent plaintext source of truth.

## Converged design

- **Recovery root.** A published checkpoint whose manifest names its complete dependency closure (table blocks, trie data and metadata, artefacts), with exact publication identities.
  The root also records the first required replica position, the history and resume token at its cut, and the custody identities needed to open it.
  Recovery acquires the root and suffix hold as one admission before replay starts.
- **Roots are holds.** A root is a ledger hold (R4) without expiry. It is released only when a newer root is complete, and completeness is checked through its manifest. The collection floor (R4) therefore never passes a live root's closure.
- **L0 becomes collectable.** The collector's L0 refusal goes, and the catalog stops retaining L0 identities after primary compaction supersedes them.
- **Reset from a root.** A compactor reset rebuilds from a root rather than from retained L0 files, and starts a new naming epoch (the epoch already exists from R3).
- **Log suffix.** The replica log may be trimmed below the oldest live root's first required record.
  Recovery holds keep that suffix through replay.
  Upstream acknowledgement may use an applied token covered by a durable root and a protected committed suffix.
  See [the upstream acknowledgement boundary](authority-boundaries.md#recovery-roots-and-upstream-acknowledgement).

## Invariants

- **DUR-2.** At least one live root has a complete, readable closure and its required log suffix, under the supported fault model.
- **E2 (shared with R7).** Every live root's closure consists only of readable units (no destroyed key, no collected file).

## What R5 deletes

- The L0 guard in `TrieCollector::collect_table` (`rust/crates/maintenance/src/gc.rs`, "L0 recovery files cannot be garbage collected").
- Retained L0 identities in the trie catalog (`dev/doc/trie-cat.allium` line 9 and the code behind it).
- `reset_to_l0` in `rust/crates/index/src/partition.rs` and `rust/crates/index/src/trie_catalog.rs`, and the L0 replacement plan in `Node::prepare_compactor_reset` (`rust/crates/node/src/operations.rs`), replaced by a reset from a root.
- The `rtdb.replication/invalid-trie-removal` restriction on L0 deletions in the replication driver.

## State on main (2026-11-21)

- Discovery already advances to a newer advertised root when collection removes the selected root or a table block during startup, and recovery admits its replica cursor before installing a checkpoint (`rust/crates/replication/src/checkpoint_discovery.rs`, `replay_floor.rs`).
- Ordinary hosted recovery validates the directory's admitted block and replica boundary against the stored catalog before loading table metadata.
  Successor selection and replay retries preserve the journal and advance both coordinates.
  This binds the recovery coordinate. The manifest for its data and artefact dependencies remains open.
- New Rust checkpoint roots bind every registered table block by its stored SHA-256 digest before publication.
  Recovery checks the map's domain against the OID registry and checks the bytes before decoding.
  This authenticates the table metadata graph relative to the loaded root. The root's provider identity and the trie and artefact publications still need their own evidence.
- Multi fresh storage probes for checkpoint contents resolve through the durable pack or the provider's loose object.
  The writer probes the recovered current checkpoint as well as historical checkpoints before issuing a new confirmation.
  The bounded comparison checks storage presence at the probe and identifies the need for a hold through submission.
- Versioned S3 pack metadata, index and member slices carry the provider identity of the response that supplied them.
  A warm pack lookup refuses a different publication at the same physical path, and a fork copy requires a fresh index at the destination.
  Cold recovery still needs a durable, externally trusted binding to the publication selected at the directory cut.
- The trie collector protects the installed checkpoint's live and nascent tries within one partition authority.
- Multi has recovery holds for the replica suffix (`rust/crates/multi/src/recovery_holds.rs`, peer commit `e298879a0`) and checkpoint-metadata holds (`rust/crates/multi/src/checkpoint_retention.rs`).
- Models: `verification/tla/{InstalledCheckpoint,RecoveryReplay,Checkpoint,ResidenceRetirement}.tla`, `verification/mcrl2/CheckpointDiscovery.mcrl2`, the AutoHyper two-run check, and the Lean checkpoint-metadata laws.
- Compactor reset names outputs in a new epoch (R3, `ae7b66e10`), so rebuilding from a root cannot collide with pre-reset names.

## Handoff notes

1. Land after R4: roots are holds, and the floor rule is what makes L0 collection safe.
2. Unify the Multi recovery and checkpoint-metadata holds with the R4 ledger instead of keeping a third retention mechanism.
3. Define the root manifest once (dependency closure plus first required replica record) and make discovery select only complete roots.
4. Remove the L0 guard and the retained identities in the same change that makes recovery root-based, so no window exists where L0 is collectable but still relied on.
5. Re-run the checkpoint replays and their controls (`verify_checkpoint_replay` in `verification/run.py`); the runner's node-faults test count must be updated if tests are added.

## Tests and proofs

- **Native:** crash during compaction and during reset, then recover from a root plus the log suffix; collect superseded L0 files and recover again; trim the log below the root and recover.
- **TLA+ / Apalache:** extend `InstalledCheckpoint.tla` and `ResidenceRetirement.tla` with roots as holds and L0 collection.
- **Mutations that must fail:** releasing a root before its successor is complete; collecting an L0 file still in a live root's closure; trimming the log above a live root's first required record.

## Open questions

- How many roots to keep, and whether a root is written per block or on a schedule (it bounds recovery time against storage).
