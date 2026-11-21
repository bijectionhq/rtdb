# R7 · The eraser

## Goal

After an ERASE commits, no clear byte of the erased entity's earlier history is readable anywhere RTDB stores data once the erasure deadline passes.
The deadline is at most 120 s, which is Bijection's storage-engine bound (`/Users/sachaichbiah/code/bijection/llm-wiki/wiki/storage-engine-contract.md`, "Erasure in the engine's own copies").
Fresh views see no erased version immediately (TIME-2, unchanged).

## Converged design

The pipeline runs on the leader (classic) or the deployment's owner (Multi), batching erased IIDs into short epochs (designers proposed 30 s).

1. **Force a block flush**, so the live index's erased rows reach files.
2. **Write successors.** For every file the catalog lists as containing an erased IID's partition path, force a merge with the ERASE. `rust/crates/maintenance/src/merge.rs` already drops events preceding an ERASE when they are merged together, so co-merging is what guarantees removal. Artefacts (search files, ordered indexes) are rebuilt with their files. Successors get new names: the naming epoch from R3, or a successor marker in the name.
3. **Publish** the rewritten set as a new generation and a new recovery root (R5).
4. **Destroy keys** (R6) in one ledger CAS: the DEKs of every superseded file and of every log segment that no live root needs and that contains an erased IID. Kafka and local log retention are left alone, because ciphertext under a destroyed key is erased.
   - Designer H also proposed log redaction overlays (keep positions, replace erased bodies) so the log stays replayable below the root. Use overlays only if some deployment must replay below its oldest root.
5. **Revoke views.** Holds whose generation predates the erasure are revoked. A pre-erase view keeps its answers until the deadline, then a read needing destroyed material fails with `rtdb/erased-under-view` (owner decision; SNAP-1 and DUR-1 are amended).
6. **Propagate to forks** whose history includes the erased rows (owner decision): forks sharing the ciphertext lose it with the same key destruction; a fork holding its own wrapped copy must have that wrapping destroyed too.
7. **Search scores.** Artefacts touched by an erasure are marked score-stale in the erasing generation; until successors land, fresh views fall back to the scan path with corpus statistics that exclude the erased IIDs (OPT-3 for scores).
8. **Overrun.** If the pipeline cannot finish before the deadline, destroy the keys anyway; reads of the un-rewritten files then fail closed with a documented error. Unreadability wins over availability.

## Invariants

- **E1.** After the deadline, no undestroyed DEK covers a byte containing a pre-ERASE event of an erased IID.
- **E2.** Every live root's closure consists only of undestroyed DEKs.
- **E3.** A hold whose generation predates the erasure is revoked, and its view returns `rtdb/erased-under-view` after the deadline.

## State on main (2026-11-21)

- Not started. Compaction already drops events before an ERASE when it merges them together, but nothing forces that merge, and nothing removes the original files, L0 files or log payloads on a deadline.
- Multi forks share physical objects, and deleting one first copies it into every store that still references it (`rust/crates/multi/src/reference.rs`). R7 must change this to key destruction or copy-on-erase.

## Handoff notes

1. Do not start before R5 (roots) and R6 (sealing) have landed; R7 composes them.
2. Budget the deadline explicitly: epoch + flush + co-merge + root publication + ledger propagation + cache notice. Record the budget with the configuration that meets it, including the maximum file size per level.
3. Decide how followers learn of destruction (the ledger CAS, observed at least every few seconds); a follower that misses notices must stop serving through hold expiry (R4), so the hold timeout must fit inside the slack.
4. Fork propagation touches Multi's reference counting in `reference.rs` and the fork lineage open question in `allium/rtdb-multi.allium`; resolve it there.
5. Bijection side: Bijection already seals erasable payloads under per-unit keys; R7 is what covers the clear parts RTDB holds (open-field index keys, blinded values, unit names).

## Tests and proofs

- **Native:** after the deadline, a provider-wide plaintext scan of every storage class finds no erased IID (extend the at-rest scan from R6); a pre-erase view gets `erased-under-view` after the deadline and its pre-erase rows before it; a fork sharing the history loses the rows; fresh-view scores exclude the erased entity.
- **TLA+:** an erasure pipeline composed with the ledger, checking E1 under deadline-forced destruction and E2 under lost replies, with forks.
- **Lean:** a successor differs from its predecessor only by the erased events.
- **Mutations that must fail:** destroying keys before the root is adopted (E2); skipping the co-merge at one level (E1); skipping one fork's key wrapping (E1).

## Open questions

- Epoch length and the deadline budget.
- Whether any deployment needs log overlays (replay below the oldest root).
