# Wave 1 journal boundary

## Delivered

- `rtdb_storage::log::SharedLog` exposes ordered append with a durability receipt, the latest durable position, and prepared replay after a record.
  Kafka, local-file, and memory logs implement it using their existing append and replay paths.
  The default writer still confirms its term on replica read-back.
- `rtdb_logkeeper::simulation::FaultJournal` wraps Logkeeper's `journal_contract::Journal`.
  It can delay dispatch, return `Unavailable` before dispatch, and hide a committed append's reply behind `Unavailable`.
- RTDB pins `journal-contract` to Logkeeper `main` at `f2b964da847ce2487c35fa1dc31fae05a32d6d86`, which contains N4’s reference journals and conformance suite.
  The simulation feature uses that dependency.
  The existing `ordered-logkeeper` runtime retains its separate pin during the transition.
- Removed the uncommitted W1 `rtdb-journal` prototype, including its duplicate trait, MemoryJournal, FileJournal, and conformance suite, after the ownership update.
  Current `Log` callers and default-mode backends are available during migration.

## Verification

- Eight focused tests passed under the RTDB guard, including Kafka and local-file `SharedLog` replay, memory replay, and three fault cases.
  Logkeeper's shared conformance suite passed through `FaultJournal` against both reference journals.

  ```sh
  mise exec cmake@3.31.12 -- /Users/sachaichbiah/code/codesign-agents/guarded-cargo.py rtdb test --locked \
    -p rtdb-storage -p rtdb-logkeeper -p rtdb-kafka --features rtdb-logkeeper/simulation \
    --test log_range --test journal_faults --test log -- shared_log fault_journal memory_replay_delivery
  ```

- The Rust workspace suite and broader default-mode integration tests are unverified.
- The first guarded run included Logkeeper library unit tests and stopped at a pre-existing assertion comparing the returned `(i64, i64)` with an `i64` in `rust/crates/logkeeper/src/client/writer.rs`.
- The first focused integration attempt stopped when the CMake shim had no selected version.
  The retry selects the existing CMake 3.31.12 for the invocation.
- The Allium CLI is unavailable.
  The spec changes use the existing contract syntax and were checked against the Allium language reference.
- Before the guard update, the removed prototype passed 15 checks with `cargo test --locked -p rtdb-journal --lib -- conformance file_journal`.

## Object roles

- `SharedLog` is a passive port implemented by the existing provider handles.
  The Kafka and local append drivers own their writer loops.
  Each prepared replay cursor owns one reader's next position, and the provider's progress monitor supplies the durable tip it can wait for.
- `FaultJournal` is a passive wrapper.
  It owns the queue of scripted outcomes and dispatches on the caller's future.

## Legacy port migration

The method names below are from `rust/crates/storage/src/log/mod.rs`.
Default means the Kafka or local mode.
Multi means the opt-in fenced journal mode.

| Legacy `Log` method | Default driver | Multi driver |
| --- | --- | --- |
| `needs_published_recovery_checkpoint` | Select a persisted block and open replica replay from its boundary. | Load the manifest named by the retained `Checkpoint` record. |
| `requires_replica_payload_permit` | Keep storage read reservations in the default storage path. | Resolve detached objects through RTDB's store and key authority using the committed fold. Retire the journal permit. |
| `install_replica_reference_source` | Construct storage and replay collaborators at startup. | Inject the store, key authority, cache, and memory budget into the checkpoint and fold drivers. |
| `publish_replica_payload` | Upload through the default store's durability and encryption path before publishing the ordered reference. | Upload through the RTDB store using the writer epoch in the file key, then publish the reference in a committed record. |
| `confirms_recovery_checkpoint` | Adopt a block on replica read-back. | Observe the committed checkpoint record and verify its manifest and cut in RTDB before recovery or trim. |
| `requires_checkpoint_digest` | Verify classic catalog integrity where the default block format defines it. | Verify the content digest and key generation in the RTDB checkpoint manifest. |
| `fences_writer_terms` | Run the existing term claim and sequence checks in the log processor. | Seal the chosen epoch and catch up through the returned end before serving. |
| `holds_writer_grant` | Derive leadership from the replica read-back state machine. | Check the writer's lease in RTDB policy and step down after a fence rejection. `head` observes position. |
| `retains_synced_history` | Select local repair using the concrete local-log startup path. | Recover at the retained checkpoint and replay the suffix. |
| `maximum_append_bytes` | Admit against the selected backend's configured record limits in the default driver. | Budget for the ciphertext envelope and handle `TooLarge { max }` at append. |
| `maximum_guarded_append_bytes` | Apply staging and timing policy in the default writer. | Apply staging and timing policy in the Multi writer. |
| `maximum_timed_original_append_bytes` | Advertise timed acceptance only when the writer can attest durable completion before the cutoff. | Use timed `Tx` plus `Settle` or `Refuse`. |
| `enqueue` | `SharedLog::append` and `AppendReceipt::durable`. | `Journal::append(stream, epoch, at, ciphertext)`. |
| `enqueue_maintenance` | Append block and maintenance records with `SharedLog::append`. | Append checkpoint and maintenance records normally. Budget checkpoint space in the driver. |
| `available_append_capacity` | Keep queue and storage budgets in the local or Kafka driver. | Handle `Full { floor }`, backpressure, and checkpoint scheduling in RTDB. |
| `record_applied` | Release in-process intake and read allocations at the applied cut. | Advance the fold watermark and trim only through the authorized checkpoint cut. |
| `enqueue_guarded` | Stage and classify the transaction in the default writer. Use the same terminal decision records for timed writes. | Use ordinary conditional append for the pending `Tx` and the terminal decision. |
| `latest` | `SharedLog::latest`. | `Journal::head`. |
| `retained_floor` | Receive the retained boundary from the cursor error or the backend's concrete recovery API. | Use `Page::floor`, `Trimmed { floor }`, or `Full { floor }` as appropriate. |
| `recovery_tail` | Open `SharedLog::read_after` from the persisted boundary and preserve the newer-block fallback on a missing prefix. | Read the retained page, verify the checkpoint, then read its suffix from the next LSN. |
| `tail` | `SharedLog::read_after` with required prepared delivery. | Page with `Journal::read` and advance to the last delivered LSN plus one. |
| `range` | Capture a durable end and consume prepared records up to the exclusive bound. Preserve cursor admission errors and payload charges. | Capture an end with `head`, page up to that cut, and handle trims according to the reader's pin and recovery policy. |
| `protect_snapshot` | Preserve default snapshot admission through the catalog and storage read scope. | Use an RTDB-owned pin and manifest, plus journal paging for any needed suffix. |
| `protect_range` | Preserve default range and storage admission where required. | Retire journal-managed holds. Enforce retention and erasure policy at the RTDB boundary. |
| `read_range` | Materialize the bounded range in the caller with the same allocation limits. | Materialize bounded `Journal::read` pages with RTDB-owned reservations. |

## Implementations and sequencing for wave 2

1. Switch the default source and replica driver collaborators to `SharedLog`.
   Keep `KafkaLog`, `LocalLog`, `MemoryLog`, and the Kafka and pg-source ingestion paths.
   Keep `ReadOnlyLocalLog` in concrete recovery and inspection, where it can expose prepared replay without implementing writes.
   Keep local byte offsets, Kafka partition offsets, timestamps, source and replica cut types, and legacy record decoding intact.
2. Move finite range capture, default checkpoint selection, storage lifetime management, and read allocation into typed default driver or storage operations.
   Replace `LogTail::replay` on the default path with the required prepared cursor.
   Keep the observed source offset that releases intake capacity and the replica offset that confirms the active term distinct.
3. Introduce the Multi journal adapter around the same pinned contract.
   Convert deployment UUIDs to the contract's `StreamId(u128)` at the Multi driver boundary with an explicit byte-order convention.
   Keep contract `Lsn` separate from the default `LogOffset`.
   Replay through the end returned by `seal`, including the genesis case, before resolving work.
   On `Unavailable`, retry the same `(epoch, at, payload)` or recover status from the ordered fold.
   On `Conflict`, read the committed position before deciding whether to retry the application operation.
   On `Fenced`, stop serving as writer.
4. Move timed-write settlement into the writers and fold.
   A timed `Tx` stays pending until its ordered `Settle { tx_lsn }` or `Refuse { tx_lsn }`.
   `Settle` attests that the original durable append completed before the exclusive cutoff within the qualified clock bound.
   A Kafka record timestamp alone cannot attest this.
   A late or unknown original outcome cannot justify `Settle`.
   A successor replays through the authority cut, preserves any committed decisions, and appends `Refuse` for unresolved timed transactions.
   The default processor confirms the decision on term-checked read-back.
   Multi waits for journal durability.
   The fold and `TxStatus` consume the same terminal decision in both modes.
5. Move installed `JournalLog` workloads to the contract and replace legacy `QuorumLog` hosted deployments through an explicit cutover.
   Move `SealedLog` encryption and object publication to RTDB writer and storage components.
   Replace Multi's `IntakeLog` with writer-owned staging and acknowledgement.
   Keep compatibility reads of existing installed histories until a verified migration or retirement path exists.
   Port `FaultLog`, `ClockedLog`, `ActorLog`, and default processor test doubles to their corresponding port, and retire hosted fixtures only once their contract coverage exists.

## Open integration decisions

- The Logkeeper specification supplies the exact empty-stream state, caller-bound seals, invalid-trim policy, and terminal `Deleted` result.
  The earlier RTDB sketch used different types and did not specify those edge cases.
  The wrapper forwards the pinned contract's types and outcomes unchanged.
- Keep the new Multi adapter and RTDB on the same contract revision so both sides share the Rust trait identity.
  Retire the separate ordered-runtime pin when the installed streams have cut over.
- The pinned development FileJournal is a synchronous append-only file.
  Trim releases its payload quota while the file retains earlier frames.
- Set an explicit compatibility and retention cut for installed streams before removing legacy hosted code.
  Decide which default deployment configurations can attest the pre-cutoff durability required for timed writes.
