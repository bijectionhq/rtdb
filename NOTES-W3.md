# W3 storage and GC handoff

## Delivered

- `rtdb_format::journal` owns the eight version-one journal record tags and the checkpoint manifest format.
  The tags are Tx=1, Checkpoint=2, Lease=3, Pin=4, Unpin=5, KeyRotate=6, Settle=7, and Refuse=8.
  Tx carries opaque resolved bytes and an explicit list of object dependencies.
  Journal positions use the unsigned values of Logkeeper's `journal-contract::Lsn` at the codec boundary.
  Settle and Refuse name the original timed Tx position.
  The retained Tx continues to protect its dependencies after either decision.
  Lease, pin, and key deadlines use UTC microseconds.
- `rtdb_crypt` provides authenticated envelope encryption, generation-scoped key custody, and a process-local KMS.
  A journal envelope binds its database and LSN.
  An object envelope binds its database, kind, epoch, and sequence.
- `rtdb_store` provides the epoch allocator, verified reads, reachability, exact-key deletion, and erasure preparation and execution.
  It uses the existing `ObjectStore` implementations so the wave can be integrated without changing the active Multi runtime.

## Wave 2 integration

- Acquire a fresh, strictly higher epoch before constructing a replacement `WriterObjects`.
  Allocate once per creation and retry the same `PreparedObject` when a provider reply is uncertain.
  The digest covers the stored ciphertext and is checked before decryption.
- In Multi mode, build `Reachability::from_records` from a complete committed snapshot after the seal, starting at the retained floor.
  Pass verified manifests for every checkpoint and pin root and for manifest dependencies declared by retained transactions.
- In default mode, call `Reachability::from_roots` with the complete retained closure from the confirmed replica-log term.
  Include current block and table catalogs, retained replica-message dependencies, exact pins, and inputs of admitted reads and compaction work.
  Let the term's allocator assign all durable output names, including outputs produced by background compaction workers.
  A result from a deposed term can be published only if its earlier-epoch dependencies remain reachable and its new output receives a current-term allocation.
- For either mode, serialize reachability changes with publication.
  At publication, validate the entire proposed object closure against the current reachability snapshot.
  Publish only uploaded allocations from this writer's epoch or earlier keys present in that closure.
  Recompute the snapshot as the floor advances.
  The resulting old-epoch deletes remain valid after handoff, even when the list or provider response is delayed.
  A delayed upload may leave an orphan for a later sweep.
- Carry active pins in each checkpoint image before advancing the floor.
  Serialize pin admission with checkpoint publication and trim.
  Use a fresh pin UUID for each admission.
  Treat `until` as an admission deadline and append Unpin once all admitted operations have drained.
  A retained Pin record remains a GC root after Unpin until the record and any checkpoint image referencing it trim away.
  Cross-database forks must acquire independent destination backing before releasing source custody.
- Seal a journal payload once at its intended LSN and retry identical ciphertext after an uncertain append.
  Reuse current historical files for an in-database old-basis branch.
  A caller that holds decrypted output must recheck generation readability at delivery.
  A checkpoint must cover unresolved timed Tx state and dependencies before trimming their original records, or wait for Settle or Refuse.
- Supply a clean checkpoint image covering the preceding LSN and rebuild every file needed by recovery under the next generation.
  Hold the cut until the adjacent checkpoint and KeyRotate commits settle.
  Record durable rotation timing, recover and arm the old generation's deadline, and then trim below the checkpoint record.
  Keep the recovery metadata and both journal entries until the deadline is armed.
  When the deadline elapses, call `Retirement::destroy` and retry until it succeeds.

## Replacement map

W3 deletes no modules.
Default Kafka, local log, pg-source, and installed Multi retain their existing storage paths during integration.

| Candidate for removal when its callers migrate | Evidence |
| --- | --- |
| `multi/src/store/exact_retirement.rs` and `exact_retirement/tombstones.rs` | These retire the original provider version under journal withdrawal and settle a tombstone. The new allocator and publication rule give each admissible file a unique key with permanently monotonic reachability after collection. |
| `multi/src/store/publication_retirement.rs` | `pack_plan` and `exclude_versioned_aliases` solve shared provider publication identity. The new manifest names the database's own immutable files directly. |
| `multi/src/store/replay_retirement.rs` and `replay_retirement/hosted.rs` | These authorize replay-payload disposal. The retained Tx object's dependency list keeps the payload in the GC closure until its record trims. |
| `multi/src/journal/hosted/prefix_retirement.rs` | The installed workflow closes old prefix authority before certifying physical disposal. The new stream fences publication by epoch, and delayed uploads under old keys are unreachable orphans. |
| `multi/src/recovery_holds.rs` | `hold`, `register`, and `retain_offloaded` attach directory holds to exact replay positions and their offloaded payloads. The new path selects the retained checkpoint and suffix from its journal and uses ordered pin records for admitted manifest exports and forks. |
| `multi/src/keys/assertion.rs`, `keys/assertion/`, and `keys/assertion_deadline.rs` | The installed path keeps assertion-stage keys, separately admitted custody, and destruction settlement. The new file and journal envelopes use database key generations. A clean checkpoint, rotation and durable KMS retirement replace assertion-stage destruction for the new stream. |

Removal requires the old provider version, prefix retirement, replay and assertion endpoints to have no callers in either mode.
The new GC planner establishes logical reachability safety.
Provider version reclamation and erasure deadline qualification are wave 2 integration responsibilities.

## Design interpretations and open questions

- The brief names C1–C4, while Study 118's published traces name individual invariants.
  The four cases exercised here map to delayed create and delete, exact export pin custody, recovery racing trim, and in-database old-basis branches.
  The tests enumerate delay orderings and listing subsets without any wall-clock grace period.
  Default mode tests supply block, replica payload, export, and compaction roots independently of the Multi record replay tests.
- The committed closure is the writer's admission boundary.
  Epoch separation alone would permit resurrection if a publisher could attach an unreachable old object found in a listing.
  A Pin deadline by itself would also open a deletion race with an admitted export.
- The design calls for the floor to contain the checkpoint record.
  The erasure plan interprets cut `c` as that record's LSN, has the image cover `c - 1`, appends KeyRotate at `c + 1`, and trims through `c - 1`.
- Logkeeper's Journal reports an LSN without a durable commit timestamp.
  The erasure executor accepts append and trim callbacks from either mode's driver and uses the rotation's original durable commit time so an uncertain reply or restart cannot shift the deadline.
  Wave 2 must choose its ordered timestamp source and close the interval between a committed rotation and KMS retirement for all serving nodes.
  Recovery must be able to locate the clean checkpoint if trim remains unavailable through the old generation's deadline.
  A production KMS must enforce the deadline and retain its decision and destroyed-key identities across restart.
  `LocalKms` is process-local and suits tests and in-memory databases.
  Persistent dev storage needs durable key custody.
- GC fails closed when any retained root's manifest cannot be verified.
  A pin to an erased generation can defer collection until its release and checkpoint trim remove the last root, unless wave 2 separately retains the authenticated closure metadata.

## Verification

- `guarded-cargo.py rtdb test --locked -p rtdb-format --lib journal::tests` passed 4 record and manifest tests.
- `guarded-cargo.py rtdb test --locked -p rtdb-crypt -p rtdb-store --lib tests::` passed 2 crypto tests and 11 storage tests.
- `guarded-cargo.py rtdb test --locked -p rtdb-store --lib tests::` passed all 11 storage tests after the final rebase and the branch-versus-trim test update.
- The guard was run from the W3 worktree's `rust` directory using Rust 1.94.1.
  The compiler reported warnings at existing code paths and none at W3 source paths.
- The earlier pre-update bare Cargo checks passed 4 format, 2 crypto, and 9 storage tests on the earlier revision.
- The workspace suite, Gradle, and end-to-end default and Multi runtime tests remain unverified.
  The Allium CLI was unavailable, so the contracts were reviewed against the language reference.
