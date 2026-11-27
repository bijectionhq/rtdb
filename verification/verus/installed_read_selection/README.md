# Installed read selection

The harness includes the shipped decision functions from the installed journal, the partition, and the node.
The laws apply to a fresh installed read under D01 and a query using the resulting capture.

| Lean definition or theorem | Shipped decision and Verus law |
| --- | --- |
| `InstalledRead.barrierMatchesInvocation` | `InstalledJournal::barrier` checks the full nonce and horizon before accepting the signed hold. `fresh_barrier_matches_invocation` rejects a different invocation. |
| `InstalledRead.readReleaseSettled` | `JournalLog::settle_reads` keeps the lost invocation until a successful `ReleaseRead` reaches its horizon. `release_preserves_the_retry_horizon` checks the inclusive decision-position test. |
| `InstalledRead.leaseAdmits` | `ServingLease::serving` checks an acquired or fallback deadline against the monotonic clock. `read_lease_expires_at_its_deadline` checks the strict comparison. The fast renewal path and lease admission and completion wiring are inspected separately. |
| `InstalledRead.FreshBarrier.cut`, `barrierOffset`, `exactReadCut`, `readCoversAcknowledgement` | `JournalLog::protect` selects the accepted hold's `through` cut. `exact_read_cut` preserves the fingerprint and converts the exclusive data LSN to the inclusive native offset, including the empty offset and overflow refusal. `read_covers_acknowledgement` bounds every included LSN after conversion. |
| `InstalledRead.captureAt`, `emptyCatalogMatchesCut`, `ReadProjection.usesSelectedCut` | `PartitionState::published_snapshot` requires all four catalog coordinates to match. `Database::capture_replica_snapshot` accepts the empty case only with no published cut and no transaction basis. `uses_selected_cut` and `empty_cut_requires_an_untracked_catalog` check those gates. |
| `InstalledRead.querySnapshot`, `queryUsesRead` | The `CatalogReadCut::Current` branch in `Node::query_context_in_placement` supplies the captured snapshot. `query_uses_read` checks that the query's snapshot comes from that capture even if the fallback could supply a different value. |

The native process compares Logkeeper request identities by their UUID and retry horizon.
The proof represents the UUID by its 128 bits and a monotonic time sample by its ordered scalar value.
The native capture compares storage version, storage epoch, replica epoch, and replica position.
RTDB's physical catalog coordinates do not carry the journal fingerprint.
Binding those coordinates to the authenticated cut requires the same attachment and the correct catalog publication by the replication driver.
The exact capture runs after the installed watermark reaches the selected offset, and a missed publication retries with a fresh barrier.
The query context also checks attachment identity and obtains the provider's catalog delivery lease.
The harness checks the pure decisions at these call sites.
The authenticated Logkeeper reducer, runtime ordering, clock qualification, and publication and query semantics are the remaining refinements described in the [X1 composition](../../multi/compose/README.md).

The linearization point is the chosen Barrier decision.
The corresponding Lean history can end with journal controls that leave the data LSN unchanged.
A later append can happen while the worker waits or materializes the read.
A query with a historical system-time cap, restored snapshot token, or a separate frozen path has its own time or capture semantics.
