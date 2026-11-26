# Default erasure inventory and provider custody

The ordered Default rewrite uses the applied partition and the existing SQL survivor builder.
The sealed successor authenticates the survivor files, the original receipt installation, and the digest of the sealed retirement inventory.
The inventory carries names and identities of old objects rather than their contents.
The writer publishes it before publishing the successor root or closing either log prefix. [Survivor publication](../../rust/crates/replication/src/default_erasure.rs) · [Inventory capture](../../rust/crates/replication/src/default_erasure/inventory.rs)

- Old checkpoint roots are read from the storage residence and checked against the original source and replica epochs and cut.
  Their exact table dependencies include primary tries, replay artefacts and serving images.
  A root holding an inline image is itself its retirement object.
  A sealed serving image adds its own object key.
  The census also covers unreferenced files in the serving checkpoint namespace from older writer terms.
- The replica interval from the first position through the survivor boundary supplies the original oversized envelope references.
  The inventory includes references in stale envelopes because their detached bodies were still published.
  A local name census includes remaining table artefacts, older orphaned oversized bodies and skipped-transaction objects through the source cut.
  A foreign versioned offload reference requires its original provider authority.
- Read-back authenticates the inventory digest and checks that the old objects are disjoint from the successor.
  After a restart, settlement opens the authenticated inventory without reading retired log frames or old checkpoint objects.

## Sealed Local storage

The ordered control binds the inventory, replacement root, source position, replica boundary, upload position and deadline.
The Local provider persists the exact recovery floor before settling source and replica frame keys.
After both prefixes settle, it closes admission to earlier catalog cuts and waits for existing catalog tickets and the original deadline.
The original vault closes per-object key slots before the provider unlinks their objects.
The Local ledger persists the name digest and settlement cursor, so repeating an interrupted batch targets the same keys. [Local custody](../../rust/crates/storage/src/local_erasure.rs) · [Catalog tickets](../../rust/crates/storage/src/local_catalog.rs)

Set `defaultLocalErasure: true` (or `RTDB_DEFAULT_LOCAL_ERASURE=1` when the key is absent) with `defaultV1Serving`, `RTDB_CLASSIC_KEY_VAULT` and `RTDB_V1_CHECKPOINT_KEY_VAULT` to enroll a fresh native Local primary for public v1 Erase.
The payload and journal directories and both private vaults must belong to the same original installation.
The Local provider serializes enrollment with attachment, verifies that the payload and journal locations are pristine, and durably binds their directory and log identities to the checkpoint vault and database incarnation.
The versioned storage marker requires this authority at reopen.
Existing Classic histories and legacy sealed enrollments retain their current transaction paths without public Erase capability.
The enrollment only covers the installation's own storage and delivery paths. [Original Local enrollment](../../rust/crates/storage/src/sealing/classic/local_erasure.rs)

The enrolled profile serves Classic SQL and unscoped v1 Erase.
It stops background compaction, derived artefact publication and automatic collection on that residence so the inventory has one publication owner.
Raw inspection, maintenance reset, and retained or offline export have no admission on this profile.
A query reading this profile still spills, but each spill file is encrypted under a key made for that file and held only in the query's memory, so no readable copy of an erased row outlives the query or the process. [Sealed spill](../../rust/crates/query/src/spill.rs)
Ordinary block writes stage Arrow data through unnamed file descriptors under the original Local I/O owner.
The provider forbids independent object copies and shared decrypted cache entries. [Node profile](../../rust/crates/node/src/database.rs) · [Private publication](../../rust/crates/replication/src/blocks.rs) · [Exact-read cache](../../rust/crates/storage/src/cache.rs)

## Completed erasure

The confirmed writer selects the sealed successor and closes predecessor admission before sampling the reader deadline.
The Local vault closes generation admission, and the catalog ledger closes older Classic cuts.
Native Query and Changes retain their original generation through response delivery.
Classic SQL retains the catalog ticket through delivery and rechecks the floor when opening or reusing a held catalog.
Bounded grants cover already admitted transport.
The provider records its exact closure witness after both kinds of holds drain. [Provider fence](../../rust/crates/replication/src/default_erasure/local_read.rs) · [Catalog holds](../../rust/crates/query/src/context.rs)

The ordered retirement intent arms the predecessor's KMS deadline.
After durable closure, the writer settles both log prefixes and the exact object inventory.
The writer destroys the predecessor and orders completion after the original deadline.
Read-back and restart verify the same closure, completed object cursor, destroyed predecessor and readable successor before arming the namespaced canonical receipt.
Later checkpoints write under the successor.
A later erasure skips a retired Local replica prefix only when its journal proof matches the completed object inventory and recovery root. [Driver order](../../rust/crates/replication/src/driver/default_erasure_runtime.rs) · [Generation custody](../../rust/crates/crypt/src/file.rs)

## Sealed Kafka

Set `defaultKafkaErasure: true` (or `RTDB_DEFAULT_KAFKA_ERASURE=1` when the key is absent) with `defaultV1Serving`, `RTDB_CLASSIC_KEY_VAULT`, and `RTDB_V1_CHECKPOINT_KEY_VAULT` for a fresh Default primary with Kafka logs and a Local object store.
The original writable node opens dedicated single-partition source and replica topics and enrolls each broker topic ID in its own role under the Local residence's Classic vault.
The Local storage path and private checkpoint vault bind to the same enrollment.
The first open creates the empty topics through the native Kafka provider with `autoCreateTopic` enabled.
Reopen requires the original topic IDs, object residence and key vaults.
Legacy Kafka uses a separate frame key namespace and its existing broker-floor settlement.

The enrolled Kafka topics keep an uncompacted suffix with automatic broker retention disabled.
Public Erase requires the active source and replica positions to be covered by the original sealed offset census and the recoverable suffix to begin at or before the exact Local recovery cut.
The writer checks this evidence before deciding a new Erase.
Retained broker frames can repair an unobserved suffix offset after an ambiguous acknowledgement.
An exact retry of a decided request reads its original ordered outcome.

Kafka uses the same inventory and drain as the sealed Local route.
Classic SQL, native Query and Changes use the original catalog or generation holds.
The enrolled profile excludes independent exports, plaintext spills, shared decrypted caches, row custody and background artefact publication.
Offset continuity is checked through the ordered boundary before recording the inventory.
The Local recovery fence must cover the erased prefix before either Kafka key floor advances.
The Kafka vault independently validates and withdraws frame keys.
The ordered terminal verifies both exact settled topic cuts before releasing the original receipt.
An exact durable Kafka intent can resume after the broker low watermark advances within the prefix covered by Local recovery. [Kafka retirement](../../rust/crates/kafka/src/erasure.rs) · [Vault intent](../../rust/crates/storage/src/sealing/classic/kafka_retirement.rs)

A sealed Default replica topic persists reference evidence on acknowledgement and can backfill it when opening a retained frame.
The vault binds each evidence record to the original offset and sealed publication identity.
It records either a detached address or an explicit inline frame and preserves both across Classic broker-floor settlement.
A new inventory requires evidence for every covered coordinate below the broker or key floor and scans the remaining interval directly.
A position that expired before its evidence was recorded remains irrecoverable to this route. [Kafka range](../../rust/crates/kafka/src/lib.rs#L295) · [Reference evidence](../../rust/crates/storage/src/sealing/classic/kafka_references.rs)

Public physical Erase remains gated on shared Kafka attachments, hosted object storage, external pg-source, copied histories, and deployments without their original enrollment and reader authorities.
