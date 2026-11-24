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

Set `RTDB_V1_CHECKPOINT_KEY_VAULT` to an existing private directory outside the Classic and token key vaults to attach a checkpoint generation to Default v1 serving.
The same vault must be available on restart and when authenticating a sealed image during offline repair.
Use `--checkpoint-key-vault` to supply it to the repair command.
Configured inline and initial in-memory views carry the initial generation. A view admitted without a configured vault carries no generation for the delivery gate to recheck.
Withdrawing the corresponding object slot also cannot revoke bytes already retained in a cache lease.
A complete provider proof needs an authority that closes and drains or revokes those deliveries, covers exports and other copies, and establishes which generations have no surviving owners.
Object slot settlement covers the authenticated inventory under its original Classic vault.
It does not establish complete custody for asynchronous publications outside the census, older generation keys, or previously retained plaintext. [Generation selection](../../rust/crates/replication/src/serving.rs#L98) · [Delivery recheck](../../rust/crates/node/src/serving/default.rs#L105) · [Cache invalidation](../../rust/crates/storage/src/cache.rs#L395)

## Completed erasure

A terminal-capable provider must atomically close admission for held native and Classic reads, plaintext caches and exports.
Its durable witness binds the exact original deadline and establishes exclusive authority over the predecessor and coverage of earlier generations.
A held copy that can still deliver beyond that deadline prevents the witness.
The checkpoint vault configuration alone supplies no such witness. [Read-fence contract](../../rust/crates/replication/src/default_erasure/terminal.rs)

The confirmed writer requires the closure before settling log prefixes and objects.
After settlement and the original deadline, the durable KMS destroys the exact predecessor while keeping the authenticated successor readable.
The writer orders a completion after the original retirement intent.
Read-back and restart verify the closure, completed object cursor, destroyed predecessor and readable successor before arming the exact canonical receipt.
Later checkpoints write under the installed successor and retain the armed receipt. [Driver order](../../rust/crates/replication/src/driver/default_erasure_runtime.rs) · [Generation custody](../../rust/crates/crypt/src/file.rs)

## Sealed Kafka

Kafka uses the same inventory with a coordinated sealed Local object store.
Offset continuity is checked through the ordered boundary before recording the inventory.
The Kafka vault independently validates and withdraws frame keys, while broker retention remains under Classic policy.
An exact durable Kafka intent can resume after the broker low watermark advances. [Kafka retirement](../../rust/crates/kafka/src/erasure.rs) · [Vault intent](../../rust/crates/storage/src/sealing/classic/kafka_retirement.rs)

A sealed Default replica topic persists reference evidence on acknowledgement and can backfill it when opening a retained frame.
The vault binds each evidence record to the original offset and sealed publication identity.
It records either a detached address or an explicit inline frame and preserves both across Classic broker-floor settlement.
A new inventory requires evidence for every covered coordinate below the broker or key floor and scans the remaining interval directly.
A position that expired before its evidence was recorded remains irrecoverable to this route. [Kafka range](../../rust/crates/kafka/src/lib.rs#L295) · [Reference evidence](../../rust/crates/storage/src/sealing/classic/kafka_references.rs)

Public physical Erase remains gated.
Hosted object storage, external pg-source, copied histories without original custody, and deployments with incomplete prefix or reader evidence stay outside this route.
