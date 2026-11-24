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

The first unresolved Local delivery limit is an already admitted native read from an inline image or an in-memory view without a checkpoint.
Those views carry no generation for the delivery gate to recheck.
Withdrawing the corresponding object slot also cannot revoke bytes already retained in a cache lease.
A complete provider proof needs an authority that closes and drains or revokes those deliveries, covers exports and other copies, and establishes which generations have no surviving owners.
Object slot settlement covers the authenticated inventory under its original Classic vault.
It does not establish complete custody for asynchronous publications outside the census, older generation keys, or previously retained plaintext. [Generation selection](../../rust/crates/replication/src/serving.rs#L98) · [Delivery recheck](../../rust/crates/node/src/serving/default.rs#L105) · [Cache invalidation](../../rust/crates/storage/src/cache.rs#L395)

## Sealed Kafka

Kafka uses the same inventory with a coordinated sealed Local object store.
Offset continuity is checked through the ordered boundary before recording the inventory.
The Kafka vault independently validates and withdraws frame keys, while broker retention remains under Classic policy.
An exact durable Kafka intent can resume after the broker low watermark advances. [Kafka retirement](../../rust/crates/kafka/src/erasure.rs) · [Vault intent](../../rust/crates/storage/src/sealing/classic/kafka_retirement.rs)

For a new inventory, the first unavailable original envelope is the irreducible Kafka history limit.
`KafkaLog::range` rejects a scan below the broker low watermark, and the vault position census stores publication IDs and digests without detached object addresses.
An existing bound inventory can survive that loss.
An uncovered historical interval needs previously persisted object-reference evidence to establish the same proof. [Kafka range](../../rust/crates/kafka/src/lib.rs#L290) · [Position census](../../rust/crates/storage/src/sealing/classic/kafka_retirement.rs)

Public physical Erase remains gated.
Hosted object storage, external pg-source, copied histories without original custody, and deployments with incomplete prefix or reader evidence stay outside this route.
