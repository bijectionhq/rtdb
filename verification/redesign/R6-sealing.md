# R6 · Seal every durable unit under destroyable data keys

## Goal

Every byte RTDB writes (log records, trie data and metadata, artefacts, checkpoints, block files) is ciphertext under a data key that can be destroyed.
Destroying a key must make the unit and every cached copy unreadable, wherever copies sit: object versions, Kafka records, replicas, backups and forks.
This is what lets R7 meet the 120 s erasure bound without rewriting append-only logs in place.

## Converged design

- **Units and keys.** Each durable unit gets its own data key (DEK): a log segment (a log range closed at each block flush), a trie file, an artefact, a checkpoint.
- **Wrapping.** DEKs are wrapped under a store key-encryption key (KEK) and listed in the ledger's key catalog (R4).
  Designer I proposed per-entity keys derived from a shard key epoch instead, which avoids rewriting files on erasure; the other two kept per-unit keys and rewrite successors (R7). Per-unit keys are the converged choice.
- **Cipher.** Reuse Multi's AES-256-GCM cipher with name-bound associated data and deterministic object nonces (`rust/crates/multi/src/cipher.rs`), moved down to the storage layer so classic and Multi deployments share it.
  Deterministic nonces keep put-if-absent convergence for redundant compactors.
- **Clear material stays inside the ciphertext.** Ordered-index keys, blinded values and unit names remain plaintext inside sealed units, so sort order and lookups work after decryption and erasure still reaches them.
- **Caches.** Decrypted pages are keyed by (object identity, DEK id). A destroyed DEK invalidates them: in-memory caches drop entries, and on-disk caches hold ciphertext only.
- **Key custody.** KEKs live in a key service that can destroy keys durably. Multi's `KeyService` (`rust/crates/multi/src/keys.rs`, with `LocalKeyService` and the `keys/` backends) already provisions, copies to forks and withdraws per-deployment keys with deletion receipts; R6 extends it to wrap per-unit DEKs.

## Invariants

- **S1.** No durable unit is written in the clear.
- **S2.** A unit whose DEK is destroyed cannot be read through any cache or copy after the destruction notice is observed (bounded by the notice latency, which R7 budgets).

## State on main (2026-11-21)

- Multi already seals every object and its log under one per-deployment key: `EncryptingStore` in `rust/crates/multi/src/store.rs` (wrapped at `store.rs` around line 269), `Cipher` in `cipher.rs`. `multi/tests/cell.rs` `nothing_a_deployment_stores_is_readable_at_rest` checks that no plaintext marker reaches disk.
- Classic deployments write everything in the clear.
- There are no per-unit keys and no key catalog yet.

## Handoff notes

1. Move `Cipher` and `EncryptingStore` into `rust/crates/storage` (or a small crate it depends on) and make Multi use them from there; delete Multi's copies.
2. Introduce the DEK hierarchy behind the existing `ObjectStore` boundary, so callers above storage do not change: a sealed store resolves a unit's DEK from the key catalog, decrypts on read and encrypts on write.
3. Seal log payloads per segment for both local logs and Kafka; segment boundaries at block flush keep the R7 destruction granularity aligned with checkpoints.
4. Classic key custody needs a decision: a local key file with real deletion for development, and a KMS-backed service for production. Mirror Multi's `KeyService` trait rather than inventing a second one.
5. Key caches by DEK id now, even before R7 destroys anything, so R7 only adds the destruction notice.
6. Watch performance: deterministic-nonce AES-GCM over whole objects means ranged reads decrypt whole units unless units are chunked. Decide the chunking before landing.

## Tests and proofs

- **Native:** extend the at-rest test to classic deployments and to logs, checkpoints, artefacts and caches; destroying a DEK makes the unit and its cached pages unreadable; redundant compactors still converge with put-if-absent.
- **Verus:** the name-bound associated data and nonce derivation (a unit's ciphertext cannot be substituted for another's).
- **Mutations that must fail:** writing one unit class in the clear (the at-rest scan finds the marker); serving a cached page after its DEK is destroyed.

## Open questions

- Classic KEK custody (local file versus KMS) and the supported KMS providers.
- Chunk size for sealed units, and its interaction with ranged reads and the disk cache.
