# R6 · Seal durable units within a custody domain

## Contract

Every durable data unit is sealed under an independently destroyable data key.
A custody domain is the authority that can enumerate and revoke every means of decrypting its units, including active grants, caches, recoverable key versions and backups.
A storage copy within the domain retains that identity.
An independent copy receives a fresh domain, new unit identities and new keys before publication.
A same-domain fork holds references to the source's units and follows erasure of their shared history.
A cross-organization export or an independent archive publishes re-encrypted data under its own authority.
A move that preserves the same logical database transfers custody with the history and obtains a receipt retiring the predecessor's serving path.

## Unit publication

The passive unit identity binds domain, purpose, generation, unique publication and chunk number where applicable.
Create an opaque key slot for that exact publication using conditional creation.
A losing writer opens the winning publication or abandons its unpublished unit.
This resolves concurrent job convergence at publication, so ciphertext from separate attempts may differ.
Authenticate the unit label and chunk coordinates.
Keep a single logical publication inside the root even when a provider retry created several physical versions.
The residence ledger inventories all recoverable wraps and copies before a unit can appear in a root.

Persisting a recoverable wrapped data key in an immutable backup would preserve decryption after ledger deletion.
Use key slots with provider-backed revocation and an inventory of any recoverable wrapping versions.
Decrypted pages need an authority-bound lease and invalidation tied to the slot.
Persistent caches contain ciphertext.
Log segments need their own slot and a manifest that describes the replacement root and replay interval.

## Enrollment

The provider must offer a permanent create-once lineage anchor or equivalent non-reusable storage identity.
Opening existing encrypted storage requires its exact anchor and key identity.
Provisioning is conditional on creating a new anchor and binding the key slot to it.
An unknown create outcome is resolved against the same attempt identity.
An existing or tombstoned anchor forbids enrollment, including after loss of directory metadata.
`KeyLineageEnrollment.tla` checks binding an installed key to its claimed anchor, refusal of re-enrollment over surviving ciphertext, and terminal withdrawal.
Its transitions assume durable conditional creation of the anchor.
The provider tests must exercise an object surviving stale or missing directory data.
The current Verus decision about boolean enrollment predicates remains applicable only after these premises have been established.

## Object roles and implementation boundary

The key service is a passive collaborator that exposes enrollment, opening and revocation for an exact slot.
The residence authority owns atomic publication of roots and their key dependencies.
The active eraser coordinates preparation and publication with that authority.
The key-grant monitor owns the current grant generation and wakes revoked readers.
Classic and Multi storage can share the sealing format and the custody contract.

Installed journal serving grants and checkpoint allocations use independently generated slots for immutable objects.
Their key-service identity binds the journal placement, provider, purpose, physical namespace, and logical object name.
The local vault serializes enrollment and withdrawal against key use, and the S3 vault conditionally creates slots in a dedicated never-versioned bucket.
Both providers retain irreversible slot tombstones.
The checkpoint manifest inventories the published members and seals under its own slot.
Checkpoint preparation copies verified plaintext into the destination allocation before publication, so the successor can open without obtaining the writer's object keys.
Local retirement fences the exact vault namespace, drains key operations, and withdraws the entire slot census before removing checkpoint or serving-prefix ciphertext.
The census includes slots created by interrupted writes.
The S3 vault registers each slot in a conditionally updated shard before issuing the provider key create.
Scope withdrawal closes the namespace and freezes each shard before tombstoning the registered slots, including registrations whose create result was lost.
A late conditional create for a registered slot races with the terminal tombstone on that same slot.
The serving and checkpoint retirement owners invoke the same vault settlement after their existing admission and drain checks.
The DynamoDB vault orders each conditional unit creation against a scope fence in a transaction and confirms the fenced slot census before reporting withdrawal.

Local and S3 checkpoint captures use separate slots for their objects and replica archive records.
Capture cryptography combines the slot with an indispensable capture authority secret, so withdrawing the capture also disables any retained slot.
An ordered checkpoint transfer stores the verified manifest, segments and detached payloads under the destination's slot namespace before seeding its live journal.
Primary and replay stage inventories place each member and manifest in separate destination slots. The directory freezes the inventory before transfer. Activation verifies its members, writes and reopens the serving closure, then withdraws the stage namespace through the original vault. A retry uses the accepted activation receipt before resuming withdrawal. Disposal also sweeps the separate stage namespace, including interrupted slots. Detached replay bodies remain in the installed broker's separate custody. Directory-managed replica logs register one key slot per frame. A cross-cell move and an ordered transfer reseal certified source pages under independent destination slots. Same-residence ensemble repair preserves the existing frames and keys. Residence disposal closes and withdraws all registered journal slots. The directory trim grant binds its authenticated per-frame census before truncation and completes only after the vault withdraws those slots. Directory-managed serving objects carry their original publication residence in the sealed envelope. A pack or same-domain fork preserves the envelope and opens the original slot in the lineage vault.
Classic local, S3, Azure and GCS object stores can enroll in the same sealed format with a private filesystem vault supplied through `RTDB_CLASSIC_KEY_VAULT`.
The vault directory must already exist and belong to the original installation outside payload and disk-cache backup roots.
The immutable provider marker selects the vault lineage and refuses enrollment over existing unsealed data.
The coordinated trie release authority authenticates the installed receipt and drains the eligible root before it asks the vault to withdraw the selected slots.
Classic local source and replica logs seal each frame under a fresh publication slot, with the physical offset and sampled timestamp authenticated.
Reopening a configured sealed log requires its original key lineage.
Classic Kafka database logs use the same vault with a namespace bound to the broker's topic UUID. Only the writer that confirms creation of an empty topic can enroll it. A reader or replacement must open that original namespace. The broker orders the resulting publication IDs at their physical offsets.
The Classic retained-repair anchor grants offline prefix retirement. The original vault verifies each selected sealed frame and records its exact withdrawal intent before wiping the ciphertext. The surviving replica boundary and suffix keep their keys. Kafka publishes each durable offset and ciphertext identity into the original topic vault before acknowledging the append. A writable owner confirms the broker floor under the same topic identity and drains exact slots through that floor in bounded batches. A missing offset census stops settlement. Remote retirement outside exact installed release grants remains open.
Provider verification includes ranged reads, object naming, lost create replies, redundant compaction jobs, independent-copy isolation, cache fencing, and nonce uniqueness.
Independent forks and directory cell moves open authenticated loose objects and pack members under their original slots and reseal each member into a destination slot. A pack rewrite preserves its predecessor list and publication time. An admitted archive overlay exposes the original archive ciphertext and opens it through the capture key authority. The directory retirement owner withdraws original publication scopes after the last co-custodied residence is disposed. It also withdraws the scopes of closed unpublished transfer generations and rejected forks. Scope retirement includes orphan registrations. Enrolled terminal S3 authenticates eligible loose publications, checks relocated copies and withdraws their last slots under the exact retirement grant.
Local retirement checks descendant references and physical loose and packed copies under the volume and lineage reservation.
It withdraws slots for eligible loose publications and members of obsolete packs before removing their last ciphertext.
Versioned S3 reads exact version prefixes and checks all data versions of every loose alias before withdrawing a slot.
The exact directory grant carries the original slot through ciphertext removal and retry.
Terminal S3 checkpoint metadata pack retirement binds a durable slot plan to the exact pack grant.
The plan derives each slot from the authenticated pack member and excludes slots with loose or packed aliases elsewhere in the lineage.
Other hosted packs and orphan slots in an open origin still need a complete publication census.

[Current cipher](../../rust/crates/storage/src/sealing/cipher.rs) · [Key service](../../rust/crates/multi/src/keys.rs) · [Fork references](../../rust/crates/multi/src/reference.rs) · [R7](R7-eraser.md)
