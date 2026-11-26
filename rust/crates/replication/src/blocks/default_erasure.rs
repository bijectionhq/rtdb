use rtdb_crypt::Generation;
use rtdb_types::rtdb_v1::Basis;

use super::*;
use crate::{InstalledPrefix, NativeServingAuthority, serving::decode_checkpoint_image};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeErasureCut {
    pub basis: Basis,
    pub installed: InstalledPrefix,
}

pub struct VerifiedNativeErasureCheckpoint {
    checkpoint: PublishedCheckpoint,
    source: SourcePosition,
    generation: Generation,
}

impl VerifiedNativeErasureCheckpoint {
    pub fn checkpoint(&self) -> PublishedCheckpoint {
        self.checkpoint
    }
    pub fn source_through(&self) -> SourcePosition {
        self.source
    }
    pub fn generation(&self) -> Generation {
        self.generation
    }
}

impl BlockStore {
    /// Reads the exact successor of a pending native erasure from storage.
    /// The caller supplies the term-admitted boundary and upload read-back and holds the ordered cut through retirement.
    /// This proof covers the successor checkpoint and its replay coordinates.
    pub async fn verify_native_erasure_checkpoint(
        &self,
        logs: &PartitionLogs,
        authority: &NativeServingAuthority,
        cut: NativeErasureCut,
        boundary: &ReplicaRecord,
        upload: &ReplicaRecord,
    ) -> Result<VerifiedNativeErasureCheckpoint> {
        if !Arc::ptr_eq(&self.store, logs.storage()) {
            return Err(invalid_cut());
        }
        let checkpoints = authority
            .checkpoints
            .as_ref()
            .ok_or_else(missing_key_authority)?;
        let next = checkpoints
            .generation
            .checked_add(1)
            .ok_or_else(missing_key_authority)?;
        let ReplicaBody::BlockUploaded(uploaded) = &upload.envelope.message else {
            return Err(invalid_cut());
        };
        let index = uploaded
            .block_index
            .filter(|block| *block >= 0)
            .map(BlockIndex)
            .ok_or_else(invalid_cut)?;
        let root = uploaded
            .catalog_sha256
            .as_deref()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .ok_or_else(invalid_cut)?;
        let checkpoint = PublishedCheckpoint {
            block: index,
            boundary: boundary.metadata.position,
            root_sha256: Some(root),
        };
        if !follows_erasure(
            logs.source_epoch,
            logs.replica_epoch,
            cut,
            boundary.metadata.position,
            upload.metadata.position,
        ) || upload.envelope.term < boundary.envelope.term
        {
            return Err(invalid_cut());
        }

        IoRegion::reading_recovery(self.store.as_ref(), async {
            let (catalog, tables) = self
                .load_checkpoint(index, Some(checkpoint), Some(root))
                .await?;
            if !matches_catalog_boundary(&catalog, boundary)
                || !matches_confirmation(
                    &catalog,
                    uploaded,
                    logs.storage_version,
                    logs.storage_epoch,
                )
                || catalog.as_proto().latest_processed_msg_id != Some(cut.installed.source.0)
            {
                return Err(invalid_cut());
            }
            if catalog.external_source_token().is_some()
                || !catalog.secondary_databases().is_empty()
            {
                return Err(unsupported_files());
            }
            let expected = Generation {
                database: uuid::Uuid::from_bytes(authority.incarnation),
                number: next,
            };
            let id = sealed_image(&catalog, expected)?;
            if u64::try_from(boundary.envelope.term.0).ok() != Some(id.epoch) {
                return Err(invalid_cut());
            }
            let key = ObjectKey::new(id.to_string())?;
            self.store.head_fresh(&key).await?;
            let image =
                rtdb_store::read_file(self.store.as_ref(), checkpoints.kms.as_ref(), id).await?;
            if image.generation != expected {
                return Err(missing_key_authority());
            }
            let (fold, position, _) = decode_checkpoint_image(&image.bytes, authority.incarnation)?;
            if fold.pending_erasure() != Some(cut.basis)
                || fold.index().head().basis != cut.basis
                || position != cut.basis.position()
            {
                return Err(invalid_cut());
            }
            if let Some(evidence) = crate::serving::checkpoint_rewrite(&image.bytes)? {
                if evidence.cut.basis != cut.basis
                    || evidence.cut.source != cut.installed.source
                    || evidence.cut.replica != cut.installed.replica
                    || evidence.cut.term > catalog.boundary_term()
                    || fold.pending_erasure_installation() != Some(evidence.cut.installation())
                    || evidence.boundary != checkpoint.boundary
                    || crate::default_erasure::members_sha256(
                        self,
                        self.store.as_ref(),
                        crate::default_erasure::physical_members(&catalog, &tables)?,
                        &CancellationToken::new(),
                    )
                    .await?
                        != evidence.members_sha256
                {
                    return Err(invalid_cut());
                }
            } else {
                for entry in catalog.table_entries()? {
                    let table = tables.get(&entry.table).ok_or_else(invalid_cut)?;
                    if table.row_count() != 0
                        || !table
                            .checkpoint_trie_files(&entry, CheckpointContents::Replay)?
                            .is_empty()
                    {
                        return Err(unsupported_files());
                    }
                }
            }
            Ok(VerifiedNativeErasureCheckpoint {
                checkpoint,
                source: cut.installed.source,
                generation: image.generation,
            })
        })
        .await
    }
}

fn follows_erasure(
    source_epoch: StorageEpoch,
    replica_epoch: StorageEpoch,
    cut: NativeErasureCut,
    boundary: ReplicaPosition,
    upload: ReplicaPosition,
) -> bool {
    cut.installed.source.after_offset(source_epoch) >= 0
        && cut.installed.replica.after_offset(replica_epoch) >= 0
        && boundary.after_offset(replica_epoch) > cut.installed.replica.after_offset(replica_epoch)
        && upload.after_offset(replica_epoch) > boundary.after_offset(replica_epoch)
}

fn sealed_image(catalog: &BlockCatalog, expected: Generation) -> Result<ObjectId> {
    if catalog.as_proto().serving_checkpoint.is_some() {
        return Err(missing_key_authority());
    }
    let id = catalog
        .as_proto()
        .serving_checkpoint_object
        .as_deref()
        .ok_or_else(missing_key_authority)?
        .parse::<ObjectId>()?;
    if id.database != expected.database || id.kind != ObjectKind::File {
        return Err(missing_key_authority());
    }
    Ok(id)
}

fn invalid_cut() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/default-erasure-cut",
        "The checkpoint and replica read-back do not cover this exact erasure cut",
    )
}

fn missing_key_authority() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/default-erasure-checkpoint-key",
        "Default erasure requires a checkpoint sealed under the next generation",
    )
}

fn unsupported_files() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/default-erasure-files",
        "Default erasure requires a verified clean replacement for the Classic checkpoint files",
    )
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use rtdb_crypt::{Kms, LocalKms};
    use rtdb_format::envelope::ReplicaEnvelope;
    use rtdb_storage::{
        MemoryStore,
        log::{LogMetadata, LogOffset, LogRecord, MemoryLog},
    };
    use rtdb_store::WriterObjects;
    use rtdb_tx::transact::{Op, PHYSICAL_ERASURE, Resolution, Transact, TransactState};
    use rtdb_types::{EntityId, Term, TermSequence, rtdb_v1::ClaimSealer};

    use crate::{NativeCheckpointAuthority, serving::SHARED_CHECKPOINT};

    use super::*;

    #[test]
    fn recovery_coordinates_bind_both_log_epochs_and_ordered_readback() -> Result<()> {
        let source = StorageEpoch(2);
        let replica = StorageEpoch(3);
        let cut = NativeErasureCut {
            basis: Basis::new([1; 16], 1),
            installed: InstalledPrefix {
                source: SourcePosition::from_epoch_offset(source, 4)?,
                replica: ReplicaPosition::from_epoch_offset(replica, 5)?,
                image_digest: [0; 32],
            },
        };
        let boundary = ReplicaPosition::from_epoch_offset(replica, 6)?;
        let upload = ReplicaPosition::from_epoch_offset(replica, 7)?;
        assert!(follows_erasure(source, replica, cut, boundary, upload));
        assert!(!follows_erasure(replica, replica, cut, boundary, upload));
        assert!(!follows_erasure(source, source, cut, boundary, upload));
        assert!(!follows_erasure(
            source,
            replica,
            cut,
            cut.installed.replica,
            upload
        ));
        assert!(!follows_erasure(source, replica, cut, boundary, boundary));
        Ok(())
    }

    #[test]
    fn inline_and_foreign_checkpoint_images_have_no_retirement_evidence() -> Result<()> {
        let database = uuid::Uuid::new_v4();
        let expected = Generation {
            database,
            number: 7,
        };
        let foreign = ObjectId {
            database: uuid::Uuid::new_v4(),
            epoch: 1,
            seq: 0,
            digest: [3; 32],
            kind: ObjectKind::File,
        };
        let inline = BlockCatalog::from_proto(block_pb::Block {
            serving_checkpoint: Some(vec![1]),
            ..Default::default()
        })?;
        assert!(sealed_image(&inline, expected).is_err());
        let object = |id: ObjectId| {
            BlockCatalog::from_proto(block_pb::Block {
                serving_checkpoint_object: Some(id.to_string()),
                ..Default::default()
            })
        };
        assert!(sealed_image(&object(foreign)?, expected).is_err());
        let local = ObjectId {
            database,
            ..foreign
        };
        assert_eq!(sealed_image(&object(local)?, expected)?, local);
        Ok(())
    }

    #[tokio::test]
    async fn exact_readback_and_the_next_generation_prove_the_pending_erasure_checkpoint()
    -> Result<()> {
        let incarnation = [7; 16];
        let database = uuid::Uuid::from_bytes(incarnation);
        let generation = Generation {
            database,
            number: 1,
        };
        let kms: Arc<dyn Kms> = Arc::new(LocalKms::new(|| 0));
        kms.create(generation).await?;
        let mut authority = NativeServingAuthority {
            incarnation,
            checkpoints: Some(NativeCheckpointAuthority {
                kms: Arc::clone(&kms),
                generation: 0,
            }),
        };
        let request = Transact {
            key: vec![2; 16],
            require: vec![PHYSICAL_ERASURE.into()],
            window: Default::default(),
            guards: vec![],
            requires: vec![],
            otherwise: vec![],
            ops: vec![Op::Erase {
                branch: None,
                table: rtdb_types::TableRef::new("public", "docs"),
                ids: vec![EntityId::Integer(1)],
            }],
            custody_request_digest: None,
            request_memory: None,
        };
        let mut fold = TransactState::new(incarnation);
        let Resolution::Append(erasure) = fold.resolve(
            &request,
            1,
            Timestamp(10),
            &[PHYSICAL_ERASURE],
            &ClaimSealer::new(incarnation, [3; 32]),
        )?
        else {
            panic!("expected an erasure decision");
        };
        let receipt = fold
            .apply(1, erasure.into_record())?
            .expect("applied erasure receipt");
        let branch_cut = fold.index().head();
        assert_eq!(receipt.basis, branch_cut.basis);
        assert_eq!(fold.pending_erasure(), Some(branch_cut.basis));
        let mut image = BytesMut::new();
        image.put_u64(1);
        image.extend_from_slice(&branch_cut.basis.to_bytes());
        image.put_i64(branch_cut.system_time.0);
        image.put_u8(SHARED_CHECKPOINT);
        image.extend_from_slice(&fold.encode_checkpoint()?);

        let source_epoch = StorageEpoch(2);
        let replica_epoch = StorageEpoch(3);
        let storage_epoch = StorageEpoch(4);
        let storage_version = StorageVersion(1);
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let cache = Arc::new(BufferPool::new(Arc::clone(&store), 1 << 20));
        let memory = MemoryBudget::new(1 << 24);
        let blocks = BlockStore::new(
            Arc::clone(&store),
            Arc::clone(&cache),
            Arc::clone(&memory),
            1 << 20,
        )?;
        let logs = PartitionLogs::new(
            Arc::new(MemoryLog::new(1 << 20)),
            Arc::new(MemoryLog::new(1 << 20)),
            source_epoch,
            replica_epoch,
            storage_version,
            storage_epoch,
            Arc::clone(&store),
            cache,
            memory,
        )?;
        let source = SourcePosition::from_epoch_offset(source_epoch, 4)?;
        let cut = NativeErasureCut {
            basis: branch_cut.basis,
            installed: InstalledPrefix {
                source,
                replica: ReplicaPosition::from_epoch_offset(replica_epoch, 5)?,
                image_digest: [0; 32],
            },
        };
        let record = |offset, sequence, message| {
            let envelope = ReplicaEnvelope {
                term: Term(1),
                term_sequence: Some(TermSequence(sequence)),
                message,
            };
            logs.decode_replica(LogRecord::new(
                LogMetadata {
                    offset: LogOffset(offset),
                    timestamp: Timestamp(10),
                },
                envelope.encode().into(),
            ))?
            .ok_or_else(invalid_cut)
        };
        let boundary = record(
            6,
            1,
            ReplicaBody::BlockBoundary(proto::BlockBoundary {
                block_index: Some(1),
                latest_processed_msg_id: Some(source.0),
                ..Default::default()
            }),
        )?;
        use crate::default_erasure::{ErasureCut, RewriteEvidence};
        let evidence = RewriteEvidence {
            cut: ErasureCut {
                basis: cut.basis,
                source,
                replica: cut.installed.replica,
                term: Term(1),
            },
            boundary: boundary.metadata.position,
            members_sha256: [1; 32],
            inventory_sha256: None,
            prior_generation: None,
        };
        let prefix = &image[..41];
        let without_witness = evidence.frame(prefix, &fold.encode_checkpoint()?)?;
        assert!(decode_checkpoint_image(&without_witness, incarnation).is_err());
        fold.install_receipt_position(None, &receipt, evidence.cut.installation().1)?;
        let state = fold.encode_checkpoint()?;
        let authenticated = evidence.frame(prefix, &state)?;
        assert!(decode_checkpoint_image(&authenticated, incarnation).is_ok());
        let wrong_term = RewriteEvidence {
            cut: ErasureCut {
                term: Term(2),
                ..evidence.cut
            },
            ..evidence
        };
        assert!(decode_checkpoint_image(&wrong_term.frame(prefix, &state)?, incarnation).is_err());
        let prepared = WriterObjects::new(database, 1)
            .file(kms.as_ref(), generation.number, &image)
            .await?;
        prepared.put(store.as_ref()).await?;
        let mut catalog = BlockCatalog::from_proto(block_pb::Block {
            block_index: Some(1),
            latest_processed_msg_id: Some(source.0),
            boundary_replica_msg_id: Some(boundary.metadata.position.0),
            term_id: Some(1),
            boundary_term_seq: Some(1),
            table_block_manifest: Some(block_pb::TableBlockManifest::default()),
            serving_checkpoint_object: Some(prepared.id().to_string()),
            ..Default::default()
        })?;
        store
            .put(
                &ObjectKey::new(block_key(BlockIndex(1))?)?,
                Bytes::from(catalog.encode_for_publication()),
            )
            .await?;
        let upload = record(
            7,
            2,
            ReplicaBody::BlockUploaded(proto::BlockUploaded {
                storage_version: Some(storage_version.0),
                storage_epoch: Some(storage_epoch.0),
                block_index: Some(1),
                latest_processed_msg_id: Some(source.0),
                catalog_sha256: Some(checkpoint_digest(&catalog)?.to_vec()),
                ..Default::default()
            }),
        )?;
        let proven = blocks
            .verify_native_erasure_checkpoint(&logs, &authority, cut, &boundary, &upload)
            .await?;
        assert_eq!(proven.source_through(), source);
        assert_eq!(proven.checkpoint().boundary, boundary.metadata.position);
        assert_eq!(proven.generation(), generation);
        authority.checkpoints.as_mut().unwrap().generation = 1;
        assert!(
            blocks
                .verify_native_erasure_checkpoint(&logs, &authority, cut, &boundary, &upload)
                .await
                .is_err()
        );
        Ok(())
    }
}
