//! Custody of a detached publication prepared for one destination position.

use rtdb_s3::{CreateAttemptPublication, VersionedInventory};

use super::*;

pub(super) fn transfer_pointer(key: &ObjectKey, envelope: &ReplicaEnvelope, body: &[u8]) -> Result<ReplicaEnvelope> {
    let block = oversized_block_index(key.as_str()).ok_or_else(invalid_frame)?;
    let allocation = key.as_str().split_once('-')
        .and_then(|(_, suffix)| suffix.strip_suffix(".binpb")).ok_or_else(invalid_frame)?;
    Ok(envelope.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), block, allocation.to_owned(), body))
}

pub(super) fn transfer_pointer_digest(pointer: &ReplicaEnvelope, version: &PhysicalObjectVersion) -> Result<[u8; 32]> {
    let mut pointer = pointer.clone();
    pointer.bind_offload_publication(version.clone())?;
    Ok(ring::digest::digest(&ring::digest::SHA256, &pointer.encode())
        .as_ref().try_into().expect("SHA-256 width"))
}

pub(super) fn current_generation_abandoned(record: &DeploymentRecord) -> bool {
    record.storage_generation.is_some_and(|generation| record.abandoned_generations.contains_key(&generation))
}

pub(super) fn transfer_excluded(record: &DeploymentRecord, route: PublishRoute) -> bool {
    match route {
        PublishRoute::Transfer { operation, bootstrap, .. } => {
            record.lifecycle == Lifecycle::Deleting
                && record.checkpoint_stage.as_ref().is_some_and(|stage| stage.transfer_only && stage.intent.operation == operation)
                && record.checkpoint_transfer_replica.as_ref().is_some_and(|replica| replica.bootstrap.ciphertext_sha256 == bootstrap)
                && record.checkpoint_transfer_activation.is_none()
        }
        PublishRoute::CellTransfer { generation, .. } => {
            record.lifecycle == Lifecycle::Moving && record.move_from.is_some()
                && record.storage_generation == Some(generation)
                && record.abandoned_generations.get(&generation).is_some_and(|closed| {
                    closed.prefix == record.storage_prefix()
                        && record.storage_namespaces == Some(closed.namespaces)
                })
        }
        PublishRoute::Writer { .. } => false,
    }
}

impl Service {
    /// A retry keeps the same position and body. After a lost reply it can
    /// reuse the recorded upload, or the version already certified there.
    pub(super) async fn reuse_transfer_upload(
        &self, inventory: &VersionedInventory, history: History, route: PublishRoute,
        key: &ObjectKey, pointer: &ReplicaEnvelope, body_sha256: [u8; 32],
    ) -> Result<Option<PhysicalObjectVersion>> {
        let record = self.current(history).await?;
        require_publication(history, route, &record, key)?;
        let physical = ObjectKey::new(format!("{}/{key}", residence(&record)))?;
        if let Some((&id, region)) = record.storage_regions.iter().find(|(_, region)|
            matches!(region, StorageRegion::ReplicaPublication { key: prepared, .. } if prepared == key.as_str()))
        {
            let StorageRegion::ReplicaPublication { draft: Some(draft), publications, closed: true, .. } = region
                else { return Err(refused()); };
            if draft.route != route || draft.body_sha256 != body_sha256 || draft.retirement.is_some()
                || !publications.is_empty()
            {
                return Err(refused());
            }
            match inventory.observe_create_attempt(&physical, draft.attempt).await? {
                CreateAttemptPublication::Published(version) => {
                    self.finish_staged_upload(history, route, id, key, &version, Some(pointer)).await?;
                    return Ok(Some(version));
                }
                CreateAttemptPublication::Absent if draft.version.is_none() => {
                    self.replace_stage(&record, id, region, None).await?;
                    return Ok(None);
                }
                _ => return Err(refused()),
            }
        }

        // A completed draft is no longer in the staging ledger. Its certified
        // pointer is the only authority for returning a pre-existing version.
        let io = IoRegion::new();
        let bucket = self.storage.bucket(Arc::clone(&io)).await?;
        let observed = bucket.retirement_version(&physical).await;
        io.close_and_wait().await;
        let Some(version) = observed? else { return Ok(None); };
        let position = route.transfer_position().ok_or_else(refused)?;
        if !self.transfer_committed(&record, route, position, transfer_pointer_digest(pointer, &version)?).await? {
            return Err(refused());
        }
        Ok(Some(version))
    }

    pub(super) async fn confirm_transfer(&self, history: History, route: PublishRoute, key: &ObjectKey) -> Result<()> {
        if !matches!(self.storage, CellStorage::S3 { .. }) { return Ok(()); }
        let record = self.current(history).await?;
        require_publication(history, route, &record, key)?;
        let Some((&id, region)) = record.storage_regions.iter().find(|(_, region)|
            matches!(region, StorageRegion::ReplicaPublication { key: prepared, .. } if prepared == key.as_str()))
            else { return Ok(()); };
        let StorageRegion::ReplicaPublication { draft: Some(draft), publications, closed: true, .. } = region
            else { return Err(refused()); };
        if draft.route != route || draft.retirement.is_some() || draft.version.is_none() || !publications.is_empty() {
            return Err(refused());
        }
        let position = route.transfer_position().ok_or_else(refused)?;
        let digest = draft.reference.ok_or_else(refused)?;
        if !self.transfer_committed(&record, route, position, digest).await? { return Err(refused()); }
        self.replace_stage(&record, id, region, None).await
    }

    pub(super) async fn transfer_committed(
        &self, record: &DeploymentRecord, route: PublishRoute, position: i64, digest: [u8; 32],
    ) -> Result<bool> {
        let maximum_message_bytes = crate::process::MAXIMUM_LOG_MESSAGE_BYTES;
        let stream = match route {
            PublishRoute::Transfer { operation, bootstrap, .. } => {
                let replica = record.checkpoint_transfer_replica.as_ref().filter(|replica| {
                    replica.bootstrap.ciphertext_sha256 == bootstrap
                        && replica.bootstrap.terminal.fence.destination.intent.operation == operation
                }).ok_or_else(refused)?;
                rtdb_logkeeper::StreamConfig {
                    stream: record.stream("replica"), write_quorum: crate::deployment::write_quorum(replica.journals.len()),
                    servers: replica.journals.clone(), maximum_message_bytes,
                }
            }
            PublishRoute::CellTransfer { generation, .. } if record.storage_generation == Some(generation) =>
                crate::admin::administration_stream(self.directory.as_ref(), record, maximum_message_bytes).await?,
            _ => return Err(refused()),
        };
        let proof = match rtdb_logkeeper::certify_committed_range(&stream, position, position).await {
            Ok(proof) => proof,
            // An unreadable or uncommitted position retains staging custody.
            Err(_) => return Ok(false),
        };
        let mut reader = rtdb_logkeeper::CertifiedRangeReader::new(&stream, &proof)?;
        let page = reader.next_page().await?.ok_or_else(refused)?;
        Ok(page.len() == 1 && page[0].lsn == position
            && crate::logs::payload_reference_digest(&page[0].payload) == Some(digest)
            && reader.next_page().await?.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::AbandonedCellGeneration;

    #[test]
    fn a_cell_transfer_exclusion_names_its_current_closed_generation() {
        let mut record = DeploymentRecord::new("orders", "org", vec![], 0);
        record.lifecycle = Lifecycle::Moving;
        record.move_from = Some(vec![]);
        let generation = Uuid::new_v4();
        record.storage_generation = Some(generation);
        let namespaces = rtdb_directory::StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        record.storage_namespaces = Some(namespaces);
        let route = PublishRoute::CellTransfer { generation, position: 12 };
        assert!(!transfer_excluded(&record, route));
        let prefix = record.storage_prefix();
        record.abandoned_generations.insert(generation, AbandonedCellGeneration {
            ordinal: 1, closed_at_ms: 0, first_empty_at_ms: None, disposition: None, prefix, namespaces,
        });
        assert!(current_generation_abandoned(&record));
        assert!(transfer_excluded(&record, route));
        record.storage_generation = Some(Uuid::new_v4());
        assert!(!current_generation_abandoned(&record));
        assert!(!transfer_excluded(&record, route));
    }
}
