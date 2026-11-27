use std::{collections::BTreeSet, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rtdb_format::{catalog::BlockCatalog, envelope::ReplicaBody, keys::block_key, log::proto};
use rtdb_index::{BlockPreparation, StagedWrite};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_tx::transact::InstalledReceipt;
use rtdb_types::{
    Anomaly, CatalogCut, ReplicaPosition, Result, SourcePosition, Term, Timestamp, rtdb_v1::Basis,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    BlockStore, ErasureTarget, PartitionLogs, ProducedBlock, PublishedCheckpoint, ReplicaRecord,
    TermFence,
    blocks::{checkpoint_digest, l0_tries, relation_oid_order_at_boundary, validate_boundary},
    serving::{self, NativeCheckpointImage, NativeServingAuthority, ServingCheckpoint},
    snapshot_replay::erasure_targets,
    sql_survivor::SurvivorInput,
};

mod inventory;
mod local_read;
pub use local_read::{LocalDefaultErasureReadFence, LocalErasureRead};
mod terminal;
#[cfg(test)]
pub(crate) use terminal::COMPLETED_FRAME;

pub(crate) use terminal::ErasureCompleted;
pub use terminal::{DefaultErasureReadCut, DefaultErasureReadFence};

use inventory::RetirementInventory;

const REWRITE_IMAGE: u8 = b'E';
pub(crate) const RETIREMENT_FRAME: u8 = 6;
const MAX_CONTROL_BYTES: usize = 2048;

#[derive(Clone)]
pub(crate) struct AppliedErasure {
    pub cut: ErasureCut,
    targets: Arc<[ErasureTarget]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ErasureCut {
    pub basis: Basis,
    pub source: SourcePosition,
    pub replica: ReplicaPosition,
    pub term: Term,
}

impl ErasureCut {
    pub fn installation(self) -> (Basis, InstalledReceipt) {
        (
            self.basis,
            InstalledReceipt {
                source: self.source,
                replica: self.replica,
                term: self.term,
            },
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RewriteEvidence {
    pub cut: ErasureCut,
    pub boundary: ReplicaPosition,
    pub members_sha256: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_sha256: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedRewrite {
    pub evidence: RewriteEvidence,
    pub checkpoint: PublishedCheckpoint,
    pub generation: u64,
    pub uploaded: Option<ReplicaPosition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ErasureRetirement {
    evidence: RewriteEvidence,
    block: rtdb_types::BlockIndex,
    root_sha256: [u8; 32],
    upload: ReplicaPosition,
    observed_at: Timestamp,
    pub unreadable_by: Timestamp,
}

pub(crate) enum DefaultErasure {
    Applied(AppliedErasure),
    Selected(SelectedRewrite),
    Retiring {
        intent: ErasureRetirement,
        ordered_intent: ReplicaPosition,
        log_prefixes_settled: bool,
        objects_settled: bool,
    },
}

impl AppliedErasure {
    pub fn capture(cut: ErasureCut, write: &StagedWrite, logs: &PartitionLogs) -> Result<Self> {
        if write.tx_key().tx_id.0 != cut.source.0
            || cut.source.after_offset(logs.source_epoch) < 0
            || cut.replica.after_offset(logs.replica_epoch) < 0
        {
            return Err(invalid());
        }
        Ok(Self {
            cut,
            targets: erasure_targets(write, logs.memory())?.into(),
        })
    }

    pub async fn produce(
        &self,
        blocks: &BlockStore,
        logs: &PartitionLogs,
        prepared: &BlockPreparation,
        boundary: &ReplicaRecord,
        mut checkpoint: ServingCheckpoint,
        cancellation: &CancellationToken,
    ) -> Result<ProducedBlock> {
        let ReplicaBody::BlockBoundary(message) = &boundary.envelope.message else {
            return Err(invalid());
        };
        if message.latest_processed_msg_id != Some(self.cut.source.0)
            || message.external_source_token.is_some()
            || boundary.metadata.position.after_offset(logs.replica_epoch)
                <= self.cut.replica.after_offset(logs.replica_epoch)
        {
            return Err(invalid());
        }
        logs.checkpoint_log_keys().ok_or_else(unsupported)?;
        let prior_generation = checkpoint.write_generation()?;
        let store = blocks.object_store();
        let local = store.local_maintenance_store().ok_or_else(unsupported)?;
        let custody = local.hold_recovery_open().await?;
        store.local_classic_key_slots().ok_or_else(unsupported)?;
        let root_key = ObjectKey::new(block_key(prepared.block_index())?)?;
        match store.head_fresh(&root_key).await {
            Ok(_) => {
                let (catalog, tables) = blocks.load(prepared.block_index()).await?;
                validate_boundary(&catalog, prepared, boundary)?;
                let members = physical_members(&catalog, &tables)?;
                let mut evidence = self.evidence(
                    boundary,
                    prior_generation,
                    members_sha256(blocks, store, members.clone(), cancellation).await?,
                );
                let mut protected = members;
                protect_checkpoint_objects(&catalog, &mut protected)?;
                let inventory =
                    RetirementInventory::read(blocks, catalog.block_index(), evidence, &protected)
                        .await?
                        .ok_or_else(inventory::incomplete)?;
                evidence.inventory_sha256 = Some(inventory.store(blocks).await?);
                checkpoint.bind_rewrite(evidence)?;
                checkpoint
                    .verify(
                        store,
                        catalog.as_proto().serving_checkpoint.as_deref(),
                        catalog.as_proto().serving_checkpoint_object.as_deref(),
                    )
                    .await?;
                return Ok(ProducedBlock {
                    tries: l0_tries(catalog.block_index(), &tables)?,
                    catalog,
                });
            }
            Err(Error::MissingObject(_)) => (),
            Err(error) => return Err(error.into()),
        }
        let cut = CatalogCut {
            storage_version: logs.storage_version,
            storage_epoch: logs.storage_epoch,
            replica_epoch: logs.replica_epoch,
            through: boundary.metadata.position,
        };
        let fence =
            TermFence::from_boundary(boundary.envelope.term, boundary.envelope.term_sequence)?;
        let snapshot = prepared.value().snapshot();
        let survivor = SurvivorInput {
            snapshot: &snapshot,
            targets: &self.targets,
            cut,
            source_position: self.cut.source,
            inherited_erasure: None,
            custody: &custody,
            store,
            memory: logs.memory(),
            fence: &fence,
            external_token: None,
        }
        .build(u64::MAX, cancellation)
        .await?;
        let members = survivor
            .store()
            .list(None)
            .await?
            .into_iter()
            .map(|entry| entry.key)
            .filter(|key| key != &root_key)
            .collect::<BTreeSet<_>>();
        let digest =
            members_sha256(blocks, survivor.store(), members.clone(), cancellation).await?;
        let mut evidence = self.evidence(boundary, prior_generation, digest);
        let inventory =
            match RetirementInventory::read(blocks, prepared.block_index(), evidence, &members)
                .await?
            {
                Some(staged) => staged,
                None => {
                    RetirementInventory::capture(
                        blocks,
                        logs,
                        snapshot.table_catalog().block(),
                        prepared.block_index(),
                        evidence,
                        &members,
                        cancellation,
                    )
                    .await?
                }
            };
        evidence.inventory_sha256 = Some(inventory.store(blocks).await?);
        checkpoint.bind_rewrite(evidence)?;
        check_cancelled(cancellation)?;
        let object = checkpoint.store_rewrite(store).await?;
        for key in members {
            check_cancelled(cancellation)?;
            store
                .put(&key, survivor.store().get_fresh(&key).await?)
                .await?;
        }
        let mut wire = survivor.catalog.as_proto().clone();
        wire.serving_checkpoint_object = Some(object);
        wire.relation_oid_order = relation_oid_order_at_boundary(prepared, message)?.wire_value();
        let mut catalog = BlockCatalog::from_proto(wire)?;
        validate_boundary(&catalog, prepared, boundary)?;
        store
            .put(&root_key, catalog.encode_for_publication().into())
            .await?;
        let (_, tables) = blocks.load(catalog.block_index()).await?;
        let tries = l0_tries(catalog.block_index(), &tables)?;
        Ok(ProducedBlock { catalog, tries })
    }

    fn evidence(
        &self,
        boundary: &ReplicaRecord,
        prior_generation: u64,
        members_sha256: [u8; 32],
    ) -> RewriteEvidence {
        RewriteEvidence {
            cut: self.cut,
            boundary: boundary.metadata.position,
            members_sha256,
            inventory_sha256: None,
            prior_generation: Some(prior_generation),
        }
    }
}

pub(crate) async fn selected_rewrite(
    authority: &NativeServingAuthority,
    blocks: &BlockStore,
    logs: &PartitionLogs,
    catalog: &BlockCatalog,
    cancellation: &CancellationToken,
) -> Result<Option<SelectedRewrite>> {
    let image = authority
        .read_checkpoint(
            blocks.object_store(),
            catalog.as_proto().serving_checkpoint.as_deref(),
            catalog.as_proto().serving_checkpoint_object.as_deref(),
        )
        .await?;
    recover_rewrite(
        authority,
        blocks,
        logs,
        catalog,
        image.as_ref(),
        cancellation,
    )
    .await
}

pub(crate) async fn recover_rewrite(
    authority: &NativeServingAuthority,
    blocks: &BlockStore,
    logs: &PartitionLogs,
    catalog: &BlockCatalog,
    image: Option<&NativeCheckpointImage>,
    cancellation: &CancellationToken,
) -> Result<Option<SelectedRewrite>> {
    let Some(image) = image else {
        return Ok(None);
    };
    let Some(evidence) = serving::checkpoint_rewrite(image.bytes())? else {
        return Ok(None);
    };
    if blocks.object_store().local_classic_key_slots().is_none() {
        return Err(unsupported());
    }
    let NativeCheckpointImage::Sealed { opened, .. } = image else {
        return Err(unsupported());
    };
    let configured = authority
        .checkpoints
        .as_ref()
        .ok_or_else(unsupported)?
        .generation;
    let prior = evidence.prior_generation.unwrap_or(configured);
    if prior < configured
        || opened.generation.database.as_bytes() != &authority.incarnation
        || Some(opened.generation.number) != prior.checked_add(1)
    {
        return Err(invalid());
    }
    let selected =
        SelectedRewrite::from_catalog(evidence, opened.generation.number, catalog, logs)?;
    let (fold, position, shared) =
        serving::decode_checkpoint_image(image.bytes(), authority.incarnation)?;
    if !shared
        || fold.pending_erasure_installation() != Some(evidence.cut.installation())
        || position != evidence.cut.basis.position()
    {
        return Err(invalid());
    }
    if evidence.inventory_sha256.is_none() {
        return Err(inventory::incomplete());
    }
    let (loaded, tables) = blocks.load(catalog.block_index()).await?;
    let mut members = physical_members(&loaded, &tables)?;
    if loaded.publication_sha256() != catalog.publication_sha256()
        || members_sha256(blocks, blocks.object_store(), members.clone(), cancellation).await?
            != evidence.members_sha256
    {
        return Err(invalid());
    }
    protect_checkpoint_objects(&loaded, &mut members)?;
    RetirementInventory::read(blocks, loaded.block_index(), evidence, &members)
        .await?
        .ok_or_else(inventory::incomplete)?;
    Ok(Some(selected))
}

impl SelectedRewrite {
    pub fn from_catalog(
        evidence: RewriteEvidence,
        generation: u64,
        catalog: &BlockCatalog,
        logs: &PartitionLogs,
    ) -> Result<Self> {
        if catalog.boundary_replica_position() != Some(evidence.boundary)
            || catalog.latest_source_position() != Some(evidence.cut.source)
            || evidence.cut.term > catalog.boundary_term()
            || evidence.cut.source.after_offset(logs.source_epoch) < 0
            || evidence.cut.replica.after_offset(logs.replica_epoch) < 0
            || evidence.boundary.after_offset(logs.replica_epoch)
                <= evidence.cut.replica.after_offset(logs.replica_epoch)
            || catalog.as_proto().serving_checkpoint_object.is_none()
            || catalog.as_proto().serving_checkpoint.is_some()
        {
            return Err(invalid());
        }
        let checkpoint = PublishedCheckpoint {
            block: catalog.block_index(),
            boundary: evidence.boundary,
            root_sha256: Some(checkpoint_digest(catalog)?),
        };
        Ok(Self {
            evidence,
            checkpoint,
            generation,
            uploaded: None,
        })
    }

    pub fn observe_upload(&mut self, record: &ReplicaRecord) -> Result<()> {
        let ReplicaBody::BlockUploaded(upload) = &record.envelope.message else {
            return Err(invalid());
        };
        if upload.block_index != Some(self.checkpoint.block.0)
            || upload.latest_processed_msg_id != Some(self.evidence.cut.source.0)
            || upload.catalog_sha256.as_deref()
                != self
                    .checkpoint
                    .root_sha256
                    .as_ref()
                    .map(|root| root.as_slice())
            || record.metadata.position.epoch() != self.evidence.boundary.epoch()
            || record.metadata.position <= self.evidence.boundary
        {
            return Err(invalid());
        }
        if self.uploaded.is_none() {
            self.uploaded = Some(record.metadata.position);
        }
        Ok(())
    }
}

impl ErasureRetirement {
    pub(crate) fn cut(self) -> ErasureCut {
        self.evidence.cut
    }

    pub(crate) fn binds_predecessor(self) -> bool {
        self.evidence.prior_generation.is_some()
    }

    fn log_prefixes(self, logs: &PartitionLogs) -> [rtdb_storage::log::CheckpointPrefix; 2] {
        let plan = |through| rtdb_storage::log::CheckpointPrefix {
            through,
            checkpoint_sha256: self.root_sha256,
            unreadable_by: self.unreadable_by,
        };
        [
            plan(self.evidence.cut.source.after_offset(logs.source_epoch)),
            plan(self.evidence.boundary.after_offset(logs.replica_epoch)),
        ]
    }

    async fn verify_kafka_log_prefixes(self, logs: &PartitionLogs) -> Result<()> {
        if logs.default_erasure_kafka_topics().is_some() {
            let (source, replica) = logs.checkpoint_log_keys().ok_or_else(unsupported)?;
            let [source_plan, replica_plan] = self.log_prefixes(logs);
            if !source.verifies_checkpoint_prefix(source_plan).await?
                || !replica.verifies_checkpoint_prefix(replica_plan).await?
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    pub async fn settle_log_prefixes(self, logs: &PartitionLogs) -> Result<bool> {
        let (source, replica) = logs.checkpoint_log_keys().ok_or_else(unsupported)?;
        let local = logs
            .storage()
            .local_maintenance_store()
            .ok_or_else(unsupported)?;
        let [source_plan, replica_plan] = self.log_prefixes(logs);
        source.validate_checkpoint_prefix(source_plan).await?;
        replica.validate_checkpoint_prefix(replica_plan).await?;
        if !local.close_erasure_recovery(self.recovery_fence()?).await? {
            return Ok(false);
        }
        let source_done = source.settle_checkpoint_prefix(source_plan).await?;
        let replica_done = replica.settle_checkpoint_prefix(replica_plan).await?;
        Ok(source_done && replica_done)
    }

    pub async fn settle_objects(
        self,
        blocks: &BlockStore,
        logs: &PartitionLogs,
        now_lower: Timestamp,
    ) -> Result<bool> {
        let store = blocks.object_store();
        let local = store.local_maintenance_store().ok_or_else(unsupported)?;
        let (fence, objects) = self.verified_inventory(blocks).await?;
        let cut = CatalogCut {
            storage_version: logs.storage_version,
            storage_epoch: logs.storage_epoch,
            replica_epoch: logs.replica_epoch,
            through: fence.boundary,
        };
        let settled = local
            .settle_erasure_objects(store, fence, cut, &objects, now_lower)
            .await?;
        if settled {
            for key in &objects {
                blocks.storage().invalidate(key).await?;
            }
        }
        Ok(settled)
    }

    async fn verified_inventory(
        self,
        blocks: &BlockStore,
    ) -> Result<(rtdb_storage::LocalErasureRecovery, Vec<ObjectKey>)> {
        let fence = self.recovery_fence()?;
        let catalog = blocks.load_fresh_catalog(fence.block).await?;
        fence.require(
            fence.block,
            catalog.latest_source_position(),
            catalog.boundary_replica_position(),
            catalog.publication_sha256(),
        )?;
        let mut protected = BTreeSet::new();
        protect_checkpoint_objects(&catalog, &mut protected)?;
        let inventory = RetirementInventory::read(blocks, fence.block, self.evidence, &protected)
            .await?
            .ok_or_else(inventory::incomplete)?;
        let objects = inventory.objects()?;
        Ok((fence, objects))
    }

    async fn objects_are_settled(self, blocks: &BlockStore) -> Result<bool> {
        let local = blocks
            .object_store()
            .local_maintenance_store()
            .ok_or_else(unsupported)?;
        let (fence, objects) = self.verified_inventory(blocks).await?;
        local.erasure_objects_complete(fence, &objects).await
    }

    fn recovery_fence(self) -> Result<rtdb_storage::LocalErasureRecovery> {
        let digest = self
            .evidence
            .inventory_sha256
            .ok_or_else(inventory::incomplete)?;
        Ok(rtdb_storage::LocalErasureRecovery {
            block: self.block,
            boundary: self.evidence.boundary,
            source: self.evidence.cut.source,
            root_sha256: self.root_sha256,
            inventory_sha256: Some(digest),
            unreadable_by: self.unreadable_by,
        })
    }

    pub fn observe(selected: SelectedRewrite, upper: Timestamp, lease_micros: u64) -> Result<Self> {
        let upload = selected.uploaded.ok_or_else(invalid)?;
        let unreadable_by = upper
            .0
            .checked_add_unsigned(lease_micros)
            .map(Timestamp)
            .ok_or_else(invalid)?;
        Ok(Self {
            evidence: selected.evidence,
            block: selected.checkpoint.block,
            root_sha256: selected.checkpoint.root_sha256.ok_or_else(invalid)?,
            upload,
            observed_at: upper,
            unreadable_by,
        })
    }

    pub fn require_selected(
        self,
        selected: SelectedRewrite,
        position: ReplicaPosition,
    ) -> Result<()> {
        if self
            .evidence
            .prior_generation
            .is_some_and(|prior| prior.checked_add(1) != Some(selected.generation))
            || self.evidence != selected.evidence
            || self.block != selected.checkpoint.block
            || Some(self.root_sha256) != selected.checkpoint.root_sha256
            || Some(self.upload) != selected.uploaded
            || position.epoch() != self.upload.epoch()
            || position <= self.upload
            || self.unreadable_by < self.observed_at
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub fn message(self) -> Result<proto::NoOp> {
        let mut frame = vec![RETIREMENT_FRAME];
        serde_json::to_writer(&mut frame, &self).map_err(|_| invalid())?;
        Ok(proto::NoOp {
            serving_record: Some(frame.into()),
            ..Default::default()
        })
    }

    pub fn decode(message: &proto::NoOp) -> Result<Option<Self>> {
        let Some(record) = message
            .serving_record
            .as_deref()
            .and_then(|frame| frame.strip_prefix(&[RETIREMENT_FRAME]))
        else {
            return Ok(None);
        };
        if message.src_msg_id.is_some()
            || message.placed_slot_reservation.is_some()
            || record.len() > MAX_CONTROL_BYTES
        {
            return Err(invalid());
        }
        serde_json::from_slice(record)
            .map(Some)
            .map_err(|_| invalid())
    }
}

impl RewriteEvidence {
    pub fn frame(self, prefix: &[u8], state: &[u8]) -> Result<Bytes> {
        let encoded = serde_json::to_vec(&self).map_err(|_| invalid())?;
        if encoded.len() > MAX_CONTROL_BYTES {
            return Err(invalid());
        }
        let mut bytes = BytesMut::with_capacity(prefix.len() + 5 + encoded.len() + state.len());
        bytes.extend_from_slice(prefix);
        bytes.put_u8(REWRITE_IMAGE);
        bytes.put_u32(encoded.len() as u32);
        bytes.extend_from_slice(&encoded);
        bytes.extend_from_slice(state);
        Ok(bytes.freeze())
    }

    pub fn read(state: &[u8]) -> Result<(Option<Self>, &[u8])> {
        let Some(rewrite) = state.strip_prefix(&[REWRITE_IMAGE]) else {
            return Ok((None, state));
        };
        let (mut length, bytes) = rewrite.split_at_checked(4).ok_or_else(invalid)?;
        let length = length.get_u32() as usize;
        if length > MAX_CONTROL_BYTES {
            return Err(invalid());
        }
        let (encoded, state) = bytes.split_at_checked(length).ok_or_else(invalid)?;
        Ok((
            Some(serde_json::from_slice(encoded).map_err(|_| invalid())?),
            state,
        ))
    }
}

fn protect_checkpoint_objects(
    catalog: &BlockCatalog,
    members: &mut BTreeSet<ObjectKey>,
) -> Result<()> {
    members.insert(ObjectKey::new(block_key(catalog.block_index())?)?);
    if let Some(path) = &catalog.as_proto().serving_checkpoint_object {
        members.insert(ObjectKey::new(path)?);
    }
    Ok(())
}

pub(crate) fn physical_members(
    catalog: &BlockCatalog,
    tables: &std::collections::BTreeMap<rtdb_types::TableRef, rtdb_format::catalog::TableBlock>,
) -> Result<BTreeSet<ObjectKey>> {
    let mut keys = crate::checkpoint_dependencies(catalog, tables)?;
    keys.remove(&ObjectKey::new(block_key(catalog.block_index())?)?);
    if let Some(object) = &catalog.as_proto().serving_checkpoint_object {
        keys.remove(&ObjectKey::new(object)?);
    }
    Ok(keys)
}

pub(crate) async fn members_sha256(
    blocks: &BlockStore,
    store: &dyn ObjectStore,
    members: BTreeSet<ObjectKey>,
    cancellation: &CancellationToken,
) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(b"rtdb.default.survivor/v1\0");
    for key in members {
        check_cancelled(cancellation)?;
        let length = usize::try_from(store.head_fresh(&key).await?.size).map_err(|_| invalid())?;
        let allocation = store.exact_object_allocation(&key, length)?;
        let bytes = allocation
            .retained_bytes
            .checked_add(allocation.scratch_bytes)
            .ok_or_else(invalid)?;
        let charge = blocks.memory().reserve(bytes)?;
        let body =
            rtdb_storage::IoRegion::retaining(charge, store.get_fresh_exact(&key, length)).await?;
        digest.update((key.as_str().len() as u64).to_be_bytes());
        digest.update(key.as_str().as_bytes());
        digest.update(Sha256::digest(&body));
    }
    Ok(digest.finalize().into())
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(crate::progress::cancelled())
    } else {
        Ok(())
    }
}

pub(crate) fn invalid() -> Anomaly {
    Anomaly::fault(
        "rtdb/default-erasure-cut",
        "Default erasure does not match the ordered recovery cut",
    )
}

pub(crate) fn unsupported() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/default-erasure-provider",
        "The provider cannot establish independent Default erasure custody",
    )
}

#[cfg(test)]
mod tests;
